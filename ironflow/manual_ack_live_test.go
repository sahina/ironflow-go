package ironflow

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLiveManualAckNakRedelivers runs against a real server. It skips unless
// IRONFLOW_TEST_SERVER is set; `make test-sdk-manual-ack-live` sets it.
func TestLiveManualAckNakRedelivers(t *testing.T) {
	// No transport option: the default is what a caller gets, so it must ack.
	runLiveNakScenario(t)
}

// TestLiveManualAckNakRedelivers_Stream is the same scenario over SubscribeBidirectional.
func TestLiveManualAckNakRedelivers_Stream(t *testing.T) {
	runLiveNakScenario(t, WithJoinTransport("grpc"))
}

func runLiveNakScenario(t *testing.T, opts ...JoinConsumerGroupOption) {
	t.Helper()
	serverURL := os.Getenv("IRONFLOW_TEST_SERVER")
	if serverURL == "" {
		t.Skip("IRONFLOW_TEST_SERVER not set; run `make test-sdk-manual-ack-live`")
	}
	client := NewClient(ClientConfig{ServerURL: serverURL, APIKey: os.Getenv("IRONFLOW_TEST_API_KEY")})
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic, group := "gosdk.ack."+suffix, "ack-group-"+suffix

	// In a MANUAL group RedeliverDelayMs is also the JetStream ack-wait. 60s
	// keeps an unacked event out of the 20s window below, so a redelivery
	// there can only come from the nak.
	if _, err := client.CreateConsumerGroup(ctx, ConsumerGroupConfig{
		Name: group, Pattern: "topic:" + topic, AckMode: AckModeManual,
		RedeliverDelayMs: 60000, MaxRedeliveries: 5,
	}); err != nil {
		t.Fatalf("create group: %v", err)
	}

	sub, err := client.JoinConsumerGroup(ctx, group, opts...)
	if err != nil {
		t.Fatalf("join group: %v", err)
	}
	defer sub.Unsubscribe()

	// The group consumer may not exist when the first publish lands, so
	// republish until an event arrives.
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopPublishing := func() { stopOnce.Do(func() { close(stop) }) }
	defer stopPublishing()
	var logOnce sync.Once
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				if _, err := client.Publish(ctx, topic, map[string]any{"n": 1}); err != nil {
					logOnce.Do(func() { t.Logf("publish: %v", err) })
				}
			}
		}
	}()

	var target string
	var nakedAt time.Time
	window := time.After(20 * time.Second)
	for {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatal("events channel closed before the nak redelivered")
			}
			if target == "" {
				target = ev.ID
				stopPublishing()
				nakedAt = time.Now()
				if err := sub.Nak(target, time.Second); err != nil {
					t.Fatalf("nak: %v", err)
				}
				continue
			}
			if ev.ID != target { // an extra publish before stopPublishing took effect
				continue
			}
			if gap := time.Since(nakedAt); gap < 800*time.Millisecond {
				t.Fatalf("redelivered %s after nak, before its 1s delay", gap)
			}
			if err := sub.Ack(target); err != nil {
				t.Fatalf("ack: %v", err)
			}
			return
		case <-window:
			t.Fatalf("nak did not redeliver %q within 20s", target)
		}
	}
}

func TestLiveManualAckGroupResumesAfterLostConnection(t *testing.T) {
	serverURL := os.Getenv("IRONFLOW_TEST_SERVER")
	if serverURL == "" {
		t.Skip("IRONFLOW_TEST_SERVER not set; run `make test-sdk-manual-ack-live`")
	}
	for name, opts := range map[string][]JoinConsumerGroupOption{
		"websocket": nil,
		"stream":    {WithJoinTransport("grpc")},
	} {
		t.Run(name, func(t *testing.T) {
			apiKey := os.Getenv("IRONFLOW_TEST_API_KEY")
			client := NewClient(ClientConfig{ServerURL: serverURL, APIKey: apiKey})
			proxy := startCutProxy(t, strings.TrimPrefix(serverURL, "http://"))
			worker := NewClient(ClientConfig{ServerURL: proxy.url(), APIKey: apiKey, Logger: NewNoopLogger()})

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			suffix := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
			topic, group := "gosdk.cut."+suffix, "cut-group-"+suffix
			if _, err := client.CreateConsumerGroup(ctx, ConsumerGroupConfig{
				Name: group, Pattern: "topic:" + topic, AckMode: AckModeManual,
				RedeliverDelayMs: 60000, MaxRedeliveries: 5,
			}); err != nil {
				t.Fatalf("create group: %v", err)
			}
			sub, err := worker.JoinConsumerGroup(ctx, group, opts...)
			if err != nil {
				t.Fatalf("join group: %v", err)
			}
			defer sub.Unsubscribe()

			awaitNewEvent := func(step string) {
				t.Helper()
				published := map[string]bool{}
				tick := time.NewTicker(300 * time.Millisecond)
				defer tick.Stop()
				deadline := time.After(25 * time.Second)
				for {
					select {
					case <-tick.C:
						res, err := client.Publish(ctx, topic, map[string]any{"step": step})
						if err == nil {
							published[res.EventID] = true
						}
					case ev, ok := <-sub.Events():
						if !ok {
							t.Fatalf("%s: the subscription ended: %+v", step, <-sub.Errors())
						}
						if !published[ev.ID] {
							continue
						}
						return
					case <-deadline:
						t.Fatalf("%s: no event within 25s", step)
					}
				}
			}

			awaitNewEvent("before any cut")
			proxy.rst()
			awaitNewEvent("after RST")
			proxy.fin()
			awaitNewEvent("after FIN")
		})
	}
}
