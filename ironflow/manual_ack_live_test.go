package ironflow

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// TestLiveManualAckNakRedelivers runs against a real server. It skips unless
// IRONFLOW_TEST_SERVER is set; `make test-sdk-manual-ack-live` sets it.
func TestLiveManualAckNakRedelivers(t *testing.T) {
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

	// No transport option: the default is what a caller gets, so it must ack.
	sub, err := client.JoinConsumerGroup(ctx, group)
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
