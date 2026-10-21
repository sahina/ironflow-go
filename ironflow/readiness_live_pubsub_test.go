package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// pubsubSubscriber is the part of the WebSocket and ConnectRPC subscriptions that the
// subscribe test uses.
type pubsubSubscriber interface {
	Events() <-chan *SubscriptionEvent
	Errors() <-chan *SubscriptionError
	Unsubscribe()
}

// TestLiveReadinessPubsub covers the pubsub group (#3003).
func TestLiveReadinessPubsub(t *testing.T) {
	var seed struct{}
	client, ctx := liveReadiness(t, &seed)

	t.Run("pubsub-subscribe", func(t *testing.T) {
		ws := client.CreateSubscriptionClient()
		if err := ws.Connect(ctx); err != nil {
			t.Fatalf("WebSocket connect: %v", err)
		}
		defer ws.Close()
		grpc := client.CreateGrpcSubscriptionClient()
		if err := grpc.Connect(ctx); err != nil {
			t.Fatalf("ConnectRPC connect: %v", err)
		}
		defer grpc.Close()

		for name, subscribe := range map[string]func(string, *SubscribeOptions) (pubsubSubscriber, error){
			"WebSocket": func(p string, o *SubscribeOptions) (pubsubSubscriber, error) { return ws.Subscribe(ctx, p, o) },
			"ConnectRPC": func(p string, o *SubscribeOptions) (pubsubSubscriber, error) {
				return grpc.Subscribe(ctx, p, o)
			},
		} {
			t.Run(name, func(t *testing.T) {
				topic := fmt.Sprintf("gosdk.sub.%d", time.Now().UnixNano())
				var published []*PublishResult
				publish := func(n int) {
					t.Helper()
					res, err := client.Publish(ctx, topic, map[string]any{"n": n})
					if err != nil {
						t.Fatalf("Publish %d: %v", n, err)
					}
					published = append(published, res)
				}
				publish(0)
				publish(1)

				// The pattern needs the topic: namespace; Publish takes the bare topic.
				sub, err := subscribe("topic:"+topic, &SubscribeOptions{Replay: 2, IncludeMetadata: true})
				if err != nil {
					t.Fatalf("Subscribe: %v", err)
				}
				defer sub.Unsubscribe()

				next := func(what string) *SubscriptionEvent {
					t.Helper()
					select {
					case ev := <-sub.Events():
						return ev
					case e := <-sub.Errors():
						t.Fatalf("%s: subscription error %v", what, e)
					case <-time.After(10 * time.Second):
						t.Fatalf("%s: nothing in 10s", what)
					}
					return nil
				}
				// The delivery time, which is not the publish time: near now.
				checkTimestamp := func(ev *SubscriptionEvent) {
					t.Helper()
					at, err := time.Parse(time.RFC3339Nano, ev.Meta.Timestamp)
					if err != nil || time.Since(at).Abs() > time.Minute {
						t.Fatalf("event %s timestamp = %q (%v), want a time near now", ev.ID, ev.Meta.Timestamp, err)
					}
				}
				decode := func(ev *SubscriptionEvent) map[string]any {
					t.Helper()
					var data map[string]any
					if err := json.Unmarshal(ev.Data, &data); err != nil {
						t.Fatalf("event %s data %s: %v", ev.ID, ev.Data, err)
					}
					return data
				}

				// Two replayed events: the data, the ID and the stream sequence Publish returned.
				for n := 0; n < 2; n++ {
					ev, want := next(fmt.Sprintf("replay %d", n)), published[n]
					if got := decode(ev); !reflect.DeepEqual(got, map[string]any{"n": float64(n)}) || ev.ID != want.EventID ||
						ev.Meta == nil || ev.Meta.Sequence != want.Sequence {
						t.Fatalf("replay %d = %+v (data %v, meta %+v), want n=%d id %s sequence %d", n, ev, got, ev.Meta, n, want.EventID, want.Sequence)
					}
					checkTimestamp(ev)
				}

				// One publish, no retry: the replay frames are in, so its snapshot is
				// taken, and a live consumer anchored there must deliver this event
				// (#3065). A retry loop would hide the replay-to-live gap coming back.
				publish(2)
				live := published[2]
				var ev *SubscriptionEvent
				select {
				case ev = <-sub.Events():
				case e := <-sub.Errors():
					t.Fatalf("live: subscription error %v", e)
				case <-time.After(5 * time.Second):
					t.Fatal("no live event after the publish")
				}
				if got := decode(ev); !reflect.DeepEqual(got, map[string]any{"n": float64(2)}) || ev.ID != live.EventID ||
					ev.Meta == nil || ev.Meta.Sequence != live.Sequence {
					t.Fatalf("live event = %+v (data %v, meta %+v), want n=2 id %s sequence %d", ev, got, ev.Meta, live.EventID, live.Sequence)
				}
				checkTimestamp(ev)
			})
		}
	})

	t.Run("pubsub-topics and pubsub-topic-stats", func(t *testing.T) {
		topic := fmt.Sprintf("gosdk.topics.%d", time.Now().UnixNano())
		var last uint64
		for n := 0; n < 3; n++ {
			res, err := client.Publish(ctx, topic, map[string]any{"n": n})
			if err != nil {
				t.Fatalf("Publish %d: %v", n, err)
			}
			last = res.Sequence
		}
		// A subscription that resumes from a cursor owns a JetStream consumer on the
		// topic's subject; the three events it reads show that the consumer exists.
		ws := client.CreateSubscriptionClient()
		if err := ws.Connect(ctx); err != nil {
			t.Fatalf("WebSocket connect: %v", err)
		}
		defer ws.Close()
		var from uint64
		sub, err := ws.Subscribe(ctx, "topic:"+topic, &SubscribeOptions{StartAfterSequence: &from})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		defer sub.Unsubscribe()
		for n := 0; n < 3; n++ {
			select {
			case <-sub.Events():
			case <-time.After(10 * time.Second):
				t.Fatalf("event %d: nothing in 10s", n)
			}
		}
		assertTopicShape(t, ctx, client, topic, last)
	})
}

// assertTopicShape reads a topic that holds exactly three messages, the last at
// stream sequence last, through ListTopics and GetTopicStats.
func assertTopicShape(t *testing.T, ctx context.Context, client *Client, topic string, last uint64) {
	t.Helper()
	topics, err := client.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	var found *TopicInfo
	for i := range topics {
		if topics[i].Name == topic {
			found = &topics[i]
		}
	}
	// Our consumer is one; the count is a floor because other suites may leave
	// consumers on the shared stream whose filter matches this topic.
	if found == nil || found.MessageCount != 3 || found.ConsumerCount < 1 {
		t.Fatalf("ListTopics has %+v for %s, want 3 messages and at least our consumer", found, topic)
	}

	stats, err := client.GetTopicStats(ctx, topic)
	if err != nil {
		t.Fatalf("GetTopicStats: %v", err)
	}
	// Sequences are stream-wide, so other topics move them: the last one is a floor.
	if stats.Name != topic || stats.MessageCount != 3 || stats.ConsumerCount < 1 || stats.Lag < 0 ||
		uint64(stats.LastSeq) < last || stats.FirstSeq == 0 || stats.FirstSeq > stats.LastSeq {
		t.Fatalf("GetTopicStats = %+v, want 3 messages, a consumer, a lag and lastSeq >= %d", stats, last)
	}

	empty, err := client.GetTopicStats(ctx, topic+".never")
	if err != nil || empty.Name != topic+".never" || empty.MessageCount != 0 {
		t.Fatalf("GetTopicStats of an unpublished topic = %+v, %v, want 0 messages", empty, err)
	}
}
