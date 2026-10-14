package broker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// StreamMessage represents a decoded message from the stream with telemetry.
type StreamMessage struct {
	ID        string
	EventID   string
	Payload   string
	PublishTS time.Time
}

// RedisBroker wraps high-performance Redis Streams operations.
type RedisBroker struct {
	client *redis.Client
}

// NewRedisBroker connects to Redis and verifies connectivity with a ping.
func NewRedisBroker(addr string) (*RedisBroker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     64,
		MinIdleConns: 8,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", addr, err)
	}

	return &RedisBroker{client: client}, nil
}

// EnsureConsumerGroup creates the stream and consumer group if they do not exist.
func (b *RedisBroker) EnsureConsumerGroup(ctx context.Context, stream, group string) error {
	// XGROUP CREATE stream group 0 MKSTREAM
	err := b.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create consumer group %s on %s: %w", group, stream, err)
	}
	return nil
}

// PublishBatch writes a slice of JSON lines to the stream using a pipelined XADD batch.
// publishTS is recorded for every event in the batch to track end-to-end latency.
func (b *RedisBroker) PublishBatch(ctx context.Context, stream string, batch []PublishItem, publishTS time.Time) error {
	pipe := b.client.Pipeline()
	tsStr := strconv.FormatInt(publishTS.UnixNano(), 10)

	for _, item := range batch {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"event_id": item.EventID,
				"payload":  item.Payload,
				"pub_ts":   tsStr,
			},
		})
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute XADD pipeline on stream %s: %w", stream, err)
	}
	return nil
}

// PublishItem holds the unmarshaled metadata and raw payload for publication.
type PublishItem struct {
	EventID string
	Payload string
}

// ReadGroupBatch reads up to count messages from the stream for the specified consumer group.
func (b *RedisBroker) ReadGroupBatch(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]StreamMessage, error) {
	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return nil, nil // No new messages within block timeout
		}
		return nil, err
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, nil
	}

	rawMsgs := streams[0].Messages
	results := make([]StreamMessage, 0, len(rawMsgs))

	for _, m := range rawMsgs {
		eventID, _ := m.Values["event_id"].(string)
		payload, _ := m.Values["payload"].(string)
		pubTSStr, _ := m.Values["pub_ts"].(string)

		var pubTS time.Time
		if pubTSStr != "" {
			if nanos, err := strconv.ParseInt(pubTSStr, 10, 64); err == nil {
				pubTS = time.Unix(0, nanos)
			}
		}

		results = append(results, StreamMessage{
			ID:        m.ID,
			EventID:   eventID,
			Payload:   payload,
			PublishTS: pubTS,
		})
	}

	return results, nil
}

// AckMessages acknowledges processed messages in the consumer group.
func (b *RedisBroker) AckMessages(ctx context.Context, stream, group string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return b.client.XAck(ctx, stream, group, ids...).Err()
}

// StreamLength returns the number of entries currently in the stream.
func (b *RedisBroker) StreamLength(ctx context.Context, stream string) (int64, error) {
	return b.client.XLen(ctx, stream).Result()
}

// Client returns the underlying redis.Client.
func (b *RedisBroker) Client() *redis.Client {
	return b.client
}

// Close gracefully closes the Redis connection.
func (b *RedisBroker) Close() error {
	return b.client.Close()
}
