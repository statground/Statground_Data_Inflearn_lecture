package inflearn

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeKafkaWriter struct {
	err    error
	writes *[][]string
}

func (w *fakeKafkaWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	keys := make([]string, 0, len(messages))
	for _, msg := range messages {
		keys = append(keys, string(msg.Key))
	}
	*w.writes = append(*w.writes, keys)
	return w.err
}

func (w *fakeKafkaWriter) Close() error {
	return nil
}

func TestWriteKafkaMessagesWithRetryRetriesOnlyFailedWriteErrors(t *testing.T) {
	svc := Service{Cfg: Config{
		KafkaWriteAttempts:   3,
		KafkaWriteBackoffMin: time.Millisecond,
		KafkaWriteBackoffMax: time.Millisecond,
	}}
	messages := []kafka.Message{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	}
	errs := []error{
		kafka.WriteErrors{nil, kafka.NotLeaderForPartition, kafka.NotLeaderForPartition},
		nil,
	}
	writes := make([][]string, 0, len(errs))
	attempt := 0
	sleeps := 0

	err := svc.writeKafkaMessagesWithRetry(context.Background(), messages, func() kafkaMessageWriter {
		if attempt >= len(errs) {
			t.Fatalf("unexpected writer attempt %d", attempt+1)
		}
		err := errs[attempt]
		attempt++
		return &fakeKafkaWriter{err: err, writes: &writes}
	}, func(context.Context, time.Duration) error {
		sleeps++
		return nil
	})
	if err != nil {
		t.Fatalf("writeKafkaMessagesWithRetry returned error: %v", err)
	}

	wantWrites := [][]string{{"a", "b", "c"}, {"b", "c"}}
	if !reflect.DeepEqual(writes, wantWrites) {
		t.Fatalf("writes mismatch\nwant: %#v\n got: %#v", wantWrites, writes)
	}
	if sleeps != 1 {
		t.Fatalf("sleep count mismatch: want 1 got %d", sleeps)
	}
}

func TestWriteKafkaMessagesWithRetryDoesNotRetryNonTemporaryWriteError(t *testing.T) {
	svc := Service{Cfg: Config{
		KafkaWriteAttempts:   3,
		KafkaWriteBackoffMin: time.Millisecond,
		KafkaWriteBackoffMax: time.Millisecond,
	}}
	messages := []kafka.Message{{Key: []byte("a"), Value: []byte("1")}}
	writes := make([][]string, 0, 1)
	attempts := 0

	err := svc.writeKafkaMessagesWithRetry(context.Background(), messages, func() kafkaMessageWriter {
		attempts++
		return &fakeKafkaWriter{
			err:    kafka.WriteErrors{kafka.SASLAuthenticationFailed},
			writes: &writes,
		}
	}, func(context.Context, time.Duration) error {
		t.Fatal("sleep should not be called for non-temporary errors")
		return nil
	})
	if err == nil {
		t.Fatal("expected non-temporary write error")
	}
	if attempts != 1 {
		t.Fatalf("attempt count mismatch: want 1 got %d", attempts)
	}
	if len(writes) != 1 {
		t.Fatalf("write count mismatch: want 1 got %d", len(writes))
	}
}

func TestRetryableKafkaWriteErrorRecognizesPartitionLeaderText(t *testing.T) {
	err := errors.New("messages to a replica that is not the leader for some partition, the client's metadata are likely out of date")
	if !retryableKafkaWriteError(err) {
		t.Fatal("expected not-leader metadata text to be retryable")
	}
	if !shouldUseKafkaPartitionFallback(err) {
		t.Fatal("expected not-leader metadata text to allow partition fallback")
	}
	if got := kafkaRetryReason(err); got != "leader-metadata-stale" {
		t.Fatalf("kafkaRetryReason = %q, want leader-metadata-stale", got)
	}
}

func TestShouldUseKafkaPartitionFallbackOnlyForLeaderMetadataErrors(t *testing.T) {
	if shouldUseKafkaPartitionFallback(errors.New("connection reset by peer")) {
		t.Fatal("connection reset should use writer retry, not fixed partition fallback")
	}
	if !shouldUseKafkaPartitionFallback(kafka.LeaderNotAvailable) {
		t.Fatal("expected leader-not-available to allow fixed partition fallback")
	}
}

func TestFixedPartitionBalancerUsesRequestedPartition(t *testing.T) {
	balancer := fixedPartitionBalancer{partition: 7}
	if got := balancer.Balance(kafka.Message{}, 1, 7, 9); got != 7 {
		t.Fatalf("fallback partition = %d, want 7", got)
	}
	if got := balancer.Balance(kafka.Message{}, 1, 2, 3); got != 1 {
		t.Fatalf("fallback partition = %d, want first available partition", got)
	}
}

func TestKafkaBackoffDurationCapsAtMax(t *testing.T) {
	got := kafkaBackoffDuration(5, time.Second, 5*time.Second)
	if got != 5*time.Second {
		t.Fatalf("backoff mismatch: want 5s got %s", got)
	}
}
