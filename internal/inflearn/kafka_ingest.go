package inflearn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type kafkaMessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type lectureKafkaEvent struct {
	EventUUID string `json:"event_uuid"`
	Source    string `json:"source"`
	Host      string `json:"host"`
	UUIDUser  string `json:"uuid_user"`
	IP        string `json:"ip"`
	URL       string `json:"url"`
	EventType string `json:"event_type"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func chIdent(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "``"
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (s *Service) UseKafkaIngest() bool {
	switch strings.ToLower(strings.TrimSpace(s.Cfg.IngestMode)) {
	case "kafka", "kafka_clickhouse", "kafka-clickhouse", "event", "events":
		return true
	default:
		return false
	}
}

func (s *Service) kafkaHost() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "github-actions"
	}
	return host
}

func (s *Service) ValidateKafkaIngest(ctx context.Context) error {
	if !s.UseKafkaIngest() {
		return nil
	}
	if len(s.Cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is empty")
	}
	if strings.TrimSpace(s.Cfg.KafkaTopic) == "" {
		return fmt.Errorf("KAFKA_TOPIC is empty")
	}
	for _, broker := range s.Cfg.KafkaBrokers {
		if isLoopbackBrokerEndpoint(broker) {
			return fmt.Errorf("KAFKA_BROKERS must be an externally reachable Kafka bootstrap address, not %q; use the server public IP/domain and ensure Kafka server .env KAFKA_PUBLIC_HOST is also public", broker)
		}
	}

	dialer := &kafka.Dialer{
		ClientID: s.Cfg.KafkaClientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(s.Cfg.KafkaBrokers, 10*time.Second),
	}
	if strings.TrimSpace(s.Cfg.KafkaUsername) != "" || strings.TrimSpace(s.Cfg.KafkaPassword) != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: s.Cfg.KafkaUsername, Password: s.Cfg.KafkaPassword}
	}

	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(probeCtx, "tcp", s.Cfg.KafkaBrokers[0])
	if err != nil {
		return fmt.Errorf("kafka preflight failed to connect to bootstrap broker %q: %w", s.Cfg.KafkaBrokers[0], err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(s.Cfg.KafkaTopic)
	if err != nil {
		return fmt.Errorf("kafka preflight failed to read metadata for topic %q: %w", s.Cfg.KafkaTopic, err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka preflight found zero partitions for topic %q", s.Cfg.KafkaTopic)
	}

	if err := validateKafkaAdvertisedLeaders(partitions, s.Cfg.KafkaBrokers, "kafka broker metadata"); err != nil {
		return err
	}

	fmt.Printf("[kafka] preflight ok topic=%s partitions=%d bootstrap=%s\n", s.Cfg.KafkaTopic, len(partitions), s.Cfg.KafkaBrokers[0])
	return nil
}

func isLoopbackBrokerEndpoint(raw string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		host = strings.TrimSpace(raw)
		if strings.Contains(host, ":") {
			host = strings.Split(host, ":")[0]
		}
	}
	return isLoopbackHost(host)
}

func validateKafkaAdvertisedLeaders(partitions []kafka.Partition, brokers []string, label string) error {
	bootstrap := kafkaBootstrapEndpointSet(brokers)
	nonBootstrapLeaders := 0
	topics := map[string]bool{}
	for _, partition := range partitions {
		leaderHost := strings.TrimSpace(partition.Leader.Host)
		if isLoopbackHost(leaderHost) {
			return fmt.Errorf("%s advertises loopback listener %s:%d for topic=%s partition=%d; fix the Kafka server KAFKA_PUBLIC_HOST/KAFKA_ADVERTISED_LISTENERS and force-recreate Kafka_Platform", label, leaderHost, partition.Leader.Port, partition.Topic, partition.ID)
		}
		leaderEndpoint := normalizedKafkaEndpoint(leaderHost, strconv.Itoa(partition.Leader.Port))
		if len(bootstrap) > 0 && !bootstrap[leaderEndpoint] {
			nonBootstrapLeaders++
			topics[partition.Topic] = true
		}
	}
	if nonBootstrapLeaders > 0 {
		fmt.Printf("[kafka] %s metadata has %d non-bootstrap advertised broker entries across %d topic(s); producer will dial via bootstrap rewrite\n", label, nonBootstrapLeaders, len(topics))
	}
	return nil
}

func kafkaBootstrapEndpointSet(brokers []string) map[string]bool {
	endpoints := make(map[string]bool, len(brokers))
	for _, broker := range brokers {
		host, port, ok := splitKafkaEndpoint(broker)
		if ok {
			endpoints[normalizedKafkaEndpoint(host, port)] = true
		}
	}
	return endpoints
}

func splitKafkaEndpoint(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		if strings.Count(raw, ":") != 1 {
			return "", "", false
		}
		parts := strings.SplitN(raw, ":", 2)
		host, port = parts[0], parts[1]
	}
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	return host, port, host != "" && port != ""
}

func normalizedKafkaEndpoint(host, port string) string {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	port = strings.TrimSpace(port)
	return host + ":" + port
}

func kafkaAdvertisedBrokerDialFunc(brokers []string, timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	if len(brokers) != 1 {
		return dialer.DialContext
	}
	bootstrapHost, bootstrapPort, ok := splitKafkaEndpoint(brokers[0])
	if !ok {
		return dialer.DialContext
	}
	bootstrapAddress := net.JoinHostPort(strings.Trim(bootstrapHost, "[]"), bootstrapPort)
	bootstrapEndpoint := normalizedKafkaEndpoint(bootstrapHost, bootstrapPort)
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		target := address
		if host, port, ok := splitKafkaEndpoint(address); ok {
			endpoint := normalizedKafkaEndpoint(host, port)
			if port == bootstrapPort && endpoint != bootstrapEndpoint {
				target = bootstrapAddress
			}
		}
		return dialer.DialContext(ctx, network, target)
	}
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	switch host {
	case "", "localhost", "127.0.0.1", "::1", "0.0.0.0":
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
}

func (s *Service) kafkaWriter() *kafka.Writer {
	return s.kafkaWriterWithBalancer(&kafka.Hash{})
}

func (s *Service) kafkaWriterWithBalancer(balancer kafka.Balancer) *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(s.Cfg.KafkaBrokers...),
		Topic:                  s.Cfg.KafkaTopic,
		Balancer:               balancer,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchSize:              s.Cfg.KafkaBatchSize,
		BatchTimeout:           s.Cfg.KafkaBatchTimeout,
	}
	transport := &kafka.Transport{
		ClientID: s.Cfg.KafkaClientID,
		Dial:     kafkaAdvertisedBrokerDialFunc(s.Cfg.KafkaBrokers, 10*time.Second),
	}
	if strings.TrimSpace(s.Cfg.KafkaUsername) != "" || strings.TrimSpace(s.Cfg.KafkaPassword) != "" {
		transport.SASL = plain.Mechanism{Username: s.Cfg.KafkaUsername, Password: s.Cfg.KafkaPassword}
	}
	w.Transport = transport
	return w
}

func (s *Service) publishKafkaEvents(ctx context.Context, events []lectureKafkaEvent) error {
	if len(events) == 0 {
		return nil
	}
	if len(s.Cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is empty")
	}

	messages := make([]kafka.Message, 0, len(events))
	for _, ev := range events {
		body, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(kafkaEventKey(ev)),
			Value: body,
			Time:  NowDT64(),
		})
	}
	return s.writeKafkaMessagesWithRetry(ctx, messages, func() kafkaMessageWriter {
		return s.kafkaWriter()
	}, sleepContext)
}

func (s *Service) writeKafkaMessagesWithRetry(ctx context.Context, messages []kafka.Message, newWriter func() kafkaMessageWriter, sleep func(context.Context, time.Duration) error) error {
	if len(messages) == 0 {
		return nil
	}
	attempts := s.Cfg.KafkaWriteAttempts
	if attempts <= 0 {
		attempts = 5
	}
	backoffMin := s.Cfg.KafkaWriteBackoffMin
	if backoffMin <= 0 {
		backoffMin = time.Second
	}
	backoffMax := s.Cfg.KafkaWriteBackoffMax
	if backoffMax <= 0 {
		backoffMax = 12 * time.Second
	}
	if backoffMax < backoffMin {
		backoffMax = backoffMin
	}

	pending := messages
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		w := newWriter()
		err := w.WriteMessages(ctx, pending...)
		_ = w.Close()
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[kafka] publish retry succeeded attempt=%d messages=%d\n", attempt, len(pending))
			}
			return nil
		}

		lastErr = err
		failed, retryable := retryableKafkaFailedMessages(pending, err)
		if len(failed) == 0 || !retryable {
			return err
		}
		if attempt == attempts {
			if s.Cfg.KafkaPartitionFallback && shouldUseKafkaPartitionFallback(err) {
				if fallbackErr := s.writeKafkaMessagesToWritablePartition(ctx, failed); fallbackErr == nil {
					return nil
				} else {
					return fmt.Errorf("kafka publish failed after fixed-partition fallback: %s; original_reason=%s", shortKafkaError(fallbackErr), kafkaRetryReason(err))
				}
			}
			return err
		}

		fmt.Printf("[kafka] retrying publish attempt=%d/%d failed_messages=%d reason=%s\n", attempt+1, attempts, len(failed), kafkaRetryReason(err))
		if err := sleep(ctx, kafkaBackoffDuration(attempt, backoffMin, backoffMax)); err != nil {
			return err
		}
		pending = failed
	}
	return lastErr
}

func (s *Service) writeKafkaMessagesToWritablePartition(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	partitions, err := s.fallbackPartitionIDs(ctx)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka partition fallback found zero partitions for topic=%s", s.Cfg.KafkaTopic)
	}

	timeout := s.Cfg.KafkaFallbackTimeout
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	pending := messages
	var lastErr error
	for _, partition := range partitions {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		w := s.kafkaWriterWithBalancer(fixedPartitionBalancer{partition: partition})
		err := w.WriteMessages(attemptCtx, pending...)
		_ = w.Close()
		cancel()
		if err == nil {
			fmt.Printf("[kafka] fixed partition fallback succeeded partition=%d messages=%d\n", partition, len(pending))
			return nil
		}

		lastErr = err
		failed, retryable := retryableKafkaFailedMessages(pending, err)
		if len(failed) == 0 {
			return nil
		}
		pending = failed
		fmt.Printf("[kafka] fixed partition fallback failed partition=%d failed_messages=%d reason=%s error=%s\n", partition, len(pending), kafkaRetryReason(err), shortKafkaError(err))
		if !retryable {
			return err
		}
	}
	return fmt.Errorf("kafka fixed partition fallback exhausted partitions=%v failed_messages=%d last_error=%s", partitions, len(pending), shortKafkaError(lastErr))
}

func (s *Service) fallbackPartitionIDs(ctx context.Context) ([]int, error) {
	if len(s.Cfg.KafkaFallbackPartitions) > 0 {
		out := append([]int(nil), s.Cfg.KafkaFallbackPartitions...)
		sort.Ints(out)
		return uniqueInts(out), nil
	}

	dialer := &kafka.Dialer{
		ClientID: s.Cfg.KafkaClientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(s.Cfg.KafkaBrokers, 10*time.Second),
	}
	if strings.TrimSpace(s.Cfg.KafkaUsername) != "" || strings.TrimSpace(s.Cfg.KafkaPassword) != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: s.Cfg.KafkaUsername, Password: s.Cfg.KafkaPassword}
	}

	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(probeCtx, "tcp", s.Cfg.KafkaBrokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafka partition fallback failed to connect to bootstrap broker: %s", kafkaRetryReason(err))
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(s.Cfg.KafkaTopic)
	if err != nil {
		return nil, fmt.Errorf("kafka partition fallback failed to read metadata for topic %q: %s", s.Cfg.KafkaTopic, kafkaRetryReason(err))
	}
	out := make([]int, 0, len(partitions))
	for _, partition := range partitions {
		if partition.Topic == s.Cfg.KafkaTopic {
			out = append(out, partition.ID)
		}
	}
	sort.Ints(out)
	return uniqueInts(out), nil
}

type fixedPartitionBalancer struct {
	partition int
}

func (b fixedPartitionBalancer) Balance(_ kafka.Message, partitions ...int) int {
	for _, partition := range partitions {
		if partition == b.partition {
			return partition
		}
	}
	if len(partitions) > 0 {
		return partitions[0]
	}
	return b.partition
}

func uniqueInts(in []int) []int {
	if len(in) == 0 {
		return nil
	}
	out := in[:0]
	var last int
	for i, n := range in {
		if i == 0 || n != last {
			out = append(out, n)
			last = n
		}
	}
	return out
}

func retryableKafkaFailedMessages(messages []kafka.Message, err error) ([]kafka.Message, bool) {
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if len(writeErrs) != len(messages) {
			return messages, retryableKafkaWriteError(err)
		}
		failed := make([]kafka.Message, 0, writeErrs.Count())
		retryable := true
		for i, writeErr := range writeErrs {
			if writeErr == nil {
				continue
			}
			failed = append(failed, messages[i])
			if !retryableKafkaWriteError(writeErr) {
				retryable = false
			}
		}
		return failed, retryable
	}
	return messages, retryableKafkaWriteError(err)
}

func retryableKafkaWriteError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if writeErrs.Count() == 0 {
			return false
		}
		for _, writeErr := range writeErrs {
			if writeErr != nil && !retryableKafkaWriteError(writeErr) {
				return false
			}
		}
		return true
	}
	var tempErr interface{ Temporary() bool }
	if errors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}
	return errors.Is(err, io.EOF) || isRetryableKafkaErrorText(err.Error())
}

func isRetryableKafkaErrorText(message string) bool {
	msg := strings.ToLower(message)
	return strings.Contains(msg, "not leader for partition") ||
		strings.Contains(msg, "not the leader for") ||
		strings.Contains(msg, "partition has no leader") ||
		strings.Contains(msg, "has no leader") ||
		strings.Contains(msg, "leader not available") ||
		strings.Contains(msg, "metadata are likely out of date") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "failed to dial") ||
		strings.Contains(msg, "failed to open connection") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "temporary failure in name resolution") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "eof")
}

func kafkaRetryReason(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "not leader for partition"),
		strings.Contains(msg, "not the leader for"),
		strings.Contains(msg, "partition has no leader"),
		strings.Contains(msg, "has no leader"),
		strings.Contains(msg, "metadata are likely out of date"):
		return "leader-metadata-stale"
	case strings.Contains(msg, "leader not available"):
		return "leader-not-available"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "eof"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "failed to dial"),
		strings.Contains(msg, "failed to open connection"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "temporary failure in name resolution"):
		return "network"
	default:
		return "temporary-kafka-error"
	}
}

func shouldUseKafkaPartitionFallback(err error) bool {
	reason := kafkaRetryReason(err)
	return reason == "leader-metadata-stale" || reason == "leader-not-available"
}

func shortKafkaError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.Join(strings.Fields(err.Error()), " ")
	if len(msg) > 240 {
		return msg[:240] + "..."
	}
	return msg
}

func kafkaBackoffDuration(attempt int, minDelay, maxDelay time.Duration) time.Duration {
	if minDelay <= 0 {
		minDelay = time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 12 * time.Second
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	delay := minDelay
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func kafkaEventKey(ev lectureKafkaEvent) string {
	if strings.TrimSpace(ev.URL) != "" {
		return ev.EventType + ":" + ev.URL
	}
	return ev.EventType + ":" + ev.EventUUID
}

func (s *Service) newInflearnEvent(eventType, eventUUID, sourceURL, createdAt string, payload map[string]any) (lectureKafkaEvent, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return lectureKafkaEvent{}, err
	}
	if strings.TrimSpace(eventUUID) == "" {
		eventUUID = UUIDv7String(NowDT64())
	}
	if strings.TrimSpace(createdAt) == "" {
		createdAt = FormatCHTime(NowDT64())
	}
	return lectureKafkaEvent{
		EventUUID: eventUUID,
		Source:    s.Cfg.ProducerSource,
		Host:      s.kafkaHost(),
		UUIDUser:  "",
		IP:        s.Cfg.ProducerIP,
		URL:       sourceURL,
		EventType: eventType,
		Payload:   string(payloadJSON),
		CreatedAt: createdAt,
	}, nil
}

func (s *Service) PublishCheckpoint(ctx context.Context, source string, cp Checkpoint) error {
	now := NowDT64()
	payload := map[string]any{
		"checkpoint_uuid": UUIDv7String(now),
		"source":          source,
		"updated_at":      FormatCHTime(now),
		"sitemap_index":   cp.SitemapIndex,
		"url_index":       cp.URLIndex,
	}
	ev, err := s.newInflearnEvent("inflearn.crawl_checkpoint.v1", asString(payload["checkpoint_uuid"]), "", FormatCHTime(now), payload)
	if err != nil {
		return err
	}
	return s.publishKafkaEvents(ctx, []lectureKafkaEvent{ev})
}

func (s *Service) PublishCourseRows(ctx context.Context, rows CourseRows) error {
	events := make([]lectureKafkaEvent, 0,
		len(rows.SnapshotRaw)+len(rows.CourseDim)+len(rows.MetricFact)+len(rows.PriceFact)+
			len(rows.CurriculumUnit)+len(rows.InstructorDim)+len(rows.CourseInstructor))

	appendRows := func(eventType string, rows []map[string]any) error {
		for _, row := range rows {
			ev, err := s.rowEvent(eventType, row)
			if err != nil {
				return err
			}
			events = append(events, ev)
		}
		return nil
	}

	if err := appendRows("inflearn.course_snapshot_raw.v1", rows.SnapshotRaw); err != nil {
		return err
	}
	if err := appendRows("inflearn.course_dim.v1", rows.CourseDim); err != nil {
		return err
	}
	if err := appendRows("inflearn.course_metric_fact.v1", rows.MetricFact); err != nil {
		return err
	}
	if err := appendRows("inflearn.course_price_fact.v1", rows.PriceFact); err != nil {
		return err
	}
	if err := appendRows("inflearn.course_curriculum_unit.v1", rows.CurriculumUnit); err != nil {
		return err
	}
	if err := appendRows("inflearn.instructor_dim.v1", rows.InstructorDim); err != nil {
		return err
	}
	if err := appendRows("inflearn.course_instructor_map.v1", rows.CourseInstructor); err != nil {
		return err
	}
	return s.publishKafkaEvents(ctx, events)
}

func (s *Service) rowEvent(eventType string, row map[string]any) (lectureKafkaEvent, error) {
	payload := cloneRowForKafka(row)
	createdAt := firstNonEmpty(asString(row["fetched_at"]), asString(row["updated_at"]), FormatCHTime(NowDT64()))
	eventUUID := rowEventUUID(eventType, row, createdAt)
	return s.newInflearnEvent(eventType, eventUUID, rowEventURL(row), createdAt, payload)
}

func cloneRowForKafka(row map[string]any) map[string]any {
	out := make(map[string]any, len(row)+1)
	for k, v := range row {
		if k == "payload" {
			out["payload_json"] = MustJSON(v)
			continue
		}
		out[k] = v
	}
	return out
}

func rowEventURL(row map[string]any) string {
	return asString(row["source_url"])
}

func rowEventUUID(eventType string, row map[string]any, createdAt string) string {
	if eventType == "inflearn.course_snapshot_raw.v1" {
		if uuid := asString(row["uuid"]); strings.TrimSpace(uuid) != "" {
			return uuid
		}
	}
	seed := strings.Join([]string{
		eventType,
		asString(row["course_id"]),
		asString(row["locale"]),
		asString(row["instructor_id"]),
		asString(row["section_id"]),
		asString(row["unit_id"]),
		asString(row["role"]),
		createdAt,
	}, "\x1f")
	if strings.Trim(seed, "\x1f") == eventType {
		return UUIDv7String(NowDT64())
	}
	if t, ok := ParseDT64(createdAt); ok {
		return UUIDv7String(t)
	}
	return UUIDv7String(NowDT64())
}

func nullableString(v any) any {
	s := strings.TrimSpace(asString(v))
	if s == "" {
		return nil
	}
	return s
}

func asUInt64(v any) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case uint32:
		return uint64(x)
	case int:
		return uint64(maxInt64(int64(x), 0))
	case int64:
		return uint64(maxInt64(x, 0))
	case int32:
		return uint64(maxInt64(int64(x), 0))
	case float64:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case float32:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case json.Number:
		if n, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return n
		}
		if n, err := x.Int64(); err == nil && n >= 0 {
			return uint64(n)
		}
	case string:
		if n, err := strconv.ParseUint(strings.TrimSpace(x), 10, 64); err == nil {
			return n
		}
	}
	return 0
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
