package inflearn

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

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

	for _, partition := range partitions {
		leaderHost := strings.TrimSpace(partition.Leader.Host)
		if isLoopbackHost(leaderHost) {
			return fmt.Errorf("kafka broker metadata advertises loopback listener %s:%d for topic=%s partition=%d; fix the Kafka server .env KAFKA_PUBLIC_HOST to the public IP/domain, then force-recreate Kafka_Platform", leaderHost, partition.Leader.Port, partition.Topic, partition.ID)
		}
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
	w := &kafka.Writer{
		Addr:                   kafka.TCP(s.Cfg.KafkaBrokers...),
		Topic:                  s.Cfg.KafkaTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchSize:              s.Cfg.KafkaBatchSize,
		BatchTimeout:           s.Cfg.KafkaBatchTimeout,
	}
	if strings.TrimSpace(s.Cfg.KafkaClientID) != "" || strings.TrimSpace(s.Cfg.KafkaUsername) != "" {
		transport := &kafka.Transport{ClientID: s.Cfg.KafkaClientID}
		if strings.TrimSpace(s.Cfg.KafkaUsername) != "" || strings.TrimSpace(s.Cfg.KafkaPassword) != "" {
			transport.SASL = plain.Mechanism{Username: s.Cfg.KafkaUsername, Password: s.Cfg.KafkaPassword}
		}
		w.Transport = transport
	}
	return w
}

func (s *Service) publishKafkaEvents(ctx context.Context, events []lectureKafkaEvent) error {
	if len(events) == 0 {
		return nil
	}
	if len(s.Cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is empty")
	}
	w := s.kafkaWriter()
	defer w.Close()

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
	return w.WriteMessages(ctx, messages...)
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
