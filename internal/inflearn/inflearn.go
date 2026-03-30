package inflearn

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	KST              = loadKST()
	nextDataPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?s)<script[^>]+id=["']__NEXT_DATA__["'][^>]*>(.*?)</script>`),
		regexp.MustCompile(`(?s)window\.__NEXT_DATA__\s*=\s*(\{.*?\})\s*;</script>`),
	}
	courseInfoRe = regexp.MustCompile(`/client/api/v1/course/(\d+)/online/info`)
)

const (
	collectCheckpointSource = "inflearn_courseDetail"
	updateCheckpointSource  = "inflearn_update_existing"
)

type Config struct {
	CHHost               string
	CHPort               int
	CHUser               string
	CHPassword           string
	CHDatabase           string
	CHSecure             bool
	SitemapBase          string
	SitemapBaseFallback  string
	SitemapPrefix        string
	BatchSize            int
	MaxURLsPerRun        int
	CheckpointFlushEvery int
	UpdateBatchSize      int
	Workers              int
	RequestSleepMin      time.Duration
	RequestSleepMax      time.Duration
	OptimizeMonths       int
	StatsLookbackDays    int
	UserAgent            string
}

type Service struct {
	Cfg        Config
	HTTPClient *http.Client
}

type chJSONResponse struct {
	Data []map[string]any `json:"data"`
}

type Checkpoint struct {
	SitemapIndex int
	URLIndex     int
}

type CourseFetchedData struct {
	CourseID      int
	Locale        string
	SourceURL     string
	SnapshotRows  []map[string]any
	OnlineInfo    map[string]any
	MetaInfo      map[string]any
	Curriculum    map[string]any
	DiscountsBest map[string]any
	Contents      map[string]any
}

type CourseRows struct {
	SnapshotRaw      []map[string]any
	CourseDim        []map[string]any
	MetricFact       []map[string]any
	PriceFact        []map[string]any
	CurriculumUnit   []map[string]any
	InstructorDim    []map[string]any
	CourseInstructor []map[string]any
}

type updatePick struct {
	CourseID  int
	Locale    string
	SourceURL string
}

type updateResult struct {
	Rows CourseRows
	URL  string
	Err  error
}

type bucketCount struct {
	Bucket string
	Count  int64
}

func loadKST() *time.Location {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		return time.FixedZone("Asia/Seoul", 9*60*60)
	}
	return loc
}

func LoadConfig() (Config, error) {
	host := envFirst("CH_HOST", "CLICKHOUSE_HOST")
	if host == "" {
		return Config{}, fmt.Errorf("missing required env: CH_HOST or CLICKHOUSE_HOST")
	}
	cfg := Config{
		CHHost:               host,
		CHPort:               parsePort(envFirst("CH_PORT", "CLICKHOUSE_PORT"), 8123),
		CHUser:               envFirstDefault([]string{"CH_USER", "CLICKHOUSE_USER"}, "default"),
		CHPassword:           envFirstDefault([]string{"CH_PASSWORD", "CLICKHOUSE_PASSWORD"}, ""),
		CHDatabase:           envFirstDefault([]string{"CH_DATABASE", "CLICKHOUSE_DATABASE"}, "statground_lecture"),
		CHSecure:             parseBool(envFirstDefault([]string{"CH_SECURE", "CLICKHOUSE_SECURE"}, "0")),
		SitemapBase:          strings.TrimRight(envDefault("SITEMAP_BASE", "https://cdn.inflearn.com/sitemaps"), "/"),
		SitemapBaseFallback:  strings.TrimRight(envDefault("SITEMAP_BASE_FALLBACK", "https://www.inflearn.com/sitemaps"), "/"),
		SitemapPrefix:        envDefault("SITEMAP_PREFIX", "sitemap-courseDetail-"),
		BatchSize:            parsePositiveInt(envDefault("BATCH_SIZE", "100"), 100),
		MaxURLsPerRun:        parsePositiveInt(envDefault("MAX_URLS_PER_RUN", "1500"), 1500),
		CheckpointFlushEvery: parsePositiveInt(envDefault("CHECKPOINT_FLUSH_EVERY", "200"), 200),
		UpdateBatchSize:      parsePositiveInt(envDefault("UPDATE_BATCH_SIZE", "100"), 100),
		Workers:              parsePositiveInt(envDefault("WORKERS", "8"), 8),
		RequestSleepMin:      parseSeconds(envDefault("REQUEST_SLEEP_MIN", "0.2")),
		RequestSleepMax:      parseSeconds(envDefault("REQUEST_SLEEP_MAX", "0.6")),
		OptimizeMonths:       parsePositiveInt(envDefault("OPTIMIZE_MONTHS", "2"), 2),
		StatsLookbackDays:    parsePositiveInt(envDefault("STATS_LOOKBACK_DAYS", "365"), 365),
		UserAgent:            envDefault("CRAWLER_USER_AGENT", "Mozilla/5.0 (compatible; StatgroundCrawler/2.0; +https://www.statground.net)"),
	}
	return cfg, nil
}

func NewService(cfg Config) *Service {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &Service{
		Cfg:        cfg,
		HTTPClient: &http.Client{Transport: transport},
	}
}

func envDefault(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

func envFirst(names ...string) string {
	for _, name := range names {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			return v
		}
	}
	return ""
}

func envFirstDefault(names []string, fallback string) string {
	if v := envFirst(names...); v != "" {
		return v
	}
	return fallback
}

func parsePort(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	re := regexp.MustCompile(`\d+`)
	match := re.FindString(raw)
	if match == "" {
		return fallback
	}
	n, err := strconv.Atoi(match)
	if err != nil {
		return fallback
	}
	return n
}

func parsePositiveInt(raw string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func parseSeconds(raw string) time.Duration {
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || v <= 0 {
		return 0
	}
	return time.Duration(v * float64(time.Second))
}

func NowDT64() time.Time {
	return time.Now().In(KST).Truncate(time.Millisecond)
}

func EpochKST() time.Time {
	return time.Date(1970, 1, 1, 0, 0, 0, 0, KST)
}

func FormatCHTime(t time.Time) string {
	return t.In(KST).Format("2006-01-02 15:04:05.000")
}

func ParseDT64(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	layouts := []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		var (
			t   time.Time
			err error
		)
		if layout == "2006-01-02 15:04:05.000" || layout == "2006-01-02 15:04:05" {
			t, err = time.ParseInLocation(layout, raw, KST)
		} else {
			t, err = time.Parse(layout, raw)
		}
		if err == nil {
			return t.In(KST).Truncate(time.Millisecond), true
		}
	}
	return time.Time{}, false
}

func JitterSleep(minDur, maxDur time.Duration) {
	if maxDur <= 0 {
		return
	}
	if minDur < 0 {
		minDur = 0
	}
	if maxDur < minDur {
		maxDur = minDur
	}
	if maxDur == minDur {
		time.Sleep(minDur)
		return
	}
	delta := maxDur - minDur
	n := time.Duration(time.Now().UnixNano() % int64(delta+1))
	time.Sleep(minDur + n)
}

func H64(text string) uint64 {
	sum := sha256.Sum256([]byte(text))
	return binary.LittleEndian.Uint64(sum[:8])
}

func MustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

func UUIDv7String(at time.Time) string {
	at = at.In(time.UTC)
	ms := uint64(at.UnixMilli())
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[0], b[1], b[2], b[3],
		b[4], b[5],
		b[6], b[7],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15],
	)
}

func ToU8(v any) int {
	if asBool(v) {
		return 1
	}
	return 0
}

func ClampU8Percent(v any) int {
	n := asInt(v)
	if n < 0 {
		return 0
	}
	if n > 100 {
		return 100
	}
	return n
}

func QuoteSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

func ParseLocaleFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "ko"
	}
	locale := strings.ToLower(strings.TrimSpace(u.Query().Get("locale")))
	if locale != "" {
		return locale
	}
	if strings.HasPrefix(u.Path, "/en/") {
		return "en"
	}
	return "ko"
}

func ParseCourseIDAndLocale(raw string) (int, string) {
	u, err := url.Parse(raw)
	if err != nil {
		return 0, "ko"
	}
	for _, key := range []string{"cid", "courseId", "course_id"} {
		if qv := strings.TrimSpace(u.Query().Get(key)); qv != "" {
			if n, err := strconv.Atoi(qv); err == nil {
				return n, ParseLocaleFromURL(raw)
			}
		}
	}
	return 0, ParseLocaleFromURL(raw)
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func asSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func asString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", x)
	}
}

func asInt(v any) int {
	switch x := v.(type) {
	case nil:
		return 0
	case int:
		return x
	case int64:
		return int(x)
	case int32:
		return int(x)
	case uint64:
		return int(x)
	case uint32:
		return int(x)
	case float64:
		return int(x)
	case float32:
		return int(x)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return int(n)
		}
		if f, err := x.Float64(); err == nil {
			return int(f)
		}
		return 0
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(x)); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
			return int(f)
		}
		return 0
	case bool:
		if x {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func asInt64(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case int:
		return int64(x)
	case int64:
		return x
	case int32:
		return int64(x)
	case uint64:
		return int64(x)
	case uint32:
		return int64(x)
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		if f, err := x.Float64(); err == nil {
			return int64(f)
		}
		return 0
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(x), 64); err == nil {
			return int64(f)
		}
		return 0
	default:
		return 0
	}
}

func asFloat64(v any) float64 {
	switch x := v.(type) {
	case nil:
		return 0
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case uint64:
		return float64(x)
	case uint32:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f
	case bool:
		if x {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func asBool(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "1", "true", "yes", "y", "on":
			return true
		default:
			return false
		}
	case int, int32, int64, uint32, uint64:
		return asInt64(v) != 0
	case float64, float32:
		return asFloat64(v) != 0
	default:
		return false
	}
}

func stringSlice(v any) []string {
	src := asSlice(v)
	out := make([]string, 0, len(src))
	for _, item := range src {
		s := asString(item)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func firstNonZero(values ...any) any {
	for _, v := range values {
		switch x := v.(type) {
		case nil:
			continue
		case string:
			if strings.TrimSpace(x) != "" && strings.TrimSpace(x) != "0" {
				return v
			}
		default:
			if asFloat64(v) != 0 {
				return v
			}
		}
	}
	return nil
}

func nullableInt(v any) any {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
		return nil
	}
	return asInt(v)
}

func nullableSig(v any) any {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
		return nil
	}
	return v
}

func (s *Service) newRequest(ctx context.Context, method, rawURL string, body io.Reader, accept string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", s.Cfg.UserAgent)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	req.Header.Set("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	return req, nil
}

func (s *Service) httpGetBytes(ctx context.Context, rawURL string, accept string) ([]byte, int, error) {
	req, err := s.newRequest(ctx, http.MethodGet, rawURL, nil, accept)
	if err != nil {
		return nil, 0, err
	}
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(resp.Body)
		if err == nil {
			defer gz.Close()
			reader = gz
		}
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		gz, err := gzip.NewReader(bytes.NewReader(body))
		if err == nil {
			defer gz.Close()
			body, err = io.ReadAll(gz)
			if err != nil {
				return nil, resp.StatusCode, err
			}
		}
	}
	return body, resp.StatusCode, nil
}

func (s *Service) httpGetJSON(ctx context.Context, rawURL string) (map[string]any, int, error) {
	body, status, err := s.httpGetBytes(ctx, rawURL, "application/json,text/plain;q=0.9,*/*;q=0.8")
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		return nil, status, err
	}
	return out, status, nil
}

func (s *Service) httpGetWithRetry(ctx context.Context, rawURL string, accept string) ([]byte, int, error) {
	var lastErr error
	var status int
	for attempt := 1; attempt <= 5; attempt++ {
		body, st, err := s.httpGetBytes(ctx, rawURL, accept)
		status = st
		if err == nil && st != http.StatusTooManyRequests && st != http.StatusInternalServerError && st != http.StatusBadGateway && st != http.StatusServiceUnavailable && st != http.StatusGatewayTimeout {
			return body, st, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("retryable http %d", st)
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	return nil, status, lastErr
}

func (s *Service) chBaseURL() string {
	scheme := "http"
	if s.Cfg.CHSecure {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s:%d/", scheme, s.Cfg.CHHost, s.Cfg.CHPort)
}

func (s *Service) chPost(ctx context.Context, sql string, data []byte, contentType string) ([]byte, error) {
	values := url.Values{}
	values.Set("database", s.Cfg.CHDatabase)
	if len(data) > 0 {
		values.Set("query", sql)
	}
	rawURL := s.chBaseURL()
	if encoded := values.Encode(); encoded != "" {
		rawURL += "?" + encoded
	}

	var body io.Reader
	if len(data) > 0 {
		body = bytes.NewReader(data)
	} else {
		body = strings.NewReader(sql)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(s.Cfg.CHUser, s.Cfg.CHPassword)
	req.Header.Set("User-Agent", s.Cfg.UserAgent)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		snippet := strings.TrimSpace(string(out))
		if len(snippet) > 500 {
			snippet = snippet[:500]
		}
		return nil, fmt.Errorf("clickhouse http %d: %s", resp.StatusCode, snippet)
	}
	return out, nil
}

func (s *Service) CHExec(ctx context.Context, sql string) error {
	execCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	_, err := s.chPost(execCtx, sql, nil, "text/plain; charset=utf-8")
	return err
}

func (s *Service) CHQueryRows(ctx context.Context, sql string) ([]map[string]any, error) {
	sql = strings.TrimSpace(sql)
	upper := strings.ToUpper(sql)
	if !strings.Contains(upper, "FORMAT ") {
		sql += " FORMAT JSON"
	}
	queryCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	body, err := s.chPost(queryCtx, sql, nil, "text/plain; charset=utf-8")
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var resp chJSONResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *Service) CHInsertRows(ctx context.Context, table string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", s.Cfg.CHDatabase, table)
	insertCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	_, err := s.chPost(insertCtx, insertSQL, buf.Bytes(), "application/x-ndjson; charset=utf-8")
	return err
}

func (s *Service) getCheckpoint(ctx context.Context, source string) (Checkpoint, error) {
	sql := fmt.Sprintf(`
        SELECT
          argMax(sitemap_index, updated_at) AS sitemap_index,
          argMax(url_index, updated_at) AS url_index
        FROM %s.inflearn_crawl_checkpoint
        WHERE source = %s
    `, s.Cfg.CHDatabase, QuoteSQLString(source))
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return Checkpoint{}, err
	}
	if len(rows) == 0 {
		return Checkpoint{}, nil
	}
	return Checkpoint{
		SitemapIndex: asInt(rows[0]["sitemap_index"]),
		URLIndex:     asInt(rows[0]["url_index"]),
	}, nil
}

func (s *Service) setCheckpoint(ctx context.Context, source string, cp Checkpoint) error {
	row := map[string]any{
		"source":        source,
		"updated_at":    FormatCHTime(NowDT64()),
		"sitemap_index": cp.SitemapIndex,
		"url_index":     cp.URLIndex,
	}
	return s.CHInsertRows(ctx, "inflearn_crawl_checkpoint", []map[string]any{row})
}

func (s *Service) courseExists(ctx context.Context, courseID int, locale string) (bool, error) {
	sql := fmt.Sprintf(`
        SELECT count() AS c
        FROM %s.inflearn_course_dim
        WHERE course_id = %d AND locale = %s
    `, s.Cfg.CHDatabase, courseID, QuoteSQLString(locale))
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	return asInt64(rows[0]["c"]) > 0, nil
}

func normalizeXMLBytes(body []byte) []byte {
	return bytes.TrimLeft(body, "\xef\xbb\xbf\r\n\t ")
}

func isAccessDeniedSitemap(body []byte) bool {
	raw := normalizeXMLBytes(body)
	if len(raw) == 0 {
		return false
	}
	head := raw
	if len(head) > 4096 {
		head = head[:4096]
	}
	if bytes.Contains(head, []byte("<Error")) && bytes.Contains(head, []byte("<Code>AccessDenied</Code>")) {
		return true
	}
	dec := xml.NewDecoder(bytes.NewReader(raw))
	rootName := ""
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false
		}
		if start, ok := tok.(xml.StartElement); ok {
			rootName = strings.ToLower(start.Name.Local)
			break
		}
	}
	if rootName != "error" {
		return false
	}
	dec = xml.NewDecoder(bytes.NewReader(raw))
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false
		}
		start, ok := tok.(xml.StartElement)
		if !ok || strings.ToLower(start.Name.Local) != "code" {
			continue
		}
		var code string
		if err := dec.DecodeElement(&code, &start); err == nil && strings.TrimSpace(code) == "AccessDenied" {
			return true
		}
	}
	return false
}

func parseSitemapLocs(body []byte) ([]string, error) {
	raw := normalizeXMLBytes(body)
	out := make([]string, 0, 256)
	dec := xml.NewDecoder(bytes.NewReader(raw))
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		start, ok := tok.(xml.StartElement)
		if !ok || strings.ToLower(start.Name.Local) != "loc" {
			continue
		}
		var loc string
		if err := dec.DecodeElement(&loc, &start); err != nil {
			return nil, err
		}
		loc = strings.TrimSpace(loc)
		if loc != "" {
			out = append(out, loc)
		}
	}
	return out, nil
}

func extractNextData(html string) map[string]any {
	for _, re := range nextDataPatterns {
		match := re.FindStringSubmatch(html)
		if len(match) < 2 {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(strings.TrimSpace(match[1])))
		dec.UseNumber()
		var out map[string]any
		if err := dec.Decode(&out); err == nil {
			return out
		}
	}
	return nil
}

func parseCourseIDFromNextData(nd map[string]any) int {
	props := asMap(nd["props"])
	pageProps := asMap(props["pageProps"])
	dehydrated := asMap(pageProps["dehydratedState"])
	queries := asSlice(dehydrated["queries"])
	for _, q := range queries {
		qm := asMap(q)
		qk := MustJSON(qm["queryKey"])
		match := courseInfoRe.FindStringSubmatch(qk)
		if len(match) == 2 {
			n, _ := strconv.Atoi(match[1])
			return n
		}
	}
	return 0
}

func findAPIData(queries []any, needles ...string) map[string]any {
	for _, q := range queries {
		qm := asMap(q)
		qk := MustJSON(qm["queryKey"])
		matched := true
		for _, needle := range needles {
			if !strings.Contains(qk, needle) {
				matched = false
				break
			}
		}
		if matched {
			return asMap(asMap(qm["state"])["data"])
		}
	}
	return nil
}

func snapshotRowFromPayload(courseID int, locale, sourceURL, queryKey string, payload map[string]any, fetchedAt time.Time) map[string]any {
	statusCode := "OK"
	var errorCode any
	if payload != nil {
		if v := asString(payload["statusCode"]); v != "" {
			statusCode = v
		}
		if v := asString(payload["errorCode"]); v != "" {
			errorCode = v
		}
	}
	payloadJSON := MustJSON(payload)
	return map[string]any{
		"uuid":           UUIDv7String(fetchedAt),
		"fetched_at":     FormatCHTime(fetchedAt),
		"course_id":      courseID,
		"locale":         locale,
		"source_url":     sourceURL,
		"query_key":      queryKey,
		"query_key_hash": H64(queryKey),
		"status_code":    statusCode,
		"error_code":     errorCode,
		"payload":        payload,
		"payload_hash":   H64(payloadJSON),
	}
}

func (r *CourseRows) Append(other CourseRows) {
	r.SnapshotRaw = append(r.SnapshotRaw, other.SnapshotRaw...)
	r.CourseDim = append(r.CourseDim, other.CourseDim...)
	r.MetricFact = append(r.MetricFact, other.MetricFact...)
	r.PriceFact = append(r.PriceFact, other.PriceFact...)
	r.CurriculumUnit = append(r.CurriculumUnit, other.CurriculumUnit...)
	r.InstructorDim = append(r.InstructorDim, other.InstructorDim...)
	r.CourseInstructor = append(r.CourseInstructor, other.CourseInstructor...)
}

func (r *CourseRows) Reset() {
	r.SnapshotRaw = nil
	r.CourseDim = nil
	r.MetricFact = nil
	r.PriceFact = nil
	r.CurriculumUnit = nil
	r.InstructorDim = nil
	r.CourseInstructor = nil
}

func (r CourseRows) InsertAll(ctx context.Context, s *Service) error {
	if err := s.CHInsertRows(ctx, "inflearn_course_snapshot_raw", r.SnapshotRaw); err != nil {
		return err
	}
	if err := s.CHInsertRows(ctx, "inflearn_course_dim", r.CourseDim); err != nil {
		return err
	}
	if err := s.CHInsertRows(ctx, "inflearn_course_metric_fact", r.MetricFact); err != nil {
		return err
	}
	if err := s.CHInsertRows(ctx, "inflearn_course_price_fact", r.PriceFact); err != nil {
		return err
	}
	if err := s.CHInsertRows(ctx, "inflearn_course_curriculum_unit", r.CurriculumUnit); err != nil {
		return err
	}
	if err := s.CHInsertRows(ctx, "inflearn_instructor_dim", r.InstructorDim); err != nil {
		return err
	}
	if err := s.CHInsertRows(ctx, "inflearn_course_instructor_map", r.CourseInstructor); err != nil {
		return err
	}
	return nil
}

func (s *Service) fetchCourseAPI(ctx context.Context, path string) (map[string]any, int, error) {
	return s.httpGetJSON(ctx, "https://www.inflearn.com"+path)
}

func (s *Service) fetchCourseData(ctx context.Context, courseURL string, fetchedAt time.Time) (*CourseFetchedData, error) {
	cidHint, locale := ParseCourseIDAndLocale(courseURL)
	data := &CourseFetchedData{
		Locale:    locale,
		SourceURL: courseURL,
	}
	seenQueryKey := map[string]struct{}{}

	pageBody, pageStatus, pageErr := s.httpGetWithRetry(ctx, courseURL, "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	if pageErr == nil && pageStatus == http.StatusOK {
		nd := extractNextData(string(pageBody))
		if nd != nil {
			if cid := parseCourseIDFromNextData(nd); cid > 0 {
				data.CourseID = cid
			}
			props := asMap(nd["props"])
			pageProps := asMap(props["pageProps"])
			dehydrated := asMap(pageProps["dehydratedState"])
			queries := asSlice(dehydrated["queries"])
			for _, q := range queries {
				qm := asMap(q)
				qk := MustJSON(qm["queryKey"])
				payloadObj := asMap(asMap(qm["state"])["data"])
				if payloadObj == nil {
					continue
				}
				seenQueryKey[qk] = struct{}{}
				data.SnapshotRows = append(data.SnapshotRows, snapshotRowFromPayload(data.CourseID, data.Locale, courseURL, qk, payloadObj, fetchedAt))
			}
			if data.CourseID > 0 {
				data.OnlineInfo = findAPIData(queries, fmt.Sprintf("/client/api/v1/course/%d/online/info", data.CourseID))
				data.MetaInfo = findAPIData(queries, fmt.Sprintf("/client/api/v1/course/%d/meta", data.CourseID))
				data.Curriculum = findAPIData(queries, fmt.Sprintf("/client/api/v2/courses/%d/curriculum", data.CourseID))
				data.DiscountsBest = findAPIData(queries, fmt.Sprintf("/client/api/v1/discounts/best?courseIds=%d", data.CourseID))
				data.Contents = findAPIData(queries, fmt.Sprintf("/client/api/v1/course/%d/contents", data.CourseID))
			}
		}
	}

	if data.CourseID == 0 {
		data.CourseID = cidHint
	}
	if data.CourseID == 0 {
		if pageErr != nil {
			return nil, pageErr
		}
		return nil, fmt.Errorf("unable to resolve course_id from url=%s", courseURL)
	}

	apiTargets := []struct {
		name string
		path string
	}{
		{"online", fmt.Sprintf("/client/api/v1/course/%d/online/info", data.CourseID)},
		{"meta", fmt.Sprintf("/client/api/v1/course/%d/meta", data.CourseID)},
		{"curriculum", fmt.Sprintf("/client/api/v2/courses/%d/curriculum", data.CourseID)},
		{"discounts", fmt.Sprintf("/client/api/v1/discounts/best?courseIds=%d", data.CourseID)},
	}

	for _, target := range apiTargets {
		have := false
		switch target.name {
		case "online":
			have = data.OnlineInfo != nil
		case "meta":
			have = data.MetaInfo != nil
		case "curriculum":
			have = data.Curriculum != nil
		case "discounts":
			have = data.DiscountsBest != nil
		}
		if have {
			continue
		}
		payload, status, err := s.fetchCourseAPI(ctx, target.path)
		if err != nil || status != http.StatusOK || payload == nil {
			continue
		}
		switch target.name {
		case "online":
			data.OnlineInfo = payload
		case "meta":
			data.MetaInfo = payload
		case "curriculum":
			data.Curriculum = payload
		case "discounts":
			data.DiscountsBest = payload
		}
		if _, exists := seenQueryKey[target.path]; !exists {
			data.SnapshotRows = append(data.SnapshotRows, snapshotRowFromPayload(data.CourseID, data.Locale, courseURL, target.path, payload, fetchedAt))
			seenQueryKey[target.path] = struct{}{}
		}
	}

	if data.Contents == nil {
		contentsPaths := []string{
			fmt.Sprintf("/client/api/v1/course/%d/contents?lang=%s", data.CourseID, data.Locale),
			fmt.Sprintf("/client/api/v1/course/%d/contents", data.CourseID),
		}
		for _, path := range contentsPaths {
			payload, status, err := s.fetchCourseAPI(ctx, path)
			if err != nil || status != http.StatusOK || payload == nil {
				continue
			}
			data.Contents = payload
			if _, exists := seenQueryKey[path]; !exists {
				data.SnapshotRows = append(data.SnapshotRows, snapshotRowFromPayload(data.CourseID, data.Locale, courseURL, path, payload, fetchedAt))
				seenQueryKey[path] = struct{}{}
			}
			break
		}
	}

	return data, nil
}

func buildCourseRows(data *CourseFetchedData, fetchedAt time.Time) CourseRows {
	rows := CourseRows{}
	rows.SnapshotRaw = append(rows.SnapshotRaw, data.SnapshotRows...)

	onlineInfo := asMap(data.OnlineInfo)
	metaInfo := asMap(data.MetaInfo)
	curriculum := asMap(data.Curriculum)
	discountsBest := asMap(data.DiscountsBest)
	contents := asMap(data.Contents)

	if onlineInfo != nil && metaInfo != nil {
		d := asMap(onlineInfo["data"])
		m := asMap(metaInfo["data"])
		category := asMap(d["category"])
		mainC := asMap(category["main"])
		subC := asMap(category["sub"])
		unit := asMap(d["unitSummary"])
		review := asMap(d["review"])

		levelCode := ""
		for _, raw := range asSlice(d["levels"]) {
			lv := asMap(raw)
			if asBool(lv["isActive"]) {
				levelCode = firstNonEmpty(asString(lv["code"]), asString(lv["title"]))
				break
			}
		}

		publishedAt, ok := ParseDT64(asString(d["publishedAt"]))
		if !ok {
			publishedAt = EpochKST()
		}
		lastUpdatedAt, ok := ParseDT64(asString(d["lastUpdatedAt"]))
		if !ok {
			lastUpdatedAt = EpochKST()
		}

		rows.CourseDim = append(rows.CourseDim, map[string]any{
			"course_id":                  data.CourseID,
			"locale":                     data.Locale,
			"fetched_at":                 FormatCHTime(fetchedAt),
			"slug":                       asString(d["slug"]),
			"en_slug":                    asString(d["enSlug"]),
			"status":                     asString(d["status"]),
			"title":                      asString(d["title"]),
			"description":                asString(d["description"]),
			"thumbnail_url":              asString(d["thumbnailUrl"]),
			"category_main_title":        asString(mainC["title"]),
			"category_main_slug":         asString(mainC["slug"]),
			"category_sub_title":         asString(subC["title"]),
			"category_sub_slug":          asString(subC["slug"]),
			"level_code":                 levelCode,
			"is_new":                     ToU8(d["isNew"]),
			"is_best":                    ToU8(d["isBest"]),
			"student_count":              asInt(d["studentCount"]),
			"like_count":                 asInt(d["likeCount"]),
			"review_count":               asInt(review["count"]),
			"average_star":               asFloat64(review["averageStar"]),
			"lecture_unit_count":         asInt(unit["lectureUnitCount"]),
			"preview_unit_count":         asInt(unit["previewUnitCount"]),
			"runtime_sec":                asInt(unit["runtime"]),
			"provides_certificate":       ToU8(d["providesCertificate"]),
			"provides_instructor_answer": ToU8(d["providesInstructorAnswer"]),
			"provides_inquiry":           ToU8(d["providesInquiry"]),
			"published_at":               FormatCHTime(publishedAt),
			"last_updated_at":            FormatCHTime(lastUpdatedAt),
			"keywords":                   asString(m["keywords"]),
			"category_slugs":             stringSlice(m["categorySlugs"]),
			"skill_slugs":                stringSlice(m["skillSlugs"]),
			"common_tag_slugs":           stringSlice(m["commonTagsSlugs"]),
		})

		pay := asMap(d["paymentInfo"])
		disc := asMap(discountsBest["data"])
		discount := asMap(pay["discount"])
		discountRate := ClampU8Percent(firstNonZero(pay["discountRate"], disc["discountRate"]))
		discountTitle := firstNonEmpty(asString(discount["title"]), asString(disc["discountTitle"]))
		var discountEndedAt any
		if t, ok := ParseDT64(asString(discount["endedAt"])); ok {
			discountEndedAt = FormatCHTime(t)
		}

		metricSig := MustJSON([]any{
			asInt(d["studentCount"]),
			asInt(d["likeCount"]),
			asInt(review["count"]),
			asFloat64(review["averageStar"]),
			asInt(pay["krwRegularPrice"]),
			asInt(pay["krwPaymentPrice"]),
			discountRate,
			discountTitle,
			discountEndedAt,
		})
		rows.MetricFact = append(rows.MetricFact, map[string]any{
			"fetched_at":        FormatCHTime(fetchedAt),
			"course_id":         data.CourseID,
			"locale":            data.Locale,
			"student_count":     asInt(d["studentCount"]),
			"like_count":        asInt(d["likeCount"]),
			"review_count":      asInt(review["count"]),
			"average_star":      asFloat64(review["averageStar"]),
			"krw_regular_price": asInt(pay["krwRegularPrice"]),
			"krw_pay_price":     asInt(pay["krwPaymentPrice"]),
			"discount_rate":     discountRate,
			"discount_title":    discountTitle,
			"discount_ended_at": discountEndedAt,
			"metric_hash":       H64(metricSig),
		})

		priceSig := MustJSON([]any{
			asFloat64(pay["regularPrice"]),
			asFloat64(pay["payPrice"]),
			discountRate,
			discountTitle,
			discountEndedAt,
			asInt(pay["krwRegularPrice"]),
			asInt(pay["krwPaymentPrice"]),
		})
		rows.PriceFact = append(rows.PriceFact, map[string]any{
			"fetched_at":        FormatCHTime(fetchedAt),
			"course_id":         data.CourseID,
			"locale":            data.Locale,
			"regular_price":     asFloat64(pay["regularPrice"]),
			"pay_price":         asFloat64(pay["payPrice"]),
			"discount_rate":     discountRate,
			"discount_title":    discountTitle,
			"discount_ended_at": discountEndedAt,
			"krw_regular_price": asInt(pay["krwRegularPrice"]),
			"krw_pay_price":     asInt(pay["krwPaymentPrice"]),
			"price_hash":        H64(priceSig),
		})
	}

	cdata := asMap(curriculum["data"])
	for _, secRaw := range asSlice(cdata["curriculum"]) {
		sec := asMap(secRaw)
		sectionID := asInt(sec["id"])
		sectionTitle := asString(sec["title"])
		for _, unitRaw := range asSlice(sec["units"]) {
			unit := asMap(unitRaw)
			unitSig := MustJSON([]any{
				sectionID,
				sectionTitle,
				asInt(unit["id"]),
				asString(unit["title"]),
				asString(unit["type"]),
				asInt(unit["runtime"]),
				ToU8(unit["isPreview"]),
				ToU8(unit["hasVideo"]),
				ToU8(unit["hasAttachment"]),
				nullableSig(unit["quizId"]),
				nullableSig(unit["readingTime"]),
				ToU8(unit["isChallengeOnly"]),
			})
			rows.CurriculumUnit = append(rows.CurriculumUnit, map[string]any{
				"fetched_at":        FormatCHTime(fetchedAt),
				"course_id":         data.CourseID,
				"locale":            data.Locale,
				"section_id":        sectionID,
				"section_title":     sectionTitle,
				"unit_id":           asInt(unit["id"]),
				"unit_title":        asString(unit["title"]),
				"unit_type":         asString(unit["type"]),
				"runtime_sec":       asInt(unit["runtime"]),
				"is_preview":        ToU8(unit["isPreview"]),
				"has_video":         ToU8(unit["hasVideo"]),
				"has_attachment":    ToU8(unit["hasAttachment"]),
				"quiz_id":           nullableInt(unit["quizId"]),
				"reading_time":      nullableInt(unit["readingTime"]),
				"is_challenge_only": ToU8(unit["isChallengeOnly"]),
				"unit_hash":         H64(unitSig),
			})
		}
	}

	contentData := asMap(contents["data"])
	for _, instRaw := range asSlice(contentData["mainInstructors"]) {
		inst := asMap(instRaw)
		instructorID := asInt(inst["id"])
		if instructorID <= 0 {
			continue
		}
		rows.InstructorDim = append(rows.InstructorDim, map[string]any{
			"instructor_id":  instructorID,
			"fetched_at":     FormatCHTime(fetchedAt),
			"name":           asString(inst["name"]),
			"slug":           asString(inst["slug"]),
			"thumbnail_url":  asString(inst["thumbnail"]),
			"course_count":   asInt(inst["courseCount"]),
			"student_count":  asInt(inst["studentCount"]),
			"review_count":   asInt(inst["reviewCount"]),
			"total_star":     asInt(inst["totalStar"]),
			"answer_count":   asInt(inst["answerCount"]),
			"introduce_html": asString(inst["introduce"]),
		})
		rows.CourseInstructor = append(rows.CourseInstructor, map[string]any{
			"fetched_at":    FormatCHTime(fetchedAt),
			"course_id":     data.CourseID,
			"instructor_id": instructorID,
			"role":          "main",
		})
	}

	return rows
}

func (s *Service) fetchSitemapURLs(ctx context.Context, sitemapIndex int) ([]string, bool, bool, error) {
	bases := []string{s.Cfg.SitemapBase, s.Cfg.SitemapBaseFallback}
	status404Count := 0
	saw403 := false
	var lastErr error
	attempts := make([]string, 0, len(bases))

	for _, base := range bases {
		if strings.TrimSpace(base) == "" {
			continue
		}
		rawURL := fmt.Sprintf("%s/%s%d.xml", base, s.Cfg.SitemapPrefix, sitemapIndex)
		body, status, err := s.httpGetWithRetry(ctx, rawURL, "application/xml,text/xml;q=0.9,*/*;q=0.8")
		if err != nil {
			lastErr = err
			attempts = append(attempts, fmt.Sprintf("%s -> error", rawURL))
			continue
		}
		switch status {
		case 404:
			status404Count++
			attempts = append(attempts, fmt.Sprintf("%s -> 404", rawURL))
			continue
		case 403:
			saw403 = true
			attempts = append(attempts, fmt.Sprintf("%s -> 403", rawURL))
			time.Sleep(3 * time.Second)
			continue
		case 200:
			if isAccessDeniedSitemap(body) {
				saw403 = true
				attempts = append(attempts, fmt.Sprintf("%s -> AccessDenied", rawURL))
				continue
			}
			urls, err := parseSitemapLocs(body)
			if err != nil {
				lastErr = err
				attempts = append(attempts, fmt.Sprintf("%s -> invalid xml", rawURL))
				continue
			}
			if len(urls) > 0 {
				return urls, false, false, nil
			}
			lastErr = fmt.Errorf("empty sitemap response")
			attempts = append(attempts, fmt.Sprintf("%s -> empty", rawURL))
		default:
			lastErr = fmt.Errorf("http %d", status)
			attempts = append(attempts, fmt.Sprintf("%s -> %d", rawURL, status))
		}
	}

	if status404Count == len(bases) {
		return nil, true, false, nil
	}
	if saw403 {
		fmt.Printf("[warn] sitemap temporarily unavailable sitemap_index=%d attempts=%s\n", sitemapIndex, strings.Join(attempts, "; "))
		return nil, false, true, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("failed to fetch sitemap %d", sitemapIndex)
	}
	return nil, false, false, lastErr
}

func (s *Service) flushCollectBatch(ctx context.Context, batch *CourseRows, cp Checkpoint) error {
	if err := batch.InsertAll(ctx, s); err != nil {
		return err
	}
	if err := s.setCheckpoint(ctx, collectCheckpointSource, cp); err != nil {
		return err
	}
	batch.Reset()
	return nil
}

func (s *Service) RunCollectNew(ctx context.Context) error {
	cp, err := s.getCheckpoint(ctx, collectCheckpointSource)
	if err != nil {
		return err
	}
	fmt.Printf("[checkpoint] start sitemap_index=%d url_index=%d\n", cp.SitemapIndex, cp.URLIndex)

	current := cp
	next := cp
	scanned := 0
	processed := 0
	batch := &CourseRows{}
	existsCache := map[string]bool{}
	flushEvery := s.Cfg.CheckpointFlushEvery
	resetAfter := false
	stop := false

	for !stop {
		urls, reachedEnd, temporarilyUnavailable, err := s.fetchSitemapURLs(ctx, current.SitemapIndex)
		if err != nil {
			return err
		}
		if reachedEnd {
			resetAfter = true
			break
		}
		if temporarilyUnavailable {
			next = current
			break
		}

		for i := current.URLIndex; i < len(urls); i++ {
			if scanned >= s.Cfg.MaxURLsPerRun {
				next = Checkpoint{SitemapIndex: current.SitemapIndex, URLIndex: i}
				fmt.Printf("[batch] scan limit hit scanned=%d max_urls=%d\n", scanned, s.Cfg.MaxURLsPerRun)
				stop = true
				break
			}

			rawURL := urls[i]
			next = Checkpoint{SitemapIndex: current.SitemapIndex, URLIndex: i + 1}
			scanned++

			courseIDHint, locale := ParseCourseIDAndLocale(rawURL)
			if courseIDHint > 0 {
				cacheKey := fmt.Sprintf("%d|%s", courseIDHint, locale)
				exists, ok := existsCache[cacheKey]
				if !ok {
					exists, err = s.courseExists(ctx, courseIDHint, locale)
					if err != nil {
						return err
					}
					existsCache[cacheKey] = exists
				}
				if exists {
					if flushEvery > 0 && scanned%flushEvery == 0 {
						if err := s.flushCollectBatch(ctx, batch, next); err != nil {
							return err
						}
						fmt.Printf("[checkpoint] flushed sitemap_index=%d url_index=%d scanned=%d processed=%d\n", next.SitemapIndex, next.URLIndex, scanned, processed)
					}
					continue
				}
			}

			fetchedAt := NowDT64()
			data, err := s.fetchCourseData(ctx, rawURL, fetchedAt)
			if err != nil {
				fmt.Printf("[warn] fetch failed url=%s error=%v\n", rawURL, err)
				if flushEvery > 0 && scanned%flushEvery == 0 {
					if err := s.flushCollectBatch(ctx, batch, next); err != nil {
						return err
					}
					fmt.Printf("[checkpoint] flushed sitemap_index=%d url_index=%d scanned=%d processed=%d\n", next.SitemapIndex, next.URLIndex, scanned, processed)
				}
				continue
			}

			cacheKey := fmt.Sprintf("%d|%s", data.CourseID, data.Locale)
			exists, ok := existsCache[cacheKey]
			if !ok {
				exists, err = s.courseExists(ctx, data.CourseID, data.Locale)
				if err != nil {
					return err
				}
				existsCache[cacheKey] = exists
			}
			if exists {
				if flushEvery > 0 && scanned%flushEvery == 0 {
					if err := s.flushCollectBatch(ctx, batch, next); err != nil {
						return err
					}
					fmt.Printf("[checkpoint] flushed sitemap_index=%d url_index=%d scanned=%d processed=%d\n", next.SitemapIndex, next.URLIndex, scanned, processed)
				}
				continue
			}

			batch.Append(buildCourseRows(data, fetchedAt))
			processed++
			existsCache[cacheKey] = true

			if processed%20 == 0 {
				fmt.Printf("[progress] scanned=%d processed=%d last_course_id=%d sitemap=%d offset=%d\n", scanned, processed, data.CourseID, current.SitemapIndex, i+1)
			}

			if flushEvery > 0 && scanned%flushEvery == 0 {
				if err := s.flushCollectBatch(ctx, batch, next); err != nil {
					return err
				}
				fmt.Printf("[checkpoint] flushed sitemap_index=%d url_index=%d scanned=%d processed=%d\n", next.SitemapIndex, next.URLIndex, scanned, processed)
			}

			if processed >= s.Cfg.BatchSize {
				fmt.Printf("[batch] new-course limit hit processed=%d batch_size=%d\n", processed, s.Cfg.BatchSize)
				stop = true
				break
			}

			JitterSleep(s.Cfg.RequestSleepMin, s.Cfg.RequestSleepMax)
		}

		if stop || resetAfter {
			break
		}
		current = Checkpoint{SitemapIndex: current.SitemapIndex + 1, URLIndex: 0}
		next = current
		JitterSleep(s.Cfg.RequestSleepMin, s.Cfg.RequestSleepMax)
	}

	finalCP := next
	if resetAfter {
		finalCP = Checkpoint{}
	}
	if err := s.flushCollectBatch(ctx, batch, finalCP); err != nil {
		return err
	}
	if resetAfter {
		fmt.Println("[checkpoint] reset to sitemap_index=0 url_index=0")
	} else {
		fmt.Printf("[checkpoint] saved sitemap_index=%d url_index=%d\n", finalCP.SitemapIndex, finalCP.URLIndex)
	}
	fmt.Printf("[done] scanned=%d processed=%d\n", scanned, processed)
	return nil
}

func (s *Service) pickUpdateURLs(ctx context.Context, limit int) ([]updatePick, error) {
	sql := fmt.Sprintf(`
        WITH latest AS (
          SELECT
            course_id,
            locale,
            argMax(source_url, fetched_at) AS source_url,
            max(fetched_at) AS last_fetched_at
          FROM %s.inflearn_course_snapshot_raw
          WHERE status_code = 'OK'
          GROUP BY course_id, locale
        )
        SELECT
          course_id,
          locale,
          source_url,
          formatDateTime(last_fetched_at, '%%Y-%%m-%%d %%H:%%i:%%s') AS last_fetched_at
        FROM latest
        ORDER BY last_fetched_at ASC
        LIMIT %d
    `, s.Cfg.CHDatabase, limit)
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	out := make([]updatePick, 0, len(rows))
	for _, row := range rows {
		rawURL := asString(row["source_url"])
		if strings.TrimSpace(rawURL) == "" {
			continue
		}
		out = append(out, updatePick{
			CourseID:  asInt(row["course_id"]),
			Locale:    asString(row["locale"]),
			SourceURL: rawURL,
		})
	}
	return out, nil
}

func (s *Service) getUpdateProgress(ctx context.Context) (int, int, error) {
	cp, err := s.getCheckpoint(ctx, updateCheckpointSource)
	if err != nil {
		return 0, 0, err
	}
	return cp.SitemapIndex, cp.URLIndex, nil
}

func (s *Service) setUpdateProgress(ctx context.Context, totalDone, lastBatchDone int) error {
	return s.setCheckpoint(ctx, updateCheckpointSource, Checkpoint{SitemapIndex: totalDone, URLIndex: lastBatchDone})
}

func (s *Service) RunUpdateExisting(ctx context.Context) error {
	totalDone, _, err := s.getUpdateProgress(ctx)
	if err != nil {
		return err
	}

	picks, err := s.pickUpdateURLs(ctx, s.Cfg.UpdateBatchSize)
	if err != nil {
		return err
	}
	fmt.Printf("[update] picked=%d (UPDATE_BATCH_SIZE=%d)\n", len(picks), s.Cfg.UpdateBatchSize)
	if len(picks) == 0 {
		if err := s.setUpdateProgress(ctx, totalDone, 0); err != nil {
			return err
		}
		fmt.Println("[update] nothing to do")
		return nil
	}

	jobs := make(chan updatePick)
	results := make(chan updateResult)
	workers := s.Cfg.Workers
	if workers < 1 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pick := range jobs {
				fetchedAt := NowDT64()
				data, err := s.fetchCourseData(ctx, pick.SourceURL, fetchedAt)
				if err != nil {
					results <- updateResult{URL: pick.SourceURL, Err: err}
					continue
				}
				results <- updateResult{URL: pick.SourceURL, Rows: buildCourseRows(data, fetchedAt)}
			}
		}()
	}

	go func() {
		for _, pick := range picks {
			jobs <- pick
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	batch := CourseRows{}
	failed := 0
	for result := range results {
		if result.Err != nil {
			failed++
			fmt.Printf("[warn] process_course_url failed url=%s error=%v\n", result.URL, result.Err)
			continue
		}
		batch.Append(result.Rows)
	}

	if err := batch.InsertAll(ctx, s); err != nil {
		return err
	}
	totalDone += len(picks)
	if err := s.setUpdateProgress(ctx, totalDone, len(picks)); err != nil {
		return err
	}

	if failed > 0 {
		fmt.Printf("[warn] failed_urls=%d\n", failed)
	}
	fmt.Printf("[done] processed=%d failed=%d\n", len(picks)-failed, failed)
	return nil
}

func recentPartitions(nMonths int) []int {
	if nMonths < 1 {
		nMonths = 1
	}
	now := time.Now().In(KST)
	year, month, _ := now.Date()
	parts := make([]int, 0, nMonths)
	for i := 0; i < nMonths; i++ {
		parts = append(parts, year*100+int(month))
		month--
		if month == 0 {
			month = 12
			year--
		}
	}
	return parts
}

func (s *Service) RunOptimize(ctx context.Context) error {
	partitioned := []string{
		"inflearn_course_snapshot_raw",
		"inflearn_course_dim",
		"inflearn_course_metric_fact",
		"inflearn_course_price_fact",
		"inflearn_course_curriculum_unit",
		"inflearn_instructor_dim",
		"inflearn_course_instructor_map",
	}
	full := []string{
		"inflearn_crawl_checkpoint",
	}
	parts := recentPartitions(s.Cfg.OptimizeMonths)
	for _, table := range partitioned {
		for _, part := range parts {
			sql := fmt.Sprintf("OPTIMIZE TABLE %s.%s PARTITION %d FINAL", s.Cfg.CHDatabase, table, part)
			if err := s.CHExec(ctx, sql); err != nil {
				continue
			}
		}
	}
	for _, table := range full {
		sql := fmt.Sprintf("OPTIMIZE TABLE %s.%s FINAL", s.Cfg.CHDatabase, table)
		if err := s.CHExec(ctx, sql); err != nil {
			continue
		}
	}
	return nil
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}

func fmtInt(v int64) string {
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	s := strconv.FormatInt(v, 10)
	if len(s) <= 3 {
		return sign + s
	}
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	parts = append([]string{s}, parts...)
	return sign + strings.Join(parts, ",")
}

func escapeXML(s string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return replacer.Replace(s)
}

func latestSnapshotAt(ctx context.Context, s *Service) (time.Time, bool, error) {
	sql := fmt.Sprintf(`
        SELECT ifNull(formatDateTime(max(fetched_at), '%%Y-%%m-%%d %%H:%%i:%%s'), '') AS mx
        FROM %s.inflearn_course_snapshot_raw
    `, s.Cfg.CHDatabase)
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return time.Time{}, false, err
	}
	if len(rows) == 0 || strings.TrimSpace(asString(rows[0]["mx"])) == "" {
		return time.Time{}, false, nil
	}
	t, ok := ParseDT64(asString(rows[0]["mx"]))
	return t, ok, nil
}

func (s *Service) queryBucketCounts(ctx context.Context, sql string) ([]bucketCount, error) {
	rows, err := s.CHQueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	out := make([]bucketCount, 0, len(rows))
	for _, row := range rows {
		out = append(out, bucketCount{
			Bucket: asString(row["bucket"]),
			Count:  asInt64(row["cnt"]),
		})
	}
	return out, nil
}

func sumCounts(rows []bucketCount) int64 {
	var total int64
	for _, row := range rows {
		total += row.Count
	}
	return total
}

func maxCount(rows []bucketCount) int64 {
	var peak int64
	for _, row := range rows {
		if row.Count > peak {
			peak = row.Count
		}
	}
	return peak
}

func writeSVGLineChart(title, yLabel string, rows []bucketCount, outPath string) error {
	if len(rows) == 0 {
		return nil
	}
	const (
		width  = 1000
		height = 420
		left   = 70
		right  = 20
		top    = 50
		bottom = 70
	)
	plotW := float64(width - left - right)
	plotH := float64(height - top - bottom)
	maxY := maxCount(rows)
	if maxY <= 0 {
		maxY = 1
	}

	type point struct{ X, Y float64 }
	points := make([]point, 0, len(rows))
	for i, row := range rows {
		var x float64
		if len(rows) == 1 {
			x = float64(left) + plotW/2
		} else {
			x = float64(left) + (float64(i)/float64(len(rows)-1))*plotW
		}
		y := float64(top) + plotH - (float64(row.Count)/float64(maxY))*plotH
		points = append(points, point{X: x, Y: y})
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, width, height, width, height))
	b.WriteString(`
  <rect width="100%" height="100%" fill="#ffffff"/>`)
	b.WriteString(fmt.Sprintf(`
  <text x="%d" y="28" font-size="20" font-weight="600">%s</text>`, left, escapeXML(title)))
	b.WriteString(fmt.Sprintf(`
  <text x="%d" y="%d" font-size="12" fill="#555">%s</text>`, left, height-18, escapeXML(yLabel)))

	for i := 0; i <= 5; i++ {
		yValue := float64(i) / 5 * float64(maxY)
		y := float64(top) + plotH - (float64(i)/5)*plotH
		b.WriteString(fmt.Sprintf(`
  <line x1="%d" y1="%.2f" x2="%d" y2="%.2f" stroke="#e5e7eb" stroke-width="1"/>`, left, y, width-right, y))
		b.WriteString(fmt.Sprintf(`
  <text x="%d" y="%.2f" font-size="11" fill="#6b7280" text-anchor="end">%s</text>`, left-8, y+4, escapeXML(fmtInt(int64(yValue)))))
	}

	b.WriteString(fmt.Sprintf(`
  <line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#111827" stroke-width="1.2"/>`, left, height-bottom, width-right, height-bottom))
	b.WriteString(fmt.Sprintf(`
  <line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#111827" stroke-width="1.2"/>`, left, top, left, height-bottom))

	step := 1
	if len(rows) > 24 {
		step = len(rows) / 12
		if step < 1 {
			step = 1
		}
	}
	tickSet := map[int]struct{}{}
	for i := 0; i < len(rows); i += step {
		tickSet[i] = struct{}{}
	}
	tickSet[len(rows)-1] = struct{}{}
	indexes := make([]int, 0, len(tickSet))
	for idx := range tickSet {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	for _, idx := range indexes {
		p := points[idx]
		b.WriteString(fmt.Sprintf(`
  <line x1="%.2f" y1="%d" x2="%.2f" y2="%d" stroke="#9ca3af" stroke-width="1"/>`, p.X, height-bottom, p.X, height-bottom+6))
		b.WriteString(fmt.Sprintf(`
  <text x="%.2f" y="%d" font-size="10" fill="#6b7280" text-anchor="middle">%s</text>`, p.X, height-bottom+22, escapeXML(rows[idx].Bucket)))
	}

	pts := make([]string, 0, len(points))
	for _, p := range points {
		pts = append(pts, fmt.Sprintf("%.2f,%.2f", p.X, p.Y))
	}
	b.WriteString(fmt.Sprintf(`
  <polyline fill="none" stroke="#2563eb" stroke-width="2.5" points="%s"/>`, strings.Join(pts, " ")))
	for _, p := range points {
		b.WriteString(fmt.Sprintf(`
  <circle cx="%.2f" cy="%.2f" r="2.5" fill="#2563eb"/>`, p.X, p.Y))
	}
	b.WriteString(`
</svg>
`)
	return os.WriteFile(outPath, []byte(b.String()), 0o644)
}

func (s *Service) RunStats(ctx context.Context) error {
	reportDir := filepath.Join("reports", "inflearn")
	chartsDir := filepath.Join(reportDir, "charts")
	if err := ensureDir(chartsDir); err != nil {
		return err
	}

	latestAt, ok, err := latestSnapshotAt(ctx, s)
	if err != nil {
		return err
	}
	since := NowDT64().AddDate(0, 0, -s.Cfg.StatsLookbackDays)
	if ok {
		since = latestAt.AddDate(0, 0, -s.Cfg.StatsLookbackDays)
	}
	sinceStr := FormatCHTime(since)

	hourly, err := s.queryBucketCounts(ctx, fmt.Sprintf(`
        SELECT formatDateTime(toStartOfHour(fetched_at), '%%Y-%%m-%%d %%H') AS bucket, count() AS cnt
        FROM %s.inflearn_course_snapshot_raw
        WHERE fetched_at >= toDateTime64(%s, 3, 'Asia/Seoul')
        GROUP BY bucket
        ORDER BY bucket
    `, s.Cfg.CHDatabase, QuoteSQLString(sinceStr)))
	if err != nil {
		return err
	}
	daily, err := s.queryBucketCounts(ctx, fmt.Sprintf(`
        SELECT formatDateTime(toDate(fetched_at), '%%Y-%%m-%%d') AS bucket, count() AS cnt
        FROM %s.inflearn_course_snapshot_raw
        WHERE fetched_at >= toDateTime64(%s, 3, 'Asia/Seoul')
        GROUP BY bucket
        ORDER BY bucket
    `, s.Cfg.CHDatabase, QuoteSQLString(sinceStr)))
	if err != nil {
		return err
	}
	monthly, err := s.queryBucketCounts(ctx, fmt.Sprintf(`
        SELECT formatDateTime(toStartOfMonth(fetched_at), '%%Y-%%m') AS bucket, count() AS cnt
        FROM %s.inflearn_course_snapshot_raw
        WHERE fetched_at >= toDateTime64(%s, 3, 'Asia/Seoul')
        GROUP BY bucket
        ORDER BY bucket
    `, s.Cfg.CHDatabase, QuoteSQLString(sinceStr)))
	if err != nil {
		return err
	}
	yearly, err := s.queryBucketCounts(ctx, fmt.Sprintf(`
        SELECT toString(toYear(fetched_at)) AS bucket, count() AS cnt
        FROM %s.inflearn_course_snapshot_raw
        WHERE fetched_at >= toDateTime64(%s, 3, 'Asia/Seoul')
        GROUP BY bucket
        ORDER BY bucket
    `, s.Cfg.CHDatabase, QuoteSQLString(sinceStr)))
	if err != nil {
		return err
	}

	pubDaily, err := s.queryBucketCounts(ctx, fmt.Sprintf(`
        SELECT formatDateTime(toDate(published_at), '%%Y-%%m-%%d') AS bucket, count() AS cnt
        FROM %s.inflearn_course_dim
        WHERE published_at >= toDateTime64(%s, 3, 'Asia/Seoul')
        GROUP BY bucket
        ORDER BY bucket
    `, s.Cfg.CHDatabase, QuoteSQLString(sinceStr)))
	if err != nil {
		return err
	}
	pubMonthly, err := s.queryBucketCounts(ctx, fmt.Sprintf(`
        SELECT formatDateTime(toStartOfMonth(published_at), '%%Y-%%m') AS bucket, count() AS cnt
        FROM %s.inflearn_course_dim
        WHERE published_at >= toDateTime64(%s, 3, 'Asia/Seoul')
        GROUP BY bucket
        ORDER BY bucket
    `, s.Cfg.CHDatabase, QuoteSQLString(sinceStr)))
	if err != nil {
		return err
	}
	pubYearly, err := s.queryBucketCounts(ctx, fmt.Sprintf(`
        SELECT toString(toYear(published_at)) AS bucket, count() AS cnt
        FROM %s.inflearn_course_dim
        WHERE published_at >= toDateTime64(%s, 3, 'Asia/Seoul')
        GROUP BY bucket
        ORDER BY bucket
    `, s.Cfg.CHDatabase, QuoteSQLString(sinceStr)))
	if err != nil {
		return err
	}

	charts := []struct {
		Title  string
		YLabel string
		Rows   []bucketCount
		File   string
	}{
		{"수집 시점 기준 - 시간별 스냅샷", "스냅샷 수", hourly, filepath.Join(chartsDir, "snapshots_hourly.svg")},
		{"수집 시점 기준 - 일별 스냅샷", "스냅샷 수", daily, filepath.Join(chartsDir, "snapshots_daily.svg")},
		{"수집 시점 기준 - 월별 스냅샷", "스냅샷 수", monthly, filepath.Join(chartsDir, "snapshots_monthly.svg")},
		{"수집 시점 기준 - 연별 스냅샷", "스냅샷 수", yearly, filepath.Join(chartsDir, "snapshots_yearly.svg")},
		{"개설 시점 기준 - 일별 신규 강의", "강의 수", pubDaily, filepath.Join(chartsDir, "published_daily.svg")},
		{"개설 시점 기준 - 월별 신규 강의", "강의 수", pubMonthly, filepath.Join(chartsDir, "published_monthly.svg")},
		{"개설 시점 기준 - 연별 신규 강의", "강의 수", pubYearly, filepath.Join(chartsDir, "published_yearly.svg")},
	}
	for _, chart := range charts {
		if err := writeSVGLineChart(chart.Title, chart.YLabel, chart.Rows, chart.File); err != nil {
			return err
		}
	}

	totalRows, err := s.CHQueryRows(ctx, fmt.Sprintf(`SELECT uniqExact(course_id) AS c FROM %s.inflearn_course_dim`, s.Cfg.CHDatabase))
	if err != nil {
		return err
	}
	totalCourses := int64(0)
	if len(totalRows) > 0 {
		totalCourses = asInt64(totalRows[0]["c"])
	}

	lines := []string{
		"# Inflearn 통계 리포트",
		"",
		fmt.Sprintf("- 생성 시각(KST): **%s**", NowDT64().Format("2006-01-02 15:04:05")),
		fmt.Sprintf("- 최근 데이터 기준 Lookback: **%d일**", s.Cfg.StatsLookbackDays),
		"",
	}

	addSection := func(title string, rows []bucketCount, imageName string) {
		total := sumCounts(rows)
		last := int64(0)
		if len(rows) > 0 {
			last = rows[len(rows)-1].Count
		}
		peak := maxCount(rows)
		lines = append(lines,
			fmt.Sprintf("## %s", title),
			"",
			fmt.Sprintf("- 합계: **%s**, 마지막: **%s**, 피크: **%s**", fmtInt(total), fmtInt(last), fmtInt(peak)),
			"",
		)
		if len(rows) > 0 {
			lines = append(lines, fmt.Sprintf("![%s](charts/%s)", title, imageName), "")
		}
	}
	addPublished := func(title string, rows []bucketCount, imageName string) {
		total := sumCounts(rows)
		lines = append(lines,
			fmt.Sprintf("## %s", title),
			"",
			fmt.Sprintf("- 합계: **%s**", fmtInt(total)),
			"",
		)
		if len(rows) > 0 {
			lines = append(lines, fmt.Sprintf("![%s](charts/%s)", title, imageName), "")
		}
	}

	addSection("수집 시점 기준 - 시간별 스냅샷", hourly, "snapshots_hourly.svg")
	addSection("수집 시점 기준 - 일별 스냅샷", daily, "snapshots_daily.svg")
	addSection("수집 시점 기준 - 월별 스냅샷", monthly, "snapshots_monthly.svg")
	addSection("수집 시점 기준 - 연별 스냅샷", yearly, "snapshots_yearly.svg")
	addPublished("개설 시점 기준 - 일별 신규 강의", pubDaily, "published_daily.svg")
	addPublished("개설 시점 기준 - 월별 신규 강의", pubMonthly, "published_monthly.svg")
	addPublished("개설 시점 기준 - 연별 신규 강의", pubYearly, "published_yearly.svg")

	lines = append(lines,
		"## 요약",
		"",
		fmt.Sprintf("- 누적 강의 수(고유 course_id): **%s**", fmtInt(totalCourses)),
		"",
	)

	reportPath := filepath.Join(reportDir, "inflearn_stats_latest.md")
	return os.WriteFile(reportPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}
