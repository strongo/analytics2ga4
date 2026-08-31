package analytics2ga4

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strongo/analytics"
)

// NoOpLogger satisfies Logger in tests without output.
var _ Logger = (*NoOpLogger)(nil)

type NoOpLogger struct{}

func (n NoOpLogger) Warningf(_ context.Context, _ string, _ ...any) {}
func (n NoOpLogger) Errorf(_ context.Context, _ string, _ ...any)   {}

// captureLogger records error calls for assertion. Guarded by mu since the
// periodic flusher goroutine can call Errorf concurrently with the test
// goroutine reading errors.
type captureLogger struct {
	mu     sync.Mutex
	errors []string
}

func (c *captureLogger) Warningf(_ context.Context, _ string, _ ...any) {}
func (c *captureLogger) Errorf(_ context.Context, f string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = append(c.errors, f)
}

func (c *captureLogger) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errors)
}

// --- helpers ---

func userCtx(id string) analytics.UserContext {
	return analytics.NewUserContext(id)
}

// newTestServer returns a test GA4 collect endpoint that records every
// decoded payload it receives, plus the mutex-guarded slice that receives
// them. Requests may arrive concurrently (e.g. from concurrent Flush calls),
// so all access to the returned slice -- from the handler and from the test
// -- must go through the returned mutex.
func newTestServer(t *testing.T, payloads *[]ga4Payload) (*httptest.Server, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var p ga4Payload
		if err = json.Unmarshal(body, &p); err != nil {
			t.Errorf("unmarshal body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		*payloads = append(*payloads, p)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	return srv, &mu
}

// snapshot returns a copy of *payloads taken under mu, safe to inspect while
// concurrent requests may still be landing.
func snapshot(mu *sync.Mutex, payloads *[]ga4Payload) []ga4Payload {
	mu.Lock()
	defer mu.Unlock()
	out := make([]ga4Payload, len(*payloads))
	copy(out, *payloads)
	return out
}

// --- NewSender ---

func TestNewSender_NoClients(t *testing.T) {
	_, err := NewSender(NoOpLogger{})
	if err == nil {
		t.Fatal("expected error with no clients")
	}
}

func TestNewSender_EmptyMeasurementID(t *testing.T) {
	_, err := NewSender(NoOpLogger{}, Client{APISecret: "secret"})
	if err == nil {
		t.Fatal("expected error for empty MeasurementID")
	}
}

func TestNewSender_EmptyAPISecret(t *testing.T) {
	_, err := NewSender(NoOpLogger{}, Client{MeasurementID: "G-TEST"})
	if err == nil {
		t.Fatal("expected error for empty APISecret")
	}
}

func TestNewSender_Valid(t *testing.T) {
	s, err := NewSender(NoOpLogger{}, Client{MeasurementID: "G-TEST", APISecret: "secret"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("sender is nil")
	}
	defer func() { _ = s.Close(context.Background()) }()
}

// --- session envelope always present ---

func TestSessionEnvelopeAlwaysPresent(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	msg := analytics.NewEvent("click", "ui", "button")
	msg.SetUserContext(userCtx("user-1"))
	s.QueueMessage(context.Background(), msg)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(got))
	}
	ev := got[0].Events[0]
	if ev.Params["session_id"] == "" {
		t.Error("session_id must not be empty")
	}
	if ev.Params["engagement_time_msec"] == "" {
		t.Error("engagement_time_msec must not be empty")
	}
}

// --- message kinds ---

func TestPageviewMessage(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	msg := analytics.NewPageview("example.com", "/home")
	msg.SetTitle("Home page")
	msg.SetUserContext(userCtx("user-2"))
	s.QueueMessage(context.Background(), msg)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(got))
	}
	ev := got[0].Events[0]
	if ev.Name != "page_view" {
		t.Errorf("expected page_view, got %s", ev.Name)
	}
	if !strings.Contains(ev.Params["page_location"], "/home") {
		t.Errorf("page_location missing path: %s", ev.Params["page_location"])
	}
	if ev.Params["page_title"] != "Home page" {
		t.Errorf("page_title = %q", ev.Params["page_title"])
	}
}

func TestErrorMessage(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	dbErr := errorString("database unavailable")
	msg := analytics.NewErrorMessage(dbErr)
	msg.SetUserContext(userCtx("user-3"))
	s.QueueMessage(context.Background(), msg)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(got))
	}
	ev := got[0].Events[0]
	if ev.Name != "app_exception" {
		t.Errorf("expected app_exception, got %s", ev.Name)
	}
	if ev.Params["description"] != "database unavailable" {
		t.Errorf("description = %q", ev.Params["description"])
	}
}

func TestEventMessage(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	msg := analytics.NewEvent("purchase", "ecommerce", "buy_now")
	msg.SetLabel("promo-2024")
	msg.SetValue(99)
	msg.SetUserContext(userCtx("user-4"))
	s.QueueMessage(context.Background(), msg)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(got))
	}
	ev := got[0].Events[0]
	if ev.Name != "purchase" {
		t.Errorf("expected purchase, got %s", ev.Name)
	}
	if ev.Params["action"] != "buy_now" {
		t.Errorf("action = %q", ev.Params["action"])
	}
	if ev.Params["label"] != "promo-2024" {
		t.Errorf("label = %q", ev.Params["label"])
	}
}

// --- name sanitization ---

func TestSanitizeEventName(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"with spaces", "with_spaces"},
		{"with-dashes", "with_dashes"},
		{"123starts_with_digit", "e_123starts_with_digit"},
		{strings.Repeat("a", 50), strings.Repeat("a", 40)},
		{"$special!@chars", "special_chars"},
	}
	for _, tc := range cases {
		got := sanitizeEventName(tc.input)
		if got != tc.want {
			t.Errorf("sanitizeEventName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// --- multi-client fan-out ---

func TestMultiClientFanOut(t *testing.T) {
	var payloads1, payloads2 []ga4Payload

	srv1, mu1 := newTestServer(t, &payloads1)
	defer srv1.Close()
	srv2, mu2 := newTestServer(t, &payloads2)
	defer srv2.Close()

	s, _ := NewSender(NoOpLogger{},
		Client{MeasurementID: "G-ONE", APISecret: "s1", Endpoint: srv1.URL, HTTPClient: srv1.Client()},
		Client{MeasurementID: "G-TWO", APISecret: "s2", Endpoint: srv2.URL, HTTPClient: srv2.Client()},
	)
	defer func() { _ = s.Close(context.Background()) }()

	msg := analytics.NewEvent("test", "category", "action")
	msg.SetUserContext(userCtx("user-5"))
	s.QueueMessage(context.Background(), msg)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := snapshot(mu1, &payloads1); len(got) != 1 {
		t.Errorf("client 1: expected 1 payload, got %d", len(got))
	}
	if got := snapshot(mu2, &payloads2); len(got) != 1 {
		t.Errorf("client 2: expected 1 payload, got %d", len(got))
	}
}

// --- surface param ---

func TestSurfaceParamDefault(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	msg := analytics.NewEvent("nav", "ui", "click")
	msg.SetUserContext(userCtx("user-6"))
	s.QueueMessage(context.Background(), msg)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) == 0 {
		t.Fatal("no payload received")
	}
	if got[0].Events[0].Params["surface"] != "telegram_bot" {
		t.Errorf("surface = %q, want telegram_bot", got[0].Events[0].Params["surface"])
	}
}

// --- error logging on non-2xx ---

func TestErrorLoggingOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cl := &captureLogger{}
	s, _ := NewSender(cl, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	msg := analytics.NewEvent("test", "cat", "act")
	msg.SetUserContext(userCtx("user-7"))
	s.QueueMessage(context.Background(), msg)
	if err := s.Flush(context.Background()); err == nil {
		t.Error("expected Flush to return an error for a non-2xx response")
	}

	if cl.count() == 0 {
		t.Error("expected at least one error to be logged for non-2xx response")
	}
}

// --- timing message ---

func TestTimingMessage(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	msg := analytics.NewTiming("db_query", 250*time.Millisecond)
	msg.SetUserContext(userCtx("user-8"))
	s.QueueMessage(context.Background(), msg)
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(got))
	}
	ev := got[0].Events[0]
	if ev.Params["duration_ms"] != "250" {
		t.Errorf("duration_ms = %q, want 250", ev.Params["duration_ms"])
	}
}

// --- batching ---

// TestBatchAccumulatesUntilFlush verifies that QueueMessage buffers events
// instead of sending one HTTP request per call: a partial batch produces no
// request until Flush is called.
func TestBatchAccumulatesUntilFlush(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	for i := 0; i < maxEventsPerBatch-1; i++ {
		msg := analytics.NewEvent(fmt.Sprintf("evt-%d", i), "cat", "act")
		msg.SetUserContext(userCtx("same-user"))
		s.QueueMessage(context.Background(), msg)
	}

	if got := snapshot(mu, &payloads); len(got) != 0 {
		t.Fatalf("expected no request before the batch fills or Flush is called, got %d", len(got))
	}

	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 request after Flush, got %d", len(got))
	}
	if len(got[0].Events) != maxEventsPerBatch-1 {
		t.Fatalf("expected %d events in the flushed request, got %d", maxEventsPerBatch-1, len(got[0].Events))
	}
}

// TestBatchFlushesExactlyOnceWhenFull verifies that a batch that reaches
// maxEventsPerBatch is sent as a single request containing every event, and
// that no further request happens until more events accumulate.
func TestBatchFlushesExactlyOnceWhenFull(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	for i := 0; i < maxEventsPerBatch; i++ {
		msg := analytics.NewEvent(fmt.Sprintf("evt-%d", i), "cat", "act")
		msg.SetUserContext(userCtx("same-user"))
		s.QueueMessage(context.Background(), msg)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 request once the batch fills, got %d", len(got))
	}
	if len(got[0].Events) != maxEventsPerBatch {
		t.Fatalf("expected %d events in the request, got %d", maxEventsPerBatch, len(got[0].Events))
	}

	// A no-op Flush must not resend anything: the full batch already shipped.
	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := snapshot(mu, &payloads); len(got) != 1 {
		t.Fatalf("Flush after a full-batch send must not add a request, got %d total", len(got))
	}
}

// TestFlushSeparatesEventsByClientID verifies that events for different end
// users are never merged into a single GA4 request, since a request only
// carries one client_id for all of its events.
func TestFlushSeparatesEventsByClientID(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})
	defer func() { _ = s.Close(context.Background()) }()

	msgA := analytics.NewEvent("a", "cat", "act")
	msgA.SetUserContext(userCtx("user-a"))
	s.QueueMessage(context.Background(), msgA)

	msgB := analytics.NewEvent("b", "cat", "act")
	msgB.SetUserContext(userCtx("user-b"))
	s.QueueMessage(context.Background(), msgB)

	if err := s.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 2 {
		t.Fatalf("expected 2 separate requests (one per client_id), got %d", len(got))
	}
	seen := map[string]bool{}
	for _, p := range got {
		if len(p.Events) != 1 {
			t.Errorf("expected 1 event per client_id request, got %d for client_id %q", len(p.Events), p.ClientID)
		}
		if p.ClientID != "user-a" && p.ClientID != "user-b" {
			t.Errorf("unexpected client_id %q", p.ClientID)
		}
		seen[p.ClientID] = true
	}
	if !seen["user-a"] || !seen["user-b"] {
		t.Errorf("expected requests for both user-a and user-b, got %+v", seen)
	}
}

// TestCloseFlushesPendingEvents verifies Close is the explicit shutdown path
// that guarantees a partial batch is not stranded.
func TestCloseFlushesPendingEvents(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})

	msg := analytics.NewEvent("shutdown", "cat", "act")
	msg.SetUserContext(userCtx("user-close"))
	s.QueueMessage(context.Background(), msg)

	if got := snapshot(mu, &payloads); len(got) != 0 {
		t.Fatalf("expected no request before Close, got %d", len(got))
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := snapshot(mu, &payloads)
	if len(got) != 1 {
		t.Fatalf("expected Close to flush the pending event, got %d requests", len(got))
	}

	// Close must be idempotent: a second call must not resend anything.
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := snapshot(mu, &payloads); len(got) != 1 {
		t.Fatalf("second Close must not add a request, got %d total", len(got))
	}
}

// TestTimeBasedFlushWithoutSizeOrExplicitFlush verifies that a batch which
// never reaches maxEventsPerBatch, and is never flushed by an explicit
// Flush/Close call, is still delivered -- purely because the periodic
// flush timer fires. This is the low-traffic-consumer scenario the timer
// exists for: a single end user rarely queues 25 events, and a caller that
// never calls Flush/Close (e.g. a Cloud Run instance recycled mid-life)
// would otherwise strand that user's events forever.
//
// Uses a short Client.FlushInterval instead of an injectable clock so the
// production code stays simple; the test itself blocks on a channel signaled
// by the test server rather than sleeping, bounded by a generous timeout
// that only matters on failure.
func TestTimeBasedFlushWithoutSizeOrExplicitFlush(t *testing.T) {
	delivered := make(chan ga4Payload, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var p ga4Payload
		if err := json.Unmarshal(body, &p); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case delivered <- p:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	s, err := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
		FlushInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()

	msg := analytics.NewEvent("idle-user-event", "cat", "act")
	msg.SetUserContext(userCtx("lonely-user"))
	s.QueueMessage(context.Background(), msg)
	// Deliberately no Flush and no Close here: only 1 of 25 events is
	// queued, so only the periodic timer can deliver it.

	select {
	case p := <-delivered:
		if len(p.Events) != 1 {
			t.Fatalf("expected 1 event in the timer-flushed request, got %d", len(p.Events))
		}
		if p.Events[0].Name != "idle_user_event" {
			t.Errorf("event name = %q, want idle_user_event", p.Events[0].Name)
		}
		if p.ClientID != "lonely-user" {
			t.Errorf("client_id = %q, want lonely-user", p.ClientID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event was not delivered by the periodic flush timer within 2s")
	}
}

// TestConcurrentQueueMessageIsRaceFree drives many goroutines through
// QueueMessage concurrently -- some sharing a user (so batches fill and
// flush inline) and some distinct (so Flush drains many small batches) --
// and must be run with -race. Every queued event must eventually be
// delivered exactly once.
func TestConcurrentQueueMessageIsRaceFree(t *testing.T) {
	var payloads []ga4Payload
	srv, mu := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})

	const goroutines = 50
	const perGoroutine = 10
	const sharedUsers = 5 // forces some goroutines to share a batch/client_id

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			userID := fmt.Sprintf("user-%d", g%sharedUsers)
			for i := 0; i < perGoroutine; i++ {
				msg := analytics.NewEvent(fmt.Sprintf("evt-%d-%d", g, i), "cat", "act")
				msg.SetUserContext(userCtx(userID))
				s.QueueMessage(context.Background(), msg)
			}
		}(g)
	}
	wg.Wait()

	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := snapshot(mu, &payloads)
	total := 0
	for _, p := range got {
		if len(p.Events) > maxEventsPerBatch {
			t.Errorf("request exceeded maxEventsPerBatch: %d events for client_id %q", len(p.Events), p.ClientID)
		}
		total += len(p.Events)
	}
	if want := goroutines * perGoroutine; total != want {
		t.Fatalf("expected %d total delivered events, got %d", want, total)
	}
}

// errorString is a minimal error implementation for tests.
type errorString string

func (e errorString) Error() string { return string(e) }
