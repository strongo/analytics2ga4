package analytics2ga4

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/strongo/analytics"
)

// NoOpLogger satisfies Logger in tests without output.
var _ Logger = (*NoOpLogger)(nil)

type NoOpLogger struct{}

func (n NoOpLogger) Warningf(_ context.Context, _ string, _ ...any) {}
func (n NoOpLogger) Errorf(_ context.Context, _ string, _ ...any)   {}

// captureLogger records error calls for assertion.
type captureLogger struct {
	errors []string
}

func (c *captureLogger) Warningf(_ context.Context, _ string, _ ...any) {}
func (c *captureLogger) Errorf(_ context.Context, f string, args ...any) {
	c.errors = append(c.errors, f)
}

// --- helpers ---

func userCtx(id string) analytics.UserContext {
	return analytics.NewUserContext(id)
}

func newTestServer(t *testing.T, payloads *[]ga4Payload) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		*payloads = append(*payloads, p)
		w.WriteHeader(http.StatusNoContent)
	}))
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
}

// --- session envelope always present ---

func TestSessionEnvelopeAlwaysPresent(t *testing.T) {
	var payloads []ga4Payload
	srv := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})

	msg := analytics.NewEvent("click", "ui", "button")
	msg.SetUserContext(userCtx("user-1"))
	s.QueueMessage(context.Background(), msg)

	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	ev := payloads[0].Events[0]
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
	srv := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})

	msg := analytics.NewPageview("example.com", "/home")
	msg.SetTitle("Home page")
	msg.SetUserContext(userCtx("user-2"))
	s.QueueMessage(context.Background(), msg)

	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	ev := payloads[0].Events[0]
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
	srv := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})

	dbErr := errorString("database unavailable")
	msg := analytics.NewErrorMessage(dbErr)
	msg.SetUserContext(userCtx("user-3"))
	s.QueueMessage(context.Background(), msg)

	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	ev := payloads[0].Events[0]
	if ev.Name != "app_exception" {
		t.Errorf("expected app_exception, got %s", ev.Name)
	}
	if ev.Params["description"] != "database unavailable" {
		t.Errorf("description = %q", ev.Params["description"])
	}
}

func TestEventMessage(t *testing.T) {
	var payloads []ga4Payload
	srv := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})

	msg := analytics.NewEvent("purchase", "ecommerce", "buy_now")
	msg.SetLabel("promo-2024")
	msg.SetValue(99)
	msg.SetUserContext(userCtx("user-4"))
	s.QueueMessage(context.Background(), msg)

	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	ev := payloads[0].Events[0]
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

	srv1 := newTestServer(t, &payloads1)
	defer srv1.Close()
	srv2 := newTestServer(t, &payloads2)
	defer srv2.Close()

	s, _ := NewSender(NoOpLogger{},
		Client{MeasurementID: "G-ONE", APISecret: "s1", Endpoint: srv1.URL, HTTPClient: srv1.Client()},
		Client{MeasurementID: "G-TWO", APISecret: "s2", Endpoint: srv2.URL, HTTPClient: srv2.Client()},
	)

	msg := analytics.NewEvent("test", "category", "action")
	msg.SetUserContext(userCtx("user-5"))
	s.QueueMessage(context.Background(), msg)

	if len(payloads1) != 1 {
		t.Errorf("client 1: expected 1 payload, got %d", len(payloads1))
	}
	if len(payloads2) != 1 {
		t.Errorf("client 2: expected 1 payload, got %d", len(payloads2))
	}
}

// --- surface param ---

func TestSurfaceParamDefault(t *testing.T) {
	var payloads []ga4Payload
	srv := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})

	msg := analytics.NewEvent("nav", "ui", "click")
	msg.SetUserContext(userCtx("user-6"))
	s.QueueMessage(context.Background(), msg)

	if len(payloads) == 0 {
		t.Fatal("no payload received")
	}
	if payloads[0].Events[0].Params["surface"] != "telegram_bot" {
		t.Errorf("surface = %q, want telegram_bot", payloads[0].Events[0].Params["surface"])
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

	msg := analytics.NewEvent("test", "cat", "act")
	msg.SetUserContext(userCtx("user-7"))
	s.QueueMessage(context.Background(), msg)

	if len(cl.errors) == 0 {
		t.Error("expected at least one error to be logged for non-2xx response")
	}
}

// --- timing message ---

func TestTimingMessage(t *testing.T) {
	var payloads []ga4Payload
	srv := newTestServer(t, &payloads)
	defer srv.Close()

	s, _ := NewSender(NoOpLogger{}, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
		Endpoint:      srv.URL,
		HTTPClient:    srv.Client(),
	})

	msg := analytics.NewTiming("db_query", 250*time.Millisecond)
	msg.SetUserContext(userCtx("user-8"))
	s.QueueMessage(context.Background(), msg)

	if len(payloads) != 1 {
		t.Fatalf("expected 1 payload, got %d", len(payloads))
	}
	ev := payloads[0].Events[0]
	if ev.Params["duration_ms"] != "250" {
		t.Errorf("duration_ms = %q, want 250", ev.Params["duration_ms"])
	}
}

// errorString is a minimal error implementation for tests.
type errorString string

func (e errorString) Error() string { return string(e) }
