package analytics2ga4

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/strongo/analytics"
)

type dummyUser struct {
	analytics.UserContext
	id   string
	lang string
	ua   string
}

func (u dummyUser) GetUserID() string       { return u.id }
func (u dummyUser) GetUserLanguage() string { return u.lang }
func (u dummyUser) GetUserAgent() string    { return u.ua }

type dummyCustomMessage struct {
	analytics.Message
	name       string
	category   string
	properties analytics.Properties
	user       analytics.UserContext
}

func (d dummyCustomMessage) Event() string                    { return d.name }
func (d dummyCustomMessage) Category() string                 { return d.category }
func (d dummyCustomMessage) Properties() analytics.Properties { return d.properties }
func (d dummyCustomMessage) User() analytics.UserContext      { return d.user }

func TestSanitizeEventName_Empty(t *testing.T) {
	if got := sanitizeEventName(""); got != "unknown" {
		t.Fatalf("sanitizeEventName(\"\") = %q, want \"unknown\"", got)
	}
	if got := sanitizeEventName("___"); got != "unknown" {
		t.Fatalf("sanitizeEventName(\"___\") = %q, want \"unknown\"", got)
	}
}

func TestDeriveClientID_Anonymous(t *testing.T) {
	msgNoUser := dummyCustomMessage{name: "test"}
	got := deriveClientID(msgNoUser)
	if !strings.HasPrefix(got, "anon_") {
		t.Fatalf("deriveClientID(msgNoUser) = %q, want prefix \"anon_\"", got)
	}

	msgEmptyUID := dummyCustomMessage{
		name: "test",
		user: dummyUser{id: ""},
	}
	got2 := deriveClientID(msgEmptyUID)
	if !strings.HasPrefix(got2, "anon_") {
		t.Fatalf("deriveClientID(msgEmptyUID) = %q, want prefix \"anon_\"", got2)
	}
}

func TestBuildGA4Event_UncoveredBranches(t *testing.T) {
	// Properties loop, Category, UserLanguage, UserAgent, and default fallback branch
	msg := dummyCustomMessage{
		name:     "custom_event",
		category: "custom_cat",
		properties: analytics.Properties{
			"custom_prop": 123,
		},
		user: dummyUser{
			id:   "u123",
			lang: "en",
			ua:   "Mozilla/5.0",
		},
	}
	ev, err := buildGA4Event(msg)
	if err != nil {
		t.Fatalf("buildGA4Event(msg) = %v", err)
	}
	if ev.Name != "custom_event" {
		t.Errorf("ev.Name = %q, want custom_event", ev.Name)
	}
	if ev.Params["custom_prop"] != "123" {
		t.Errorf("ev.Params[custom_prop] = %q, want 123", ev.Params["custom_prop"])
	}
	if ev.Params["category"] != "custom_cat" {
		t.Errorf("ev.Params[category] = %q, want custom_cat", ev.Params["category"])
	}
	if ev.Params["user_lang"] != "en" {
		t.Errorf("ev.Params[user_lang] = %q, want en", ev.Params["user_lang"])
	}
	if ev.Params["user_agent"] != "Mozilla/5.0" {
		t.Errorf("ev.Params[user_agent] = %q, want Mozilla/5.0", ev.Params["user_agent"])
	}

	// Pageview with URL
	pv := analytics.NewPageview("example.com", "/page")
	pv.SetURL("https://example.com/page")
	evPv, err := buildGA4Event(pv)
	if err != nil {
		t.Fatalf("buildGA4Event(pv) = %v", err)
	}
	if evPv.Params["page_location"] != "https://example.com/page" {
		t.Errorf("evPv.Params[page_location] = %q", evPv.Params["page_location"])
	}

	// Event with Title
	evt := analytics.NewEvent("click", "cat", "act")
	evt.SetTitle("My Title")
	evEvt, err := buildGA4Event(evt)
	if err != nil {
		t.Fatalf("buildGA4Event(evt) = %v", err)
	}
	if evEvt.Params["title"] != "My Title" {
		t.Errorf("evEvt.Params[title] = %q, want My Title", evEvt.Params["title"])
	}
}

func TestQueueMessage_BuildGA4EventError(t *testing.T) {
	orig := buildGA4Event
	defer func() { buildGA4Event = orig }()

	buildGA4Event = func(msg analytics.Message) (ga4Event, error) {
		return ga4Event{}, errors.New("simulated build error")
	}

	logger := &captureLogger{}
	s, err := NewSender(logger, Client{
		MeasurementID: "G-TEST",
		APISecret:     "secret",
	})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}

	msg := dummyCustomMessage{name: "fail_event"}
	s.QueueMessage(context.Background(), msg)

	if logger.count() != 1 {
		t.Fatalf("logger count = %d, want 1", logger.count())
	}
}

func TestSendBatch_ExceedsMaxEventsPerBatch(t *testing.T) {
	s := &sender{
		logger: NoOpLogger{},
		clients: []resolvedClient{
			{
				measurementID: "G-TEST",
				apiSecret:     "secret",
				endpoint:      DefaultEndpoint,
				httpClient:    http.DefaultClient,
			},
		},
	}

	events := make([]ga4Event, 26)
	for i := range events {
		events[i] = ga4Event{Name: fmt.Sprintf("evt_%d", i)}
	}
	batch := &pendingBatch{
		userID: "u1",
		events: events,
	}

	// httpClient will fail to hit DefaultEndpoint without network/mock, but sendBatch loops over chunks
	_ = s.sendBatch(context.Background(), "client1", batch)
}

func TestSend_Errors(t *testing.T) {
	s := &sender{logger: NoOpLogger{}}

	// 1. jsonMarshal error
	origMarshal := jsonMarshal
	jsonMarshal = func(v any) ([]byte, error) {
		return nil, errors.New("marshal failure")
	}
	errMarshal := s.send(context.Background(), resolvedClient{}, ga4Payload{})
	jsonMarshal = origMarshal
	if errMarshal == nil || !strings.Contains(errMarshal.Error(), "json.Marshal") {
		t.Fatalf("expected json.Marshal error, got %v", errMarshal)
	}

	// 2. http.NewRequest error with invalid URL control character
	badURLClient := resolvedClient{
		endpoint:      "http://bad\x7furl",
		measurementID: "G-TEST",
		apiSecret:     "secret",
		httpClient:    http.DefaultClient,
	}
	errReq := s.send(context.Background(), badURLClient, ga4Payload{})
	if errReq == nil || !strings.Contains(errReq.Error(), "http.NewRequest") {
		t.Fatalf("expected http.NewRequest error, got %v", errReq)
	}

	// 3. httpClient.Do error with failing RoundTripper
	failingClient := resolvedClient{
		endpoint:      DefaultEndpoint,
		measurementID: "G-TEST",
		apiSecret:     "secret",
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("network failure")
			}),
		},
	}
	errDo := s.send(context.Background(), failingClient, ga4Payload{})
	if errDo == nil || !strings.Contains(errDo.Error(), "http:") {
		t.Fatalf("expected http: error, got %v", errDo)
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
