package analytics2ga4

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/strongo/analytics"
)

// DefaultEndpoint is the GA4 Measurement Protocol collection endpoint.
const DefaultEndpoint = "https://www.google-analytics.com/mp/collect"

// maxEventsPerBatch is the GA4 Measurement Protocol limit for events in a
// single request body (https://developers.google.com/analytics/devguides/collection/protocol/ga4/sending-events#limitations).
const maxEventsPerBatch = 25

// Client holds credentials and HTTP config for one GA4 property stream.
type Client struct {
	// MeasurementID is the GA4 Measurement ID (G-XXXXXXXXXX).
	MeasurementID string

	// APISecret is the Measurement Protocol API secret created in GA4 Admin.
	APISecret string

	// Endpoint overrides the default MP collect URL (useful for testing).
	// Defaults to DefaultEndpoint when empty.
	Endpoint string

	// HTTPClient is the HTTP client to use.  Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// Sender is the analytics2ga4 sender. It satisfies analytics.Sender, so it
// can be registered with analytics.AddSender like before, and additionally
// exposes explicit batch control.
//
// QueueMessage buffers events in memory and only ships them to GA4 once a
// batch fills up or Flush/Close is called -- it never blocks on a network
// call per message. Callers MUST call Flush or Close during application
// shutdown (or whenever a guaranteed delivery point is needed): any events
// still buffered when the process exits without a final Flush/Close are
// lost, the same way any other in-memory buffer would lose them.
type Sender interface {
	analytics.Sender

	// Flush immediately sends every buffered event, even if a batch is not
	// yet full. Safe to call concurrently with QueueMessage and with itself.
	Flush(ctx context.Context) error

	// Close flushes any buffered events and marks the sender closed. It is
	// safe to call multiple times; only the first call flushes.
	Close(ctx context.Context) error
}

var _ Sender = (*sender)(nil)

// NewSender creates an analytics.Sender that fans messages out to every
// provided GA4 Client.  At least one client must be given.
func NewSender(logger Logger, clients ...Client) (Sender, error) {
	if len(clients) == 0 {
		return nil, errors.New("analytics2ga4: no clients provided")
	}
	resolved := make([]resolvedClient, 0, len(clients))
	for i, c := range clients {
		if c.MeasurementID == "" {
			return nil, fmt.Errorf("analytics2ga4: client #%d has empty MeasurementID", i+1)
		}
		if c.APISecret == "" {
			return nil, fmt.Errorf("analytics2ga4: client #%d has empty APISecret", i+1)
		}
		endpoint := c.Endpoint
		if endpoint == "" {
			endpoint = DefaultEndpoint
		}
		httpClient := c.HTTPClient
		if httpClient == nil {
			httpClient = http.DefaultClient
		}
		resolved = append(resolved, resolvedClient{
			measurementID: c.MeasurementID,
			apiSecret:     c.APISecret,
			endpoint:      endpoint,
			httpClient:    httpClient,
		})
	}
	return &sender{logger: logger, clients: resolved}, nil
}

type resolvedClient struct {
	measurementID string
	apiSecret     string
	endpoint      string
	httpClient    *http.Client
}

// pendingBatch buffers the events queued for one GA4 client_id. A single
// Measurement Protocol request carries exactly one client_id/user_id for
// every event in it, so events for different end users can never share a
// request -- they are kept in separate batches, keyed by client_id, and
// only events within the same batch are sent together.
type pendingBatch struct {
	userID string
	events []ga4Event
}

type sender struct {
	logger  Logger
	clients []resolvedClient

	mu sync.Mutex
	// pending buffers events awaiting a batch flush, grouped by GA4
	// client_id (see pendingBatch). Flushed groups are removed from the
	// map, so its size tracks only what is currently un-sent.
	pending map[string]*pendingBatch

	closeOnce sync.Once
}

// QueueMessage converts the analytics.Message to a GA4 event and enqueues it
// for all configured clients. The event is buffered, not sent immediately:
// it ships once its client_id's batch reaches maxEventsPerBatch, or when
// Flush/Close is called.
func (s *sender) QueueMessage(ctx context.Context, message analytics.Message) {
	ev, err := buildGA4Event(message)
	if err != nil {
		s.logger.Errorf(ctx, "analytics2ga4: buildGA4Event(%T{event=%s}): %v", message, message.Event(), err)
		return
	}

	// Determine client_id from the message's user identity.
	clientID := deriveClientID(message)

	var userID string
	if uid := message.User().GetUserID(); uid != "" {
		userID = uid
	}

	full := s.enqueue(clientID, userID, ev)
	if full != nil {
		// Per-client failures are already logged inside sendBatch; QueueMessage
		// has no error return (it satisfies analytics.Sender), so there is
		// nothing further to do with the aggregate error here.
		_ = s.sendBatch(ctx, clientID, full)
	}
}

// enqueue appends ev to the batch for clientID under mu. When the batch
// reaches maxEventsPerBatch it is removed from pending and returned for the
// caller to send outside the lock; otherwise it returns nil.
func (s *sender) enqueue(clientID, userID string, ev ga4Event) *pendingBatch {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending == nil {
		s.pending = make(map[string]*pendingBatch)
	}
	batch, ok := s.pending[clientID]
	if !ok {
		batch = &pendingBatch{userID: userID}
		s.pending[clientID] = batch
	}
	batch.events = append(batch.events, ev)

	if len(batch.events) >= maxEventsPerBatch {
		delete(s.pending, clientID)
		return batch
	}
	return nil
}

// Flush sends every currently buffered batch, even if not full. It is safe
// to call concurrently with QueueMessage: batches queued after the snapshot
// below (or that fill and flush concurrently) are left for a later flush.
func (s *sender) Flush(ctx context.Context) error {
	s.mu.Lock()
	batches := s.pending
	s.pending = nil
	s.mu.Unlock()

	var errs []error
	for clientID, batch := range batches {
		if err := s.sendBatch(ctx, clientID, batch); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close flushes any buffered events. It is safe to call multiple times;
// only the first call performs a flush, so it is safe to defer alongside an
// earlier explicit Flush.
func (s *sender) Close(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() {
		err = s.Flush(ctx)
	})
	return err
}

// sendBatch posts every event in batch to every configured GA4 client,
// chunking defensively at maxEventsPerBatch even though batches are never
// grown past that size by enqueue.
func (s *sender) sendBatch(ctx context.Context, clientID string, batch *pendingBatch) error {
	var errs []error
	events := batch.events
	for len(events) > 0 {
		n := len(events)
		if n > maxEventsPerBatch {
			n = maxEventsPerBatch
		}
		chunk := events[:n]
		events = events[n:]

		payload := ga4Payload{ClientID: clientID, UserID: batch.userID, Events: chunk}
		for _, c := range s.clients {
			if err := s.send(ctx, c, payload); err != nil {
				s.logger.Errorf(ctx, "analytics2ga4: send to %s failed: %v", c.measurementID, err)
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// send posts a ga4Payload to one GA4 property.
func (s *sender) send(ctx context.Context, c resolvedClient, payload ga4Payload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}

	url := fmt.Sprintf("%s?measurement_id=%s&api_secret=%s", c.endpoint, c.measurementID, c.apiSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("http.NewRequest: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("non-2xx response: %d", resp.StatusCode)
	}
	return nil
}
