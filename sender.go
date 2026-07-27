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

// maxEventsPerBatch is the GA4 limit for events in a single request.
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

var _ analytics.Sender = (*sender)(nil)

// NewSender creates an analytics.Sender that fans messages out to every
// provided GA4 Client.  At least one client must be given.
func NewSender(logger Logger, clients ...Client) (analytics.Sender, error) {
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

type sender struct {
	logger  Logger
	clients []resolvedClient
	mu      sync.Mutex
	// pending buffers events grouped by API client ID (empty string = all clients)
	pending []ga4Event
}

// QueueMessage converts the analytics.Message to a GA4 event and enqueues it
// for all configured clients.
func (s *sender) QueueMessage(ctx context.Context, message analytics.Message) {
	ev, err := buildGA4Event(message)
	if err != nil {
		s.logger.Errorf(ctx, "analytics2ga4: buildGA4Event(%T{event=%s}): %v", message, message.Event(), err)
		return
	}

	// Determine client_id from the message's user identity.
	clientID := deriveClientID(message)

	payload := ga4Payload{
		ClientID: clientID,
		Events:   []ga4Event{ev},
	}

	// Set user_id when we have one.
	if uid := message.User().GetUserID(); uid != "" {
		payload.UserID = uid
	}

	for _, c := range s.clients {
		if err2 := s.send(ctx, c, payload); err2 != nil {
			s.logger.Errorf(ctx, "analytics2ga4: send to %s failed: %v", c.measurementID, err2)
		}
	}
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
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("non-2xx response: %d", resp.StatusCode)
	}
	return nil
}
