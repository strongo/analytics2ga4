package analytics2ga4

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/strongo/analytics"
)

// ga4Payload is the top-level JSON body for GA4 Measurement Protocol.
type ga4Payload struct {
	ClientID string     `json:"client_id"`
	UserID   string     `json:"user_id,omitempty"`
	Events   []ga4Event `json:"events"`
}

// ga4Event is a single event in a GA4 payload.
type ga4Event struct {
	Name   string            `json:"name"`
	Params map[string]string `json:"params"`
}

// reInvalidChar matches characters that are not alphanumeric or underscore.
var reInvalidChar = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// sanitizeEventName enforces GA4 event name rules:
//   - Only alphanumerics and underscores
//   - Must start with a letter (prefix with "e_" if not)
//   - Max 40 characters
func sanitizeEventName(name string) string {
	name = strings.TrimSpace(name)
	name = reInvalidChar.ReplaceAllString(name, "_")
	// Collapse consecutive underscores.
	for strings.Contains(name, "__") {
		name = strings.ReplaceAll(name, "__", "_")
	}
	name = strings.Trim(name, "_")
	if name == "" {
		name = "unknown"
	}
	// Must start with a letter.
	if !unicode.IsLetter(rune(name[0])) {
		name = "e_" + name
	}
	// Max 40 chars.
	if len(name) > 40 {
		name = name[:40]
	}
	return name
}

// deriveClientID returns a stable client_id for the message.
// GA4 client_id identifies a device/session; we use the user ID as the stable
// identifier (mirroring analytics2posthog's DistinctId usage).
// When no user ID is available we fall back to a synthetic day-scoped value.
func deriveClientID(msg analytics.Message) string {
	if user := msg.User(); user != nil {
		if uid := user.GetUserID(); uid != "" {
			return uid
		}
	}
	// Stable fallback: per-day anonymous client.
	return "anon_" + time.Now().UTC().Format("20060102")
}

// sessionID returns a stable session_id string derived from client_id and the
// current UTC day.  GA4 requires session_id in every event's params.
func sessionID(clientID string) string {
	day := time.Now().UTC().Format("20060102")
	// Simple numeric-looking value: hash-free, just concatenate day + len of id
	// so it is stable within the day for the same client.
	return fmt.Sprintf("%s%d", day, len(clientID))
}

// buildGA4Event converts an analytics.Message into a ga4Event.
func buildGA4Event(msg analytics.Message) (ga4Event, error) {
	params := make(map[string]string, 20)

	// Copy custom properties first (lowest precedence).
	for k, v := range msg.Properties() {
		params[sanitizeEventName(k)] = fmt.Sprintf("%v", v)
	}

	// Always include surface unless already set.
	if _, ok := params["surface"]; !ok {
		params["surface"] = "telegram_bot"
	}

	// GA4 requires session_id and engagement_time_msec in every event.
	clientID := deriveClientID(msg)
	if _, ok := params["session_id"]; !ok {
		params["session_id"] = sessionID(clientID)
	}
	if _, ok := params["engagement_time_msec"]; !ok {
		params["engagement_time_msec"] = "1"
	}

	// Common fields.
	if category := msg.Category(); category != "" {
		params["category"] = category
	}
	if user := msg.User(); user != nil {
		if lang := user.GetUserLanguage(); lang != "" {
			params["user_lang"] = lang
		}
		if ua := user.GetUserAgent(); ua != "" {
			params["user_agent"] = ua
		}
	}

	var eventName string

	switch m := msg.(type) {
	case analytics.Pageview:
		eventName = "page_view"
		if loc := m.URL(); loc != "" {
			params["page_location"] = loc
		} else {
			host := m.Host()
			path := m.Path()
			if host != "" || path != "" {
				params["page_location"] = "https://" + host + path
			}
		}
		if title := m.Title(); title != "" {
			params["page_title"] = title
		}

	case analytics.ErrorMessage:
		eventName = "app_exception"
		params["description"] = m.ErrorText()
		params["fatal"] = "0"

	case analytics.Event:
		eventName = sanitizeEventName(msg.Event())
		if action := m.Action(); action != "" {
			params["action"] = action
		}
		if label := m.Label(); label != "" {
			params["label"] = label
		}
		if v := m.Value(); v != 0 {
			params["value"] = fmt.Sprintf("%d", v)
		}
		if title := m.Title(); title != "" {
			params["title"] = title
		}

	case analytics.Timing:
		eventName = sanitizeEventName(msg.Event())
		params["duration_ms"] = fmt.Sprintf("%d", m.Duration().Milliseconds())

	default:
		// Generic fallback: use the message event name.
		eventName = sanitizeEventName(msg.Event())
	}

	return ga4Event{Name: eventName, Params: params}, nil
}
