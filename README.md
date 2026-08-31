# analytics2ga4

Google Analytics 4 (Measurement Protocol) sender for [strongo/analytics](https://github.com/strongo/analytics).

Sends analytics events to GA4 via the [Measurement Protocol](https://developers.google.com/analytics/devguides/collection/protocol/ga4) using HTTP POST — no browser or JavaScript required.

## Installation

```bash
go get github.com/strongo/analytics2ga4
```

## Setup in GA4

1. In GA4 Admin → Data Streams, open your web/app stream and note the **Measurement ID** (e.g. `G-XXXXXXXXXX`).
2. Scroll down to **Measurement Protocol API secrets** and create a new secret. Copy the **secret value**.
3. Both values go into the `Client` struct. Each GA4 property × stream combination needs its own `Client`.

> **Same property / same stream**: All events from one `Client` land in one GA4 data stream. To fan events to multiple properties (e.g. dev + prod), add multiple `Client` entries to `NewSender`.

## Usage

```go
import (
    "github.com/strongo/analytics"
    "github.com/strongo/analytics2ga4"
)

// Wire up once during application startup.
sender, err := analytics2ga4.NewSender(myLogger,
    analytics2ga4.Client{
        MeasurementID: "G-XXXXXXXXXX",
        APISecret:     "your-api-secret",
    },
)
if err != nil {
    log.Fatal(err)
}
analytics.AddSender(sender)

// Then anywhere in your code:
msg := analytics.NewEvent("command_received", "bot", "start")
msg.SetUserContext(analytics.NewUserContext(strconv.FormatInt(telegramUserID, 10)))
analytics.QueueMessage(ctx, msg)

// During shutdown: QueueMessage only buffers events in memory and never
// sends synchronously, so this MUST be called or any not-yet-full batch is
// lost.
defer sender.Close(context.Background())
```

### Batching

`QueueMessage` never makes an HTTP call itself. Events are buffered per GA4
`client_id` (derived from the message's user) and shipped to GA4 once that
client's batch reaches `maxEventsPerBatch` (25, the Measurement Protocol
limit on events per request), `DefaultFlushInterval` elapses, or
`Flush`/`Close` is called. Events for different end users are never merged
into one request, since a single GA4 request only carries one
`client_id`/`user_id` for all of its events.

The periodic timer (`DefaultFlushInterval`, 30s by default; override per
`Client` via `FlushInterval`) exists because a single end user rarely
generates a full 25-event batch: without a timer, a low-traffic caller that
never calls `Flush`/`Close` (e.g. a Cloud Run instance recycled mid-session)
would silently strand that user's events forever. `NewSender` starts this
timer goroutine; `Close` stops it.

Call `Flush(ctx)` whenever you want buffered events sent immediately without
tearing the sender down (e.g. before a deploy or a batch job exits), and call
`Close(ctx)` during application shutdown — it stops the timer, flushes once,
and is safe to call more than once. Calling `Close` is still recommended even
though the timer is a safety net: it gives a deterministic delivery point
instead of relying on the next tick.

### Custom HTTP client / endpoint

```go
analytics2ga4.Client{
    MeasurementID: "G-XXXXXXXXXX",
    APISecret:     "secret",
    Endpoint:      "https://www.google-analytics.com/mp/collect", // default
    HTTPClient:    &http.Client{Timeout: 5 * time.Second},
}
```

## GA4 correctness notes

- Every event automatically includes `session_id` (derived from client ID + UTC day) and `engagement_time_msec=1`; without these, events are silently dropped from standard and realtime GA4 reports.
- Event names are sanitised to GA4 rules: alphanumerics and underscores, starting with a letter, max 40 characters.
- `surface=telegram_bot` is injected into every event's params unless the message already carries a `surface` property.
- Multiple clients → fan-out: one HTTP POST per client per flushed batch (see [Batching](#batching)).

## Message mapping

| analytics type       | GA4 event name  | Notable params                               |
|----------------------|-----------------|----------------------------------------------|
| `analytics.Event`    | sanitised event name | `action`, `label`, `value`, `category`  |
| `analytics.Pageview` | `page_view`     | `page_location`, `page_title`                |
| `analytics.ErrorMessage` | `app_exception` | `description`, `fatal=0`               |
| `analytics.Timing`   | sanitised event name | `duration_ms`                           |

## License

MIT
