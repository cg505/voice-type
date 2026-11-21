package voicetype

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/coder/websocket"
)

// Tuned once, on purpose, and not user-facing.
const (
	streamDialTimeout  = 5 * time.Second        // dial until ready for audio; observed 0.5-1.2 s
	streamWriteTimeout = 5 * time.Second        // one frame; a stall is a dead connection
	streamFlushTimeout = 5 * time.Second        // stop -> the rest of the audio uploaded
	streamDoneTimeout  = 3 * time.Second        // end of audio -> the service's last word; observed 0.2-0.4 s
	streamDrainTimeout = 5 * time.Second        // typer landing the last state at stop, plus its own strokes
	streamStopSettle   = 250 * time.Millisecond // let the stop hotkey's modifiers come up before stop-time strokes
	streamPollInterval = 50 * time.Millisecond  // sender polls the recorder buffer
	streamReadLimit    = 1 << 20                // one message can carry every word of the dictation
	// A sender this far behind the microphone has an uplink that cannot carry
	// the audio; the text would only fall further behind. It has to exceed the
	// dial timeout, because everything said during the dial goes up at once.
	streamMaxBacklog = 10 * wavSampleRate
	// The most of a service's own words that reaches a log line or an HTTP reply.
	streamMessageMax = 300
)

// streamService is the live service a model is served by. The zero value is a
// batch model.
type streamService int

const (
	streamNone    streamService = iota
	streamXAI                   // xAI's own endpoint (xai.go)
	streamGateway               // the Vercel AI Gateway (gateway.go)
)

// serviceInfo is what differs between the live services outside their wire
// protocols.
type serviceInfo struct {
	name     string // in logs and errors
	title    string // in messages to the user
	keyEnv   string // the environment variable that holds the key
	keyField string // the config field that holds it
	keyHint  string // what one looks like, for the preflight message
	// levels sends the audio through the AGC. xAI's voice activity gate judges
	// level; MAI through the gateway scored the same with and without it on
	// every recording, so it gets what the microphone heard.
	levels bool
	// tailSilence follows the last sample. MAI streaming dropped the final
	// word in two of three runs when the recording stopped 0.6 s after it, and
	// in none of three with a second of silence behind it. xAI flushes on
	// audio.done alone.
	tailSilence time.Duration
}

func (s streamService) info() serviceInfo {
	if s == streamGateway {
		return serviceInfo{
			name: "gateway", title: "Vercel AI Gateway",
			keyEnv: "AI_GATEWAY_API_KEY", keyField: "vercel_api_key", keyHint: "...",
			tailSilence: gatewayTailSilence,
		}
	}
	return serviceInfo{
		name: "xai", title: "xAI",
		keyEnv: "XAI_API_KEY", keyField: "xai_api_key", keyHint: "xai-...",
		levels: true,
	}
}

// streamUpdate is one server message, reduced to what the session acts on.
type streamUpdate struct {
	kind    streamUpdateKind
	text    liveText // updateText
	message string   // updateError, as the service sent it
}

type streamUpdateKind int

const (
	updateNone  streamUpdateKind = iota // nothing to act on
	updateText                          // what should be on screen changed
	updateDone                          // the service has said everything it will
	updateError                         // the service reported a failure
)

// streamProtocol is a service's wire protocol once the socket is up: the frame
// that ends the audio, and what a server message means. It holds the
// assembler, so it belongs to the one goroutine that reads.
type streamProtocol interface {
	audioDone() []byte
	// decode reports false for a frame that is not JSON at all.
	decode(data []byte) (streamUpdate, bool)
}

// decodeFrame fills v from one server frame. A field of the wrong type is left
// at its zero value and the rest still decodes, so a service changing a field
// cannot end a dictation over it; only a frame that is not JSON reports false.
func decodeFrame(data []byte, v any) bool {
	err := json.Unmarshal(data, v)
	var mismatch *json.UnmarshalTypeError
	return err == nil || errors.As(err, &mismatch)
}

// streamError is a server-reported failure: an error event, or a handshake
// answered with an HTTP status instead of 101.
type streamError struct {
	Service streamService
	Status  int    // 0 for an error event
	Message string // an error event's text, scrubbed
	Detail  string // the gateway's own explanation of a refused handshake, scrubbed
}

func (e *streamError) Error() string {
	name := e.Service.info().name
	if e.Status != 0 {
		return fmt.Sprintf("%s handshake %d %s", name, e.Status, http.StatusText(e.Status))
	}
	return name + ": " + e.Message
}

// Fatal reports a rejected key: retrying will not help until the key is fixed.
func (e *streamError) Fatal() bool {
	return e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden
}

// logStreamFailure logs a stream failure at the right severity. It never
// retains audio itself: what happens to the recording is decided at stop.
func logStreamFailure(err error, suffix string) {
	var se *streamError
	if !errors.As(err, &se) {
		logf("STREAM", "failed: %v%s", err, suffix)
		return
	}
	info := se.Service.info()
	detail := ""
	if se.Detail != "" {
		detail = ": " + se.Detail
	}
	switch {
	case se.Status == 0:
		logf("STREAM", "server error: %s%s", se.Message, suffix)
	case se.Detail != "":
		// 403 is also how the gateway says the account is not set up (no card
		// on file, model not enabled); its own words tell that from a bad key.
		logf("STREAM", "%s refused the stream (%d)%s%s", info.name, se.Status, detail, suffix)
	case se.Fatal():
		logf("STREAM", "authentication rejected (%d)%s", se.Status, suffix)
	case se.Status >= 500:
		logf("STREAM", "transient failure (%d)%s", se.Status, suffix)
	default:
		logf("STREAM", "failed: %v%s", se, suffix)
	}
	if se.Fatal() {
		logf("STREAM", "check %s or the %s field in %s", info.keyEnv, info.keyField, configFilePath())
	}
}

// streamHTTPClient makes the handshake request. It follows no redirects: Go
// drops Authorization when one leaves the host, but not the subprotocol
// header the gateway's key travels in.
var streamHTTPClient = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// streamClient opens streams for the configured model. A session takes a copy,
// so a config reload cannot change anything under one that is running.
type streamClient struct {
	apiKey  string
	model   string // the catalog Slug
	service streamService
	options string // gateway: the providerOptions namespace
	// keyterms is the vocabulary within xAI's limits.
	keyterms []string
	// One endpoint per service, so a config reload can switch between them.
	endpoint        string
	gatewayEndpoint string
	// Copied from the constants so tests can shorten them, like sttClient.endpoint.
	dialTimeout, writeTimeout, flushTimeout, doneTimeout, drainTimeout, stopSettle time.Duration
	maxBacklog                                                                     int // samples
}

func newStreamClient(apiKey string, model sttSpec, vocabulary []string) *streamClient {
	c := &streamClient{
		apiKey:          apiKey,
		endpoint:        xaiStreamEndpoint,
		gatewayEndpoint: gatewayStreamEndpoint,
		dialTimeout:     streamDialTimeout,
		writeTimeout:    streamWriteTimeout,
		flushTimeout:    streamFlushTimeout,
		doneTimeout:     streamDoneTimeout,
		drainTimeout:    streamDrainTimeout,
		stopSettle:      streamStopSettle,
		maxBacklog:      streamMaxBacklog,
	}
	c.setModel(model)
	c.setVocabulary(vocabulary)
	return c
}

func (c *streamClient) setModel(model sttSpec) {
	c.model, c.service, c.options = model.Slug, model.Stream, model.Provider
}

func (c *streamClient) setVocabulary(vocabulary []string) {
	c.keyterms = streamKeyterms(vocabulary)
}

func (c *streamClient) info() serviceInfo { return c.service.info() }

// open connects to the service the model is served by. The connection is
// ready for audio when it returns.
func (c *streamClient) open(ctx context.Context) (*streamConn, error) {
	if c.service == streamGateway {
		return c.openGateway(ctx)
	}
	return c.openXAI(ctx)
}

// scrub makes text that came from a service fit to log and to return over
// HTTP. The key is taken out wherever it appears -- the gateway's travels in a
// header a server could quote back -- control characters become spaces, so a
// newline cannot start a forged log line, and the length is capped.
func (c *streamClient) scrub(text string) string {
	if c.apiKey != "" {
		text = strings.ReplaceAll(text, c.apiKey, "<key>")
	}
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, text)
	return truncate(squashSpace(text), streamMessageMax)
}

// scrubError is scrub for an error whose text may quote the far end: a close
// reason, a dial failure. An error with nothing to scrub keeps its chain.
func (c *streamClient) scrubError(err error) error {
	if clean := c.scrub(err.Error()); clean != err.Error() {
		return errors.New(clean)
	}
	return err
}

// streamConn is one transcription session. coder/websocket allows one reader
// and one writer at a time: the reader goroutine reads, the sender writes.
type streamConn struct {
	ws      *websocket.Conn
	proto   streamProtocol
	service string
	skipped bool // an undecodable frame has been logged
}

func (c *streamConn) writeAudio(ctx context.Context, pcm []byte) error {
	return c.ws.Write(ctx, websocket.MessageBinary, pcm)
}

// writeDone tells the service the audio is over.
func (c *streamConn) writeDone(ctx context.Context) error {
	return c.ws.Write(ctx, websocket.MessageText, c.proto.audioDone())
}

// read returns the next server message as an update. The library closes the
// connection when ctx ends, so callers pass the session context, never one
// with a deadline: a quiet stretch is normal, and a read timeout would end the
// session.
//
// A frame that is not JSON is skipped, and logged once: whatever it was, the
// frames that follow are worth more than ending the dictation over it. Its
// content never reaches the log.
func (c *streamConn) read(ctx context.Context) (streamUpdate, error) {
	data, err := c.readRaw(ctx)
	if err != nil {
		return streamUpdate{}, err
	}
	up, ok := c.proto.decode(data)
	if !ok && !c.skipped {
		c.skipped = true
		logf("STREAM", "skipped a %d-byte frame from %s that is not JSON", len(data), c.service)
	}
	return up, nil
}

func (c *streamConn) readRaw(ctx context.Context) ([]byte, error) {
	_, data, err := c.ws.Read(ctx)
	if err != nil {
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return nil, io.EOF
		}
		return nil, err
	}
	return data, nil
}

func (c *streamConn) closeNow() { c.ws.CloseNow() }
