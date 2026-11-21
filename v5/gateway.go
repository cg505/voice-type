package voicetype

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// The Vercel AI Gateway's streaming transcription socket. Vercel documents it
// only through its JavaScript SDK; the frames here are the ones that SDK
// sends, taken from a working client and confirmed by probes:
//
//	client: {"type":"transcription-stream.start", ...}   text, first
//	        raw PCM                                       binary, at most 64 KiB each
//	        {"type":"transcription-stream.audio-done"}   text, last
//	server: stream-start, then transcript-delta / transcript-partial /
//	        transcript-final, then finish; error at any point
//
// stream-start answers the start frame without waiting for audio (0.3 s), and
// the server leaves the socket open after finish.
const (
	gatewayStreamEndpoint = "wss://ai-gateway.vercel.sh/v4/ai/transcription-model"
	// The key travels in the subprotocol list, the one place a browser socket
	// can put it. The marker beside it is the protocol the server selects.
	gatewaySubprotocol = "ai-gateway-transcription.v1"
	gatewayAuthPrefix  = "ai-gateway-auth."
	gatewayTailSilence = time.Second
)

// gatewayStart is the first frame. MAI streaming takes a language hint and
// nothing else: a phrase list comes back as an "unsupported" warning.
type gatewayStart struct {
	Type             string                    `json:"type"`
	InputAudioFormat gatewayAudioFormat        `json:"inputAudioFormat"`
	ProviderOptions  map[string]map[string]any `json:"providerOptions,omitempty"`
}

type gatewayAudioFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

// gatewayEvent is the useful part of every server message. The two fields
// whose shape is only known from a handful of examples stay raw, so a shape
// not seen yet costs a log line's detail rather than the frame.
type gatewayEvent struct {
	Type     string          `json:"type"`
	Text     string          `json:"text"`     // transcript-partial, transcript-final, finish
	Delta    string          `json:"delta"`    // transcript-delta
	Warnings json.RawMessage `json:"warnings"` // stream-start: an array, empty when all is well
	Error    json.RawMessage `json:"error"`    // error: {"message": ..., "type": ...}
}

// errorText is the error's message, with its type when there is one. Both are
// the service's own words: the caller scrubs them.
func (e gatewayEvent) errorText() string {
	var detail struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	}
	var plain string
	switch {
	case decodeFrame(e.Error, &detail) && detail.Message != "" && detail.Type != "":
		return detail.Message + " [" + detail.Type + "]"
	case detail.Message != "":
		return detail.Message
	case detail.Type != "":
		return detail.Type
	case json.Unmarshal(e.Error, &plain) == nil && plain != "":
		return plain
	}
	return "an error without a message"
}

func (e gatewayEvent) hasWarnings() bool {
	warnings := strings.TrimSpace(string(e.Warnings))
	return warnings != "" && warnings != "[]" && warnings != "null"
}

// gatewayAssembler turns the gateway's events into the text so far. With MAI
// streaming, as observed (the docs do not say):
//
//   - a delta appends settled text, which did not change afterwards in any
//     recorded run;
//   - a partial replaces the open tail. The tail runs two seconds ahead of the
//     settled text and guesses: it ends in a period that moves along, and it
//     can guess whole words wrong;
//   - a delta settles words the tail was already showing, and the tail without
//     them only follows in the next partial;
//   - a final closes the stretch with its whole text. One arrives at the end
//     of the audio; a session can go its whole length without another.
//
// done and open are kept as sent: a delta can end mid-word, and the pieces
// carry their own spaces.
type gatewayAssembler struct {
	closed string // join of every stretch that got its final
	done   string // the open stretch's deltas
	open   string // the open stretch's replaceable tail
}

// apply reports whether a final contradicted text the stretch had settled.
func (a *gatewayAssembler) apply(ev gatewayEvent) (diverged bool) {
	switch ev.Type {
	case "transcript-delta":
		// When the wording differs from the tail's guess, the tail is stale
		// either way; the next partial brings a fresh one.
		if strings.HasPrefix(a.open, ev.Delta) {
			a.open = a.open[len(ev.Delta):]
		} else {
			a.open = ""
		}
		a.done += ev.Delta
	case "transcript-final":
		text := squashSpace(ev.Text)
		// Settled text is already promised to the typer as stable. A final
		// that disagrees with it has never been seen; if one comes, the
		// stretch closes as it stands on screen, tail included, rather than
		// the typer refusing to go on or words that were shown being erased.
		if !strings.HasPrefix(text, squashSpace(a.done)) {
			text, diverged = squashSpace(a.done+a.open), true
		}
		a.closed = joinPieces(a.closed, text)
		a.done, a.open = "", ""
	case "transcript-partial":
		a.open = ev.Text
	}
	return diverged
}

// current is the whole session's text, guessed tail included. Everything
// settled is stable: the closed stretches and the open one's deltas. A
// separator before the first delta of a stretch belongs to the tail, as in
// xaiAssembler.
func (a *gatewayAssembler) current() liveText {
	return newLiveText(joinPieces(a.closed, squashSpace(a.done)), joinPieces(a.closed, squashSpace(a.done+a.open)))
}

// gatewayProtocol is the gateway's side of a streamConn.
type gatewayProtocol struct {
	asm      gatewayAssembler
	diverged bool // logged once per session
}

func (p *gatewayProtocol) audioDone() []byte {
	return []byte(`{"type":"transcription-stream.audio-done"}`)
}

func (p *gatewayProtocol) decode(data []byte) (streamUpdate, bool) {
	var ev gatewayEvent
	if !decodeFrame(data, &ev) {
		return streamUpdate{}, false
	}
	switch ev.Type {
	case "transcript-delta", "transcript-partial", "transcript-final":
		if p.asm.apply(ev) && !p.diverged {
			p.diverged = true
			logf("STREAM", "a final disagreed with text the gateway had settled -- kept what was typed")
		}
		return streamUpdate{kind: updateText, text: p.asm.current()}, true
	case "finish":
		// finish repeats the whole transcript. It is the end signal only, but a
		// mismatch means the assembly above has drifted from the service.
		if whole := sanitizeLive(squashSpace(ev.Text)); whole != "" && whole != p.asm.current().text {
			logf("STREAM", "the gateway's closing transcript (%d chars) differs from the text typed (%d chars)",
				len(whole), len(p.asm.current().text))
		}
		return streamUpdate{kind: updateDone}, true
	case "error":
		return streamUpdate{kind: updateError, message: ev.errorText()}, true
	}
	return streamUpdate{}, true
}

func (c *streamClient) gatewayURL() string {
	return c.gatewayEndpoint + "?ai-model-id=" + url.QueryEscape(c.model)
}

func (c *streamClient) gatewayStartFrame() ([]byte, error) {
	start := gatewayStart{
		Type:             "transcription-stream.start",
		InputAudioFormat: gatewayAudioFormat{Type: "audio/pcm", Rate: wavSampleRate},
	}
	if c.options != "" {
		start.ProviderOptions = map[string]map[string]any{c.options: {"language": "en"}}
	}
	return json.Marshal(start)
}

// openGateway dials, sends the start frame and waits for stream-start, all
// within dialTimeout.
//
// The key is in a request header, so it cannot reach a URL. Unlike xAI's, a
// refused handshake keeps its body: it is how the gateway says whether the key
// is wrong, the account has no card on file, or the model is not enabled. That
// body is a third party's text (or a proxy's, or a captive portal's), so it is
// scrubbed like everything else the far end says, and goes to the log only.
func (c *streamClient) openGateway(ctx context.Context) (*streamConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()

	ws, resp, err := websocket.Dial(dialCtx, c.gatewayURL(), &websocket.DialOptions{
		HTTPClient:   streamHTTPClient,
		Subprotocols: []string{gatewaySubprotocol, gatewayAuthPrefix + c.apiKey},
	})
	if err != nil {
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			return nil, &streamError{Service: streamGateway, Status: resp.StatusCode, Detail: c.gatewayRefusal(resp.Body)}
		}
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("connect to the gateway: %w", c.scrubError(err))
	}
	ws.SetReadLimit(streamReadLimit)
	conn := &streamConn{ws: ws, proto: &gatewayProtocol{}, service: c.info().name}

	start, err := c.gatewayStartFrame()
	if err == nil {
		err = ws.Write(dialCtx, websocket.MessageText, start)
	}
	if err != nil {
		conn.closeNow()
		return nil, fmt.Errorf("send the start frame: %w", c.scrubError(err))
	}

	var ev gatewayEvent
	data, err := conn.readRaw(dialCtx)
	if err != nil {
		conn.closeNow()
		return nil, fmt.Errorf("waiting for stream-start: %w", c.scrubError(err))
	}
	decodeFrame(data, &ev)
	switch ev.Type {
	case "stream-start":
		// An option the model does not take comes back here instead of failing
		// the stream.
		if ev.hasWarnings() {
			logf("STREAM", "gateway warnings: %s", c.scrub(string(ev.Warnings)))
		}
		return conn, nil
	case "error":
		conn.closeNow()
		return nil, &streamError{Service: streamGateway, Message: c.scrub(ev.errorText())}
	default:
		conn.closeNow()
		return nil, fmt.Errorf("expected stream-start, got %q", c.scrub(ev.Type))
	}
}

// gatewayRefusal is the gateway's own explanation of a refused handshake: the
// error message and type when the body is the usual JSON, else the text. The
// library hands over at most the body's first kilobyte.
func (c *streamClient) gatewayRefusal(body io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(body, 1024))
	var ev gatewayEvent
	if decodeFrame(raw, &ev) && len(ev.Error) > 0 {
		return c.scrub(ev.errorText())
	}
	return c.scrub(string(raw))
}
