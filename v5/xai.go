package voicetype

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/coder/websocket"
)

const (
	xaiStreamEndpoint = "wss://api.x.ai/v1/stt"
	// Documented limits for keyterm biasing.
	xaiKeytermMax    = 100
	xaiKeytermMaxLen = 50
)

// xaiEvent is the useful part of every server message. start/duration are
// deliberately not decoded: a chunk final reports the utterance's span, not
// the chunk's, and placing text by them produced phantom rewrites.
type xaiEvent struct {
	Type        string `json:"type"` // transcript.created | transcript.partial | transcript.done | error
	Text        string `json:"text"`
	IsFinal     bool   `json:"is_final"`
	SpeechFinal bool   `json:"speech_final"`
	Message     string `json:"message"` // error only
}

func (e xaiEvent) partial() bool { return e.Type == "transcript.partial" }

// speech_final closes the utterance whether or not is_final came with it.
func (e xaiEvent) interim() bool        { return e.partial() && !e.IsFinal && !e.SpeechFinal }
func (e xaiEvent) chunkFinal() bool     { return e.partial() && e.IsFinal && !e.SpeechFinal }
func (e xaiEvent) utteranceFinal() bool { return e.partial() && e.SpeechFinal }

// xaiAssembler turns xAI's events into the text so far. The protocol, as
// observed (the docs do not say):
//
//   - an interim holds the current, unlocked chunk -- before the utterance's
//     first chunk final that is the whole utterance so far;
//   - a chunk final locks a chunk, and can cut a word in half;
//   - an utterance final (speech_final) carries the whole utterance, repaired,
//     and replaces its chunks.
//
// Each field is trimmed and already joined.
type xaiAssembler struct {
	closed  string // join of every utterance that got its speech_final
	chunks  string // join of the open utterance's chunk finals
	interim string // the current unlocked chunk
}

func (a *xaiAssembler) apply(ev xaiEvent) {
	text := strings.TrimSpace(ev.Text)
	switch {
	case ev.interim():
		a.interim = text
	case ev.chunkFinal():
		// A flush (audio.done, finalize) is answered with the utterance so far.
		// Appending it would hold the earlier chunks twice until the utterance
		// final replaces them. When the flush repairs a word a chunk had cut,
		// this test misses and they are held twice anyway; that state only
		// comes after the stop, when the session types nothing until the last
		// word is in. A genuine repeat ("I think. I think.") can misfire into a
		// replace; the utterance final restores it.
		if a.chunks != "" && strings.HasPrefix(text, a.chunks) {
			a.chunks = text
		} else {
			a.chunks = joinPieces(a.chunks, text)
		}
		a.interim = ""
	case ev.utteranceFinal():
		if text != "" {
			a.closed = joinPieces(a.closed, text)
		} else {
			// A flush during silence closes an utterance with no text. Every one
			// observed had no chunks either; if one ever has, they are the only
			// record of what was said.
			a.closed = joinPieces(a.closed, a.chunks)
		}
		a.chunks = ""
		a.interim = ""
	}
	// transcript.done carried empty text in every session; the session uses it
	// as the end signal only.
}

// current is the whole session's text. Only closed utterances are stable. The
// separator after them belongs to the revisable tail: whether there is one
// depends on how the open utterance starts.
func (a *xaiAssembler) current() liveText {
	return newLiveText(a.closed, joinPieces(joinPieces(a.closed, a.chunks), a.interim))
}

// xaiProtocol is xAI's side of a streamConn.
type xaiProtocol struct{ asm xaiAssembler }

func (p *xaiProtocol) audioDone() []byte { return []byte(`{"type":"audio.done"}`) }

func (p *xaiProtocol) decode(data []byte) (streamUpdate, bool) {
	var ev xaiEvent
	if !decodeFrame(data, &ev) {
		return streamUpdate{}, false
	}
	switch ev.Type {
	case "transcript.partial":
		p.asm.apply(ev)
		return streamUpdate{kind: updateText, text: p.asm.current()}, true
	case "transcript.done":
		return streamUpdate{kind: updateDone}, true
	case "error":
		return streamUpdate{kind: updateError, message: ev.Message}, true
	}
	return streamUpdate{}, true
}

// streamKeyterms applies the documented limits. Length is measured in bytes,
// which can only be stricter than the server's characters. When more than 100
// terms remain the first ones go: general comes first in vocabularyFor, and the
// workspace the user switched to on purpose is the half worth keeping. The
// warning carries counts only -- a term is the user's content, not a log line.
func streamKeyterms(vocabulary []string) []string {
	kept := make([]string, 0, len(vocabulary))
	long := 0
	for _, term := range vocabulary {
		if len(term) > xaiKeytermMaxLen {
			long++
			continue
		}
		kept = append(kept, term)
	}
	over := max(len(kept)-xaiKeytermMax, 0)
	kept = kept[over:]

	warning := ""
	switch {
	case long > 0 && over > 0:
		warning = fmt.Sprintf("xAI takes at most %d terms of %d characters -- dropped %d longer terms and the first %d of the rest",
			xaiKeytermMax, xaiKeytermMaxLen, long, over)
	case long > 0:
		warning = fmt.Sprintf("xAI takes terms of at most %d characters -- dropped %d longer terms", xaiKeytermMaxLen, long)
	case over > 0:
		warning = fmt.Sprintf("xAI takes at most %d terms -- dropped the first %d", xaiKeytermMax, over)
	}
	configWarn("keyterm", warning)
	return kept
}

// url carries everything but the key, which travels as a header so that no
// URL ever holds a secret. url.Values sorts the keys; the server does not care.
func (c *streamClient) url() string {
	q := url.Values{}
	q.Set("model", c.model)
	q.Set("sample_rate", strconv.Itoa(wavSampleRate))
	q.Set("encoding", "pcm")
	q.Set("interim_results", "true")
	q.Set("language", "en")
	for _, term := range c.keyterms {
		q.Add("keyterm", term)
	}
	return c.endpoint + "?" + q.Encode()
}

// openXAI dials and waits for transcript.created, both within dialTimeout. No
// audio may be written before transcript.created arrives.
//
// Errors never carry the URL: a dial failure is a *url.Error from the HTTP
// client whose text quotes the whole request URL, keyterms included, so only
// its cause is kept. A non-101 handshake becomes *streamError with the status
// alone; its body is not read, for the same reason.
func (c *streamClient) openXAI(ctx context.Context) (*streamConn, error) {
	// The library clears its deadline after every call, and cancelling this
	// context after the handshake does not close the connection, so it bounds
	// the dial and the first read and nothing later.
	dialCtx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()

	ws, resp, err := websocket.Dial(dialCtx, c.url(), &websocket.DialOptions{
		HTTPClient: streamHTTPClient,
		HTTPHeader: http.Header{"Authorization": {"Bearer " + c.apiKey}},
	})
	if err != nil {
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			return nil, &streamError{Service: streamXAI, Status: resp.StatusCode}
		}
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("connect to xai: %w", c.scrubError(err))
	}
	ws.SetReadLimit(streamReadLimit)
	conn := &streamConn{ws: ws, proto: &xaiProtocol{}, service: c.info().name}

	var ev xaiEvent
	data, err := conn.readRaw(dialCtx)
	if err != nil {
		conn.closeNow()
		return nil, fmt.Errorf("waiting for transcript.created: %w", c.scrubError(err))
	}
	decodeFrame(data, &ev)
	switch ev.Type {
	case "transcript.created":
		return conn, nil
	case "error":
		conn.closeNow()
		return nil, &streamError{Service: streamXAI, Message: c.scrub(ev.Message)}
	default:
		conn.closeNow()
		return nil, fmt.Errorf("expected transcript.created, got %q", c.scrub(ev.Type))
	}
}
