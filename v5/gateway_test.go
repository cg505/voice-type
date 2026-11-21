package voicetype

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const gatewayModelID = "mai-transcribe-2-streaming"

// fakeGateway is the Vercel AI Gateway's streaming socket as the probes
// observed it: it answers the start frame with stream-start, forwards the
// events a test pushes, and answers audio-done with a final for the open
// stretch and a finish carrying the whole transcript -- and then leaves the
// socket open, as the real one does.
type fakeGateway struct {
	srv *httptest.Server

	// Set before the client dials.
	handshake  int           // HTTP status instead of upgrading; 0 = upgrade
	refusal    string        // the body of that status
	redirect   string        // with a 3xx handshake: the Location
	startDelay time.Duration // before stream-start; very long = hang
	firstEvent string        // sent instead of stream-start
	ignoreDone bool          // never answer audio-done
	deaf       atomic.Bool   // stop reading after the start frame, as a stalled link does

	events  chan string   // the test pushes server frames; forwarded in order
	release chan struct{} // closed at cleanup: lets a deaf server's handler go

	mu       sync.Mutex
	received []fakeMessage // beforeCreated: before stream-start was sent
	dials    int
	offered  []string // the subprotocols the client offered
	auth     string
	rawQuery string
	started  bool
	asm      gatewayAssembler // what the pushed events add up to
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	g := &fakeGateway{events: make(chan string, 256), release: make(chan struct{})}
	g.srv = httptest.NewUnstartedServer(http.HandlerFunc(g.serve))
	// A deaf server also keeps its receive buffer small, so the client's
	// writes stall after kilobytes rather than megabytes.
	g.srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
		if tcp, ok := c.(*net.TCPConn); ok && state == http.StateNew && g.deaf.Load() {
			tcp.SetReadBuffer(4096)
		}
	}
	g.srv.Start()
	t.Cleanup(func() {
		close(g.release)
		g.srv.Close()
	})
	return g
}

func (g *fakeGateway) wsURL() string {
	return "ws" + strings.TrimPrefix(g.srv.URL, "http") + "/v4/ai/transcription-model"
}

func gatewayFrame(fields map[string]any) string {
	data, _ := json.Marshal(fields)
	return string(data)
}

func (g *fakeGateway) delta(text string) {
	g.events <- gatewayFrame(map[string]any{"type": "transcript-delta", "id": "item_1", "delta": text})
}

func (g *fakeGateway) partial(text string) {
	g.events <- gatewayFrame(map[string]any{"type": "transcript-partial", "id": "item_1", "text": text})
}

func (g *fakeGateway) messages() []fakeMessage {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.received)
}

func (g *fakeGateway) dialCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.dials
}

func (g *fakeGateway) serve(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	g.dials++
	g.offered = nil
	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(header, ",") {
			g.offered = append(g.offered, strings.TrimSpace(p))
		}
	}
	g.auth = r.Header.Get("Authorization")
	g.rawQuery = r.URL.RawQuery
	g.mu.Unlock()
	if g.handshake != 0 {
		if g.redirect != "" {
			w.Header().Set("Location", g.redirect)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(g.handshake)
		io.WriteString(w, g.refusal)
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{gatewaySubprotocol}})
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startSeen := make(chan struct{}, 1)
	doneSeen := make(chan struct{}, 1)
	readerGone := make(chan struct{})
	go func() {
		defer close(readerGone)
		for {
			typ, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			g.mu.Lock()
			msg := fakeMessage{binary: typ == websocket.MessageBinary, beforeCreated: !g.started}
			if msg.binary {
				msg.data = data
			} else {
				msg.text = string(data)
			}
			g.received = append(g.received, msg)
			g.mu.Unlock()
			if msg.binary {
				continue
			}
			signal := startSeen
			if strings.Contains(msg.text, `"transcription-stream.audio-done"`) {
				signal = doneSeen
			}
			select {
			case signal <- struct{}{}:
			default:
			}
			if g.deaf.Load() {
				<-g.release
				return
			}
		}
	}()

	write := func(frame string) error { return c.Write(ctx, websocket.MessageText, []byte(frame)) }
	forward := func(frame string) error {
		var ev gatewayEvent
		json.Unmarshal([]byte(frame), &ev)
		g.mu.Lock()
		g.asm.apply(ev)
		g.mu.Unlock()
		return write(frame)
	}

	select {
	case <-startSeen:
	case <-readerGone:
		return
	}
	select {
	case <-time.After(g.startDelay):
	case <-readerGone:
		return
	}
	first := `{"type":"stream-start","warnings":[]}`
	if g.firstEvent != "" {
		first = g.firstEvent
	}
	g.mu.Lock()
	g.started = true
	g.mu.Unlock()
	if write(first) != nil {
		return
	}

	for {
		select {
		case frame := <-g.events:
			if forward(frame) != nil {
				return
			}
		case <-doneSeen:
			if g.ignoreDone {
				continue
			}
			// Events the test queued were in flight before the client's audio-done.
			for queued := true; queued; {
				select {
				case frame := <-g.events:
					if forward(frame) != nil {
						return
					}
				default:
					queued = false
				}
			}
			g.mu.Lock()
			stretch := squashSpace(g.asm.done + g.asm.open)
			g.mu.Unlock()
			if forward(gatewayFrame(map[string]any{"type": "transcript-final", "id": "item_1", "text": stretch})) != nil {
				return
			}
			g.mu.Lock()
			whole := g.asm.closed
			g.mu.Unlock()
			if write(gatewayFrame(map[string]any{"type": "finish", "text": whole, "segments": []any{}})) != nil {
				return
			}
			<-readerGone // the gateway does not close after finish
			return
		case <-readerGone:
			return
		}
	}
}

func testGatewayClient(g *fakeGateway) *streamClient {
	c := newStreamClient("vck-test-key", testSpec(gatewayModelID), nil)
	c.gatewayEndpoint = g.wsURL()
	return c
}

// newGatewayHarness is a streamHarness whose session talks to a fake gateway.
func newGatewayHarness(t *testing.T) (*streamHarness, *fakeGateway) {
	t.Helper()
	g := newFakeGateway(t)
	c := testGatewayClient(g)
	c.stopSettle = 0
	c.doneTimeout = 2 * time.Second
	c.drainTimeout = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	h := &streamHarness{rec: &fakeRecorder{}, kb: newFakeKeyboard(), client: c, ctx: ctx, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		if h.s != nil {
			<-h.s.runDone
		}
	})
	return h, g
}

func TestGatewayHandshakeAndStartFrame(t *testing.T) {
	g := newFakeGateway(t)
	conn, err := testGatewayClient(g).open(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.closeNow()

	g.mu.Lock()
	offered, auth, raw := g.offered, g.auth, g.rawQuery
	g.mu.Unlock()
	if want := []string{gatewaySubprotocol, "ai-gateway-auth.vck-test-key"}; !slices.Equal(offered, want) {
		t.Errorf("subprotocols = %q, want %q", offered, want)
	}
	if auth != "" {
		t.Errorf("Authorization = %q, want the key only in the subprotocol", auth)
	}
	if raw != "ai-model-id=microsoft%2Fmai-transcribe-2-streaming" {
		t.Errorf("query = %q", raw)
	}

	msgs := g.messages()
	if len(msgs) != 1 || msgs[0].binary {
		t.Fatalf("frames before any audio = %+v, want the start frame alone", msgs)
	}
	var start map[string]any
	if err := json.Unmarshal([]byte(msgs[0].text), &start); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"type":             "transcription-stream.start",
		"inputAudioFormat": map[string]any{"type": "audio/pcm", "rate": float64(wavSampleRate)},
		"providerOptions":  map[string]any{"azure": map[string]any{"language": "en"}},
	}
	if got, _ := json.Marshal(start); !bytes.Equal(got, mustJSON(t, want)) {
		t.Errorf("start frame = %s, want %s", got, mustJSON(t, want))
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestGatewayOpenWaitsForStreamStart(t *testing.T) {
	g := newFakeGateway(t)
	g.startDelay = 150 * time.Millisecond
	started := time.Now()
	conn, err := testGatewayClient(g).open(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.closeNow()
	if elapsed := time.Since(started); elapsed < g.startDelay {
		t.Errorf("open returned after %v, before stream-start", elapsed)
	}
}

func TestGatewayOpenTimesOut(t *testing.T) {
	g := newFakeGateway(t)
	g.startDelay = time.Hour
	c := testGatewayClient(g)
	c.dialTimeout = 100 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := c.open(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "stream-start") {
			t.Fatalf("open error = %v, want a missing stream-start", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("open is not bounded by the dial timeout")
	}
}

func TestGatewayOpenRejectsBadFirstEvent(t *testing.T) {
	t.Run("partial", func(t *testing.T) {
		g := newFakeGateway(t)
		g.firstEvent = `{"type":"transcript-partial","text":"early"}`
		if _, err := testGatewayClient(g).open(context.Background()); err == nil || !strings.Contains(err.Error(), "stream-start") {
			t.Fatalf("open error = %v, want a missing stream-start", err)
		}
	})
	t.Run("not JSON", func(t *testing.T) {
		g := newFakeGateway(t)
		g.firstEvent = `<html>captive portal</html>`
		_, err := testGatewayClient(g).open(context.Background())
		if err == nil || !strings.Contains(err.Error(), "stream-start") || strings.Contains(err.Error(), "captive") {
			t.Fatalf("open error = %v, want a missing stream-start and none of the frame", err)
		}
	})
	t.Run("error event", func(t *testing.T) {
		g := newFakeGateway(t)
		g.firstEvent = `{"type":"error","error":{"message":"Unsupported inputAudioFormat","type":"invalid_request_error"}}`
		_, err := testGatewayClient(g).open(context.Background())
		var se *streamError
		if !errors.As(err, &se) || se.Service != streamGateway || se.Fatal() {
			t.Fatalf("open error = %#v, want the gateway's error event", err)
		}
		if got := se.Error(); got != "gateway: Unsupported inputAudioFormat [invalid_request_error]" {
			t.Errorf("Error() = %q", got)
		}
	})
}

// Only a handful of stream-start frames have ever been seen. One whose
// warnings are not the usual array must not fail the dial.
func TestGatewayOpenToleratesUnfamiliarStreamStart(t *testing.T) {
	logs := captureLogs(t)
	g := newFakeGateway(t)
	// A real line break inside the raw value, and the key quoted back.
	g.firstEvent = "{\"type\":\"stream-start\",\"warnings\":{\"note\":\"an object\",\n\"echo\":\"vck-test-key\"},\"extra\":[1,2]}"
	conn, err := testGatewayClient(g).open(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	conn.closeNow()
	got := logs.String()
	if !strings.Contains(got, "gateway warnings:") || !strings.Contains(got, "an object") {
		t.Errorf("the warnings were not logged:\n%s", got)
	}
	if strings.Contains(got, "vck-test-key") || strings.Count(got, "\n") != 1 {
		t.Errorf("the warnings reached the log unscrubbed:\n%s", got)
	}
}

// A refused handshake is how the gateway says the account is not set up, and
// only its body tells that from a wrong key.
func TestGatewayOpenReportsRefusal(t *testing.T) {
	logs := captureLogs(t)
	g := newFakeGateway(t)
	g.handshake = http.StatusForbidden
	g.refusal = `{"error":{"message":"AI Gateway requires a valid credit card on file to service requests.","type":"customer_verification_required"}}`

	_, err := testGatewayClient(g).open(context.Background())
	var se *streamError
	if !errors.As(err, &se) || se.Status != http.StatusForbidden || se.Service != streamGateway || !se.Fatal() {
		t.Fatalf("open error = %#v, want a fatal gateway 403", err)
	}
	if want := "AI Gateway requires a valid credit card on file to service requests. [customer_verification_required]"; se.Detail != want {
		t.Errorf("Detail = %q, want %q", se.Detail, want)
	}
	// The explanation is for the log; it stays out of what /stop returns.
	if got := se.Error(); got != "gateway handshake 403 Forbidden" {
		t.Errorf("Error() = %q", got)
	}

	logStreamFailure(err, "")
	for _, want := range []string{"gateway refused the stream (403): AI Gateway requires a valid credit card", "AI_GATEWAY_API_KEY", "vercel_api_key"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log lacks %q:\n%s", want, logs.String())
		}
	}
}

// The body is a third party's text, or a proxy's: whatever it is, the key, a
// line break and more than a few hundred bytes of it stay out of the log.
func TestGatewayRefusalIsScrubbed(t *testing.T) {
	g := newFakeGateway(t)
	g.handshake = http.StatusTooManyRequests
	g.refusal = "<html>rate limited for key\nvck-test-key " + strings.Repeat("x", 2000)

	_, err := testGatewayClient(g).open(context.Background())
	var se *streamError
	if !errors.As(err, &se) || se.Status != http.StatusTooManyRequests || se.Fatal() {
		t.Fatalf("open error = %#v, want a retryable gateway 429", err)
	}
	if strings.Contains(se.Detail, "vck-test-key") || !strings.Contains(se.Detail, "rate limited for key <key>") {
		t.Errorf("Detail kept the key, or a line break: %q", se.Detail)
	}
	if len(se.Detail) > streamMessageMax+len("...") {
		t.Errorf("Detail is %d bytes, want at most %d", len(se.Detail), streamMessageMax)
	}
}

// Go drops Authorization when a redirect leaves the host, but not the
// subprotocol header the gateway key travels in, so no redirect is followed.
func TestGatewayDoesNotFollowRedirects(t *testing.T) {
	elsewhere := newFakeGateway(t)
	g := newFakeGateway(t)
	g.handshake = http.StatusTemporaryRedirect
	g.redirect = elsewhere.srv.URL + "/v4/ai/transcription-model"

	_, err := testGatewayClient(g).open(context.Background())
	var se *streamError
	if !errors.As(err, &se) || se.Status != http.StatusTemporaryRedirect {
		t.Fatalf("open error = %#v, want the redirect reported as a refused handshake", err)
	}
	if n := elsewhere.dialCount(); n != 0 {
		t.Errorf("the redirect was followed %d times, key and all", n)
	}
}

func TestGatewayLogsDoNotExposeKey(t *testing.T) {
	logs := captureLogs(t)
	const key = "vck-secret-key-sentinel"
	c := newStreamClient(key, testSpec(gatewayModelID), nil)
	c.gatewayEndpoint = "ws://127.0.0.1:1/v4/ai/transcription-model"
	_, err := c.open(context.Background())
	if err == nil {
		t.Fatal("dial to a closed port succeeded")
	}
	logStreamFailure(err, "")
	for _, where := range []string{logs.String(), err.Error()} {
		if strings.Contains(where, key) {
			t.Errorf("the key is exposed in: %s", where)
		}
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("dial error lost its cause: %v", err)
	}
}

func gatewayDelta(text string) gatewayEvent {
	return gatewayEvent{Type: "transcript-delta", Delta: text}
}

func gatewayPartial(text string) gatewayEvent {
	return gatewayEvent{Type: "transcript-partial", Text: text}
}

func gatewayFinal(text string) gatewayEvent {
	return gatewayEvent{Type: "transcript-final", Text: text}
}

type gatewayStep struct {
	ev     gatewayEvent
	text   string
	stable string // the stable prefix
}

func runGatewayAssembler(t *testing.T, steps []gatewayStep) *gatewayAssembler {
	t.Helper()
	a := &gatewayAssembler{}
	var prev liveText
	for i, step := range steps {
		if a.apply(step.ev) {
			t.Errorf("step %d (%+v): reported a divergence", i, step.ev)
		}
		got := a.current()
		if got.text != step.text {
			t.Errorf("step %d (%+v): text = %q, want %q", i, step.ev, got.text, step.text)
		}
		if stable := string([]rune(got.text)[:got.stable]); stable != step.stable {
			t.Errorf("step %d (%+v): stable = %q, want %q", i, step.ev, stable, step.stable)
		}
		checkStable(t, i, prev, got)
		prev = got
	}
	return a
}

// The opening of a recorded MAI streaming session, frame for frame.
func TestGatewayAssemblerRecordedSession(t *testing.T) {
	runGatewayAssembler(t, []gatewayStep{
		{gatewayPartial(""), "", ""},
		{gatewayPartial("Hey, quick."), "Hey, quick.", ""},
		{gatewayPartial("Hey, quick update."), "Hey, quick update.", ""},
		{gatewayPartial("Hey, quick update on the dictation project."), "Hey, quick update on the dictation project.", ""},
		// A delta settles words the tail was showing; nothing on screen moves.
		{gatewayDelta("Hey, quick update"), "Hey, quick update on the dictation project.", "Hey, quick update"},
		// The tail guesses: "this." becomes "the streaming".
		{gatewayPartial(" on the dictation project. I tried this."), "Hey, quick update on the dictation project. I tried this.", "Hey, quick update"},
		{gatewayDelta(" on"), "Hey, quick update on the dictation project. I tried this.", "Hey, quick update on"},
		{gatewayPartial(" the dictation project. I tried the streaming"), "Hey, quick update on the dictation project. I tried the streaming", "Hey, quick update on"},
		{gatewayDelta(" the"), "Hey, quick update on the dictation project. I tried the streaming", "Hey, quick update on the"},
		{gatewayPartial(" dictation project. I tried the streaming trans."), "Hey, quick update on the dictation project. I tried the streaming trans.", "Hey, quick update on the"},
		{gatewayDelta(" dictation project"), "Hey, quick update on the dictation project. I tried the streaming trans.", "Hey, quick update on the dictation project"},
		{gatewayPartial(". I tried the streaming transcription from"), "Hey, quick update on the dictation project. I tried the streaming transcription from", "Hey, quick update on the dictation project"},
		// A delta can be punctuation alone.
		{gatewayDelta("."), "Hey, quick update on the dictation project. I tried the streaming transcription from", "Hey, quick update on the dictation project."},
		{gatewayPartial(" I tried the streaming transcription from XAI."), "Hey, quick update on the dictation project. I tried the streaming transcription from XAI.", "Hey, quick update on the dictation project."},
		{gatewayDelta(" I tried the streaming transcription from xAI."), "Hey, quick update on the dictation project. I tried the streaming transcription from xAI.", "Hey, quick update on the dictation project. I tried the streaming transcription from xAI."},
		{gatewayFinal("Hey, quick update on the dictation project. I tried the streaming transcription from xAI."), "Hey, quick update on the dictation project. I tried the streaming transcription from xAI.", "Hey, quick update on the dictation project. I tried the streaming transcription from xAI."},
	})
}

// A delta whose wording is not what the tail guessed leaves the tail stale;
// it is dropped until the next partial rather than shown after the delta.
func TestGatewayAssemblerDeltaReplacesWrongGuess(t *testing.T) {
	runGatewayAssembler(t, []gatewayStep{
		{gatewayPartial("I tried this."), "I tried this.", ""},
		{gatewayDelta("I tried the"), "I tried the", "I tried the"},
		{gatewayPartial(" streaming"), "I tried the streaming", "I tried the"},
	})
}

func TestGatewayAssemblerStretchesJoin(t *testing.T) {
	a := runGatewayAssembler(t, []gatewayStep{
		{gatewayPartial("First one."), "First one.", ""},
		{gatewayFinal("First one."), "First one.", "First one."},
		// An empty final, as a flush during silence sends, changes nothing.
		{gatewayPartial(""), "First one.", "First one."},
		{gatewayFinal(""), "First one.", "First one."},
		// The separator before a new stretch belongs to the tail.
		{gatewayPartial("Second"), "First one. Second", "First one."},
		{gatewayDelta("Second"), "First one. Second", "First one. Second"},
		{gatewayPartial(" one."), "First one. Second one.", "First one. Second"},
		// A stretch that opens with punctuation joins without a space.
		{gatewayFinal("Second one."), "First one. Second one.", "First one. Second one."},
		{gatewayPartial(", and more"), "First one. Second one., and more", "First one. Second one."},
	})
	if a.closed != "First one. Second one." {
		t.Errorf("closed = %q", a.closed)
	}
}

// Pieces carry their own spaces and can end mid-word, so they are joined as
// sent; only runs of whitespace are folded.
func TestGatewayAssemblerKeepsPiecesAsSent(t *testing.T) {
	runGatewayAssembler(t, []gatewayStep{
		{gatewayDelta(" Hey"), "Hey", "Hey"},
		{gatewayDelta(","), "Hey,", "Hey,"},
		{gatewayDelta(" dict"), "Hey, dict", "Hey, dict"},
		{gatewayDelta("ation "), "Hey, dictation", "Hey, dictation"},
		{gatewayPartial(" project\n“now”"), "Hey, dictation project \"now\"", "Hey, dictation"},
	})
}

// The stable count is taken on the text as typed: a settled rune that types as
// three characters counts as three, and one that types as none as none.
func TestGatewayAssemblerCountsStableRunesAsTyped(t *testing.T) {
	runGatewayAssembler(t, []gatewayStep{
		{gatewayDelta("Café costs €5"), "Cafe costs EUR5", "Cafe costs EUR5"},
		{gatewayPartial(" at 20°…"), "Cafe costs EUR5 at 20 degrees...", "Cafe costs EUR5"},
		{gatewayDelta(" at 20°"), "Cafe costs EUR5 at 20 degrees...", "Cafe costs EUR5 at 20 degrees"},
		{gatewayDelta("\u200b🙂"), "Cafe costs EUR5 at 20 degrees?", "Cafe costs EUR5 at 20 degrees?"},
	})
}

// No final has ever disagreed with settled text. If one does, the stretch
// closes as it stands on screen: the typer was promised the settled words, and
// erasing the tail beside them would lose words that had been shown.
func TestGatewayAssemblerFinalCannotRewriteSettledText(t *testing.T) {
	logs := captureLogs(t)
	p := &gatewayProtocol{}
	feed := func(ev gatewayEvent) liveText {
		t.Helper()
		up, ok := p.decode(mustJSON(t, map[string]any{"type": ev.Type, "text": ev.Text, "delta": ev.Delta}))
		if !ok || up.kind != updateText {
			t.Fatalf("decode(%+v) = %+v, %t", ev, up, ok)
		}
		return up.text
	}
	feed(gatewayDelta("We could switch the default"))
	before := feed(gatewayPartial(" and keep Grok as the fallback"))
	after := feed(gatewayFinal("We could switch to default and keep Grok as the fallback."))
	if after.text != before.text || after.stable != len(after.text) {
		t.Errorf("after a disagreeing final: %+v, want everything on screen kept: %q", after, before.text)
	}
	checkStable(t, 0, before, after)
	if !strings.Contains(logs.String(), "disagreed with text the gateway had settled") {
		t.Errorf("the divergence was not logged:\n%s", logs.String())
	}

	// The next stretch carries on after it, and the warning is not repeated.
	feed(gatewayPartial("Next."))
	next := feed(gatewayFinal("Next."))
	if next.text != "We could switch the default and keep Grok as the fallback Next." {
		t.Errorf("text = %q", next.text)
	}
	feed(gatewayDelta("One"))
	feed(gatewayFinal("Two"))
	if n := strings.Count(logs.String(), "disagreed"); n != 1 {
		t.Errorf("the divergence was logged %d times, want once", n)
	}
}

func TestGatewayProtocolDecodes(t *testing.T) {
	logs := captureLogs(t)
	p := &gatewayProtocol{}
	for _, tc := range []struct {
		frame string
		want  streamUpdate
	}{
		{`{"type":"stream-start","warnings":[]}`, streamUpdate{}},
		{`{"type":"transcript-partial","id":"item_1","text":"Hello."}`, streamUpdate{kind: updateText, text: liveText{text: "Hello."}}},
		{`{"type":"transcript-delta","id":"item_1","delta":"Hello"}`, streamUpdate{kind: updateText, text: liveText{text: "Hello.", stable: 5}}},
		{`{"type":"raw","rawValue":{}}`, streamUpdate{}},
		{`{"type":"transcript-final","id":"item_1","text":"Hello."}`, streamUpdate{kind: updateText, text: liveText{text: "Hello.", stable: 6}}},
		{`{"type":"finish","text":"Hello.","segments":[],"language":"en","durationInSeconds":1.5}`, streamUpdate{kind: updateDone}},
		{`{"type":"error","error":{"message":"upstream unavailable","type":"api_error"}}`, streamUpdate{kind: updateError, message: "upstream unavailable [api_error]"}},
		{`{"type":"error","error":{"type":"api_error"}}`, streamUpdate{kind: updateError, message: "api_error"}},
	} {
		got, ok := p.decode([]byte(tc.frame))
		if !ok || got != tc.want {
			t.Errorf("decode(%s) = %+v, %t; want %+v", tc.frame, got, ok, tc.want)
		}
	}
	if logs.String() != "" {
		t.Errorf("a finish that matches the screen logged:\n%s", logs.String())
	}

	// finish is only the end signal, but text that does not match what was
	// typed means the assembly has drifted from the service.
	if up, ok := p.decode([]byte(`{"type":"finish","text":"Hello there."}`)); !ok || up.kind != updateDone {
		t.Fatalf("decode(finish) = %+v, %t", up, ok)
	}
	if !strings.Contains(logs.String(), "closing transcript (12 chars) differs from the text typed (6 chars)") {
		t.Errorf("a finish that differs from the screen did not log:\n%s", logs.String())
	}
}

// The protocol is known from a few sessions. A frame in a shape not seen yet
// costs that frame's detail at most, never the dictation.
func TestGatewayProtocolToleratesUnfamiliarShapes(t *testing.T) {
	p := &gatewayProtocol{}
	for _, tc := range []struct {
		frame string
		ok    bool
		want  streamUpdate
	}{
		{`{"type":"error","error":"a plain string"}`, true, streamUpdate{kind: updateError, message: "a plain string"}},
		{`{"type":"error"}`, true, streamUpdate{kind: updateError, message: "an error without a message"}},
		{`{"type":"error","error":{"message":42}}`, true, streamUpdate{kind: updateError, message: "an error without a message"}},
		{`{"type":"ping","warnings":{"not":"an array"}}`, true, streamUpdate{}},
		// A field of the wrong type reads as empty; the frame still counts.
		{`{"type":"transcript-partial","text":123}`, true, streamUpdate{kind: updateText}},
		{`{"type":"transcript-delta","delta":null}`, true, streamUpdate{kind: updateText}},
		{`{"type":"finish","text":["a","b"]}`, true, streamUpdate{kind: updateDone}},
		{`not json at all`, false, streamUpdate{}},
		{``, false, streamUpdate{}},
	} {
		got, ok := p.decode([]byte(tc.frame))
		if ok != tc.ok || got != tc.want {
			t.Errorf("decode(%s) = %+v, %t; want %+v, %t", tc.frame, got, ok, tc.want, tc.ok)
		}
	}
}

// MAI scored the same with and without gain control, so the gateway gets the
// recording as it is -- followed by the second of silence it needs to finish
// the last word, and only then the end of the audio.
func TestGatewaySessionSendsRawAudioThenSilenceThenDone(t *testing.T) {
	h, g := newGatewayHarness(t)
	// Quiet speech: the AGC would raise it.
	quiet := concat(silence(0.3, 15), speech(0.75, 300))
	h.rec.feed(quiet)
	h.start()
	if _, streamErr, typeErr := h.stop(t); streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}

	msgs := g.messages()
	var audio []byte
	for i, m := range msgs {
		switch {
		case m.binary && m.beforeCreated:
			t.Fatal("audio was written before stream-start")
		case m.binary:
			audio = append(audio, m.data...)
			if len(m.data) > streamChunkBytes {
				t.Errorf("frame %d is %d bytes, want at most %d", i, len(m.data), streamChunkBytes)
			}
		case i != 0 && i != len(msgs)-1:
			t.Errorf("text frame %d of %d: %s", i, len(msgs), m.text)
		}
	}
	if last := msgs[len(msgs)-1]; last.binary || last.text != `{"type":"transcription-stream.audio-done"}` {
		t.Errorf("last frame = %+v, want audio-done", last)
	}
	speechBytes, tail := pcm16(quiet), int(gatewayTailSilence.Seconds()*wavSampleRate*2)
	if len(audio) != len(speechBytes)+tail {
		t.Fatalf("server received %d bytes, want the %d recorded and %d of silence", len(audio), len(speechBytes), tail)
	}
	if !bytes.Equal(audio[:len(speechBytes)], speechBytes) {
		t.Error("the recording was altered on its way to the gateway")
	}
	if !bytes.Equal(audio[len(speechBytes):], make([]byte, tail)) {
		t.Error("the tail is not silence")
	}
	if got, want := h.s.stats().audioSeconds, wavDurationSeconds(quiet); got != want {
		t.Errorf("audio sent = %.2fs, want the %.2fs recorded, silence not counted", got, want)
	}
}

func TestGatewaySessionTypesTheGuessAndRevisesIt(t *testing.T) {
	h, g := newGatewayHarness(t)
	h.start()
	h.waitReady(t)
	h.rec.feed(speech(0.2, 5000))

	g.partial("I tried this.")
	h.waitScreen(t, "I tried this.")
	g.delta("I tried")
	g.partial(" the streaming.")
	h.waitScreen(t, "I tried the streaming.")
	g.delta(" the streaming")
	g.partial(" version")
	h.waitScreen(t, "I tried the streaming version")

	// finish is the end: the gateway leaves the socket open, and a stop that
	// waited for it to close would run into the done timeout and fail.
	text, streamErr, typeErr := h.stop(t)
	if streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}
	if text != "I tried the streaming version" || h.screen() != text {
		t.Errorf("text = %q, screen = %q", text, h.screen())
	}
	// Only the guessed tail was ever retyped: "this." and the moving period.
	if st := h.s.stats(); st.maxRewrite > len("this.") {
		t.Errorf("max rewrite = %d characters, want the guess only", st.maxRewrite)
	}
}

func TestGatewayErrorWhileRecordingIsFailure(t *testing.T) {
	h, g := newGatewayHarness(t)
	h.start()
	h.waitReady(t)
	g.partial("typed so far")
	h.waitScreen(t, "typed so far")

	g.events <- `{"type":"error","error":{"message":"upstream unavailable","type":"api_error"}}`
	waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
	text, streamErr, typeErr := h.stop(t)
	var se *streamError
	if !errors.As(streamErr, &se) || se.Message != "upstream unavailable [api_error]" || se.Service != streamGateway || typeErr != nil {
		t.Errorf("finish = stream %#v, typing %v", streamErr, typeErr)
	}
	if text != "typed so far" {
		t.Errorf("text = %q", text)
	}
}

func TestGatewayUnsolicitedFinishWhileRecordingIsFailure(t *testing.T) {
	h, g := newGatewayHarness(t)
	h.start()
	h.waitReady(t)
	g.events <- `{"type":"finish","text":""}`
	waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
	if got := h.s.failure().Error(); got != "gateway ended the stream while recording" {
		t.Errorf("failure = %q", got)
	}
}

// A stop the gateway never answers ends, bounded, as a failure: what was on
// screen stays, but the last words may never have been transcribed.
func TestGatewayMissingFinishIsFailure(t *testing.T) {
	h, g := newGatewayHarness(t)
	g.ignoreDone = true
	h.client.doneTimeout = 50 * time.Millisecond
	h.start()
	h.waitReady(t)
	g.partial("kept")
	h.waitScreen(t, "kept")

	text, streamErr, typeErr := h.stop(t)
	if streamErr == nil || !strings.Contains(streamErr.Error(), "gateway did not finish within 50ms") || typeErr != nil {
		t.Errorf("finish = stream %v, typing %v; want the missing finish reported", streamErr, typeErr)
	}
	if text != "kept" || h.screen() != "kept" {
		t.Errorf("text %q, screen %q", text, h.screen())
	}
}

// A link that stops carrying data passes no write and fails none: the stop
// would wait on the upload for as long as the kernel is willing to.
func TestStreamStopBoundsStalledUpload(t *testing.T) {
	h, g := newGatewayHarness(t)
	g.deaf.Store(true)
	h.client.flushTimeout = 100 * time.Millisecond
	h.client.writeTimeout = time.Hour
	h.client.maxBacklog = 1 << 40
	h.start()
	h.waitReady(t)
	// Far more than the socket buffers of both ends can hold.
	h.rec.feed(make([]int16, 8<<20))

	finished := make(chan error, 1)
	go func() {
		_, streamErr, _ := h.stop(t)
		finished <- streamErr
	}()
	select {
	case streamErr := <-finished:
		if streamErr == nil || !strings.Contains(streamErr.Error(), "did not upload within 100ms") {
			t.Errorf("stream error = %v, want the stalled upload reported", streamErr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the stop is still waiting on a stalled upload")
	}
}
