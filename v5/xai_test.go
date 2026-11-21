package voicetype

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeMessage is one frame the client sent to fakeXAI.
type fakeMessage struct {
	binary bool
	text   string // text frames
	data   []byte // binary frames
	// beforeCreated marks a frame that arrived before transcript.created was
	// sent, which the real server would not accept.
	beforeCreated bool
}

// fakeXAI is the xAI streaming endpoint as the probes observed it: it greets
// with transcript.created, forwards the events a test pushes, and answers
// audio.done with the open utterance as a chunk final + utterance final pair,
// then transcript.done, then a normal close.
type fakeXAI struct {
	srv *httptest.Server

	// Set before the client dials.
	handshake    int           // HTTP status instead of upgrading; 0 = upgrade
	createdDelay time.Duration // before transcript.created; very long = hang
	firstEvent   *xaiEvent     // sent instead of transcript.created
	ignoreDone   bool          // never answer audio.done
	doneDelay    time.Duration // before answering audio.done
	doneReply    []xaiEvent    // sent instead of the usual answer to audio.done

	events   chan xaiEvent // the test pushes server events; forwarded in order
	raw      chan string   // frames sent exactly as given
	closeCmd chan struct{} // the server closes: normally, unless closeStatus says otherwise
	dropCmd  chan struct{} // the server drops the connection without a close frame
	closed   chan struct{} // the first client connection ended

	closeStatus websocket.StatusCode // with closeCmd; 0 = normal closure
	closeReason string
	refusal     string // the body of a refused handshake

	closedOnce sync.Once
	mu         sync.Mutex
	received   []fakeMessage
	dials      int
	auth       string
	rawQuery   string
	created    bool
	open       string // the open utterance, echoed back on audio.done
}

func newFakeXAI(t *testing.T) *fakeXAI {
	t.Helper()
	f := &fakeXAI{
		events:   make(chan xaiEvent, 256),
		raw:      make(chan string, 16),
		closeCmd: make(chan struct{}, 1),
		dropCmd:  make(chan struct{}, 1),
		refusal:  `{"error":"rejected"}`,
		closed:   make(chan struct{}),
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeXAI) wsURL() string { return "ws" + strings.TrimPrefix(f.srv.URL, "http") + "/v1/stt" }

func (f *fakeXAI) partial(text string, isFinal, speechFinal bool) {
	f.events <- xaiEvent{Type: "transcript.partial", Text: text, IsFinal: isFinal, SpeechFinal: speechFinal}
}

func (f *fakeXAI) push(ev xaiEvent) { f.events <- ev }

func (f *fakeXAI) messages() []fakeMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.received)
}

// audioBytes is everything the client streamed, in order.
func (f *fakeXAI) audioBytes() []byte {
	var out []byte
	for _, m := range f.messages() {
		if m.binary {
			out = append(out, m.data...)
		}
	}
	return out
}

func (f *fakeXAI) texts() []string {
	var out []string
	for _, m := range f.messages() {
		if !m.binary {
			out = append(out, m.text)
		}
	}
	return out
}

func (f *fakeXAI) dialCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials
}

func (f *fakeXAI) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.dials++
	f.auth = r.Header.Get("Authorization")
	f.rawQuery = r.URL.RawQuery
	f.mu.Unlock()
	if f.handshake != 0 {
		http.Error(w, f.refusal, f.handshake)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	doneSeen := make(chan struct{}, 1)
	readerGone := make(chan struct{})
	go func() {
		defer close(readerGone)
		defer f.closedOnce.Do(func() { close(f.closed) })
		for {
			typ, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			f.mu.Lock()
			msg := fakeMessage{binary: typ == websocket.MessageBinary, beforeCreated: !f.created}
			if msg.binary {
				msg.data = data
			} else {
				msg.text = string(data)
			}
			f.received = append(f.received, msg)
			f.mu.Unlock()
			if !msg.binary && strings.Contains(msg.text, `"audio.done"`) {
				select {
				case doneSeen <- struct{}{}:
				default:
				}
			}
		}
	}()

	write := func(ev xaiEvent) error {
		data, _ := json.Marshal(ev)
		return c.Write(ctx, websocket.MessageText, data)
	}
	wait := func(d time.Duration) bool {
		select {
		case <-time.After(d):
			return true
		case <-readerGone:
			return false
		}
	}

	if !wait(f.createdDelay) {
		return
	}
	first := xaiEvent{Type: "transcript.created"}
	if f.firstEvent != nil {
		first = *f.firstEvent
	}
	f.mu.Lock()
	f.created = true
	f.mu.Unlock()
	if write(first) != nil {
		return
	}

	forward := func(ev xaiEvent) error {
		f.mu.Lock()
		switch {
		case ev.utteranceFinal():
			f.open = ""
		case ev.Type == "transcript.partial" && ev.Text != "":
			f.open = ev.Text
		}
		f.mu.Unlock()
		return write(ev)
	}

	for {
		select {
		case ev := <-f.events:
			if forward(ev) != nil {
				return
			}
		case frame := <-f.raw:
			if c.Write(ctx, websocket.MessageText, []byte(frame)) != nil {
				return
			}
		case <-f.closeCmd:
			status := websocket.StatusNormalClosure
			if f.closeStatus != 0 {
				status = f.closeStatus
			}
			c.Close(status, f.closeReason)
			return
		case <-f.dropCmd:
			return // the deferred CloseNow drops the connection
		case <-doneSeen:
			if f.ignoreDone || !wait(f.doneDelay) {
				continue
			}
			// Events the test queued were in flight before the client's audio.done.
			for queued := true; queued; {
				select {
				case ev := <-f.events:
					if forward(ev) != nil {
						return
					}
				default:
					queued = false
				}
			}
			f.mu.Lock()
			open := f.open
			f.mu.Unlock()
			reply := f.doneReply
			if reply == nil {
				reply = []xaiEvent{
					{Type: "transcript.partial", Text: open, IsFinal: true},
					{Type: "transcript.partial", Text: open, IsFinal: true, SpeechFinal: true},
					{Type: "transcript.done"},
				}
			}
			for _, ev := range reply {
				if write(ev) != nil {
					return
				}
			}
			c.Close(websocket.StatusNormalClosure, "")
			return
		case <-readerGone:
			return
		}
	}
}

// waitUntil polls cond until it holds or a generous deadline passes.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

const xaiModelID = "grok-voice-transcribe-2.0"

func testStreamClient(f *fakeXAI, vocabulary ...string) *streamClient {
	c := newStreamClient("test-xai-key", testSpec(xaiModelID), vocabulary)
	c.endpoint = f.wsURL()
	return c
}

func TestStreamURLAndAuth(t *testing.T) {
	f := newFakeXAI(t)
	c := testStreamClient(f, "dotool", "Pipe Wire", "C++")
	conn, err := c.open(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	conn.closeNow()

	f.mu.Lock()
	auth, raw := f.auth, f.rawQuery
	f.mu.Unlock()
	if auth != "Bearer test-xai-key" {
		t.Errorf("Authorization = %q", auth)
	}
	if strings.Contains(raw, "test-xai-key") {
		t.Errorf("the key reached the URL: %s", raw)
	}
	if !strings.Contains(raw, "keyterm=Pipe+Wire") {
		t.Errorf("a space in a keyterm is not encoded as +: %s", raw)
	}
	query, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := url.Values{
		"model":           {"grok-voice-transcribe-2.0"},
		"sample_rate":     {"16000"},
		"encoding":        {"pcm"},
		"interim_results": {"true"},
		"language":        {"en"},
		"keyterm":         {"dotool", "Pipe Wire", "C++"},
	}
	if !reflect.DeepEqual(query, want) {
		t.Errorf("query = %v, want %v", query, want)
	}
}

func TestStreamKeytermsLimits(t *testing.T) {
	lastWarning = sync.Map{}
	t.Cleanup(func() { lastWarning = sync.Map{} })

	var vocabulary []string
	for i := range 120 {
		vocabulary = append(vocabulary, fmt.Sprintf("term%03d", i))
	}
	long := strings.Repeat("x", xaiKeytermMaxLen+1)
	vocabulary = append(vocabulary[:10], append([]string{long}, vocabulary[10:]...)...)
	exact := strings.Repeat("y", xaiKeytermMaxLen)
	vocabulary = append(vocabulary, exact)

	got := streamKeyterms(vocabulary)
	if len(got) != xaiKeytermMax {
		t.Fatalf("kept %d terms, want %d", len(got), xaiKeytermMax)
	}
	// The workspace comes last in vocabularyFor, so the tail is what survives.
	if got[len(got)-1] != exact || got[0] != "term021" {
		t.Errorf("kept %q ... %q, want term021 ... the 50-character term", got[0], got[len(got)-1])
	}
	if slices.Contains(got, long) {
		t.Error("a term over 50 characters was kept")
	}
	if _, warned := lastWarning.Load("keyterm"); !warned {
		t.Error("truncation did not warn")
	}

	if got := streamKeyterms([]string{"a", "b"}); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("short list changed: %q", got)
	}
	if _, warned := lastWarning.Load("keyterm"); warned {
		t.Error("a list within the limits did not clear the warning")
	}
}

func TestStreamOpenWaitsForCreated(t *testing.T) {
	f := newFakeXAI(t)
	f.createdDelay = 150 * time.Millisecond
	c := testStreamClient(f)

	started := time.Now()
	conn, err := c.open(context.Background())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.closeNow()
	if elapsed := time.Since(started); elapsed < f.createdDelay {
		t.Errorf("open returned after %v, before transcript.created", elapsed)
	}
	if err := conn.writeAudio(context.Background(), []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the frame", func() bool { return len(f.messages()) == 1 })
	if f.messages()[0].beforeCreated {
		t.Error("audio reached the server before transcript.created")
	}
}

func TestStreamOpenRejectsBadFirstEvent(t *testing.T) {
	t.Run("partial", func(t *testing.T) {
		f := newFakeXAI(t)
		f.firstEvent = &xaiEvent{Type: "transcript.partial", Text: "early"}
		if _, err := testStreamClient(f).open(context.Background()); err == nil || !strings.Contains(err.Error(), "transcript.created") {
			t.Fatalf("open error = %v, want a missing transcript.created", err)
		}
	})
	t.Run("error event", func(t *testing.T) {
		f := newFakeXAI(t)
		f.firstEvent = &xaiEvent{Type: "error", Message: "model unavailable"}
		_, err := testStreamClient(f).open(context.Background())
		var se *streamError
		if !errors.As(err, &se) || se.Message != "model unavailable" || se.Fatal() {
			t.Fatalf("open error = %#v, want the server's error event", err)
		}
	})
}

func TestStreamOpenReportsHandshakeStatus(t *testing.T) {
	for _, tc := range []struct {
		status int
		fatal  bool
	}{
		{http.StatusUnauthorized, true},
		{http.StatusForbidden, true},
		{http.StatusServiceUnavailable, false},
	} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			f := newFakeXAI(t)
			f.handshake = tc.status
			_, err := testStreamClient(f).open(context.Background())
			var se *streamError
			if !errors.As(err, &se) || se.Status != tc.status {
				t.Fatalf("open error = %#v, want status %d", err, tc.status)
			}
			if se.Fatal() != tc.fatal {
				t.Errorf("Fatal() = %t, want %t", se.Fatal(), tc.fatal)
			}
		})
	}
}

func TestStreamOpenTimesOut(t *testing.T) {
	f := newFakeXAI(t)
	f.createdDelay = time.Hour
	c := testStreamClient(f)
	c.dialTimeout = 100 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := c.open(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("open succeeded without transcript.created")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("open is not bounded by the dial timeout")
	}
}

// xAI's refusal body is never read: an error from that endpoint quotes the
// request, keyterms and all, and nothing vouches for what else it holds.
func TestStreamRefusalBodyIsNeverEchoed(t *testing.T) {
	logs := captureLogs(t)
	f := newFakeXAI(t)
	f.handshake = http.StatusUnauthorized
	f.refusal = `{"error":"REFUSAL-BODY-SENTINEL for keyterm=dotool"}`
	_, err := testStreamClient(f, "dotool").open(context.Background())
	var se *streamError
	if !errors.As(err, &se) || se.Status != http.StatusUnauthorized || se.Service != streamXAI {
		t.Fatalf("open error = %#v", err)
	}
	logStreamFailure(err, "")
	for name, where := range map[string]string{"the error": err.Error(), "its detail": se.Detail, "the log": logs.String()} {
		if strings.Contains(where, "SENTINEL") || strings.Contains(where, "dotool") {
			t.Errorf("the refusal body reached %s: %s", name, where)
		}
	}
	if got := logs.String(); !strings.Contains(got, "authentication rejected (401)") || !strings.Contains(got, "XAI_API_KEY") {
		t.Errorf("the log lacks the key hint:\n%s", got)
	}
}

func TestStreamDoesNotFollowRedirects(t *testing.T) {
	elsewhere := newFakeXAI(t)
	redirect := httptest.NewServer(http.RedirectHandler(elsewhere.srv.URL+"/v1/stt", http.StatusTemporaryRedirect))
	t.Cleanup(redirect.Close)
	c := newStreamClient("test-xai-key", testSpec(xaiModelID), nil)
	c.endpoint = "ws" + strings.TrimPrefix(redirect.URL, "http") + "/v1/stt"

	_, err := c.open(context.Background())
	var se *streamError
	if !errors.As(err, &se) || se.Status != http.StatusTemporaryRedirect {
		t.Fatalf("open error = %#v, want the redirect reported as a refused handshake", err)
	}
	if n := elsewhere.dialCount(); n != 0 {
		t.Errorf("the redirect was followed %d times", n)
	}
}

// An error event's text is the service's own: scrubbed before it is kept.
func TestStreamOpenScrubsErrorEvent(t *testing.T) {
	f := newFakeXAI(t)
	f.firstEvent = &xaiEvent{Type: "error", Message: "bad key test-xai-key\nsecond line"}
	_, err := testStreamClient(f).open(context.Background())
	var se *streamError
	if !errors.As(err, &se) || se.Message != "bad key <key> second line" {
		t.Fatalf("open error = %#v, want the message scrubbed", err)
	}
}

func TestStreamReadMapsNormalCloseToEOF(t *testing.T) {
	f := newFakeXAI(t)
	conn, err := testStreamClient(f).open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.closeNow()
	f.closeCmd <- struct{}{}
	if _, err := conn.read(context.Background()); err != io.EOF {
		t.Errorf("read after a normal close = %v, want io.EOF", err)
	}
}

func TestStreamReadAssemblesEvents(t *testing.T) {
	f := newFakeXAI(t)
	conn, err := testStreamClient(f).open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.closeNow()

	f.partial("Hey", false, false)
	f.partial("Hey,", true, false)
	f.partial("Hey, there.", true, true)
	f.push(xaiEvent{Type: "error", Message: "boom"})

	var got []string
	for range 4 {
		up, err := conn.read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		switch up.kind {
		case updateText:
			got = append(got, fmt.Sprintf("text:%s|%d", up.text.text, up.text.stable))
		case updateError:
			got = append(got, "error:"+up.message)
		default:
			got = append(got, fmt.Sprintf("kind %d", up.kind))
		}
	}
	// Each message is already assembled: only the closed utterance is stable.
	want := []string{"text:Hey|0", "text:Hey,|0", "text:Hey, there.|11", "error:boom"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("updates = %q, want %q", got, want)
	}
}

// A frame in a shape not seen before costs that frame at most. One that is
// not JSON is skipped, and the log says so once, without its content.
func TestStreamReadToleratesUnfamiliarFrames(t *testing.T) {
	logs := captureLogs(t)
	p := &xaiProtocol{}
	for _, tc := range []struct {
		frame string
		ok    bool
		want  streamUpdate
	}{
		{`{"type":"transcript.partial","text":"a","is_final":"true"}`, true, streamUpdate{kind: updateText, text: liveText{text: "a"}}},
		{`{"type":"transcript.partial","text":null}`, true, streamUpdate{kind: updateText}},
		{`{"type":"error","message":{"code":1}}`, true, streamUpdate{kind: updateError}},
		{`{"type":"transcript.done","text":17}`, true, streamUpdate{kind: updateDone}},
		{`{"type":"ping"}`, true, streamUpdate{}},
		{`<html>`, false, streamUpdate{}},
	} {
		got, ok := p.decode([]byte(tc.frame))
		if ok != tc.ok || got != tc.want {
			t.Errorf("decode(%s) = %+v, %t; want %+v, %t", tc.frame, got, ok, tc.want, tc.ok)
		}
	}

	f := newFakeXAI(t)
	conn, err := testStreamClient(f).open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.closeNow()
	f.raw <- "PRIVATE-GARBAGE-1"
	f.raw <- "PRIVATE-GARBAGE-2"
	f.partial("still here", false, false)
	// The fake does not order its raw frames against its events.
	skipped, text := 0, ""
	for i := range 3 {
		up, err := conn.read(context.Background())
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if up == (streamUpdate{}) {
			skipped++
		} else {
			text = up.text.text
		}
	}
	if skipped != 2 || text != "still here" {
		t.Errorf("skipped %d frames and read %q; want the two that are not JSON skipped and the event read", skipped, text)
	}
	if got := logs.String(); strings.Count(got, "that is not JSON") != 1 || strings.Contains(got, "PRIVATE") {
		t.Errorf("want one log line about the skipped frames, without their content:\n%s", got)
	}
}

func TestStreamLogsDoNotExposeKeyOrVocabulary(t *testing.T) {
	lastWarning = sync.Map{}
	t.Cleanup(func() { lastWarning = sync.Map{} })
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })

	const key = "xai-secret-key-sentinel"
	terms := []string{"PrivateTermSentinel", "Another Private Term"}
	c := newStreamClient(key, testSpec(xaiModelID), terms)
	c.endpoint = "ws://127.0.0.1:1/v1/stt"
	_, err := c.open(context.Background())
	if err == nil {
		t.Fatal("dial to a closed port succeeded")
	}
	logStreamFailure(err, "")
	failure := err.Error()

	// The truncation warning names counts, never terms.
	var many []string
	for i := range xaiKeytermMax {
		many = append(many, fmt.Sprintf("filler%d", i))
	}
	streamKeyterms(append(append(many, terms...), strings.Repeat("LongPrivateTerm", 5)))

	for _, secret := range append([]string{key, "keyterm=", "LongPrivateTerm"}, terms...) {
		for _, where := range []string{logs.String(), failure} {
			if strings.Contains(where, secret) || strings.Contains(where, url.QueryEscape(secret)) {
				t.Errorf("%q exposed in: %s", secret, where)
			}
		}
	}
	if !strings.Contains(failure, "connection refused") {
		t.Errorf("dial error lost its cause: %s", failure)
	}
}
