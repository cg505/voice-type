package voicetype

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// streamHarness is a session wired to a fake recorder, keyboard and server.
type streamHarness struct {
	f      *fakeXAI
	rec    *fakeRecorder
	kb     *fakeKeyboard
	client *streamClient
	s      *streamSession
	ctx    context.Context
	cancel context.CancelFunc
}

func newStreamHarness(t *testing.T) *streamHarness {
	t.Helper()
	f := newFakeXAI(t)
	c := testStreamClient(f)
	c.stopSettle = 0
	c.doneTimeout = 2 * time.Second
	c.drainTimeout = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	h := &streamHarness{f: f, rec: &fakeRecorder{}, kb: newFakeKeyboard(), client: c, ctx: ctx, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		if h.s != nil {
			<-h.s.runDone
		}
	})
	return h
}

// start creates the session from the client as configured so far and runs it.
func (h *streamHarness) start() *streamSession {
	h.s = newStreamSession(h.client, h.rec, &Typer{kb: h.kb})
	go h.s.run(h.ctx)
	return h.s
}

func (h *streamHarness) waitReady(t *testing.T) {
	t.Helper()
	select {
	case <-h.s.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("the dial never resolved")
	}
}

// stop is the daemon's stop path: the recorder stops, then finish.
func (h *streamHarness) stop(t *testing.T) (string, error, error) {
	t.Helper()
	samples, err := h.rec.Stop()
	if err != nil {
		t.Fatal(err)
	}
	return h.s.finish(h.ctx, samples)
}

// finishWithin is stop, failed if it has not returned in a time no bounded
// stop comes near. A stop that hangs is the bug; how fast a healthy one runs
// is not this suite's business.
func (h *streamHarness) finishWithin(t *testing.T, limit time.Duration) (string, error, error) {
	t.Helper()
	type result struct {
		text               string
		streamErr, typeErr error
	}
	done := make(chan result, 1)
	samples, err := h.rec.Stop()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		text, streamErr, typeErr := h.s.finish(h.ctx, samples)
		done <- result{text, streamErr, typeErr}
	}()
	select {
	case r := <-done:
		return r.text, r.streamErr, r.typeErr
	case <-time.After(limit):
		t.Fatalf("finish had not returned after %v", limit)
		return "", nil, nil
	}
}

func (h *streamHarness) screen() string { return screenText(h.kb.snapshot()) }

func (h *streamHarness) waitScreen(t *testing.T, want string) {
	t.Helper()
	waitUntil(t, fmt.Sprintf("the screen to read %q", want), func() bool { return h.screen() == want })
}

func pcm16(samples []int16) []byte {
	out := make([]byte, 0, 2*len(samples))
	for _, v := range samples {
		out = binary.LittleEndian.AppendUint16(out, uint16(v))
	}
	return out
}

func TestStreamSessionSendsBacklogAfterHandshake(t *testing.T) {
	h := newStreamHarness(t)
	h.f.createdDelay = 200 * time.Millisecond
	// Loud speech: the AGC leaves it untouched, so the bytes must match.
	before, after := speech(0.5, 5000), speech(0.5, -5000)
	h.rec.feed(before)
	h.start()
	h.waitReady(t)
	h.rec.feed(after)

	if _, streamErr, typeErr := h.stop(t); streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}
	for _, m := range h.f.messages() {
		if m.beforeCreated {
			t.Fatal("audio was written before transcript.created")
		}
	}
	got, want := h.f.audioBytes(), pcm16(concat(before, after))
	if len(got) != 2*len(before)+2*len(after) || !bytes.Equal(got, want) {
		t.Errorf("server received %d bytes, want the %d fed, in order", len(got), len(want))
	}
}

func TestStreamSessionChunksAt100ms(t *testing.T) {
	h := newStreamHarness(t)
	quiet := concat(silence(0.3, 15), speech(0.75, 300))
	h.rec.feed(quiet)
	h.start()
	if _, streamErr, _ := h.stop(t); streamErr != nil {
		t.Fatal(streamErr)
	}

	var sizes []int
	for _, m := range h.f.messages() {
		if m.binary {
			sizes = append(sizes, len(m.data))
		}
	}
	if len(sizes) != 11 {
		t.Fatalf("frames = %v, want ten 100 ms chunks and the 50 ms remainder", sizes)
	}
	for _, n := range sizes[:10] {
		if n != streamChunkBytes {
			t.Fatalf("frames = %v, want %d bytes each", sizes, streamChunkBytes)
		}
	}
	if sizes[10] != streamChunkBytes/2 {
		t.Errorf("last frame = %d bytes, want the %d-byte remainder", sizes[10], streamChunkBytes/2)
	}

	// Quiet speech goes up levelled; the recording itself stays raw.
	got := h.f.audioBytes()
	if bytes.Equal(got, pcm16(quiet)) {
		t.Error("quiet speech was sent without gain")
	}
	sent := make([]int16, len(got)/2)
	for i := range sent {
		sent[i] = int16(binary.LittleEndian.Uint16(got[2*i:]))
	}
	if rms := frameRMS(sent[len(sent)-agcFrameSamples:]); rms < agcTargetRMS*0.9 {
		t.Errorf("sent speech RMS %.0f, want near %.0f", rms, agcTargetRMS)
	}
}

// The recording finish is handed is the one the batch fallback uploads and a
// failed dictation retains. The sender takes its audio from that very slice
// at the flush, and levels what it sends; it must level a copy.
func TestStreamFlushLeavesTheRecordingRaw(t *testing.T) {
	h := newStreamHarness(t)
	quiet := concat(silence(0.3, 15), speech(0.75, 300))
	recording := slices.Clone(quiet)
	h.start()
	// Nothing was fed, so every sample reaches the sender through finish.
	if _, streamErr, typeErr := h.s.finish(h.ctx, recording); streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}
	if got := h.f.audioBytes(); len(got) != 2*len(quiet) || bytes.Equal(got, pcm16(quiet)) {
		t.Fatalf("server received %d bytes, want all %d, levelled", len(got), 2*len(quiet))
	}
	if !slices.Equal(recording, quiet) {
		t.Error("levelling the stream rescaled the recording itself")
	}
}

// The same events as TestAssemblerBriefExample, through the server, typed.
func briefEvents() []xaiEvent {
	const (
		u2c1 = ", quick update on the dictation project."
		u2c2 = "I tried the streaming transcription from xAI today, and it was mostly fine, but it sometimes dropped entire sentences when I spoke quietly."
	)
	return []xaiEvent{
		interim("Hey"), chunkFinal("Hey"), utteranceFinal("Hey"),
		interim(", quick update on the."), interim(u2c1), chunkFinal(u2c1),
		interim("I tried the stream..."),
		interim("I tried the streaming transcription from"),
		interim("I tried the streaming transcription from xAI today, and it..."),
		interim("I tried the streaming transcription from xAI today, and it was mostly fine, but it sometimes dropped entire sentences when I"),
		chunkFinal(u2c2), utteranceFinal(u2c1 + " " + u2c2),
		interim("I think the laptop"),
		interim("I think the laptop microphone is just not very loud."),
	}
}

func TestStreamSessionTypesLiveAndRevises(t *testing.T) {
	h := newStreamHarness(t)
	h.start()
	h.waitReady(t)

	// The same states through a liveTyper directly, one at a time.
	reference := &liveTyper{typer: &Typer{kb: newFakeKeyboard()}}
	var asm xaiAssembler
	for _, ev := range briefEvents() {
		asm.apply(ev)
		want := asm.current()
		if err := reference.apply(context.Background(), want); err != nil {
			t.Fatal(err)
		}
		h.f.push(ev)
		h.waitScreen(t, want.text)
	}

	text, streamErr, typeErr := h.stop(t)
	if streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}
	if text != asm.current().text || h.screen() != text {
		t.Errorf("finish text %q, screen %q, want %q", text, h.screen(), asm.current().text)
	}
	stats := h.s.stats()
	if stats.backspaces != reference.backspaces || stats.backspaces == 0 {
		t.Errorf("backspaces = %d, want %d (as typed one state at a time)", stats.backspaces, reference.backspaces)
	}
}

// A keyboard slower than the events ends on the latest text, and never costs
// more backspaces than typing every state would. That states are actually
// skipped is TestLiveOutputCoalesces' to prove; here it depends on timing.
func TestStreamSessionSlowKeyboardEndsOnLatestText(t *testing.T) {
	h := newStreamHarness(t)
	h.kb.delay = time.Millisecond
	h.start()
	h.waitReady(t)

	reference := &liveTyper{typer: &Typer{kb: newFakeKeyboard()}}
	var last string
	for i := range 20 {
		last = fmt.Sprintf("counting %d", i*7)
		if err := reference.apply(context.Background(), liveText{text: last}); err != nil {
			t.Fatal(err)
		}
		h.f.partial(last, false, false)
	}

	text, streamErr, typeErr := h.stop(t)
	if streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}
	if text != last || h.screen() != last {
		t.Errorf("finish text %q, screen %q, want %q", text, h.screen(), last)
	}
	if got := h.s.stats().backspaces; got > reference.backspaces {
		t.Errorf("backspaces = %d, more than the %d of typing every state", got, reference.backspaces)
	}
}

func TestStreamFinishSendsAudioDoneAndWaitsForDone(t *testing.T) {
	h := newStreamHarness(t)
	h.rec.feed(speech(0.3, 5000))
	h.start()
	h.waitReady(t)
	h.f.partial("Hello there", false, false)
	h.waitScreen(t, "Hello there")

	text, streamErr, typeErr := h.stop(t)
	if streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}
	if text != "Hello there" {
		t.Errorf("text = %q", text)
	}
	msgs := h.f.messages()
	if last := msgs[len(msgs)-1]; last.binary || last.text != `{"type":"audio.done"}` {
		t.Errorf("last frame = %+v, want audio.done", last)
	}
	if texts := h.f.texts(); len(texts) != 1 {
		t.Errorf("control messages = %q, want audio.done alone (no finalize)", texts)
	}
	if h.s.stats().reply <= 0 {
		t.Error("finish returned before transcript.done")
	}
}

// A stop the service never answers is bounded, and it is a failure: what is on
// screen stays, but nobody can say the last words were transcribed.
func TestStreamMissingDoneIsFailure(t *testing.T) {
	logs := captureLogs(t)
	h := newStreamHarness(t)
	h.f.ignoreDone = true
	h.client.doneTimeout = 50 * time.Millisecond
	h.start()
	h.waitReady(t)
	h.f.partial("an interim that never got its final", false, false)
	h.waitScreen(t, "an interim that never got its final")

	text, streamErr, typeErr := h.finishWithin(t, 10*time.Second)
	if streamErr == nil || streamErr.Error() != "xai did not finish within 50ms of the end of the audio" || typeErr != nil {
		t.Errorf("finish = stream %v, typing %v; want the missing end reported", streamErr, typeErr)
	}
	if text != "an interim that never got its final" {
		t.Errorf("text = %q", text)
	}
	if got := logs.String(); !strings.Contains(got, "did not finish within 50ms") || !strings.Contains(got, "the last words may be missing") || strings.Contains(got, "still recording") {
		t.Errorf("the log does not say what happened at the stop:\n%s", got)
	}
	select {
	case <-h.f.closed:
	case <-time.After(time.Second):
		t.Error("the connection was not closed")
	}
}

func TestStreamFailureKeepsSessionOpen(t *testing.T) {
	h := newStreamHarness(t)
	h.start()
	h.waitReady(t)
	h.rec.feed(speech(0.2, 5000))
	h.f.partial("typed so far", false, false)
	h.waitScreen(t, "typed so far")

	h.f.push(xaiEvent{Type: "error", Message: "upstream unavailable"})
	waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
	select {
	case <-h.s.senderDone:
	case <-time.After(time.Second):
		t.Fatal("the sender kept running after a failure")
	}
	sent := len(h.f.audioBytes())
	h.rec.feed(speech(0.3, 5000))
	time.Sleep(3 * streamPollInterval)
	if got := len(h.f.audioBytes()); got != sent {
		t.Errorf("%d bytes were sent after the failure", got-sent)
	}

	if h.rec.stopCalls.Load() != 0 {
		t.Error("the failure stopped the recorder")
	}

	// The stream is already down: the stop has nothing to wait for.
	h.client.flushTimeout, h.client.doneTimeout = time.Hour, time.Hour
	text, streamErr, typeErr := h.finishWithin(t, 10*time.Second)
	var se *streamError
	if !errors.As(streamErr, &se) || se.Message != "upstream unavailable" || typeErr != nil {
		t.Errorf("finish = stream %v, typing %v", streamErr, typeErr)
	}
	if text != "typed so far" {
		t.Errorf("text = %q", text)
	}
}

// A transcript.done nobody asked for ends the transcription while the user is
// still talking. Treated as the end, it would leave the sender uploading audio
// nobody transcribes and the stop reporting success with the text cut short.
func TestStreamUnsolicitedDoneWhileRecordingIsFailure(t *testing.T) {
	h := newStreamHarness(t)
	h.start()
	h.waitReady(t)
	h.rec.feed(speech(0.2, 5000))
	h.f.partial("typed so far", false, false)
	h.waitScreen(t, "typed so far")

	h.f.push(xaiEvent{Type: "transcript.done"})
	waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
	select {
	case <-h.s.senderDone:
	case <-time.After(time.Second):
		t.Fatal("the sender kept streaming after the server ended the transcription")
	}

	text, streamErr, typeErr := h.stop(t)
	if streamErr == nil || !strings.Contains(streamErr.Error(), "while recording") || typeErr != nil {
		t.Errorf("finish = stream %v, typing %v; want the early end reported", streamErr, typeErr)
	}
	if text != "typed so far" {
		t.Errorf("text = %q", text)
	}
}

// An error in answer to the end of the audio means the service never gave its
// last word. The text stays, and the stop reports the error.
func TestStreamErrorAfterStopIsFailure(t *testing.T) {
	logs := captureLogs(t)
	h := newStreamHarness(t)
	h.f.doneReply = []xaiEvent{{Type: "error", Message: "late hiccup"}}
	h.start()
	h.waitReady(t)
	h.f.partial("landed before the stop", false, false)
	h.waitScreen(t, "landed before the stop")

	text, streamErr, typeErr := h.stop(t)
	var se *streamError
	if !errors.As(streamErr, &se) || se.Message != "late hiccup" || typeErr != nil {
		t.Errorf("finish = stream %v, typing %v; want the late error reported", streamErr, typeErr)
	}
	if text != "landed before the stop" || h.screen() != text {
		t.Errorf("text %q, screen %q", text, h.screen())
	}
	got := logs.String()
	if !strings.Contains(got, "server error: late hiccup -- the last words may be missing") {
		t.Errorf("the late error was not logged as one at the stop:\n%s", got)
	}
	if strings.Contains(got, "still recording") {
		t.Errorf("the late error was logged as a mid-session failure:\n%s", got)
	}
}

// A normal close in place of the last word is no better than silence.
func TestStreamCloseAfterStopWithoutDoneIsFailure(t *testing.T) {
	h := newStreamHarness(t)
	h.f.doneReply = []xaiEvent{} // answer audio.done with nothing, then close
	h.start()
	h.waitReady(t)
	h.f.partial("First sentence.", true, true)
	h.waitScreen(t, "First sentence.")

	text, streamErr, typeErr := h.stop(t)
	if streamErr == nil || streamErr.Error() != "xai closed the stream before its last word" || typeErr != nil {
		t.Errorf("finish = stream %v, typing %v", streamErr, typeErr)
	}
	if text != "First sentence." {
		t.Errorf("text = %q", text)
	}
}

// From the stop press the screen holds still. Text that was in flight lands
// with the service's last word, not on a keyboard whose stop key is down.
func TestStreamHoldsTextFromStopUntilTheLastWord(t *testing.T) {
	h := newStreamHarness(t)
	h.f.doneDelay = 300 * time.Millisecond
	h.client.stopSettle = 100 * time.Millisecond
	h.start()
	h.waitReady(t)
	h.f.partial("typed before", false, false)
	h.waitScreen(t, "typed before")

	type result struct {
		text string
		err  error
	}
	finished := make(chan result, 1)
	samples, _ := h.rec.Stop()
	go func() {
		text, streamErr, _ := h.s.finish(h.ctx, samples)
		finished <- result{text, streamErr}
	}()
	waitUntil(t, "the stop to be requested", h.s.stopping)
	h.f.partial("typed before, and in flight at the stop", false, false)
	// The server holds its answer back, so while audio.done is its newest
	// frame nothing sent after the stop can have been released to the typer.
	waitUntil(t, "audio.done", func() bool { return len(h.f.texts()) == 1 })
	if got := h.screen(); got != "typed before" {
		t.Errorf("screen = %q before the service's last word, want it unchanged since the stop", got)
	}

	r := <-finished
	if r.err != nil {
		t.Fatal(r.err)
	}
	if want := "typed before, and in flight at the stop"; r.text != want || h.screen() != want {
		t.Errorf("text %q, screen %q, want %q", r.text, h.screen(), want)
	}
}

// xAI answers the end of the audio with the utterance so far as a chunk final,
// then as the utterance final. When that restates a word an earlier chunk had
// cut, the two do not line up and the assembler holds the utterance twice for
// a moment. A typist that followed along would type it twice and erase once.
func TestStreamFlushRestatementIsTypedOnce(t *testing.T) {
	const (
		cut      = "I tried the streaming transcription from xAI today and it was mostly fine but it sometimes dr"
		repaired = "I tried the streaming transcription from xAI today, and it was mostly fine, but it sometimes dropped entire sentences"
	)
	h := newStreamHarness(t)
	h.f.doneReply = []xaiEvent{chunkFinal(repaired), utteranceFinal(repaired), {Type: "transcript.done"}}
	h.start()
	h.waitReady(t)
	h.f.push(chunkFinal(cut))
	h.f.push(interim("opped entire sentences"))
	before := cut + " opped entire sentences"
	h.waitScreen(t, before)

	text, streamErr, typeErr := h.stop(t)
	if streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}
	if text != repaired || h.screen() != repaired {
		t.Fatalf("text %q, screen %q", text, h.screen())
	}
	back, add := textDiff(before, repaired)
	if st := h.s.stats(); st.rewrites != 1 || st.backspaces != back || st.typed != len(before)+len(add) {
		t.Errorf("typed %d, backspaces %d in %d rewrites; want the one rewrite from what was on screen (%d back, %d typed)",
			st.typed, st.backspaces, st.rewrites, back, len(before)+len(add))
	}
}

// A write that fails ends the stream. The library closes the connection on a
// failed write, so the reader can report the loss first; either way the stop
// is told, and bounded.
func TestStreamWriteFailureIsFailure(t *testing.T) {
	h := newStreamHarness(t)
	h.client.writeTimeout = time.Nanosecond
	h.rec.feed(speech(0.5, 5000))
	h.start()
	waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
	if got := h.s.failure().Error(); !strings.HasPrefix(got, "send audio: ") && !strings.HasPrefix(got, "connection lost: ") {
		t.Errorf("failure = %q, want the failed write or the connection it closed", got)
	}
	if _, streamErr, typeErr := h.finishWithin(t, 10*time.Second); streamErr == nil || typeErr != nil {
		t.Errorf("finish = stream %v, typing %v", streamErr, typeErr)
	}
}

// The sender's own report of a failed write, whose text can quote the far end.
func TestStreamWriteFailureIsScrubbed(t *testing.T) {
	logs := captureLogs(t)
	h := newStreamHarness(t)
	s := newStreamSession(h.client, h.rec, &Typer{kb: h.kb})
	s.writeFailed(context.Background(), errors.New("write: peer said test-xai-key\nand more"))
	if got := s.failure(); got == nil || got.Error() != "send audio: write: peer said <key> and more" {
		t.Errorf("failure = %v", got)
	}
	if got := logs.String(); strings.Contains(got, "test-xai-key") || strings.Count(got, "\n") != 1 || !strings.Contains(got, "still recording") {
		t.Errorf("log = %q", got)
	}
	// Once the context is cancelled a failed write is the shutdown, not news.
	quiet := newStreamSession(h.client, h.rec, &Typer{kb: h.kb})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	quiet.writeFailed(ctx, errors.New("use of closed network connection"))
	if quiet.failure() != nil {
		t.Errorf("a write that failed during shutdown was reported: %v", quiet.failure())
	}
}

// A connection that ends without a close frame, or with one that is not a
// normal closure, is lost. The close reason is the far end's text.
func TestStreamConnectionLostIsFailure(t *testing.T) {
	t.Run("dropped", func(t *testing.T) {
		h := newStreamHarness(t)
		h.start()
		h.waitReady(t)
		h.f.dropCmd <- struct{}{}
		waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
		if got := h.s.failure().Error(); !strings.HasPrefix(got, "connection lost: ") {
			t.Errorf("failure = %q", got)
		}
	})
	t.Run("closed with a reason", func(t *testing.T) {
		logs := captureLogs(t)
		h := newStreamHarness(t)
		h.f.closeStatus = websocket.StatusPolicyViolation
		h.f.closeReason = "bad token test-xai-key\n[00:00:00] STREAM: forged"
		h.start()
		h.waitReady(t)
		h.f.closeCmd <- struct{}{}
		waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
		got := h.s.failure().Error()
		if !strings.HasPrefix(got, "connection lost: ") || !strings.Contains(got, "bad token <key>") {
			t.Errorf("failure = %q, want the close reason without the key", got)
		}
		for _, where := range []string{got, logs.String()} {
			if strings.Contains(where, "test-xai-key") || strings.Contains(where, "\n[00:00:00]") {
				t.Errorf("the close reason arrived unscrubbed: %q", where)
			}
		}
	})
}

// A sender that falls this far behind the microphone has an uplink that cannot
// carry the audio. Going on would only put the text, and the stop, further
// behind.
func TestStreamBacklogBeyondTheCapIsFailure(t *testing.T) {
	h := newStreamHarness(t)
	h.client.maxBacklog = wavSampleRate / 10
	h.start()
	h.waitReady(t)
	h.rec.feed(speech(1, 5000))
	waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
	if got := h.s.failure().Error(); got != "the upload is 1s behind the microphone" {
		t.Errorf("failure = %q", got)
	}
	select {
	case <-h.s.senderDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the sender kept running")
	}
	if n := len(h.f.audioBytes()); n != 0 {
		t.Errorf("%d bytes of a backlog past the cap were sent", n)
	}
}

// A keyboard that fails ends the stream: nothing more can be typed, so nothing
// more is worth uploading. The stop reports the typing failure.
func TestStreamTypingFailureEndsTheStream(t *testing.T) {
	h := newStreamHarness(t)
	h.kb.failAt = 3 // the first event of the second stroke
	h.start()
	h.waitReady(t)
	h.f.partial("ab", false, false)
	waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
	select {
	case <-h.s.senderDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the sender kept uploading after the keyboard failed")
	}
	_, streamErr, typeErr := h.finishWithin(t, 10*time.Second)
	if streamErr == nil || typeErr == nil {
		t.Errorf("finish = stream %v, typing %v; want both reported", streamErr, typeErr)
	}
}

// The last state can take back the whole unstable tail and type it again, at
// the typer's fixed pace; the stop allows for exactly that on top of its
// fixed wait.
func TestStreamDrainAllowanceCoversTheUnstableTail(t *testing.T) {
	h := newStreamHarness(t)
	h.client.drainTimeout = 2 * time.Second
	h.s = newStreamSession(h.client, h.rec, &Typer{kb: h.kb, keyHold: 8 * time.Millisecond, keyDelay: 2 * time.Millisecond})
	if got := h.s.drainAllowance(); got != 2*time.Second {
		t.Errorf("allowance with nothing unstable = %v", got)
	}
	h.s.unstable = 300
	if got, want := h.s.drainAllowance(), 2*time.Second+600*10*time.Millisecond; got != want {
		t.Errorf("allowance for 300 unstable runes = %v, want %v", got, want)
	}
	h.s = nil
}

func TestStreamCloseWhileRecordingIsFailure(t *testing.T) {
	h := newStreamHarness(t)
	h.start()
	h.waitReady(t)
	h.f.closeCmd <- struct{}{}
	waitUntil(t, "the failure", func() bool { return h.s.failure() != nil })
	if _, streamErr, _ := h.stop(t); streamErr == nil {
		t.Error("a close while recording was not reported")
	}
}

func TestStreamFinishWaitsForDial(t *testing.T) {
	h := newStreamHarness(t)
	h.f.createdDelay = 300 * time.Millisecond
	samples := speech(0.5, 5000)
	h.rec.feed(samples)
	h.f.partial("said during the dial", false, false)
	h.start()

	text, streamErr, typeErr := h.stop(t)
	if streamErr != nil || typeErr != nil {
		t.Fatalf("finish: stream %v, typing %v", streamErr, typeErr)
	}
	if text != "said during the dial" || h.screen() != text {
		t.Errorf("text %q, screen %q", text, h.screen())
	}
	if got := h.f.audioBytes(); !bytes.Equal(got, pcm16(samples)) {
		t.Errorf("server received %d bytes, want the %d recorded", len(got), 2*len(samples))
	}
	if texts := h.f.texts(); len(texts) != 1 || !strings.Contains(texts[0], "audio.done") {
		t.Errorf("control messages = %q", texts)
	}
}

func TestStreamDialFailureStillFinishes(t *testing.T) {
	h := newStreamHarness(t)
	h.f.handshake = http.StatusServiceUnavailable
	h.rec.feed(speech(0.5, 5000))
	h.start()

	text, streamErr, typeErr := h.finishWithin(t, 10*time.Second)
	var se *streamError
	if text != "" || !errors.As(streamErr, &se) || se.Status != http.StatusServiceUnavailable || typeErr != nil {
		t.Errorf("finish = %q, stream %v, typing %v", text, streamErr, typeErr)
	}
	if len(h.f.messages()) != 0 {
		t.Error("frames reached a server that refused the handshake")
	}
}

func TestStreamDrainTimeoutSilencesKeyboard(t *testing.T) {
	h := newStreamHarness(t)
	h.kb.delay = 10 * time.Millisecond
	h.client.drainTimeout = 50 * time.Millisecond
	h.start()
	h.waitReady(t)
	h.f.partial(strings.Repeat("a slow keyboard ", 10), false, false)
	waitUntil(t, "typing to start", func() bool { return len(h.kb.snapshot()) > 0 })

	_, _, typeErr := h.stop(t)
	if typeErr == nil || !strings.Contains(typeErr.Error(), "did not settle") {
		t.Errorf("typing error = %v, want the drain timeout", typeErr)
	}
	after := len(h.kb.snapshot())
	time.Sleep(100 * time.Millisecond)
	if got := h.kb.snapshot(); len(got) != after {
		t.Errorf("%d key events after finish returned", len(got)-after)
	}
}

func TestStreamCancelledContextTypesNothing(t *testing.T) {
	h := newStreamHarness(t)
	h.start()
	h.waitReady(t)
	h.f.partial("before", false, false)
	h.waitScreen(t, "before")

	h.cancel()
	select {
	case <-h.s.runDone:
	case <-time.After(time.Second):
		t.Fatal("run did not exit on cancellation")
	}
	select {
	case <-h.f.closed:
	case <-time.After(time.Second):
		t.Fatal("the connection stayed open")
	}
	h.f.partial("after", false, false)
	time.Sleep(50 * time.Millisecond)
	if h.screen() != "before" {
		t.Errorf("screen = %q after cancellation", h.screen())
	}
	if _, _, err := h.s.finish(h.ctx, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("finish after cancellation = %v, want context canceled", err)
	}
}
