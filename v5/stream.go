package voicetype

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
	"unicode/utf8"
)

// streamChunkBytes is 100 ms of PCM16, the chunk size xAI recommends and far
// under the gateway's 64 KiB frame limit.
const streamChunkBytes = wavSampleRate * 2 / 10

// streamSession is one live dictation: audio goes up while the recorder runs,
// and text comes back and is typed as it arrives.
//
// run owns the connection from dial to close, and is the only goroutine the
// daemon starts. Its children are the sender (recorder buffer -> AGC, for a
// service that needs it -> 100 ms chunks), the reader (the service's events,
// assembled by the connection -> liveOutput), and the typer (liveOutput.run).
// finish is the stop path.
//
// A failure -- the dial, a write, an error event, a dropped connection, a
// service that does not finish -- never ends the dictation: the sender and
// reader stop, the typer lands what it already has, and the recorder keeps
// going until the user stops or the cap does. What happens to the recording is
// decided at stop. Only a stream that ran to the service's own last word
// counts as a success.
type streamSession struct {
	client *streamClient
	rec    audioRecorder
	out    *liveOutput

	// Sender goroutine only.
	agc     *agc    // nil when the service takes the audio as recorded
	pending []int16 // less than one frame
	chunk   []byte  // less than one chunk
	offset  int     // samples consumed from the recorder buffer

	mu          sync.Mutex
	conn        *streamConn
	failed      error
	final       []int16 // the whole recording, handed over by finish
	typerCancel context.CancelFunc
	unstable    int // runes of the latest text past its stable prefix
	// For the session log.
	samplesSent int
	events      int
	connectTime time.Duration // dial to ready for audio
	doneSent    time.Time     // end of audio written
	doneTime    time.Duration // end of audio to the service's last word

	ready         chan struct{} // closed when the dial has resolved, either way
	stopRequested chan struct{} // closed when finish begins: the screen holds still
	stopOnce      sync.Once
	audioDone     chan struct{} // closed by finish: flush the tail, end the audio
	audioDoneOnce sync.Once
	failedCh      chan struct{} // closed by the first failure
	readerDone    chan struct{}
	senderDone    chan struct{}
	runDone       chan struct{}
}

// newStreamSession takes its own copy of the client, so a config reload while
// a session runs cannot change the key or keyterms under it.
func newStreamSession(client *streamClient, rec audioRecorder, typer *Typer) *streamSession {
	c := *client
	s := &streamSession{
		client:        &c,
		rec:           rec,
		out:           newLiveOutput(typer),
		chunk:         make([]byte, 0, streamChunkBytes),
		ready:         make(chan struct{}),
		stopRequested: make(chan struct{}),
		audioDone:     make(chan struct{}),
		failedCh:      make(chan struct{}),
		readerDone:    make(chan struct{}),
		senderDone:    make(chan struct{}),
		runDone:       make(chan struct{}),
	}
	if c.info().levels {
		s.agc = newAGC()
	}
	return s
}

func (s *streamSession) run(ctx context.Context) {
	defer close(s.runDone)
	typerCtx, cancelTyper := context.WithCancel(ctx)
	defer cancelTyper()
	s.mu.Lock()
	s.typerCancel = cancelTyper
	s.mu.Unlock()
	go s.out.run(typerCtx)
	go s.watchTyper()

	started := time.Now()
	conn, err := s.client.open(ctx)
	if err != nil {
		if ctx.Err() != nil {
			s.setFailure(ctx.Err())
		} else if s.setFailure(err) {
			logStreamFailure(err, " -- no live text; the recording is handled at stop")
		}
		close(s.ready)
		s.out.closeInput()
		<-s.out.done
		return
	}
	s.mu.Lock()
	s.conn = conn
	s.connectTime = time.Since(started)
	s.mu.Unlock()
	logf("STREAM", "connected in %v", s.connectTime.Round(time.Millisecond))
	close(s.ready)

	go s.sendLoop(ctx, conn)
	go s.readLoop(ctx, conn)
	select {
	case <-s.readerDone:
	case <-s.failedCh:
		// A failed write leaves the reader waiting on a connection that will
		// never answer; closing it is what lets the reader go.
		conn.closeNow()
		<-s.readerDone
	}
	<-s.senderDone
	s.out.closeInput()
	<-s.out.done
	conn.closeNow()
}

// watchTyper ends the stream when the keyboard fails: nothing more can be
// typed, so nothing more is worth uploading, or paying for. The recorder goes
// on, and the stop reports the typing failure.
func (s *streamSession) watchTyper() {
	select {
	case <-s.out.failed:
		s.fail(errors.New("live typing failed"))
	case <-s.out.done:
	}
}

// setFailure records the first failure and reports whether this was it.
func (s *streamSession) setFailure(err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return false
	}
	s.failed = err
	close(s.failedCh)
	return true
}

// fail records the first failure and says so at once, so the log shows when
// the text stopped, not only what happened at stop.
func (s *streamSession) fail(err error) {
	if !s.setFailure(err) {
		return
	}
	suffix := " -- still recording; the rest is handled at stop"
	if s.stopping() {
		suffix = " -- the last words may be missing"
	}
	logStreamFailure(err, suffix)
}

func (s *streamSession) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

// stopping reports whether the stop has been requested. The end of the audio
// goes out later: after the settle wait, the dial and the rest of the upload.
func (s *streamSession) stopping() bool {
	select {
	case <-s.stopRequested:
		return true
	default:
		return false
	}
}

// phase names where a failure fell, for its message.
func (s *streamSession) phase() string {
	if s.stopping() {
		return "before its last word"
	}
	return "while recording"
}

// sendLoop streams the recording as it grows. The first poll after the
// handshake carries everything captured during the dial, which the server
// accepts as a burst, so no early speech is lost to the connect time.
func (s *streamSession) sendLoop(ctx context.Context, conn *streamConn) {
	defer close(s.senderDone)
	ticker := time.NewTicker(streamPollInterval)
	defer ticker.Stop()
	for {
		samples := s.rec.Samples(s.offset)
		// Each write is bounded, but a link that is merely too slow passes
		// every one of them while the text falls further behind and the stop
		// waits for the whole backlog.
		if len(samples) > s.client.maxBacklog {
			s.fail(fmt.Errorf("the upload is %.0fs behind the microphone", wavDurationSeconds(samples)))
			return
		}
		if err := s.send(ctx, conn, samples, false); err != nil {
			s.writeFailed(ctx, err)
			return
		}
		select {
		case <-ticker.C:
		case <-s.audioDone:
			// The recorder has stopped; finish handed over the whole recording,
			// whose first offset samples have already gone up.
			s.mu.Lock()
			final := s.final
			s.mu.Unlock()
			var tail []int16
			if s.offset < len(final) {
				tail = final[s.offset:]
			}
			err := s.send(ctx, conn, tail, true)
			if err == nil {
				err = s.writeTailSilence(ctx, conn)
			}
			if err == nil {
				err = s.writeAudioDone(ctx, conn)
			}
			if err != nil {
				s.writeFailed(ctx, err)
			}
			return
		case <-s.failedCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// send takes samples frame by frame, levelling them when the service needs
// it, and writes every whole chunk. At the end of the session (flush) the
// short remainder goes too. samples is copied, never modified: at the flush
// it is the raw recording that the batch fallback and a retained WAV still
// need.
func (s *streamSession) send(ctx context.Context, conn *streamConn, samples []int16, flush bool) error {
	s.offset += len(samples)
	s.pending = append(s.pending, samples...)
	end := len(s.pending) / agcFrameSamples * agcFrameSamples
	if flush {
		end = len(s.pending)
	}
	for start := 0; start < end; start += agcFrameSamples {
		frame := s.pending[start:min(start+agcFrameSamples, end)]
		if s.agc != nil {
			s.agc.process(frame)
		}
		for _, v := range frame {
			s.chunk = binary.LittleEndian.AppendUint16(s.chunk, uint16(v))
		}
		if len(s.chunk) >= streamChunkBytes {
			if err := s.writeAudio(ctx, conn); err != nil {
				return err
			}
		}
	}
	s.pending = append(s.pending[:0], s.pending[end:]...)
	if flush && len(s.chunk) > 0 {
		return s.writeAudio(ctx, conn)
	}
	return nil
}

// writeAudio sends the chunk under construction. A write that stalls past
// writeTimeout is a dead connection; the library closes it, which is the
// outcome wanted.
func (s *streamSession) writeAudio(ctx context.Context, conn *streamConn) error {
	wctx, cancel := context.WithTimeout(ctx, s.client.writeTimeout)
	defer cancel()
	if err := conn.writeAudio(wctx, s.chunk); err != nil {
		return err
	}
	s.mu.Lock()
	s.samplesSent += len(s.chunk) / 2
	s.mu.Unlock()
	s.chunk = s.chunk[:0]
	return nil
}

// writeTailSilence follows the last sample with the silence a service needs
// to finish its last word. It goes up at once, not in real time: over twelve
// runs of six recordings the gateway did as well either way, and the stop
// waits 0.2 s longer for it instead of a second. It is not counted as audio
// sent.
func (s *streamSession) writeTailSilence(ctx context.Context, conn *streamConn) error {
	silence := make([]byte, streamChunkBytes)
	for left := int(s.client.info().tailSilence.Seconds() * wavSampleRate * 2); left > 0; left -= len(silence) {
		wctx, cancel := context.WithTimeout(ctx, s.client.writeTimeout)
		err := conn.writeAudio(wctx, silence[:min(left, len(silence))])
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// writeAudioDone ends the audio. With xAI, finalize is not sent first:
// audio.done alone flushed the open utterance with the same text and latency
// in every probe.
func (s *streamSession) writeAudioDone(ctx context.Context, conn *streamConn) error {
	wctx, cancel := context.WithTimeout(ctx, s.client.writeTimeout)
	defer cancel()
	if err := conn.writeDone(wctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.doneSent = time.Now()
	s.mu.Unlock()
	return nil
}

func (s *streamSession) writeFailed(ctx context.Context, err error) {
	if ctx.Err() != nil || s.failure() != nil {
		return // shutdown, or the connection was closed because of a failure
	}
	s.fail(fmt.Errorf("send audio: %w", s.client.scrubError(err)))
}

// readLoop hands every change in the text to the typer. Reads carry the
// session context and no deadline: the library closes the connection when a
// read's context ends, and long quiet stretches are normal.
//
// Once the stop is requested the screen holds still, and what arrives after it
// is typed in one go when the service has had its last word (or has failed to
// give it). Typed as it came, text landing in the first instants would reach
// the keyboard while the stop key is still down, and the service's reply to
// the end of the audio can restate a whole utterance before replacing it,
// which a typist that follows along types twice and erases once.
func (s *streamSession) readLoop(ctx context.Context, conn *streamConn) {
	defer close(s.readerDone)
	var held *liveText
	defer func() {
		if held != nil {
			s.out.set(*held)
		}
	}()
	for {
		up, err := conn.read(ctx)
		if err != nil {
			s.readFailed(ctx, err)
			return
		}
		s.mu.Lock()
		s.events++
		s.mu.Unlock()
		switch up.kind {
		case updateText:
			text := up.text
			s.mu.Lock()
			s.unstable = utf8.RuneCountInString(text.text) - text.stable
			s.mu.Unlock()
			if s.stopping() {
				held = &text
				continue
			}
			s.out.set(text)
		case updateDone:
			s.mu.Lock()
			asked := !s.doneSent.IsZero()
			if asked {
				s.doneTime = time.Since(s.doneSent)
			}
			s.mu.Unlock()
			// Unasked for, it ends a stream the user is still talking into (or
			// whose last audio has not gone up): the sender would go on
			// uploading to a service that is no longer listening.
			if !asked {
				s.fail(errors.New(s.client.info().name + " ended the stream " + s.phase()))
			}
			return
		case updateError:
			s.fail(&streamError{Service: s.client.service, Message: s.client.scrub(up.message)})
			return
		}
	}
}

// readFailed decides whether a read error is a failure. Every one that ends a
// stream before the service's last word is: the stop path, shutdown and a
// failure elsewhere are the only ones that close the connection on purpose,
// and each has already said why.
func (s *streamSession) readFailed(ctx context.Context, err error) {
	switch {
	case ctx.Err() != nil || s.failure() != nil:
	// Exactly io.EOF is readRaw's word for a normal closure. A connection
	// that just ends is a lost one, though its error wraps io.EOF too.
	case err == io.EOF:
		s.fail(errors.New(s.client.info().name + " closed the stream " + s.phase()))
	default:
		s.fail(fmt.Errorf("connection lost: %w", s.client.scrubError(err)))
	}
}

// finish ends the session: it waits for the dial, hands the sender the final
// samples, ends the audio, waits for the service's last word and for the typer
// to land the last state, all bounded, then waits for run to exit. It is
// idempotent and safe after a failure. samples is the slice Recorder.Stop()
// returned; the sender sends samples[offset:] before the end of the audio.
//
// text is what this session typed and left on screen. streamErr is set unless
// the service confirmed it had nothing more to say: a stop that ran out of
// time is a failure like any other, because the last words may never have been
// transcribed. A cancelled ctx (shutdown) returns at once with ctx's error as
// typeErr, and nothing more is typed.
func (s *streamSession) finish(ctx context.Context, samples []int16) (text string, streamErr, typeErr error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	// From here no new text is typed until the service is done (readLoop).
	// Strokes that land while the stop hotkey's modifiers are still down
	// become shortcuts; the wait below is for a state the typer was already
	// working through when the key went down, which cannot be recalled.
	s.stopOnce.Do(func() { close(s.stopRequested) })
	if s.client.stopSettle > 0 {
		timer := time.NewTimer(s.client.stopSettle)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return "", nil, ctx.Err()
		}
	}
	// A stop during the dial waits for it (bounded by dialTimeout): the
	// speech so far is in the recording and goes up once the handshake lands.
	select {
	case <-s.ready:
	case <-ctx.Done():
		return "", nil, ctx.Err()
	}

	if s.failure() == nil {
		s.mu.Lock()
		s.final = samples
		conn := s.conn
		s.mu.Unlock()
		s.audioDoneOnce.Do(func() { close(s.audioDone) })

		// Closing the connection is what frees a sender or a reader that is
		// waiting on it; the failure is recorded first, so neither reports the
		// close as its own.
		flush := time.NewTimer(s.client.flushTimeout)
		defer flush.Stop()
		select {
		case <-s.senderDone:
		case <-flush.C:
			s.fail(fmt.Errorf("the rest of the audio did not upload within %v", s.client.flushTimeout))
			conn.closeNow()
			<-s.senderDone
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
		done := time.NewTimer(s.client.doneTimeout)
		defer done.Stop()
		select {
		case <-s.readerDone:
		case <-done.C:
			s.fail(fmt.Errorf("%s did not finish within %v of the end of the audio", s.client.info().name, s.client.doneTimeout))
			conn.closeNow()
			<-s.readerDone
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	}

	allowance := s.drainAllowance()
	drainCtx, cancel := context.WithTimeout(ctx, allowance)
	defer cancel()
	text, typeErr, landed := s.out.wait(drainCtx)
	if !landed {
		if ctx.Err() != nil {
			return "", nil, ctx.Err()
		}
		// shown is only safe to read once the typer has exited; it does at its
		// next stroke.
		s.mu.Lock()
		cancelTyper := s.typerCancel
		s.mu.Unlock()
		cancelTyper()
		text, _, _ = s.out.wait(context.Background())
		typeErr = fmt.Errorf("live typing did not settle within %v", allowance)
	}
	<-s.runDone
	return text, s.failure(), typeErr
}

// drainAllowance is how long the typer gets to land the last state. That state
// can take back the whole unstable tail and type it again, at a fixed pace per
// stroke, and a long tail needs more than the fixed wait.
func (s *streamSession) drainAllowance() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client.drainTimeout + s.out.strokesTime(2*s.unstable)
}

// streamStats is the session log's summary.
type streamStats struct {
	typed, backspaces, maxRewrite, rewrites, events int
	audioSeconds                                    float64
	connect, reply                                  time.Duration
}

// stats may only be called once finish has returned without a cancelled
// context. The typing counters belong to the typer goroutine, and that is
// the point at which finish has waited for it to exit; s.mu does not guard
// them.
func (s *streamSession) stats() streamStats {
	lt := s.out.lt
	st := streamStats{typed: lt.typed, backspaces: lt.backspaces, maxRewrite: lt.maxRewrite, rewrites: lt.rewrites}
	s.mu.Lock()
	defer s.mu.Unlock()
	st.events = s.events
	st.audioSeconds = float64(s.samplesSent) / wavSampleRate
	st.connect = s.connectTime
	st.reply = s.doneTime
	return st
}
