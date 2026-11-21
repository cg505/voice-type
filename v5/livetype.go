package voicetype

import (
	"context"
	"errors"
	"sync"
	"time"
)

// liveTyper turns successive on-screen states into keystrokes. shown is what
// this session has typed and not erased, and a backspace is only ever sent for
// a rune of it.
//
// That keeps the session from erasing the user's own text only while the caret
// stays put. Nothing can see the caret: uinput is write-only, and a Wayland
// client cannot read focus. Keystrokes go wherever focus is, so a rewrite after
// the user moves the caret deletes whatever precedes it there. One state can
// take back the whole unstable tail and no more: text an earlier state declared
// stable is never backspaced. How long that tail gets is up to the service --
// the open utterance with xAI, about two seconds of speech with MAI.
//
// It also assumes one stroke puts one rune on screen and one backspace takes
// one off, which sanitizeLive's plain ASCII makes true of the typer's side; an
// editor that pairs brackets or completes words on its own breaks it.
type liveTyper struct {
	typer  *Typer
	shown  []rune
	stable int // runes of shown that an applied state promised never change
	// For the session log.
	typed, backspaces, maxRewrite, rewrites int
}

func (l *liveTyper) apply(ctx context.Context, want liveText) error {
	w := []rune(want.text)
	keep := 0
	for keep < len(l.shown) && keep < len(w) && l.shown[keep] == w[keep] {
		keep++
	}
	if keep < l.stable {
		// The assembler promised that prefix never changes. Typing past this
		// point would erase text we promised to leave; stop output instead.
		return errors.New("live text diverged inside its stable prefix")
	}

	back := len(l.shown) - keep
	add, err := typeCompile(string(w[keep:]))
	if err != nil {
		return err
	}
	if back == 0 && len(add) == 0 {
		l.stable = want.stable
		return nil
	}
	if err := l.typer.typeStrokes(ctx, append(backspaceStrokes(back), add...)); err != nil {
		// What is on screen is now unknown; the caller stops output.
		return err
	}

	l.shown = w
	l.stable = want.stable
	l.typed += len(w) - keep
	l.backspaces += back
	l.maxRewrite = max(l.maxRewrite, back)
	if back > 0 {
		l.rewrites++
	}
	return nil
}

// liveOutput hands states from the reader goroutine to the typer goroutine,
// latest wins: a slow typist must never work through a backlog of stale states.
type liveOutput struct {
	lt        *liveTyper
	ch        chan liveText // capacity 1; the reader is the only sender
	done      chan struct{} // closed when the typer goroutine exits
	failed    chan struct{} // closed when the keyboard fails
	closeOnce sync.Once

	mu  sync.Mutex
	err error // first typing failure; nothing is typed after it
}

func newLiveOutput(t *Typer) *liveOutput {
	return &liveOutput{
		lt:     &liveTyper{typer: t},
		ch:     make(chan liveText, 1),
		done:   make(chan struct{}),
		failed: make(chan struct{}),
	}
}

// set replaces any state the typer has not picked up yet. The reader is the
// only sender, so once the slot is drained the send cannot block.
func (o *liveOutput) set(t liveText) {
	select {
	case <-o.ch:
	default:
	}
	o.ch <- t
}

// run is the typer goroutine. After the first failure it keeps draining, so
// set never blocks, but types nothing more: shown no longer matches the screen.
// A cancelled context is not the keyboard failing, and does not close failed.
func (o *liveOutput) run(ctx context.Context) {
	defer close(o.done)
	for t := range o.ch {
		if o.failure() != nil {
			continue
		}
		if err := o.lt.apply(ctx, t); err != nil {
			o.mu.Lock()
			o.err = err
			o.mu.Unlock()
			if ctx.Err() == nil {
				close(o.failed)
			}
		}
	}
}

func (o *liveOutput) closeInput() { o.closeOnce.Do(func() { close(o.ch) }) }

func (o *liveOutput) failure() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}

// strokesTime is how long the typer takes over n strokes.
func (o *liveOutput) strokesTime(n int) time.Duration {
	return time.Duration(n) * (o.lt.typer.keyHold + o.lt.typer.keyDelay)
}

// wait blocks until the typer goroutine has exited or ctx ends. landed is
// true only when it has exited; shown and err are valid only then (reading
// shown while the goroutine runs would be a data race).
func (o *liveOutput) wait(ctx context.Context) (shown string, err error, landed bool) {
	select {
	case <-o.done:
	case <-ctx.Done():
		select {
		case <-o.done:
		default:
			return "", nil, false
		}
	}
	return string(o.lt.shown), o.failure(), true
}
