package voicetype

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// The assemblers promise the typer that a stable prefix never changes and
// never shrinks. liveTyper refuses to go on when that is broken, which would
// end a dictation's output, so the promise is tried against random event
// sequences as well as the recorded ones.

var fuzzPieces = []string{
	"", " ", "  ", "a", " a", "a ", "ab", ".", ",", ", b", " . ", "\n", "\r\n", "\t",
	"é", " é", "x\x00y", "\x00", "hello world", " world", "wor", "ld", "“q”", "…",
	" ", "a b", "🙂", " 🙂 ", "I think.", "I think. I think.", "?", "!x", ";", ":",
	"\u0085", "​", "é", "-", "—", "€", "°",
}

func fuzzText(rng *rand.Rand) string {
	var b strings.Builder
	for range 1 + rng.Intn(3) {
		b.WriteString(fuzzPieces[rng.Intn(len(fuzzPieces))])
	}
	return b.String()
}

// promiseChecker applies a state exactly as liveTyper.apply decides it,
// without the keyboard.
type promiseChecker struct {
	shown  []rune
	stable int
}

func (c *promiseChecker) apply(want liveText) error {
	w := []rune(want.text)
	keep := 0
	for keep < len(c.shown) && keep < len(w) && c.shown[keep] == w[keep] {
		keep++
	}
	switch {
	case keep < c.stable:
		return fmt.Errorf("diverged inside the stable prefix: shown %q, stable %d, want %q", string(c.shown), c.stable, want.text)
	case want.stable > len(w):
		return fmt.Errorf("stable %d is past the end of %q", want.stable, want.text)
	case want.stable < c.stable:
		return fmt.Errorf("stable fell from %d to %d", c.stable, want.stable)
	}
	if _, err := typeCompile(string(w[keep:])); err != nil {
		return fmt.Errorf("untypeable: %v", err)
	}
	c.shown, c.stable = w, want.stable
	return nil
}

// checkPromise feeds every state to one checker and every third to another,
// as the latest-wins typer does when it is slower than the events.
func checkPromise(t *testing.T, runs, steps int, next func(rng *rand.Rand) func() (liveText, string)) {
	t.Helper()
	rng := rand.New(rand.NewSource(20261117))
	for run := range runs {
		step := next(rng)
		all, sparse := &promiseChecker{}, &promiseChecker{}
		for i := range steps {
			state, event := step()
			if err := all.apply(state); err != nil {
				t.Fatalf("run %d step %d (%s): %v", run, i, event, err)
			}
			if i%3 == 2 {
				if err := sparse.apply(state); err != nil {
					t.Fatalf("run %d step %d (%s), coalesced: %v", run, i, event, err)
				}
			}
		}
	}
}

func fuzzGatewayEvent(rng *rand.Rand, a *gatewayAssembler) gatewayEvent {
	switch rng.Intn(10) {
	case 0, 1, 2, 3:
		return gatewayEvent{Type: "transcript-delta", Delta: fuzzText(rng)}
	case 4, 5, 6:
		return gatewayEvent{Type: "transcript-partial", Text: fuzzText(rng)}
	case 7:
		// A final that agrees with everything shown.
		return gatewayEvent{Type: "transcript-final", Text: a.done + a.open}
	case 8:
		// A final that extends the settled text.
		return gatewayEvent{Type: "transcript-final", Text: a.done + fuzzText(rng)}
	}
	// A final that may contradict it.
	return gatewayEvent{Type: "transcript-final", Text: fuzzText(rng)}
}

func TestGatewayAssemblerKeepsItsPromise(t *testing.T) {
	checkPromise(t, 3000, 40, func(rng *rand.Rand) func() (liveText, string) {
		a := &gatewayAssembler{}
		return func() (liveText, string) {
			ev := fuzzGatewayEvent(rng, a)
			a.apply(ev)
			return a.current(), fmt.Sprintf("%+v", ev)
		}
	})
}

func TestXAIAssemblerKeepsItsPromise(t *testing.T) {
	checkPromise(t, 3000, 40, func(rng *rand.Rand) func() (liveText, string) {
		a := &xaiAssembler{}
		return func() (liveText, string) {
			ev := xaiEvent{Type: "transcript.partial", Text: fuzzText(rng)}
			switch rng.Intn(8) {
			case 0, 1, 2:
			case 3, 4:
				ev.IsFinal = true
			case 5, 6:
				ev.IsFinal, ev.SpeechFinal = true, true
			default:
				ev.SpeechFinal = true // without is_final
			}
			if rng.Intn(6) == 0 && a.chunks != "" {
				ev.Text = a.chunks + " " + ev.Text // the flush shape: the utterance so far
			}
			a.apply(ev)
			return a.current(), fmt.Sprintf("%+v", ev)
		}
	})
}

// The same through the real typer and a keyboard: the screen reads the
// assembler's text after every state.
func TestGatewayAssemblerThroughTheTyper(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for run := range 150 {
		a := &gatewayAssembler{}
		kb := newFakeKeyboard()
		lt := &liveTyper{typer: &Typer{kb: kb}}
		for step := range 25 {
			ev := fuzzGatewayEvent(rng, a)
			a.apply(ev)
			want := a.current()
			if err := lt.apply(context.Background(), want); err != nil {
				t.Fatalf("run %d step %d (%+v): %v", run, step, ev, err)
			}
			if screen := screenText(kb.snapshot()); screen != want.text {
				t.Fatalf("run %d step %d (%+v): screen %q, want %q", run, step, ev, screen, want.text)
			}
		}
	}
}
