package voicetype

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bendahl/uinput"
)

// screenText replays key events into the text they leave in a plain text
// field: Shift and Ctrl are tracked, Ctrl+Shift+U starts hex entry that Enter
// commits as one rune, and Backspace deletes the last rune.
func screenText(events []testKeyEvent) string {
	type keyShift struct {
		key   int
		shift bool
	}
	reverse := make(map[keyShift]rune, len(typeKeyMap))
	for r, s := range typeKeyMap {
		reverse[keyShift{s.key, len(s.modifiers) > 0}] = r
	}

	var out []rune
	shift, ctrl, inHex := false, false, false
	hex := ""
	for _, e := range events {
		switch e.key {
		case uinput.KeyLeftshift:
			shift = e.op == "down"
			continue
		case uinput.KeyLeftctrl:
			ctrl = e.op == "down"
			continue
		}
		if e.op != "down" {
			continue
		}
		switch {
		case ctrl && shift && e.key == uinput.KeyU:
			inHex, hex = true, ""
		case inHex && e.key == uinput.KeyEnter:
			v, _ := strconv.ParseInt(hex, 16, 32)
			out = append(out, rune(v))
			inHex = false
		case inHex:
			hex += string(reverse[keyShift{e.key, false}])
		case e.key == uinput.KeyBackspace:
			if len(out) > 0 {
				out = out[:len(out)-1]
			}
		case e.key == uinput.KeyEnter:
			out = append(out, '\n')
		default:
			out = append(out, reverse[keyShift{e.key, shift}])
		}
	}
	return string(out)
}

func countKey(events []testKeyEvent, key int) int {
	return countEvent(events, testKeyEvent{op: "down", key: key})
}

func TestScreenTextRoundTrips(t *testing.T) {
	for _, text := range []string{"Hey, quick update.", "café 🙂", `Quote "x" & 100%`, ""} {
		kb := newFakeKeyboard()
		if err := (&Typer{kb: kb}).Type(text); err != nil {
			t.Fatal(err)
		}
		if got := screenText(kb.events); got != text {
			t.Errorf("screenText = %q, want %q", got, text)
		}
	}
}

func TestLiveTyperAppendsWithoutBackspacing(t *testing.T) {
	kb := newFakeKeyboard()
	lt := &liveTyper{typer: &Typer{kb: kb}}
	for _, s := range []string{"Hey", "Hey, quick", "Hey, quick update."} {
		if err := lt.apply(context.Background(), liveText{text: s}); err != nil {
			t.Fatal(err)
		}
		if got := screenText(kb.events); got != s {
			t.Fatalf("screen = %q, want %q", got, s)
		}
	}
	if n := countKey(kb.events, uinput.KeyBackspace); n != 0 {
		t.Errorf("appending sent %d backspaces", n)
	}
	if lt.typed != len("Hey, quick update.") || lt.backspaces != 0 || lt.rewrites != 0 {
		t.Errorf("stats typed=%d backspaces=%d rewrites=%d", lt.typed, lt.backspaces, lt.rewrites)
	}
}

func TestLiveTyperBackspacesDivergentTail(t *testing.T) {
	kb := newFakeKeyboard()
	lt := &liveTyper{typer: &Typer{kb: kb}}
	ctx := context.Background()
	if err := lt.apply(ctx, liveText{text: "Hey, quicker."}); err != nil {
		t.Fatal(err)
	}
	before := len(kb.events)
	if err := lt.apply(ctx, liveText{text: "Hey, quick update."}); err != nil {
		t.Fatal(err)
	}

	add, _ := typeCompile(" update.")
	var want []testKeyEvent
	for _, s := range append(backspaceStrokes(3), add...) {
		for _, m := range s.modifiers {
			want = append(want, testKeyEvent{"down", m})
		}
		want = append(want, testKeyEvent{"down", s.key})
		for _, m := range s.modifiers {
			want = append(want, testKeyEvent{"up", m})
		}
		want = append(want, testKeyEvent{"up", s.key})
	}
	if got := kb.events[before:]; !reflect.DeepEqual(got, want) {
		t.Errorf("events = %v, want %v", got, want)
	}
	if got := screenText(kb.events); got != "Hey, quick update." {
		t.Errorf("screen = %q", got)
	}
	if lt.backspaces != 3 || lt.maxRewrite != 3 || lt.rewrites != 1 || lt.typed != len("Hey, quicker.")+len(" update.") {
		t.Errorf("stats backspaces=%d maxRewrite=%d rewrites=%d typed=%d", lt.backspaces, lt.maxRewrite, lt.rewrites, lt.typed)
	}
}

func TestLiveTyperRefusesToEnterStablePrefix(t *testing.T) {
	kb := newFakeKeyboard()
	lt := &liveTyper{typer: &Typer{kb: kb}}
	ctx := context.Background()
	if err := lt.apply(ctx, liveText{text: "AB", stable: 2}); err != nil {
		t.Fatal(err)
	}
	before := len(kb.events)
	if err := lt.apply(ctx, liveText{text: "AC", stable: 2}); err == nil {
		t.Fatal("rewrote text an earlier state declared stable")
	}
	if len(kb.events) != before {
		t.Errorf("a refused state still sent %v", kb.events[before:])
	}
	if string(lt.shown) != "AB" {
		t.Errorf("shown = %q, want AB", string(lt.shown))
	}
}

// An utterance final routinely revises the interim it closes: the text it
// declares stable was not stable when it was typed.
func TestLiveTyperRevisesUnstableTailAsItCloses(t *testing.T) {
	kb := newFakeKeyboard()
	lt := &liveTyper{typer: &Typer{kb: kb}}
	ctx := context.Background()
	for _, want := range []liveText{
		{text: "Done. Hey, quicker.", stable: 5},
		{text: "Done. Hey, quick update.", stable: 24},
	} {
		if err := lt.apply(ctx, want); err != nil {
			t.Fatalf("apply(%+v): %v", want, err)
		}
	}
	if got := screenText(kb.events); got != "Done. Hey, quick update." {
		t.Errorf("screen = %q", got)
	}
}

func TestLiveTyperUnicodeBackspacePerRune(t *testing.T) {
	kb := newFakeKeyboard()
	lt := &liveTyper{typer: &Typer{kb: kb}}
	ctx := context.Background()
	for _, s := range []string{"café 🙂", "cafe"} {
		if err := lt.apply(ctx, liveText{text: s}); err != nil {
			t.Fatal(err)
		}
	}
	// "é 🙂" is three runes: three backspaces, whatever each took to type.
	if n := countKey(kb.events, uinput.KeyBackspace); n != 3 {
		t.Errorf("backspaces = %d, want 3", n)
	}
	if got := screenText(kb.events); got != "cafe" {
		t.Errorf("screen = %q", got)
	}
}

func TestLiveTyperEmptyDiffSendsNothing(t *testing.T) {
	kb := newFakeKeyboard()
	lt := &liveTyper{typer: &Typer{kb: kb}}
	ctx := context.Background()
	if err := lt.apply(ctx, liveText{text: "same"}); err != nil {
		t.Fatal(err)
	}
	before := len(kb.events)
	if err := lt.apply(ctx, liveText{text: "same", stable: 4}); err != nil {
		t.Fatal(err)
	}
	if len(kb.events) != before {
		t.Errorf("an unchanged state sent %v", kb.events[before:])
	}
	if lt.stable != 4 {
		t.Errorf("stable = %d, want the unchanged state's promise recorded", lt.stable)
	}
}

func TestLiveOutputCoalesces(t *testing.T) {
	kb := newFakeKeyboard()
	kb.gate = make(chan struct{})
	kb.gateEntered = make(chan struct{}, 1)
	o := newLiveOutput(&Typer{kb: kb})
	go o.run(context.Background())

	o.set(liveText{text: "a"})
	<-kb.gateEntered // the typer is inside "a"
	// "ax" would cost a backspace if it were ever typed; it is superseded
	// before the typer is free.
	o.set(liveText{text: "ax"})
	o.set(liveText{text: "abc"})
	close(kb.gate)
	o.closeInput()

	shown, err, landed := o.wait(context.Background())
	if !landed || err != nil {
		t.Fatalf("wait = landed %t, err %v", landed, err)
	}
	if shown != "abc" || screenText(kb.events) != "abc" {
		t.Errorf("shown %q, screen %q, want abc", shown, screenText(kb.events))
	}
	if o.lt.backspaces != 0 {
		t.Errorf("a superseded state was typed: %d backspaces", o.lt.backspaces)
	}
}

func TestLiveOutputWaitReturnsLastState(t *testing.T) {
	kb := newFakeKeyboard()
	o := newLiveOutput(&Typer{kb: kb})
	go o.run(context.Background())
	for _, s := range []string{"one", "one two", "one, two three"} {
		o.set(liveText{text: s})
	}
	o.closeInput()
	shown, err, landed := o.wait(context.Background())
	if !landed || err != nil || shown != "one, two three" {
		t.Fatalf("wait = %q, %v, %t", shown, err, landed)
	}
	if got := screenText(kb.events); got != shown {
		t.Errorf("screen = %q, shown = %q", got, shown)
	}
}

func TestLiveOutputStopsAfterTypingError(t *testing.T) {
	kb := newFakeKeyboard()
	kb.failAt = 3 // the first event of the second stroke: "b" down
	o := newLiveOutput(&Typer{kb: kb})
	go o.run(context.Background())

	o.set(liveText{text: "ab"})
	waitUntil(t, "the typing error", func() bool { return o.failure() != nil })
	after := len(kb.snapshot())
	o.set(liveText{text: "abc"})
	o.set(liveText{text: "abcd"})
	o.closeInput()

	_, err, landed := o.wait(context.Background())
	if !landed || err == nil {
		t.Fatalf("wait = landed %t, err %v; want the typing error", landed, err)
	}
	if got := kb.snapshot(); len(got) != after {
		t.Errorf("states after the failure were typed: %v", got[after:])
	}
}

func TestLiveOutputCancelledContextTypesNothing(t *testing.T) {
	kb := newFakeKeyboard()
	o := newLiveOutput(&Typer{kb: kb})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	go o.run(ctx)
	o.set(liveText{text: "must not appear"})
	o.closeInput()

	_, err, landed := o.wait(context.Background())
	if !landed || !errors.Is(err, context.Canceled) {
		t.Fatalf("wait = landed %t, err %v; want context canceled", landed, err)
	}
	if len(kb.snapshot()) != 0 {
		t.Errorf("cancelled output typed %v", kb.snapshot())
	}
}

func TestLiveOutputCancelStopsSlowTyper(t *testing.T) {
	kb := newFakeKeyboard()
	kb.delay = 5 * time.Millisecond
	o := newLiveOutput(&Typer{kb: kb})
	ctx, cancel := context.WithCancel(context.Background())
	go o.run(ctx)
	// As at stop: the input is closed and the typer is landing the last state.
	o.set(liveText{text: strings.Repeat("slow ", 40)})
	o.closeInput()
	waitUntil(t, "typing to start", func() bool { return len(kb.snapshot()) > 4 })

	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if _, _, landed := o.wait(short); landed {
		t.Fatal("a 200-character state landed in 10 ms on a slow keyboard")
	}
	cancel()
	_, err, landed := o.wait(context.Background())
	if !landed || !errors.Is(err, context.Canceled) {
		t.Fatalf("wait = landed %t, err %v", landed, err)
	}
	after := len(kb.snapshot())
	time.Sleep(50 * time.Millisecond)
	if got := kb.snapshot(); len(got) != after {
		t.Errorf("keys after the typer exited: %v", got[after:])
	}
	if len(kb.held) != 0 {
		t.Errorf("cancellation left keys held: %v", kb.held)
	}
}
