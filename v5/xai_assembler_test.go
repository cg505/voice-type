package voicetype

import (
	"testing"
	"unicode/utf8"
)

func interim(text string) xaiEvent {
	return xaiEvent{Type: "transcript.partial", Text: text}
}

func chunkFinal(text string) xaiEvent {
	return xaiEvent{Type: "transcript.partial", Text: text, IsFinal: true}
}

func utteranceFinal(text string) xaiEvent {
	return xaiEvent{Type: "transcript.partial", Text: text, IsFinal: true, SpeechFinal: true}
}

// textDiff is what the typer would do to get from prev to next: backspaces,
// then the runes to type.
func textDiff(prev, next string) (back int, add string) {
	p, n := []rune(prev), []rune(next)
	keep := 0
	for keep < len(p) && keep < len(n) && p[keep] == n[keep] {
		keep++
	}
	return len(p) - keep, string(n[keep:])
}

// assemblerStep is one event and the text the assembler must show after it.
type assemblerStep struct {
	ev   xaiEvent
	want string
}

// runAssembler checks every step and the assembler's contract to the typer:
// the stable prefix never changes and stable never decreases.
func runAssembler(t *testing.T, steps []assemblerStep) (*xaiAssembler, []liveText) {
	t.Helper()
	var a xaiAssembler
	var states []liveText
	prev := liveText{}
	for i, step := range steps {
		a.apply(step.ev)
		got := a.current()
		if got.text != step.want {
			t.Fatalf("step %d (%+v): text = %q, want %q", i, step.ev, got.text, step.want)
		}
		checkStable(t, i, prev, got)
		prev = got
		states = append(states, got)
	}
	return &a, states
}

func checkStable(t *testing.T, i int, prev, got liveText) {
	t.Helper()
	if got.stable < prev.stable {
		t.Fatalf("step %d: stable fell from %d to %d", i, prev.stable, got.stable)
	}
	if got.stable > utf8.RuneCountInString(got.text) {
		t.Fatalf("step %d: stable %d is past the end of %q", i, got.stable, got.text)
	}
	if string([]rune(got.text)[:prev.stable]) != string([]rune(prev.text)[:prev.stable]) {
		t.Fatalf("step %d: stable prefix changed from %q to %q", i, prev.text, got.text)
	}
}

// The real sequence from a replay of the louder recording, text only. The
// second utterance opens with a comma, and its chunk final at 11.75 s reports
// the utterance's start, not the chunk's -- placing text by start is what
// produced phantom rewrites in the first prototype.
func TestAssemblerBriefExample(t *testing.T) {
	const (
		u2c1 = ", quick update on the dictation project."
		u2c2 = "I tried the streaming transcription from xAI today, and it was mostly fine, but it sometimes dropped entire sentences when I spoke quietly."
	)
	_, states := runAssembler(t, []assemblerStep{
		{interim("Hey"), "Hey"},
		{chunkFinal("Hey"), "Hey"},
		{utteranceFinal("Hey"), "Hey"},
		{interim(", quick update on the."), "Hey, quick update on the."},
		{interim(u2c1), "Hey" + u2c1},
		{chunkFinal(u2c1), "Hey" + u2c1},
		{interim("I tried the stream..."), "Hey" + u2c1 + " I tried the stream..."},
		{interim("I tried the streaming transcription from"), "Hey" + u2c1 + " I tried the streaming transcription from"},
		{interim("I tried the streaming transcription from xAI today, and it..."), "Hey" + u2c1 + " I tried the streaming transcription from xAI today, and it..."},
		{interim("I tried the streaming transcription from xAI today, and it was mostly fine, but it sometimes dropped entire sentences when I"),
			"Hey" + u2c1 + " I tried the streaming transcription from xAI today, and it was mostly fine, but it sometimes dropped entire sentences when I"},
		{chunkFinal(u2c2), "Hey" + u2c1 + " " + u2c2},
		{utteranceFinal(u2c1 + " " + u2c2), "Hey" + u2c1 + " " + u2c2},
		{interim("I think the laptop"), "Hey" + u2c1 + " " + u2c2 + " I think the laptop"},
	})

	if states[2].stable != 3 {
		t.Errorf("after the first utterance final, stable = %d, want 3", states[2].stable)
	}
	// The utterance final repeats what its chunks already showed: no strokes.
	if back, add := textDiff(states[10].text, states[11].text); back != 0 || add != "" {
		t.Errorf("utterance final rewrote %d runes and added %q", back, add)
	}
	if want := utf8.RuneCountInString("Hey" + u2c1 + " " + u2c2); states[11].stable != want {
		t.Errorf("stable = %d, want the whole second utterance (%d)", states[11].stable, want)
	}
	// "stream..." -> "streaming transcription from" is the tail being revised.
	if back, _ := textDiff(states[6].text, states[7].text); back != 3 {
		t.Errorf("revising the interim backspaced %d, want 3", back)
	}
}

func TestAssemblerInterimIsCurrentChunkNotUtterance(t *testing.T) {
	runAssembler(t, []assemblerStep{
		{interim("Hey, quick"), "Hey, quick"},
		{chunkFinal("Hey, quick"), "Hey, quick"},
		// After a chunk final the interim is only the unlocked remainder.
		{interim("update on the"), "Hey, quick update on the"},
		{interim(", update on the"), "Hey, quick, update on the"},
	})
}

// A chunk final can cut a word in half, and the cut shows ("dr opped") until
// the utterance final repairs it: when the chunk arrives there is no telling a
// cut from a word boundary.
func TestAssemblerChunkFinalAppendsAndUtteranceFinalReplaces(t *testing.T) {
	a, _ := runAssembler(t, []assemblerStep{
		{chunkFinal("I tried it"), "I tried it"},
		{chunkFinal("but it sometimes dr"), "I tried it but it sometimes dr"},
		{interim("opped"), "I tried it but it sometimes dr opped"},
		{utteranceFinal("I tried it, but it sometimes dropped"), "I tried it, but it sometimes dropped"},
	})
	if a.chunks != "" || a.interim != "" {
		t.Errorf("utterance final left chunks=%q interim=%q", a.chunks, a.interim)
	}
}

func TestAssemblerSilenceEventsChangeNothing(t *testing.T) {
	var steps []assemblerStep
	for range 3 {
		steps = append(steps, assemblerStep{interim(""), ""}, assemblerStep{chunkFinal(""), ""})
	}
	steps = append(steps, assemblerStep{utteranceFinal(""), ""})
	runAssembler(t, steps)

	steps = []assemblerStep{{utteranceFinal("Closed."), "Closed."}}
	for range 3 {
		steps = append(steps, assemblerStep{interim(""), "Closed."}, assemblerStep{chunkFinal(""), "Closed."})
	}
	steps = append(steps, assemblerStep{utteranceFinal(""), "Closed."})
	runAssembler(t, steps)
}

// Unobserved in every log and probe: an empty utterance final after real
// chunks. The chunks are the only record of what was said, so they stay.
func TestAssemblerEmptyUtteranceFinalKeepsChunks(t *testing.T) {
	_, states := runAssembler(t, []assemblerStep{
		{utteranceFinal("First."), "First."},
		{chunkFinal("Second part"), "First. Second part"},
		{interim("trailing"), "First. Second part trailing"},
		{utteranceFinal(""), "First. Second part"},
		{interim("Third"), "First. Second part Third"},
	})
	if states[3].stable != utf8.RuneCountInString("First. Second part") {
		t.Errorf("stable = %d, want the kept chunks closed", states[3].stable)
	}
}

func TestAssemblerDoneTextIgnored(t *testing.T) {
	runAssembler(t, []assemblerStep{
		{utteranceFinal("Kept."), "Kept."},
		{xaiEvent{Type: "transcript.done", Text: "Something else entirely."}, "Kept."},
		{xaiEvent{Type: "error", Message: "boom", Text: "nope"}, "Kept."},
		{xaiEvent{Type: "transcript.created"}, "Kept."},
	})
}

func TestAssemblerOpenUtteranceIsCommittedAtEnd(t *testing.T) {
	var a xaiAssembler
	for _, ev := range []xaiEvent{
		utteranceFinal("Closed."),
		chunkFinal("Locked chunk"),
		interim("and an unfinished interim"),
	} {
		a.apply(ev)
	}
	got := a.current()
	if got.text != "Closed. Locked chunk and an unfinished interim" {
		t.Errorf("text = %q", got.text)
	}
	if got.stable != len("Closed.") {
		t.Errorf("stable = %d, want only the closed utterance", got.stable)
	}
}

// speech_final is what closes an utterance. One that arrives without is_final
// must not be taken for an interim, which the next interim would replace.
func TestAssemblerSpeechFinalWithoutIsFinalClosesTheUtterance(t *testing.T) {
	a, states := runAssembler(t, []assemblerStep{
		{xaiEvent{Type: "transcript.partial", Text: "Hello there.", SpeechFinal: true}, "Hello there."},
		{interim("Next"), "Hello there. Next"},
	})
	if a.closed != "Hello there." || states[0].stable != len("Hello there.") {
		t.Errorf("closed = %q, stable = %d; want the utterance closed", a.closed, states[0].stable)
	}
}

// probe-c-stop8.5-done: audio.done mid-word. The reply is a chunk final and
// an utterance final with the same text, the whole utterance so far.
func TestAssemblerFixtureStopMidWord(t *testing.T) {
	const pair = "Hey, quick update on the dictation project. I tried the streaming transcription from XAI today, and it was mostly fine, but it sometimes"
	a, states := runAssembler(t, []assemblerStep{
		{interim(""), ""},
		{chunkFinal(""), ""},
		{interim("Hey, quick update on the"), "Hey, quick update on the"},
		{interim("Hey, quick update on the dictation project."), "Hey, quick update on the dictation project."},
		{interim("Hey, quick update on the dictation project. I tried the"), "Hey, quick update on the dictation project. I tried the"},
		{interim("Hey, quick update on the dictation project. I tried the streaming transcription from"),
			"Hey, quick update on the dictation project. I tried the streaming transcription from"},
		{interim("Hey, quick update on the dictation project. I tried the streaming transcription from XAI today."),
			"Hey, quick update on the dictation project. I tried the streaming transcription from XAI today."},
		{interim("Hey, quick update on the dictation project. I tried the streaming transcription from XAI today, and it was mostly fine,"),
			"Hey, quick update on the dictation project. I tried the streaming transcription from XAI today, and it was mostly fine,"},
		{chunkFinal(pair), pair},
		{utteranceFinal(pair), pair},
		{xaiEvent{Type: "transcript.done"}, pair},
	})
	if a.closed != pair {
		t.Errorf("closed = %q, want the pair's text once", a.closed)
	}
	if back, add := textDiff(states[7].text, states[8].text); back != 0 || add != " but it sometimes" {
		t.Errorf("stop reply from the last interim: %d backspaces, %q typed; want an append", back, add)
	}
}

// probe-a1, 3.0-12.5 s: finalize mid-sentence closes the utterance there, and
// the rest of the sentence arrives as a new utterance.
func TestAssemblerFixtureFinalizeMidSentence(t *testing.T) {
	const (
		u1 = "Hey, quick update on the"
		u2 = "dictation project. I tried the streaming transcription from XAI today, and it was mostly fine, but it sometimes"
		u3 = "dropped entire sentences when I spoke quietly."
	)
	_, states := runAssembler(t, []assemblerStep{
		{interim(u1), u1},
		{chunkFinal(u1), u1},
		{utteranceFinal(u1), u1},
		{interim("dictation project."), u1 + " dictation project."},
		{interim("dictation project. I tried the"), u1 + " dictation project. I tried the"},
		{chunkFinal(u2), u1 + " " + u2},
		{utteranceFinal(u2), u1 + " " + u2},
		{interim("dropped entire sentences."), u1 + " " + u2 + " dropped entire sentences."},
		{chunkFinal(u3), u1 + " " + u2 + " " + u3},
		{utteranceFinal(u3), u1 + " " + u2 + " " + u3},
	})
	for _, i := range []int{2, 6, 9} {
		if states[i].stable <= states[i-1].stable {
			t.Errorf("utterance final at step %d did not advance stable (%d -> %d)", i, states[i-1].stable, states[i].stable)
		}
		if states[i].stable != utf8.RuneCountInString(states[i].text) {
			t.Errorf("step %d: stable = %d, want all of %q", i, states[i].stable, states[i].text)
		}
	}
}

// probe-a1, 50.9-58 s: an utterance with two chunk finals. The server's own
// utterance final is exactly joinPieces of the two, so nothing is retyped.
func TestAssemblerFixtureTwoChunkUtterance(t *testing.T) {
	const (
		closed = "If one of those fixes it, we can build"
		c1     = "streaming into the Go version."
		c2     = "and type the text as I speak. Otherwise, we might stay on version four for now."
	)
	_, states := runAssembler(t, []assemblerStep{
		{utteranceFinal(closed), closed},
		{interim("streaming into the"), closed + " streaming into the"},
		{chunkFinal(c1), closed + " " + c1},
		{interim("and type the text as I speak."), closed + " " + c1 + " and type the text as I speak."},
		{interim("and type the text as I speak. Otherwise, we might stay on version four."),
			closed + " " + c1 + " and type the text as I speak. Otherwise, we might stay on version four."},
		{chunkFinal(c2), closed + " " + c1 + " " + c2},
		{utteranceFinal(c1 + " " + c2), closed + " " + c1 + " " + c2},
	})
	if back, add := textDiff(states[5].text, states[6].text); back != 0 || add != "" {
		t.Errorf("utterance final rewrote %d runes and added %q", back, add)
	}
}

// Synthetic, from the probe summary: a flush answered with the utterance so
// far as a chunk final, in an utterance that already had a chunk final.
func TestAssemblerFixtureRepeatedPrefixChunkFinal(t *testing.T) {
	runAssembler(t, []assemblerStep{
		{chunkFinal("A B."), "A B."},
		{chunkFinal("A B. C D"), "A B. C D"},
		{utteranceFinal("A B. C D"), "A B. C D"},
	})
}

// probe-a1, 25 s: finalize during silence answers with an empty chunk final
// and an empty utterance final.
func TestAssemblerFixtureSilenceFlush(t *testing.T) {
	const closed = "anything, so it handles quiet speech better."
	runAssembler(t, []assemblerStep{
		{utteranceFinal(closed), closed},
		{interim(""), closed},
		{chunkFinal(""), closed},
		{utteranceFinal(""), closed},
		{interim("Next."), closed + " Next."},
	})
}

// probe-d1: 2.6 s of backlog written in one burst. The first interim is a
// guess the second one revises in place.
func TestAssemblerFixtureBurstStart(t *testing.T) {
	_, states := runAssembler(t, []assemblerStep{
		{interim("Hey, quicker."), "Hey, quicker."},
		{interim("Hey, quick update."), "Hey, quick update."},
	})
	back, add := textDiff(states[0].text, states[1].text)
	if back != 3 || add != " update." {
		t.Errorf("diff = %d backspaces + %q, want a 3-rune rewrite", back, add)
	}
	if states[1].stable != 0 {
		t.Errorf("interims closed %d runes", states[1].stable)
	}
}
