package voicetype

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/bendahl/uinput"
)

func TestJoinPiecesPunctuation(t *testing.T) {
	cases := []struct{ a, b, want string }{
		{"Hey", ", quick update", "Hey, quick update"},
		{"project.", "I tried", "project. I tried"},
		{"done", ". Next", "done. Next"},
		{"a", "; b", "a; b"},
		{"a", ": b", "a: b"},
		{"a", "! b", "a! b"},
		{"a", "? b", "a? b"},
		{"  a  ", "  b  ", "a b"},
		{"", " b ", "b"},
		{" a ", "", "a"},
		{"", "", ""},
		{"version", "\"quoted\"", "version \"quoted\""},
		{"café", "über", "café über"},
	}
	for _, c := range cases {
		if got := joinPieces(c.a, c.b); got != c.want {
			t.Errorf("joinPieces(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}

func TestSanitizeLive(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain ASCII, as it is: ~!@#", "plain ASCII, as it is: ~!@#"},
		{"it’s ‘a’ “quote” – dash — and…", "it's 'a' \"quote\" - dash - and..."},
		{"line\r\nbreak\ttab\x00nul\u0085next", "line  break tab nul next"},
		{"café, naïve, Zürich, Łódź, Ærø, straße", "cafe, naive, Zurich, Lodz, AEro, strasse"},
		{"€5, £3 and 20° or 5 × 4 ≥ ½", "EUR5, GBP3 and 20 degrees or 5 x 4 >= 1/2"},
		{"no break, zero​width, soft­hyphen", "no break, zerowidth, softhyphen"},
		{"é decomposed", "e decomposed"},
		{"¿Qué? ¡Sí!", "Que? Si!"},
		{"\U0001F642 日本 \xff", "? ?? ?"},
		{"", ""},
	}
	for _, c := range cases {
		if got := sanitizeLive(c.in); got != c.want {
			t.Errorf("sanitizeLive(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Whatever the transcript holds, what is typed is plain ASCII: one key per
// rune, and never the hex entry whose Enter would submit the user's message.
func TestSanitizeLiveNeverNeedsHexEntry(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var in strings.Builder
	for r := rune(0); r < 0x3000; r++ {
		if utf8.ValidRune(r) {
			in.WriteRune(r)
		}
	}
	for range 2000 {
		in.WriteRune(rune(rng.Intn(0x10FFFF)))
	}
	in.WriteString("\xff\xfe\xc0")

	got := sanitizeLive(in.String())
	for i := 0; i < len(got); i++ {
		if got[i] < 0x20 || got[i] > 0x7e {
			t.Fatalf("byte %#x at %d is not printable ASCII", got[i], i)
		}
	}
	kb := newFakeKeyboard()
	lt := &liveTyper{typer: &Typer{kb: kb}}
	if err := lt.apply(context.Background(), liveText{text: got}); err != nil {
		t.Fatal(err)
	}
	events := kb.snapshot()
	for _, key := range []int{uinput.KeyEnter, uinput.KeyLeftctrl} {
		if n := countKey(events, key); n != 0 {
			t.Errorf("typing the sanitized text pressed key %d %d times", key, n)
		}
	}
	if screen := screenText(events); screen != got {
		t.Error("the screen does not read what was sanitized")
	}
}

// The stable count is taken on the typed form, where one rune can become
// several characters or none. Mapping rune by rune is what keeps the settled
// part a prefix of the whole.
func TestNewLiveTextKeepsSettledPrefix(t *testing.T) {
	pieces := []string{"a", "é", "€", "…", "°", "​", "é", " ", "ß", "🙂", "\n", "”", "Ĳ"}
	rng := rand.New(rand.NewSource(2))
	for range 2000 {
		var settled, rest strings.Builder
		for range rng.Intn(6) {
			settled.WriteString(pieces[rng.Intn(len(pieces))])
		}
		for range rng.Intn(6) {
			rest.WriteString(pieces[rng.Intn(len(pieces))])
		}
		got := newLiveText(settled.String(), settled.String()+rest.String())
		if want := sanitizeLive(settled.String()); string([]rune(got.text)[:got.stable]) != want {
			t.Fatalf("settled %q of %q: stable prefix = %q, want %q",
				settled.String(), settled.String()+rest.String(), string([]rune(got.text)[:got.stable]), want)
		}
	}
}
