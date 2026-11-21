package voicetype

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// liveText is what should be on screen now. stable is the number of leading
// runes that will never change again; the typer may backspace only past that
// point.
type liveText struct {
	text   string
	stable int
}

// newLiveText is the state for text whose leading part, settled, will not
// change. settled must be a prefix of text. Both are made typeable the same
// way, rune by rune, so it still is one afterwards.
func newLiveText(settled, text string) liveText {
	return liveText{text: sanitizeLive(text), stable: utf8.RuneCountInString(sanitizeLive(settled))}
}

// joinPieces joins two transcript pieces with one space, except before a
// leading , . ; : ! ? -- a piece can start with punctuation and carries no
// leading space. Both sides are trimmed; an empty side yields the other.
func joinPieces(a, b string) string {
	a, b = strings.TrimSpace(a), strings.TrimSpace(b)
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case strings.ContainsAny(b[:1], ",.;:!?"):
		return a + b
	default:
		return a + " " + b
	}
}

func squashSpace(s string) string { return strings.Join(strings.Fields(s), " ") }

// sanitizeLive makes a transcript typeable live: printable ASCII and nothing
// else, one key per rune.
//
// The typer's only way to produce any other rune is GTK's Ctrl+Shift+U hex
// entry, which ends in Enter. In a window without that input method the
// shortcut does whatever the application binds to it, the digits land as text,
// and the Enter submits the message the user is still dictating. Batch output
// is pasted, not typed, and keeps the transcript's own characters.
//
// So accents come off (café -> cafe), typographic punctuation becomes its
// ASCII form, a few symbols are spelled out, and a rune with no plain
// equivalent becomes "?". Each rune maps on its own, which is what keeps a
// prefix a prefix (newLiveText).
func sanitizeLive(s string) string {
	plain := true
	for i := 0; i < len(s) && plain; i++ {
		plain = s[i] >= 0x20 && s[i] <= 0x7e
	}
	if plain {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		b.WriteString(liveRune(r))
	}
	return b.String()
}

func liveRune(r rune) string {
	if r >= 0x20 && r <= 0x7e {
		return string(r)
	}
	if plain, ok := liveFold[r]; ok {
		return plain
	}
	switch {
	case r == utf8.RuneError:
		return "?"
	// typeCompile rejects control runes, and "\r\n" is one Enter stroke that
	// one backspace could not undo.
	case unicode.IsControl(r), unicode.IsSpace(r):
		return " "
	// Combining marks and invisible format runes (zero-width joiners, the
	// soft hyphen) add nothing a key could type.
	case unicode.Is(unicode.Mn, r), unicode.Is(unicode.Cf, r):
		return ""
	}
	return "?"
}

// liveFold is the plain form of the runes an English transcript can plausibly
// carry: the Latin letters with diacritics, typographic punctuation, and the
// symbols a model writes for spoken words.
var liveFold = func() map[rune]string {
	fold := map[rune]string{}
	for _, entry := range []struct{ runes, plain string }{
		{"ÀÁÂÃÄÅĀĂĄ", "A"}, {"àáâãäåāăą", "a"}, {"Æ", "AE"}, {"æ", "ae"},
		{"ÇĆĈĊČ", "C"}, {"çćĉċč", "c"}, {"ÐĎĐ", "D"}, {"ðďđ", "d"},
		{"ÈÉÊËĒĔĖĘĚ", "E"}, {"èéêëēĕėęě", "e"},
		{"ĜĞĠĢ", "G"}, {"ĝğġģ", "g"}, {"ĤĦ", "H"}, {"ĥħ", "h"},
		{"ÌÍÎÏĨĪĬĮİ", "I"}, {"ìíîïĩīĭįı", "i"}, {"Ĳ", "IJ"}, {"ĳ", "ij"},
		{"Ĵ", "J"}, {"ĵ", "j"}, {"Ķ", "K"}, {"ķĸ", "k"},
		{"ĹĻĽĿŁ", "L"}, {"ĺļľŀł", "l"}, {"ÑŃŅŇŊ", "N"}, {"ñńņňŉŋ", "n"},
		{"ÒÓÔÕÖØŌŎŐ", "O"}, {"òóôõöøōŏő", "o"}, {"Œ", "OE"}, {"œ", "oe"},
		{"ŔŖŘ", "R"}, {"ŕŗř", "r"}, {"ŚŜŞŠȘ", "S"}, {"śŝşšșſ", "s"}, {"ß", "ss"},
		{"ŢŤŦȚ", "T"}, {"ţťŧț", "t"}, {"Þ", "Th"}, {"þ", "th"},
		{"ÙÚÛÜŨŪŬŮŰŲ", "U"}, {"ùúûüũūŭůűų", "u"}, {"Ŵ", "W"}, {"ŵ", "w"},
		{"ÝŶŸ", "Y"}, {"ýÿŷ", "y"}, {"ŹŻŽ", "Z"}, {"źżž", "z"},

		{"‘’‚‛′‹›", "'"}, {"“”„‟″«»", `"`}, {"‐‑‒–—―−", "-"}, {"…", "..."},
		{"•", "*"}, {"·", "."}, {"¡¿", ""},

		{"€", "EUR"}, {"£", "GBP"}, {"¥", "JPY"}, {"¢", "c"}, {"°", " degrees"},
		{"×", "x"}, {"÷", "/"}, {"±", "+/-"}, {"≤", "<="}, {"≥", ">="}, {"≠", "!="}, {"≈", "~"},
		{"→", "->"}, {"←", "<-"}, {"⇒", "=>"},
		{"½", "1/2"}, {"¼", "1/4"}, {"¾", "3/4"}, {"²", "^2"}, {"³", "^3"}, {"µ", "u"},
		{"©", "(c)"}, {"®", "(R)"}, {"™", "(TM)"}, {"№", "No."},
	} {
		for _, r := range entry.runes {
			fold[r] = entry.plain
		}
	}
	return fold
}()
