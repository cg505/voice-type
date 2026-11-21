package voicetype

import (
	"slices"
	"testing"
)

// The streaming sender polls Samples while the pulse callback keeps appending,
// so what it gets must be its own copy, and only what is new.
func TestRecorderSamplesCopiesNewAudio(t *testing.T) {
	r := &Recorder{samples: []int16{1, 2, 3, 4}}

	got := r.Samples(1)
	if !slices.Equal(got, []int16{2, 3, 4}) {
		t.Fatalf("Samples(1) = %v", got)
	}
	got[0] = 99
	if r.samples[1] != 2 {
		t.Error("Samples returned a view of the capture buffer")
	}
	if got := r.Samples(4); got != nil {
		t.Errorf("Samples at the end = %v, want nil", got)
	}
	if got := r.Samples(10); got != nil {
		t.Errorf("Samples past the end (a poll racing Stop) = %v, want nil", got)
	}
}
