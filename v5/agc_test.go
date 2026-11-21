package voicetype

import (
	"math"
	"slices"
	"testing"
)

// runAGC processes samples frame by frame, as the sender does, and returns
// the output alongside the AGC so tests can inspect its state.
func runAGC(g *agc, samples []int16) []int16 {
	out := slices.Clone(samples)
	for i := 0; i < len(out); i += agcFrameSamples {
		g.process(out[i:min(i+agcFrameSamples, len(out))])
	}
	return out
}

func closeTo(got, want, tolerance float64) bool { return math.Abs(got-want) <= tolerance }

// Quiet laptop-mic speech (-40 dBFS) after room tone comes out near -20 dBFS.
func TestAGCBoostsQuietSpeech(t *testing.T) {
	g := newAGC()
	in := concat(silence(1, 15), speech(2, 300))
	out := runAGC(g, in)

	speechStart := wavSampleRate
	// The first speech frame ramps up to the new gain; every one after it is on target.
	for i := speechStart + agcFrameSamples; i < len(out); i += agcFrameSamples * 25 {
		frame := out[i : i+agcFrameSamples]
		if rms := frameRMS(frame); !closeTo(rms, agcTargetRMS, agcTargetRMS*0.05) {
			t.Fatalf("speech frame at %.2fs: RMS %.0f, want about %.0f", float64(i)/wavSampleRate, rms, agcTargetRMS)
		}
	}
	if !slices.Equal(out[:speechStart], in[:speechStart]) {
		t.Error("room tone before any speech was rescaled")
	}
}

func TestAGCFirstVoicedFrameJumpsExactly(t *testing.T) {
	g := newAGC()
	runAGC(g, silence(0.5, 15))
	if g.voiced || g.gain != 1 {
		t.Fatalf("room tone set voiced=%t gain=%f", g.voiced, g.gain)
	}

	frame := speech(0.02, 300)
	g.process(frame)
	want := agcTargetRMS / 300
	if !g.voiced || g.level != 300 || !closeTo(g.gain, want, 1e-9) {
		t.Fatalf("after the first voiced frame: voiced=%t level=%f gain=%f, want gain %f", g.voiced, g.level, g.gain, want)
	}
	// The interpolation ends on the new gain: the opening word is boosted
	// within its first frame rather than faded in over seconds.
	if last := frame[len(frame)-1]; last != clamp16(-300*want) {
		t.Errorf("last sample = %d, want %d", last, clamp16(-300*want))
	}
}

func TestAGCLeavesLoudSpeechAlone(t *testing.T) {
	g := newAGC()
	in := concat(silence(0.5, 30), speech(2, 5000), silence(0.5, 30), speech(1, 4000))
	if out := runAGC(g, in); !slices.Equal(out, in) {
		t.Error("speech above the target level was rescaled")
	}
	if g.gain != 1 {
		t.Errorf("gain = %f, want 1", g.gain)
	}
}

func TestAGCDoesNotPumpSilence(t *testing.T) {
	g := newAGC()
	tone := silence(10, 60)
	if out := runAGC(g, tone); !slices.Equal(out, tone) {
		t.Error("ten seconds of room tone were rescaled")
	}
	if g.voiced {
		t.Error("room tone counted as speech")
	}

	runAGC(g, speech(1, 300))
	afterSpeech := g.gain
	runAGC(g, silence(5, 60))
	if g.gain > afterSpeech+1e-9 {
		t.Errorf("a pause raised the gain from %f to %f", afterSpeech, g.gain)
	}
}

func TestAGCDucksFastRecoversSlowly(t *testing.T) {
	quiet := agcTargetRMS / 300

	g := &agc{gain: 1, level: 300, floor: 10, voiced: true}
	g.process(speech(0.02, 300))
	if want := 1 + (quiet-1)*agcRecoverStep; !closeTo(g.gain, want, 1e-9) {
		t.Errorf("recovery step: gain = %f, want %f", g.gain, want)
	}

	g = &agc{gain: quiet, level: 300, floor: 10, voiced: true}
	g.process(speech(0.02, 5000))
	if want := quiet + (1-quiet)*agcDuckStep; !closeTo(g.gain, want, 1e-9) {
		t.Errorf("duck step: gain = %f, want %f", g.gain, want)
	}

	// A shout ducks within a third of a second; quiet speech after it takes
	// seconds to come back up.
	g = newAGC()
	runAGC(g, concat(silence(0.5, 15), speech(1, 300)))
	runAGC(g, speech(0.3, 5000))
	if g.gain > 1.1 {
		t.Errorf("gain %f after 300 ms of loud speech, want ducked to about 1", g.gain)
	}
	runAGC(g, speech(0.2, 300))
	if g.gain > 1.5 {
		t.Errorf("gain %f 200 ms after the shout, want a slow recovery", g.gain)
	}
}

func TestAGCGainCap(t *testing.T) {
	g := newAGC()
	runAGC(g, concat(silence(1, 15), speech(1, 40)))
	if !closeTo(g.gain, agcMaxGain, 1e-9) {
		t.Errorf("gain = %f, want capped at %f", g.gain, agcMaxGain)
	}
}

func TestAGCClampsWithoutWraparound(t *testing.T) {
	g := &agc{gain: 10, level: 300, floor: 10, voiced: true}
	frame := speech(0.02, 20000)
	g.process(frame)
	for i, v := range frame {
		if v != 32767 && v != -32767 {
			t.Fatalf("sample %d = %d, want clamped to the full scale of its sign", i, v)
		}
	}
	if frame[0] != 32767 || frame[40] != -32767 {
		t.Errorf("clamping flipped a sign: %d, %d", frame[0], frame[40])
	}
	if clamp16(-40000) != -32767 || clamp16(1.9) != 1 || clamp16(-1.9) != -1 {
		t.Errorf("clamp16 does not truncate toward zero within range")
	}
}

func TestAGCPartialFrame(t *testing.T) {
	g := newAGC()
	runAGC(g, silence(0.5, 15))
	frame := speech(0.02, 300)[:100]
	g.process(frame)
	if len(frame) != 100 {
		t.Fatalf("frame length changed to %d", len(frame))
	}
	// The last sample of a short frame still lands exactly on the new gain.
	if want := clamp16(300 * agcTargetRMS / 300); frame[99] != want {
		t.Errorf("last sample = %d, want %d", frame[99], want)
	}

	before := *g
	g.process(nil)
	if *g != before {
		t.Error("an empty frame changed the state")
	}
}
