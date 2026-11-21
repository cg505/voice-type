package voicetype

// Automatic gain control for the stream. xAI's voice activity gate decides
// what gets transcribed, and it judges level: quiet laptop-mic speech at
// -40 dBFS lost whole sentences to it (26% WER), and this causal gain brought
// the same recording to 11%. Loud speech (-36 dBFS) is left alone (0.7%
// either way). The gap between speech and the noise floor matters more than
// raw level, so the floor is tracked and frames near it never raise the gain:
// pauses do not get pumped up into hiss.
//
// It only ever runs on what is streamed. Batch uploads, the batch fallback and
// retained WAVs are the raw recording.

// Tuned once, on purpose, and not user-facing. Ported from the prototype the
// WER figures above were measured with.
const (
	agcFrameSamples = trimFrameSamples   // 20 ms, the same frame the trimmer uses
	agcTargetRMS    = 3276.8             // -20 dBFS
	agcMaxGain      = 31.622776601683793 // +30 dB
	agcInitialFloor = 30.0               // about -60 dBFS
	agcFloorRise    = 1.002              // the floor creeps up by this factor per frame...
	agcFloorStep    = 0.5                // ...plus this much, so it can only climb slowly
	agcVoiceRatio   = 3.0                // a frame is voiced above this multiple of the floor...
	agcVoiceMinRMS  = 30.0               // ...and above this absolute level
	agcLevelDecay   = 0.9933555062550344 // exp(-0.02/3): the speech level forgets over ~3 s
	agcDuckStep     = 0.3                // gain moves this fraction of the way down per frame
	agcRecoverStep  = 0.02               // and this fraction of the way up
)

type agc struct {
	gain, level, floor float64
	voiced             bool // level has been initialised by a voiced frame
}

func newAGC() *agc { return &agc{gain: 1, floor: agcInitialFloor} }

// process rescales one frame in place. Frames are agcFrameSamples long except
// the last one of a session, which may be shorter and is handled the same way.
func (g *agc) process(frame []int16) {
	if len(frame) == 0 {
		return
	}
	r := frameRMS(frame)
	g.floor = min(r, g.floor*agcFloorRise+agcFloorStep)
	first := false
	if r > max(g.floor*agcVoiceRatio, agcVoiceMinRMS) {
		if !g.voiced {
			g.level, g.voiced, first = r, true, true
		} else {
			g.level = max(r, g.level*agcLevelDecay)
		}
	}

	want := 1.0
	if g.voiced {
		want = min(agcMaxGain, max(1, agcTargetRMS/max(g.level, 1)))
	}
	// Jump straight to the first estimate so the opening words are boosted too;
	// after that, duck fast and recover slowly.
	step := agcRecoverStep
	if first {
		step = 1
	} else if want < g.gain {
		step = agcDuckStep
	}
	start := g.gain
	g.gain += (want - g.gain) * step

	// Interpolating across the frame keeps a gain change from being a step,
	// which would click.
	n := float64(len(frame))
	for j, v := range frame {
		frame[j] = clamp16(float64(v) * (start + (g.gain-start)*float64(j+1)/n))
	}
}

// clamp16 truncates toward zero and hard-clips. The clipping measured at
// 0.002-0.04% of samples on the reference recordings, and the WER figures were
// obtained with exactly this clamp, so there is no soft limiter.
func clamp16(v float64) int16 {
	switch {
	case v >= 32767:
		return 32767
	case v <= -32767:
		return -32767
	}
	return int16(v)
}
