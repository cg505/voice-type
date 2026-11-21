package voicetype

import (
	"context"
	"encoding/binary"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestLiveTranscribeStream streams a recording through a real streaming
// service with the real session code, at real-time pace, typing into a fake
// keyboard -- never into the focused window. It is skipped unless
// VOICE_TYPE_LIVE=1 and the service's key are set, so `go test ./...` stays
// hermetic.
//
// VOICE_TYPE_MODEL picks the streaming model (default: xAI's).
// VOICE_TYPE_PCM is a 16 kHz mono s16le file, headerless or WAV.
// VOICE_TYPE_STOP_AT=<seconds> stops there, mid-speech if that is where it
// falls, the way a user's stop press does.
func TestLiveTranscribeStream(t *testing.T) {
	if os.Getenv("VOICE_TYPE_LIVE") != "1" {
		t.Skip("set VOICE_TYPE_LIVE=1 to run the live streaming test")
	}
	cfg := realConfig(t)
	if id := os.Getenv("VOICE_TYPE_MODEL"); id != "" {
		cfg.Model = id
	} else {
		cfg.Model = "grok-voice-transcribe-2.0"
	}
	spec, ok := sttLookup(cfg.Model)
	if !ok || !spec.Streams() {
		t.Fatalf("VOICE_TYPE_MODEL=%q is not a streaming model", cfg.Model)
	}
	key := configStreamKey(cfg)
	if key == "" {
		t.Skipf("no key for %s: set XAI_API_KEY or AI_GATEWAY_API_KEY", spec.ID)
	}
	pcmPath := os.Getenv("VOICE_TYPE_PCM")
	if pcmPath == "" {
		t.Skip("set VOICE_TYPE_PCM to a 16 kHz mono s16le file")
	}

	raw, err := os.ReadFile(pcmPath)
	if err != nil {
		t.Fatalf("read pcm: %v", err)
	}
	if len(raw) >= wavHeaderSize && string(raw[:4]) == "RIFF" {
		raw = raw[wavHeaderSize:]
	}
	samples := make([]int16, len(raw)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
	}
	if stopAt := os.Getenv("VOICE_TYPE_STOP_AT"); stopAt != "" {
		seconds, err := strconv.ParseFloat(stopAt, 64)
		if err != nil {
			t.Fatalf("VOICE_TYPE_STOP_AT: %v", err)
		}
		samples = samples[:min(len(samples), int(seconds*wavSampleRate))]
	}
	t.Logf("audio: %.2fs", wavDurationSeconds(samples))

	client := newStreamClient(key, spec, streamVocabulary(cfg, workspaceLoad()))
	t.Logf("model: %s; keyterms: %d", spec.ID, len(client.keyterms))
	rec := &fakeRecorder{}
	kb := newFakeKeyboard()
	// The production key timing, so a slow typist and coalescing are exercised.
	typer := &Typer{kb: kb, keyHold: typeKeyHold, keyDelay: typeKeyDelay}
	s := newStreamSession(client, rec, typer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Feeding starts before the dial, so the backlog goes up as a burst after
	// the handshake, as it does in the daemon.
	fed := make(chan struct{})
	go func() {
		defer close(fed)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < len(samples); i += agcFrameSamples {
			<-ticker.C
			rec.feed(samples[i:min(i+agcFrameSamples, len(samples))])
		}
	}()
	go s.run(ctx)
	<-fed

	recording, _ := rec.Stop()
	stopped := time.Now()
	text, streamErr, typeErr := s.finish(ctx, recording)
	st := s.stats()
	t.Logf("connect:      %v", st.connect.Round(time.Millisecond))
	t.Logf("stop -> done: %v (end of audio -> the service's last word %v)", time.Since(stopped).Round(time.Millisecond), st.reply.Round(time.Millisecond))
	t.Logf("events:       %d; typed %d, backspaces %d (max rewrite %d, %d rewrites); %.1fs audio sent",
		st.events, st.typed, st.backspaces, st.maxRewrite, st.rewrites, st.audioSeconds)
	t.Logf("text:         %q", text)
	if screen := screenText(kb.snapshot()); screen != text {
		t.Errorf("the screen reads %q, not the returned text", screen)
	}
	if streamErr != nil || typeErr != nil {
		t.Fatalf("stream %v, typing %v", streamErr, typeErr)
	}
	if text == "" {
		t.Error("empty transcript")
	}
}
