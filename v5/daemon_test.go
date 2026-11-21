package voicetype

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These cover the guard clauses, which run before any audio hardware is
// touched, so the state machine stays testable without a microphone.

type fakeRecorder struct {
	startEntered chan struct{}
	startRelease chan struct{}
	startCalls   atomic.Int32
	stopCalls    atomic.Int32
	stopErr      error

	mu      sync.Mutex
	samples []int16 // the capture buffer; feed appends to it
}

// feed appends captured audio, as the pulse callback does.
func (r *fakeRecorder) feed(samples []int16) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, samples...)
}

func (r *fakeRecorder) Samples(from int) []int16 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if from >= len(r.samples) {
		return nil
	}
	return slices.Clone(r.samples[from:])
}

func (r *fakeRecorder) Start() error {
	r.startCalls.Add(1)
	if r.startEntered != nil {
		close(r.startEntered)
	}
	if r.startRelease != nil {
		<-r.startRelease
	}
	return nil
}

// Stop empties the capture buffer, as Recorder.Stop does: afterwards Samples
// returns nothing, and the recording exists only in what Stop returned.
func (r *fakeRecorder) Stop() ([]int16, error) {
	r.stopCalls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	out := slices.Clone(r.samples)
	r.samples = r.samples[:0]
	return out, r.stopErr
}

func (r *fakeRecorder) Close() {}

func assumeOnline(d *daemon) {
	d.checkOnline = func(context.Context) error { return nil }
}

func TestStartRejectsWhenBusy(t *testing.T) {
	for _, state := range []sessionState{stateChecking, stateRecording, stateTranscribing} {
		d := &daemon{cfg: configDefaults(), state: state}
		status, msg := d.startSession()
		if status != http.StatusConflict {
			t.Errorf("state %v: status = %d, want 409", state, status)
		}
		if msg == "" {
			t.Errorf("state %v: expected a message", state)
		}
		if d.currentState() != state {
			t.Errorf("state %v: a rejected start must not change state", state)
		}
	}
}

func TestOfflineStartTypesMarkerWithoutOpeningRecorder(t *testing.T) {
	rec := &fakeRecorder{}
	kb := newFakeKeyboard()
	d := newDaemon(configDefaults(), &resources{Recorder: rec, Typer: &Typer{kb: kb}})
	d.checkOnline = func(context.Context) error { return errors.New("network unreachable") }

	status, message := d.startSession()
	if status != http.StatusServiceUnavailable || message != "offline" {
		t.Fatalf("start = %d/%q, want 503/offline", status, message)
	}
	if rec.startCalls.Load() != 0 || rec.stopCalls.Load() != 0 {
		t.Fatalf("offline start touched recorder: starts=%d stops=%d", rec.startCalls.Load(), rec.stopCalls.Load())
	}
	question := typeKeyMap['?']
	want := []testKeyEvent{
		{op: "down", key: question.modifiers[0]},
		{op: "down", key: question.key},
		{op: "up", key: question.modifiers[0]},
		{op: "up", key: question.key},
	}
	if len(kb.events) != len(want) {
		t.Fatalf("offline marker events = %#v, want %#v", kb.events, want)
	}
	for i := range want {
		if kb.events[i] != want[i] {
			t.Fatalf("offline marker events = %#v, want %#v", kb.events, want)
		}
	}
	if state := d.currentState(); state != stateIdle {
		t.Errorf("state = %s, want idle", state)
	}
}

func TestShutdownCancelsConnectivityCheckWithoutTypingMarker(t *testing.T) {
	entered := make(chan struct{})
	rec := &fakeRecorder{}
	kb := newFakeKeyboard()
	d := newDaemon(configDefaults(), &resources{Recorder: rec, Typer: &Typer{kb: kb}})
	d.checkOnline = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}

	startDone := make(chan int, 1)
	go func() {
		status, _ := d.startSession()
		startDone <- status
	}()
	<-entered
	d.shutdown()

	if status := <-startDone; status != http.StatusConflict {
		t.Errorf("cancelled start status = %d, want 409", status)
	}
	if rec.startCalls.Load() != 0 {
		t.Errorf("cancelled connectivity check opened recorder %d times", rec.startCalls.Load())
	}
	if len(kb.events) != 0 {
		t.Errorf("shutdown typed offline marker: %#v", kb.events)
	}
}

func TestStopIsNoOpUnlessRecording(t *testing.T) {
	for _, state := range []sessionState{stateIdle, stateTranscribing} {
		d := &daemon{cfg: configDefaults(), state: state}
		status, _ := d.stopSession()
		if status != http.StatusOK {
			t.Errorf("state %v: status = %d, want 200", state, status)
		}
		if d.currentState() != state {
			t.Errorf("state %v: a no-op stop must not change state", state)
		}
	}
}

func TestStopCancelsRecorderStart(t *testing.T) {
	rec := &fakeRecorder{
		startEntered: make(chan struct{}),
		startRelease: make(chan struct{}),
	}
	d := newDaemon(configDefaults(), &resources{Recorder: rec})
	assumeOnline(d)

	startDone := make(chan int, 1)
	go func() {
		status, _ := d.startSession()
		startDone <- status
	}()
	<-rec.startEntered

	stopDone := make(chan int, 1)
	go func() {
		status, _ := d.stopSession()
		stopDone <- status
	}()

	if status := <-stopDone; status != http.StatusOK {
		t.Fatalf("stop status = %d, want 200", status)
	}
	close(rec.startRelease)
	if status := <-startDone; status != http.StatusConflict {
		t.Fatalf("cancelled start status = %d, want 409", status)
	}
	if calls := rec.stopCalls.Load(); calls != 1 {
		t.Errorf("recorder stops = %d, want 1", calls)
	}
	if state := d.currentState(); state != stateIdle {
		t.Errorf("state = %s, want idle", state)
	}
}

func TestShutdownCanCancelBlockedRecorderStart(t *testing.T) {
	rec := &fakeRecorder{
		startEntered: make(chan struct{}),
		startRelease: make(chan struct{}),
	}
	d := newDaemon(configDefaults(), &resources{Recorder: rec})
	assumeOnline(d)

	startDone := make(chan int, 1)
	go func() {
		status, _ := d.startSession()
		startDone <- status
	}()
	<-rec.startEntered

	shutdownDone := make(chan struct{})
	go func() {
		d.shutdown()
		close(shutdownDone)
	}()

	deadline := time.Now().Add(time.Second)
	for d.currentState() != stateIdle && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if state := d.currentState(); state != stateIdle {
		t.Fatalf("state while recorder start blocked = %s, want idle", state)
	}
	close(rec.startRelease)

	if status := <-startDone; status != http.StatusConflict {
		t.Errorf("start after shutdown = %d, want 409", status)
	}
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown remained blocked after recorder start returned")
	}
	if rec.stopCalls.Load() != 1 {
		t.Errorf("cancelled recorder start stopped %d times, want 1", rec.stopCalls.Load())
	}
}

func TestHealthReportsState(t *testing.T) {
	d := &daemon{cfg: configDefaults(), state: stateRecording}

	rec := httptest.NewRecorder()
	d.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %v", body["status"])
	}
	if body["state"] != "recording" {
		t.Errorf("state = %v, want recording", body["state"])
	}
	if body["fallback"] != "gpt-transcribe" {
		t.Errorf("fallback = %v, want gpt-transcribe", body["fallback"])
	}
}

// /toggle while recording must not be routed to start.
func TestToggleWhileRecordingDoesNotStart(t *testing.T) {
	d := &daemon{cfg: configDefaults(), state: stateTranscribing}

	rec := httptest.NewRecorder()
	d.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/toggle", nil))

	// transcribing is not recording, so toggle tries to start and is refused
	if rec.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rec.Code)
	}
}

func TestExitShutsDown(t *testing.T) {
	d := newDaemon(configDefaults(), &resources{})

	rec := httptest.NewRecorder()
	d.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/exit", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	select {
	case <-d.done:
	default:
		t.Error("/exit did not signal shutdown")
	}

	// A second /exit must not panic on a closed channel.
	d.shutdown()
}

func TestShutdownStopsRecordingAndRejectsRestart(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"text": "must not appear"})
	}))
	defer srv.Close()

	rec := &fakeRecorder{samples: speech(1, 5000)}
	kb := newFakeKeyboard()
	d := newDaemon(configDefaults(), &resources{Recorder: rec, Typer: &Typer{kb: kb}})
	d.stt.endpoint = srv.URL
	assumeOnline(d)
	if status, _ := d.startSession(); status != http.StatusOK {
		t.Fatalf("start status = %d, want 200", status)
	}

	d.shutdown()

	if calls := rec.stopCalls.Load(); calls != 1 {
		t.Errorf("recorder stops = %d, want 1", calls)
	}
	if state := d.currentState(); state != stateIdle {
		t.Errorf("state = %s, want idle", state)
	}
	if status, _ := d.startSession(); status != http.StatusConflict {
		t.Errorf("start during shutdown = %d, want 409", status)
	}
	if requests.Load() != 0 {
		t.Errorf("shutdown sent %d transcription requests, want 0", requests.Load())
	}
	if len(kb.events) != 0 {
		t.Errorf("shutdown produced keyboard events: %#v", kb.events)
	}
}

func TestExpiredTimerDoesNotStopNewSession(t *testing.T) {
	rec := &fakeRecorder{}
	d := newDaemon(configDefaults(), &resources{Recorder: rec})
	d.state = stateRecording
	d.sessionID = 2
	d.sessionCtx = context.Background()

	status, _ := d.stopSessionFor(1)
	if status != http.StatusOK {
		t.Fatalf("stale stop status = %d, want 200", status)
	}
	if calls := rec.stopCalls.Load(); calls != 0 {
		t.Errorf("stale timer stopped recorder %d times", calls)
	}
	if state := d.currentState(); state != stateRecording {
		t.Errorf("state = %s, want recording", state)
	}

	d.stopSession()
}

func TestCaptureErrorIsNotReportedAsSilence(t *testing.T) {
	rec := &fakeRecorder{samples: speech(1, 5000), stopErr: errors.New("connection closed")}
	d := newDaemon(configDefaults(), &resources{Recorder: rec})
	d.state = stateRecording
	d.sessionCtx = context.Background()

	status, message := d.stopSession()
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", status)
	}
	if !strings.Contains(message, "connection closed") {
		t.Errorf("message = %q, want capture error", message)
	}
}

func TestShutdownWaitsForTranscription(t *testing.T) {
	d := newDaemon(configDefaults(), &resources{})
	d.state = stateTranscribing
	d.sessionWG.Add(1)

	shutdownDone := make(chan struct{})
	go func() {
		d.shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned while a session was in flight")
	case <-time.After(20 * time.Millisecond):
	}

	d.mu.Lock()
	d.state = stateIdle
	d.mu.Unlock()
	d.sessionWG.Done()
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after the session completed")
	}
}

func TestShutdownCancelsTranscriptionAndSkipsOutput(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-releaseRequest
	}))
	defer srv.Close()

	rec := &fakeRecorder{samples: concat(silence(0.1, 0), speech(0.6, 5000))}
	kb := newFakeKeyboard()
	d := newDaemon(configDefaults(), &resources{Recorder: rec, Typer: &Typer{kb: kb}})
	d.stt.endpoint = srv.URL
	assumeOnline(d)
	if status, message := d.startSession(); status != http.StatusOK {
		t.Fatalf("start = %d/%q", status, message)
	}

	type stopResult struct {
		status  int
		message string
	}
	stopDone := make(chan stopResult, 1)
	go func() {
		status, message := d.stopSession()
		stopDone <- stopResult{status: status, message: message}
	}()
	<-requestStarted

	started := time.Now()
	d.shutdown()
	close(releaseRequest)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("shutdown cancellation took %v", elapsed)
	}
	result := <-stopDone
	if result.status != http.StatusOK || result.message != "cancelled" {
		t.Errorf("stop after shutdown = %d/%q, want 200/cancelled", result.status, result.message)
	}
	if len(kb.events) != 0 {
		t.Errorf("cancelled transcription reached keyboard: %#v", kb.events)
	}
}

func TestLongRoomToneSkipsTranscription(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"text": "hallucination"})
	}))
	defer srv.Close()

	kb := newFakeKeyboard()
	d := newDaemon(configDefaults(), &resources{
		Recorder: &fakeRecorder{samples: silence(30, 300)},
		Typer:    &Typer{kb: kb},
	})
	d.stt.endpoint = srv.URL
	d.state = stateRecording
	d.sessionCtx = context.Background()

	status, message := d.stopSession()
	if status != http.StatusOK || message != "no speech detected" {
		t.Fatalf("stop = %d/%q, want 200/no speech detected", status, message)
	}
	if requests.Load() != 0 {
		t.Errorf("room tone sent %d transcription requests", requests.Load())
	}
	question := typeKeyMap['?']
	want := []testKeyEvent{
		{op: "down", key: question.modifiers[0]},
		{op: "down", key: question.key},
		{op: "up", key: question.modifiers[0]},
		{op: "up", key: question.key},
	}
	if len(kb.events) != len(want) {
		t.Fatalf("room-tone marker events = %#v, want %#v", kb.events, want)
	}
	for i := range want {
		if kb.events[i] != want[i] {
			t.Fatalf("room-tone marker events = %#v, want %#v", kb.events, want)
		}
	}
}

func TestShortCaptureTypesNoSpeechMarker(t *testing.T) {
	kb := newFakeKeyboard()
	d := newDaemon(configDefaults(), &resources{
		Recorder: &fakeRecorder{samples: silence(0.2, 300)},
		Typer:    &Typer{kb: kb},
	})
	d.state = stateRecording
	d.sessionCtx = context.Background()

	status, message := d.stopSession()
	if status != http.StatusOK || message != "no speech detected" {
		t.Fatalf("stop = %d/%q, want 200/no speech detected", status, message)
	}
	question := typeKeyMap['?']
	if len(kb.events) != 4 || kb.events[1].key != question.key {
		t.Errorf("short-capture marker events = %#v, want question mark", kb.events)
	}
}

func TestSTTErrorsRetainAudio(t *testing.T) {
	cases := map[string]error{
		"unauthorized": &sttError{Status: http.StatusUnauthorized, Message: "bad key"},
		"forbidden":    &sttError{Status: http.StatusForbidden, Message: "forbidden"},
		"too large":    &sttError{Status: http.StatusRequestEntityTooLarge, Message: "large"},
		"bad request":  &sttError{Status: http.StatusBadRequest, Message: "bad"},
		"retryable":    &sttError{Status: http.StatusServiceUnavailable, Message: "busy"},
		"transport":    errors.New("connection reset"),
	}
	wav := []byte("irreplaceable audio")

	for name, sttErr := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			d := &daemon{}
			d.handleSTTError(sttErr, wav)

			matches, err := filepath.Glob(filepath.Join(os.TempDir(), "voice-type", "*.wav"))
			if err != nil {
				t.Fatal(err)
			}
			if len(matches) != 1 {
				t.Fatalf("retained files = %d, want 1", len(matches))
			}
			got, err := os.ReadFile(matches[0])
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(wav) {
				t.Errorf("retained audio = %q, want %q", got, wav)
			}
			info, err := os.Stat(matches[0])
			if err != nil {
				t.Fatal(err)
			}
			if perm := info.Mode().Perm(); perm != 0o600 {
				t.Errorf("retained mode = %o, want 600", perm)
			}
		})
	}
}

func TestRetainedAudioNamesDoNotCollide(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	for range 2 {
		if _, err := retainWAV([]byte("audio")); err != nil {
			t.Fatal(err)
		}
	}
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "voice-type", "*.wav"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Errorf("retained files = %d, want 2", len(matches))
	}
}

// A hand edit has to reach the next dictation without a restart. That is the
// reload at the top of startSession.
func TestDaemonReloadsConfigBetweenDictations(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("OPENROUTER_API_KEY", "")
	path := filepath.Join(home, "voice-type.jsonc")
	if err := os.WriteFile(path, []byte(`{
    "api_key": "first",
    "port": 3232,
    "model": "parakeet-v3"
}`), 0o600); err != nil {
		t.Fatal(err)
	}

	d := newDaemon(configLoad(), &resources{Recorder: &fakeRecorder{}})
	assumeOnline(d)
	beforeModel, beforeHTTP := d.stt.model.ID, d.stt.http
	if beforeModel != "parakeet-v3" || len(d.stt.vocabulary) != 0 {
		t.Fatalf("started with model=%s vocabulary=%q", beforeModel, d.stt.vocabulary)
	}

	const reloadedModel = defaultModelID
	if err := os.WriteFile(path, []byte(`{
    "api_key": "second",
    "port": 9999,
    "model": "`+reloadedModel+`",
    "vocabulary": ["Numbero"]
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if status, message := d.startSession(); status != http.StatusOK {
		t.Fatalf("start after hand edit = %d/%q", status, message)
	}

	after := d.stt
	if after.model.ID != reloadedModel || after.apiKey != "second" || len(after.vocabulary) != 1 {
		t.Errorf("after reload: model=%s key=%s vocabulary=%q", after.model.ID, after.apiKey, after.vocabulary)
	}
	if after.fallbackID() != "gpt-transcribe" {
		t.Errorf("after reload fallback = %q, want gpt-transcribe", after.fallbackID())
	}
	// The listener is already bound, so a port change waits for a restart.
	if d.cfg.Port != 3232 {
		t.Errorf("port = %d, want the bound 3232", d.cfg.Port)
	}
	// Reusing the transport keeps the warm TLS connection to OpenRouter, which
	// is worth more than the handful of bytes a fresh client would cost.
	if after.http != beforeHTTP {
		t.Error("reload replaced the HTTP client, dropping the connection pool")
	}

	health := httptest.NewRecorder()
	d.routes().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	var body map[string]any
	if err := json.Unmarshal(health.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["state"] != "recording" || body["model"] != reloadedModel || body["fallback"] != "gpt-transcribe" || body["vocabulary"] != float64(1) {
		t.Errorf("health after reload = %#v", body)
	}
	d.stopSession()
}

func TestVocabularyEchoRetainsAudioAndSkipsOutput(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	vocabulary := []string{"ProjectTerm", "Maintainer Name"}
	// Only a prompt-biased model can echo; the phrase-list default never sees a
	// prompt to send back.
	const promptModel = "gpt-4o-transcribe"
	prompt := sttVocabularyPrompt(testSpec(promptModel), vocabulary)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"text": prompt})
	}))
	defer srv.Close()

	kb := newFakeKeyboard()
	samples := concat(speech(0.5, 2500), speech(2, 5000))
	cfg := configDefaults()
	cfg.Model = promptModel
	cfg.Vocabulary = vocabLists{{generalWorkspace, vocabulary}}
	d := newDaemon(cfg, &resources{
		Recorder: &fakeRecorder{samples: samples},
		Typer:    &Typer{kb: kb},
	})
	d.stt.endpoint = srv.URL
	d.state = stateRecording
	d.sessionCtx = context.Background()

	status, message := d.stopSession()
	if status != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; message=%q", status, http.StatusBadGateway, message)
	}
	if !strings.Contains(message, "vocabulary prompt") || !strings.Contains(message, "audio retained at") {
		t.Errorf("message = %q", message)
	}
	if len(kb.events) != 0 {
		t.Fatalf("echo reached keyboard: %#v", kb.events)
	}
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "voice-type", "*.wav"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("retained WAVs = %d, want 1", len(matches))
	}
	wav, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(wav) < 4 || string(wav[:4]) != "RIFF" {
		t.Errorf("retained file is not a WAV")
	}
}

func TestFallbackVocabularyEchoRetainsAudioAndSkipsOutput(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	vocabulary := []string{"ProjectTerm", "Maintainer Name"}
	prompt := sttVocabularyPrompt(testSpec("gpt-transcribe"), vocabulary)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["model"] == "microsoft/mai-transcribe-2" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"text": prompt})
	}))
	defer srv.Close()

	kb := newFakeKeyboard()
	cfg := configDefaults()
	cfg.Vocabulary = vocabLists{{generalWorkspace, vocabulary}}
	d := newDaemon(cfg, &resources{
		Recorder: &fakeRecorder{samples: concat(speech(0.5, 2500), speech(2, 5000))},
		Typer:    &Typer{kb: kb},
	})
	d.stt.endpoint = srv.URL
	d.state = stateRecording
	d.sessionCtx = context.Background()

	status, message := d.stopSession()
	if status != http.StatusBadGateway || !strings.Contains(message, "matched the vocabulary prompt") {
		t.Fatalf("status/message = %d/%q", status, message)
	}
	if len(kb.events) != 0 {
		t.Errorf("echo reached keyboard: %#v", kb.events)
	}
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "voice-type", "dictation-*.wav"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("retained WAVs = %d, err=%v", len(matches), err)
	}
}

func TestVocabularyInLegitimateFallbackTranscriptIsOutput(t *testing.T) {
	// No clipboard tools in PATH forces the production fallback to direct typing.
	t.Setenv("PATH", t.TempDir())
	var models []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		models = append(models, body["model"].(string))
		if len(models) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"text": "CommonTerm is spelled correctly."})
	}))
	defer srv.Close()

	kb := newFakeKeyboard()
	cfg := configDefaults()
	cfg.Vocabulary = vocabLists{{generalWorkspace, []string{"CommonTerm"}}}
	d := newDaemon(cfg, &resources{
		Recorder: &fakeRecorder{samples: concat(speech(0.5, 2500), speech(2, 5000))},
		Typer:    &Typer{kb: kb},
	})
	d.stt.endpoint = srv.URL
	d.state = stateRecording
	d.sessionCtx = context.Background()

	status, message := d.stopSession()
	if status != http.StatusOK || message != "CommonTerm is spelled correctly." {
		t.Fatalf("status/message = %d/%q", status, message)
	}
	if len(kb.events) == 0 {
		t.Fatal("legitimate transcript was not output")
	}
	if !reflect.DeepEqual(models, []string{"microsoft/mai-transcribe-2", "openai/gpt-transcribe"}) {
		t.Errorf("models = %#v", models)
	}
}

func TestFallbackExhaustionRetainsAudio(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	d := newDaemon(configDefaults(), &resources{
		Recorder: &fakeRecorder{samples: concat(speech(0.5, 2500), speech(2, 5000))},
		Typer:    &Typer{kb: newFakeKeyboard()},
	})
	d.stt.endpoint = srv.URL
	d.state = stateRecording
	d.sessionCtx = context.Background()

	status, _ := d.stopSession()
	if status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", status)
	}
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "voice-type", "dictation-*.wav"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("retained WAVs = %d, err=%v", len(matches), err)
	}
}

// A `voice-type vocab set` runs in a second process and only writes the state
// file, so the daemon has to pick it up on the next dictation the same way it
// picks up a hand edit.
func TestDaemonPicksUpWorkspaceSetBetweenDictations(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("OPENROUTER_API_KEY", "k")
	config := `{
    "vocabulary": {
        "general":     ["CommonTerm"],
        "project-one": ["ProjectTerm", "AnotherTerm"],
        "voice-type": ["dotool"]
    }
}`
	if err := os.WriteFile(filepath.Join(home, "voice-type.jsonc"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := workspaceSave("project-one"); err != nil {
		t.Fatal(err)
	}

	d := newDaemon(configLoad(), &resources{Recorder: &fakeRecorder{}})
	assumeOnline(d)
	if got := d.stt.vocabulary; !reflect.DeepEqual(got, []string{"CommonTerm", "ProjectTerm", "AnotherTerm"}) {
		t.Fatalf("started with vocabulary %q", got)
	}

	if err := workspaceSave("voice-type"); err != nil {
		t.Fatal(err)
	}
	if status, message := d.startSession(); status != http.StatusOK {
		t.Fatalf("start after set = %d/%q", status, message)
	}
	if got := d.stt.vocabulary; !reflect.DeepEqual(got, []string{"CommonTerm", "dotool"}) {
		t.Errorf("after set, vocabulary = %q", got)
	}

	health := httptest.NewRecorder()
	d.routes().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	var body map[string]any
	if err := json.Unmarshal(health.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["workspace"] != "voice-type" || body["vocabulary"] != float64(2) {
		t.Errorf("health = %#v", body)
	}
	d.stopSession()
}

// streamingFixture is a daemon configured for the streaming model, wired to a
// fake recorder, keyboard and xAI server, with OPENROUTER_API_KEY empty
// unless withOpenRouter is called.
type streamingFixture struct {
	d   *daemon
	rec *fakeRecorder
	kb  *fakeKeyboard
	xai *fakeXAI

	mu      sync.Mutex
	uploads []string // the model of every OpenRouter request
}

const streamingConfig = `{"model": "grok-voice-transcribe-2.0", "vocabulary": ["dotool", "PipeWire"]}`

func newStreamingDaemon(t *testing.T) *streamingFixture {
	t.Helper()
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	// Tests that switch workspace must not leave it switched for later tests.
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XAI_API_KEY", "xai-test-key")
	t.Setenv("OPENROUTER_API_KEY", "")
	t.Setenv("TMPDIR", t.TempDir())
	// No clipboard tools: the batch fallback's paste types instead.
	t.Setenv("PATH", t.TempDir())
	if err := os.WriteFile(filepath.Join(home, "voice-type.jsonc"), []byte(streamingConfig), 0o600); err != nil {
		t.Fatal(err)
	}

	fx := &streamingFixture{rec: &fakeRecorder{}, kb: newFakeKeyboard(), xai: newFakeXAI(t)}
	fx.d = newDaemon(configLoad(), &resources{Recorder: fx.rec, Typer: &Typer{kb: fx.kb}})
	assumeOnline(fx.d)
	fx.d.streamer.endpoint = fx.xai.wsURL()
	fx.d.streamer.stopSettle = 0
	fx.d.streamer.doneTimeout = 2 * time.Second
	fx.d.streamer.drainTimeout = 2 * time.Second
	t.Cleanup(fx.d.shutdown)
	return fx
}

// withOpenRouter sets an OpenRouter key and points the batch client at a fake
// that transcribes everything as text.
func (fx *streamingFixture) withOpenRouter(t *testing.T, text string) {
	t.Helper()
	t.Setenv("OPENROUTER_API_KEY", "sk-or-test")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		model, _ := body["model"].(string)
		fx.mu.Lock()
		fx.uploads = append(fx.uploads, model)
		fx.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"text": text})
	}))
	t.Cleanup(srv.Close)
	fx.d.stt.endpoint = srv.URL
	fx.d.stt.apiKey = "sk-or-test"
}

func (fx *streamingFixture) uploadModels() []string {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return slices.Clone(fx.uploads)
}

func (fx *streamingFixture) start(t *testing.T) *streamSession {
	t.Helper()
	if status, message := fx.d.startSession(); status != http.StatusOK {
		t.Fatalf("start = %d/%q", status, message)
	}
	fx.d.mu.Lock()
	defer fx.d.mu.Unlock()
	if fx.d.stream == nil {
		t.Fatal("a streaming start left no session")
	}
	return fx.d.stream
}

func (fx *streamingFixture) connected(t *testing.T, s *streamSession) {
	t.Helper()
	select {
	case <-s.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("the dial never resolved")
	}
}

func (fx *streamingFixture) screen() string { return screenText(fx.kb.snapshot()) }

func (fx *streamingFixture) waitScreen(t *testing.T, want string) {
	t.Helper()
	waitUntil(t, fmt.Sprintf("the screen to read %q", want), func() bool { return fx.screen() == want })
}

func retainedWAVs(t *testing.T) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "voice-type", "dictation-*.wav"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func captureLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	logs := &lockedBuffer{}
	previous := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	return logs
}

func TestStreamingStartReturnsBeforeDial(t *testing.T) {
	fx := newStreamingDaemon(t)
	// The handshake never completes, so /start returning at all shows it did
	// not wait for the dial -- however slow the machine running the test.
	fx.xai.createdDelay = time.Hour

	s := fx.start(t)
	select {
	case <-s.ready:
		t.Error("the dial had resolved before /start returned")
	default:
	}
	if fx.rec.startCalls.Load() != 1 || fx.d.currentState() != stateRecording {
		t.Errorf("starts = %d, state = %s", fx.rec.startCalls.Load(), fx.d.currentState())
	}
}

func TestStreamingStopTypesRemainingTextAndReturnsIt(t *testing.T) {
	fx := newStreamingDaemon(t)
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(speech(0.5, 5000))
	fx.xai.partial("Hey", true, true)
	fx.xai.partial(", quick update on", false, false)
	fx.waitScreen(t, "Hey, quick update on")
	fx.xai.partial(", quick update on the dictation project.", false, false)

	status, text := fx.d.stopSession()
	if status != http.StatusOK || text != "Hey, quick update on the dictation project." {
		t.Fatalf("stop = %d/%q", status, text)
	}
	if fx.screen() != text {
		t.Errorf("screen = %q, response = %q", fx.screen(), text)
	}
	if fx.d.currentState() != stateIdle {
		t.Errorf("state = %s", fx.d.currentState())
	}
	fx.d.mu.Lock()
	leftover := fx.d.stream
	fx.d.mu.Unlock()
	if leftover != nil {
		t.Error("the session outlived its stop")
	}
	if texts := fx.xai.texts(); len(texts) != 1 || !strings.Contains(texts[0], "audio.done") {
		t.Errorf("control messages = %q", texts)
	}
	select {
	case <-fx.xai.closed:
	case <-time.After(10 * time.Second):
		t.Error("the connection stayed open")
	}
}

func TestStreamingNoTextNoSpeechTypesMarker(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.withOpenRouter(t, "must not be typed")
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(silence(1, 300))
	fx.xai.partial("", false, false)
	fx.xai.partial("", true, false)

	status, message := fx.d.stopSession()
	if status != http.StatusOK || message != "no speech detected" {
		t.Fatalf("stop = %d/%q", status, message)
	}
	if fx.screen() != "?" {
		t.Errorf("screen = %q, want the no-speech marker", fx.screen())
	}
	if len(fx.uploadModels()) != 0 {
		t.Error("room tone was uploaded")
	}
}

func TestStreamingNoTextWithSpeechFallsBackToBatch(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.withOpenRouter(t, "Transcribed in batch.")
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(concat(speech(0.5, 2500), speech(2, 5000)))
	fx.xai.partial("", true, false)

	status, text := fx.d.stopSession()
	if status != http.StatusOK || text != "Transcribed in batch." {
		t.Fatalf("stop = %d/%q", status, text)
	}
	if fx.screen() != text {
		t.Errorf("screen = %q", fx.screen())
	}
	if got := fx.uploadModels(); !slices.Equal(got, []string{testSpec(defaultModelID).Slug}) {
		t.Errorf("uploads = %q, want one to the batch default", got)
	}
}

func TestStreamingNoTextWithSpeechWithoutKeyRetainsAndMarks(t *testing.T) {
	fx := newStreamingDaemon(t)
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(concat(speech(0.5, 2500), speech(2, 5000)))

	status, message := fx.d.stopSession()
	if status != http.StatusBadGateway || !strings.Contains(message, "audio retained at") {
		t.Fatalf("stop = %d/%q", status, message)
	}
	if fx.screen() != "?" {
		t.Errorf("screen = %q, want the marker", fx.screen())
	}
	if n := len(retainedWAVs(t)); n != 1 {
		t.Errorf("retained WAVs = %d, want 1", n)
	}
}

func TestStreamingDialFailureFallsBackToBatchAtStop(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.xai.handshake = http.StatusServiceUnavailable
	fx.withOpenRouter(t, "Recovered in batch.")
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(concat(speech(0.5, 2500), speech(2, 5000)))
	if s.failure() == nil {
		t.Fatal("a refused handshake was not a failure")
	}
	if fx.d.currentState() != stateRecording {
		t.Errorf("a failed dial ended the session: %s", fx.d.currentState())
	}

	status, text := fx.d.stopSession()
	if status != http.StatusOK || text != "Recovered in batch." || fx.screen() != text {
		t.Fatalf("stop = %d/%q, screen %q", status, text, fx.screen())
	}
	if len(fx.uploadModels()) != 1 {
		t.Errorf("uploads = %q", fx.uploadModels())
	}
}

func TestStreamingUnauthorizedLogsKeyHint(t *testing.T) {
	logs := captureLogs(t)
	fx := newStreamingDaemon(t)
	fx.xai.handshake = http.StatusUnauthorized
	s := fx.start(t)
	fx.connected(t, s)
	fx.d.stopSession()

	got := logs.String()
	if !strings.Contains(got, "authentication rejected (401)") || !strings.Contains(got, "XAI_API_KEY") {
		t.Errorf("log lacks the key hint:\n%s", got)
	}
	for _, secret := range []string{"xai-test-key", "dotool", "PipeWire"} {
		if strings.Contains(got, secret) {
			t.Errorf("log exposed %q:\n%s", secret, got)
		}
	}
}

func TestStreamingStopDuringDialDeliversText(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.xai.createdDelay = 300 * time.Millisecond
	fx.start(t)
	samples := speech(0.5, 5000)
	fx.rec.feed(samples)
	fx.xai.partial("spoken while connecting", false, false)

	status, text := fx.d.stopSession()
	if status != http.StatusOK || text != "spoken while connecting" || fx.screen() != text {
		t.Fatalf("stop = %d/%q, screen %q", status, text, fx.screen())
	}
	for _, m := range fx.xai.messages() {
		if m.beforeCreated {
			t.Fatal("audio went out before the handshake")
		}
	}
	if got := fx.xai.audioBytes(); !bytes.Equal(got, pcm16(samples)) {
		t.Errorf("server received %d bytes, want the %d recorded", len(got), 2*len(samples))
	}
}

func TestStreamingMidSessionErrorKeepsRecordingUntilStop(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.withOpenRouter(t, "must not be typed")
	s := fx.start(t)
	fx.connected(t, s)
	first := speech(0.5, 5000)
	fx.rec.feed(first)
	fx.xai.partial("Typed before the drop.", true, true)
	fx.waitScreen(t, "Typed before the drop.")

	fx.xai.push(xaiEvent{Type: "error", Message: "stream reset"})
	waitUntil(t, "the failure", func() bool { return s.failure() != nil })
	if fx.d.currentState() != stateRecording || fx.rec.stopCalls.Load() != 0 {
		t.Fatalf("the failure ended the session: state %s, recorder stops %d", fx.d.currentState(), fx.rec.stopCalls.Load())
	}
	events := len(fx.kb.snapshot())
	second := speech(1, -5000)
	fx.rec.feed(second)

	status, message := fx.d.stopSession()
	if status != http.StatusBadGateway || !strings.Contains(message, "stream reset") {
		t.Fatalf("stop = %d/%q", status, message)
	}
	if got := fx.kb.snapshot(); len(got) != events {
		t.Errorf("keys after the failure: %v", got[events:])
	}
	if len(fx.uploadModels()) != 0 {
		t.Error("text was on screen, yet the recording was transcribed again")
	}
	wavs := retainedWAVs(t)
	if len(wavs) != 1 {
		t.Fatalf("retained WAVs = %d, want 1", len(wavs))
	}
	wav, err := os.ReadFile(wavs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wav, WavEncode(concat(first, second))) {
		t.Error("the retained WAV is not the whole raw recording")
	}
}

func TestStreamingShutdownClosesConnectionAndTypesNothingMore(t *testing.T) {
	fx := newStreamingDaemon(t)
	s := fx.start(t)
	fx.connected(t, s)
	fx.xai.partial("before shutdown", false, false)
	fx.waitScreen(t, "before shutdown")

	done := make(chan struct{})
	go func() {
		fx.d.shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not drain the streaming session")
	}
	select {
	case <-fx.xai.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection stayed open")
	}
	events := len(fx.kb.snapshot())
	fx.xai.partial("after shutdown", false, false)
	time.Sleep(50 * time.Millisecond)
	if got := fx.kb.snapshot(); len(got) != events {
		t.Errorf("keys after shutdown: %v", got[events:])
	}
}

func TestStreamingShutdownDuringFinishReturnsCancelled(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.xai.doneDelay = 2 * time.Second
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(concat(speech(0.5, 2500), speech(2, 5000)))
	fx.xai.partial("on screen", false, false)
	fx.waitScreen(t, "on screen")

	type result struct {
		status  int
		message string
	}
	stopped := make(chan result, 1)
	go func() {
		status, message := fx.d.stopSession()
		stopped <- result{status, message}
	}()
	waitUntil(t, "audio.done", func() bool { return len(fx.xai.texts()) > 0 })
	fx.d.shutdown()

	r := <-stopped
	if r.status != http.StatusOK || r.message != "cancelled" {
		t.Errorf("stop = %d/%q, want 200/cancelled", r.status, r.message)
	}
	if fx.screen() != "on screen" {
		t.Errorf("screen = %q", fx.screen())
	}
	if n := len(retainedWAVs(t)); n != 0 {
		t.Errorf("shutdown retained %d WAVs", n)
	}
}

func TestStreamingCapTimerStopsSession(t *testing.T) {
	logs := captureLogs(t)
	fx := newStreamingDaemon(t)
	s := fx.start(t)
	fx.connected(t, s)
	fx.xai.partial("capped", false, false)
	fx.waitScreen(t, "capped")

	fx.d.mu.Lock()
	id := fx.d.sessionID
	fx.d.mu.Unlock()
	// Any other nonzero ID is a timer left over from another session; zero
	// would be the user's own stop.
	if status, _ := fx.d.stopSessionFor(id + 1); status != http.StatusOK || fx.d.currentState() != stateRecording {
		t.Fatalf("a stale timer stopped the session: state %s", fx.d.currentState())
	}
	if status, text := fx.d.stopSessionFor(id); status != http.StatusOK || text != "capped" {
		t.Fatalf("cap stop = %d/%q", status, text)
	}
	if !strings.Contains(logs.String(), "duration cap") {
		t.Error("the cap stop was not logged as one")
	}

	before := len(logs.String())
	s = fx.start(t)
	fx.connected(t, s)
	fx.d.stopSession()
	if strings.Contains(logs.String()[before:], "duration cap") {
		t.Error("a user stop was logged as the duration cap")
	}
}

func TestStreamingCaptureErrorRetainsAudio(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.rec.stopErr = errors.New("connection closed")
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(speech(0.5, 5000))
	fx.xai.partial("kept on screen", false, false)
	fx.waitScreen(t, "kept on screen")

	status, message := fx.d.stopSession()
	if status != http.StatusInternalServerError || !strings.Contains(message, "connection closed") {
		t.Fatalf("stop = %d/%q", status, message)
	}
	if fx.screen() != "kept on screen" {
		t.Errorf("screen = %q", fx.screen())
	}
	if n := len(retainedWAVs(t)); n != 1 {
		t.Errorf("retained WAVs = %d, want 1", n)
	}
}

func TestStreamingReloadPicksUpKeyAndKeyterms(t *testing.T) {
	fx := newStreamingDaemon(t)
	t.Setenv("XAI_API_KEY", "")
	config := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "voice-type.jsonc")
	if err := os.WriteFile(config, []byte(`{
    "model": "grok-voice-transcribe-2.0",
    "xai_api_key": "xai-from-file",
    "vocabulary": {"general": ["SkyPilot"], "go": ["coder/websocket"]}
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := workspaceSave("go"); err != nil {
		t.Fatal(err)
	}

	s := fx.start(t)
	fx.connected(t, s)
	fx.xai.mu.Lock()
	auth, raw := fx.xai.auth, fx.xai.rawQuery
	fx.xai.mu.Unlock()
	if auth != "Bearer xai-from-file" {
		t.Errorf("Authorization = %q", auth)
	}
	query, _ := url.ParseQuery(raw)
	if got := query["keyterm"]; !slices.Equal(got, []string{"SkyPilot", "coder/websocket"}) {
		t.Errorf("keyterms = %q", got)
	}
	fx.d.stopSession()
}

// The config is re-read at every start, so switching the model to one on the
// other service takes that service's endpoint and key with it.
func TestStreamingReloadSwitchesToGateway(t *testing.T) {
	fx := newStreamingDaemon(t)
	gateway := newFakeGateway(t)
	fx.d.streamer.gatewayEndpoint = gateway.wsURL()
	t.Setenv("AI_GATEWAY_API_KEY", "")
	config := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "voice-type.jsonc")
	if err := os.WriteFile(config, []byte(`{
    "model": "mai-transcribe-2-streaming",
    "vercel_api_key": "vck-from-file",
    "vocabulary": ["dotool", "PipeWire"]
}`), 0o600); err != nil {
		t.Fatal(err)
	}

	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(speech(0.5, 5000))
	gateway.partial("Typed by the gateway.")
	fx.waitScreen(t, "Typed by the gateway.")
	if status, text := fx.d.stopSession(); status != http.StatusOK || text != "Typed by the gateway." {
		t.Errorf("stop = %d/%q", status, text)
	}

	gateway.mu.Lock()
	offered, raw := gateway.offered, gateway.rawQuery
	gateway.mu.Unlock()
	if !slices.Contains(offered, "ai-gateway-auth.vck-from-file") {
		t.Errorf("subprotocols = %q, want the Vercel key from the file", offered)
	}
	if strings.Contains(raw, "keyterm") || strings.Contains(raw, "dotool") {
		t.Errorf("the vocabulary reached the gateway: %s", raw)
	}
	if n := fx.xai.dialCount(); n != 0 {
		t.Errorf("a gateway model dialled xAI %d times", n)
	}
}

func TestGatewayRefusalFallsBackToBatchAtStop(t *testing.T) {
	logs := captureLogs(t)
	fx := newStreamingDaemon(t)
	fx.withOpenRouter(t, "Batch text.")
	gateway := newFakeGateway(t)
	gateway.handshake = http.StatusForbidden
	gateway.refusal = `{"error":{"message":"AI Gateway requires a valid credit card on file.","type":"customer_verification_required"}}`
	fx.d.streamer.gatewayEndpoint = gateway.wsURL()
	t.Setenv("AI_GATEWAY_API_KEY", "vck-test-key")
	config := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "voice-type.jsonc")
	if err := os.WriteFile(config, []byte(`{"model": "mai-transcribe-2-streaming"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(concat(speech(0.5, 2500), speech(2, 5000)))
	if status, text := fx.d.stopSession(); status != http.StatusOK || text != "Batch text." {
		t.Errorf("stop = %d/%q, want the batch fallback's text", status, text)
	}
	if got := fx.uploadModels(); !slices.Equal(got, []string{"microsoft/mai-transcribe-2"}) {
		t.Errorf("uploads = %q, want the default batch model", got)
	}
	for _, want := range []string{"gateway refused the stream (403): AI Gateway requires a valid credit card on file.", "vercel_api_key"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log lacks %q:\n%s", want, logs.String())
		}
	}
	if strings.Contains(logs.String(), "vck-test-key") {
		t.Errorf("the key reached the log:\n%s", logs.String())
	}
}

// Text is on screen, more was said, and the service never answers the end of
// the audio. The words after the last text may never have been transcribed, so
// the recording is kept and the stop says it failed.
func TestStreamingStopWithoutTheLastWordRetainsTheRecording(t *testing.T) {
	logs := captureLogs(t)
	fx := newStreamingDaemon(t)
	fx.withOpenRouter(t, "must not be uploaded")
	fx.xai.ignoreDone = true
	fx.d.streamer.doneTimeout = 50 * time.Millisecond
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(concat(speech(0.5, 2500), speech(2, 5000)))
	fx.xai.partial("First sentence.", true, true)
	fx.waitScreen(t, "First sentence.")
	fx.rec.feed(speech(3, 5000)) // three more seconds nobody transcribes

	status, body := fx.d.stopSession()
	if status != http.StatusBadGateway || !strings.Contains(body, "xai did not finish within 50ms") {
		t.Errorf("stop = %d/%q, want the unfinished stream reported", status, body)
	}
	if fx.screen() != "First sentence." {
		t.Errorf("screen = %q, want the typed text left alone", fx.screen())
	}
	wavs := retainedWAVs(t)
	if len(wavs) != 1 {
		t.Fatalf("retained %d recordings, want the one that holds the untranscribed words", len(wavs))
	}
	if info, err := os.Stat(wavs[0]); err != nil || info.Size() < int64(2*wavSampleRate*5) {
		t.Errorf("the retained recording is not the whole dictation: %v, %v", info, err)
	}
	// The text that did land is on screen; a batch transcript would be typed after it.
	if got := fx.uploadModels(); len(got) != 0 {
		t.Errorf("uploads = %q, want none once text has been typed", got)
	}
	if got := logs.String(); !strings.Contains(got, "the last words may be missing") || !strings.Contains(got, "audio retained at "+wavs[0]) {
		t.Errorf("the log does not say what happened or where the audio went:\n%s", got)
	}
}

// The same with nothing typed: the whole dictation goes to the batch fallback.
func TestStreamingStopWithoutAnyWordFallsBackToBatch(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.withOpenRouter(t, "Transcribed in batch.")
	fx.xai.doneReply = []xaiEvent{} // answer audio.done with a close and nothing else
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(concat(speech(0.5, 2500), speech(2, 5000)))

	if status, text := fx.d.stopSession(); status != http.StatusOK || text != "Transcribed in batch." {
		t.Errorf("stop = %d/%q, want the batch transcript", status, text)
	}
	if got := fx.uploadModels(); len(got) != 1 {
		t.Errorf("uploads = %q, want one", got)
	}
}

// What a service says about a failure is its own text. The key it may quote
// and a line break in it reach neither the log nor the /stop reply.
func TestStreamingServerMessageIsScrubbed(t *testing.T) {
	logs := captureLogs(t)
	fx := newStreamingDaemon(t)
	gateway := newFakeGateway(t)
	fx.d.streamer.gatewayEndpoint = gateway.wsURL()
	t.Setenv("AI_GATEWAY_API_KEY", "vck-secret-sentinel")
	config := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "voice-type.jsonc")
	if err := os.WriteFile(config, []byte(`{"model": "mai-transcribe-2-streaming"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s := fx.start(t)
	fx.connected(t, s)
	fx.rec.feed(speech(0.5, 5000))
	gateway.partial("typed so far")
	fx.waitScreen(t, "typed so far")
	gateway.events <- gatewayFrame(map[string]any{"type": "error", "error": map[string]any{
		"message": "invalid credential ai-gateway-auth.vck-secret-sentinel\n[00:00:00] STREAM: forged line " + strings.Repeat("x", 2000),
		"type":    "auth_error",
	}})
	waitUntil(t, "the failure", func() bool { return s.failure() != nil })

	status, body := fx.d.stopSession()
	if status != http.StatusBadGateway || !strings.Contains(body, "invalid credential ai-gateway-auth.<key> [00:00:00] STREAM: forged line") {
		t.Errorf("stop = %d/%q, want the scrubbed message", status, body)
	}
	for name, where := range map[string]string{"the log": logs.String(), "the /stop reply": body} {
		if strings.Contains(where, "vck-secret-sentinel") {
			t.Errorf("the key reached %s", name)
		}
		if strings.Contains(where, "\n[00:00:00] STREAM: forged line") {
			t.Errorf("a line the service wrote reached %s as a line of its own", name)
		}
		if strings.Contains(where, strings.Repeat("x", streamMessageMax)) {
			t.Errorf("the message reached %s at full length", name)
		}
	}
}

// /health names the model a failed stream falls back to, as `voice-type
// config` does, and none when there is no key to fall back with.
func TestHealthReportsStreamingFallback(t *testing.T) {
	fx := newStreamingDaemon(t)
	health := func() map[string]any {
		t.Helper()
		rec := httptest.NewRecorder()
		fx.d.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	if got := health(); got["model"] != "grok-voice-transcribe-2.0" || got["fallback"] != "" {
		t.Errorf("health without an OpenRouter key = %v", got)
	}
	t.Setenv("OPENROUTER_API_KEY", "sk-or-test")
	if got := health(); got["fallback"] != defaultModelID {
		t.Errorf("health with an OpenRouter key = %v, want fallback %s", got, defaultModelID)
	}
}

func TestBatchModeNeverStartsStream(t *testing.T) {
	fx := newStreamingDaemon(t)
	fx.withOpenRouter(t, "Batch text.")
	config := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "voice-type.jsonc")
	if err := os.WriteFile(config, []byte(`{"model": "mai-transcribe-2"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if status, _ := fx.d.startSession(); status != http.StatusOK {
		t.Fatal("start failed")
	}
	fx.d.mu.Lock()
	stream := fx.d.stream
	fx.d.mu.Unlock()
	if stream != nil {
		t.Fatal("a batch model started a stream")
	}
	fx.rec.feed(concat(speech(0.5, 2500), speech(2, 5000)))
	if status, text := fx.d.stopSession(); status != http.StatusOK || text != "Batch text." {
		t.Errorf("stop = %d/%q", status, text)
	}
	if n := fx.xai.dialCount(); n != 0 {
		t.Errorf("batch mode dialled xAI %d times", n)
	}
}
