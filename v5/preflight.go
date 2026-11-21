package voicetype

import (
	"fmt"
	"net"
	"time"
)

// preflightResult distinguishes "already running" (a clean no-op exit) from a
// real misconfiguration.
type preflightResult int

const (
	preflightOK preflightResult = iota
	preflightAlreadyRunning
	preflightFailed
)

// resources are the long-lived handles preflight proves are usable. They are
// handed to the daemon rather than recreated, so the 201 ms keyboard
// registration and the Pulse connect are paid exactly once.
type resources struct {
	Typer    *Typer
	Recorder audioRecorder
}

func (r *resources) Close() {
	if r == nil {
		return
	}
	if r.Typer != nil {
		r.Typer.Close()
	}
	if r.Recorder != nil {
		r.Recorder.Close()
	}
}

// portInUse reports whether something is already listening on the daemon port.
func portInUse(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 300*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// preflightKeys requires the key the configured model needs. A streaming model
// needs its service's key -- xAI's or the Vercel AI Gateway's; the OpenRouter
// key is then optional, because it only serves the batch fallback when a
// stream fails.
func preflightKeys(cfg Config) error {
	if spec := cfg.modelSpec(); spec.Streams() {
		if configStreamKey(cfg) == "" {
			service := spec.Stream.info()
			return fmt.Errorf(
				"no %s API key. Set %s in the environment, or add\n"+
					"  \"%s\": \"%s\"\n"+
					"to %s", service.title, service.keyEnv, service.keyField, service.keyHint, configFilePath())
		}
		if configAPIKey(cfg) == "" {
			logf("PREFLIGHT", "no OpenRouter API key -- a failed stream will type ? instead of falling back to batch")
		}
		return nil
	}
	if configAPIKey(cfg) == "" {
		return fmt.Errorf(
			"no OpenRouter API key. Set OPENROUTER_API_KEY in the environment, or add\n"+
				"  \"api_key\": \"sk-or-...\"\n"+
				"to %s", configFilePath())
	}
	return nil
}

// runPreflight mirrors the order in src/preflight.ts.
func runPreflight(cfg Config) (preflightResult, *resources, error) {
	// 1. Port free -- in use means a daemon is already up, which is not a failure.
	if portInUse(cfg.Port) {
		return preflightAlreadyRunning, nil, fmt.Errorf("port %d is already in use -- daemon already running?", cfg.Port)
	}

	// 2. API keys present.
	if err := preflightKeys(cfg); err != nil {
		return preflightFailed, nil, err
	}

	// 3. /dev/uinput usable. Creating the keyboard is the real test -- opening
	//    the device can succeed where the ioctl setup still fails.
	typer, err := newTyper()
	if err != nil {
		return preflightFailed, nil, fmt.Errorf(
			"cannot create the virtual keyboard: %w\n"+
				"voice-type needs write access to /dev/uinput. Add yourself to the 'input' group:\n"+
				"  sudo usermod -aG input $USER\n"+
				"then log out and back in (or run: newgrp input)", err)
	}

	// 4. PulseAudio reachable.
	rec, err := NewRecorder()
	if err != nil {
		typer.Close()
		return preflightFailed, nil, fmt.Errorf(
			"cannot reach the audio server: %w\n"+
				"voice-type needs PulseAudio or pipewire-pulse running", err)
	}

	return preflightOK, &resources{Typer: typer, Recorder: rec}, nil
}
