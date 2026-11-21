package voicetype

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestPrintConfigReportsResolvedValuesWithoutKey(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("OPENROUTER_API_KEY", "sk-or-environment-secret")
	config := `{
    "api_key": "sk-or-file-secret",
    "port": 4321,
    "model": "whisper-large-v3",
    "vocabulary": ["Numbero", "Erik Novikov"]
}`
	if err := os.WriteFile(filepath.Join(home, "voice-type.jsonc"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := printConfig(&out); err != nil {
		t.Fatalf("printConfig: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"api_key:    set (from OPENROUTER_API_KEY)",
		"port:       4321",
		"model:      whisper-large-v3  ($0.027/hour, reads vocabulary)",
		"fallback:   none",
		"vocabulary: 2 terms",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
	for _, secret := range []string{"sk-or-environment-secret", "sk-or-file-secret"} {
		if strings.Contains(got, secret) {
			t.Errorf("output exposed API key %q", secret)
		}
	}
}

func TestPrintConfigRejectsMalformedFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	path := filepath.Join(home, "voice-type.jsonc")
	if err := os.WriteFile(path, []byte(`{"port":`), 0o600); err != nil {
		t.Fatal(err)
	}

	err := printConfig(&bytes.Buffer{})
	if err == nil {
		t.Fatal("printConfig accepted malformed JSONC")
	}
	if !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "could not parse config file") {
		t.Errorf("error = %q, want path and parse context", err)
	}
}

func TestPrintModelsListsAllowlistAndDefault(t *testing.T) {
	var out bytes.Buffer
	printModels(&out)
	got := out.String()
	for _, model := range sttModels {
		if !strings.Contains(got, model.ID) {
			t.Errorf("models output lacks %q", model.ID)
		}
	}
	defaultLine := ""
	for _, line := range strings.Split(got, "\n") {
		// The first field, not a prefix: mai-transcribe-2-streaming starts the same way.
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == defaultModelID {
			defaultLine = line
		}
	}
	spec, _ := sttLookup(defaultModelID)
	price := strconv.FormatFloat(spec.USDPerHour, 'f', 3, 64)
	if !strings.Contains(defaultLine, price) || !strings.Contains(defaultLine, "yes") || !strings.Contains(defaultLine, "(default)") {
		t.Errorf("default model not identified:\n%s", got)
	}
	if !strings.Contains(defaultLine, "gpt-transcribe") {
		t.Errorf("default model fallback not identified:\n%s", got)
	}
}

func TestInstallerTemplateUsesMAIDefault(t *testing.T) {
	raw, err := os.ReadFile("install.sh")
	if err != nil {
		t.Fatal(err)
	}
	want := `"model": "` + defaultModelID + `"`
	if !strings.Contains(string(raw), want) {
		t.Errorf("installer template lacks %s", want)
	}
}

// writeVocabConfig lays down a general list plus two workspaces. The keys are
// deliberately not in alphabetical order: `vocab ls` must print the file's
// order, so a fixture that sorts the same way would prove nothing.
func writeVocabConfig(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	config := `{
    "model": "gpt-4o-transcribe",
    "vocabulary": {
        "voice-type": ["dotool", "JSONC"],
        "general":     ["CommonTerm", "SharedTerm"],
        "project-one": ["ProjectTerm", "AnotherTerm", "WorkspaceTerm"]
    }
}`
	if err := os.WriteFile(filepath.Join(home, "voice-type.jsonc"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestVocabLsListsInFileOrderAndMarksActive(t *testing.T) {
	writeVocabConfig(t)

	check := func(active, want string) {
		t.Helper()
		if err := workspaceSave(active); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := runVocab(&out, []string{"ls"}); err != nil {
			t.Fatalf("runVocab: %v", err)
		}
		if out.String() != want {
			t.Errorf("with %q active, got:\n%s\nwant:\n%s", active, out.String(), want)
		}
	}

	// File order, not alphabetical, and general is a row like any other.
	check("project-one", "sending: 5\ncurrent: project-one\nvoice-type: 2\ngeneral: 2\nproject-one*: 3\n")
	check("", "sending: 2\ncurrent: none\nvoice-type: 2\ngeneral: 2\nproject-one: 3\n")
	// A list that has since been renamed away says so instead of going quiet.
	check("gone", "sending: 2\ncurrent: gone\nvoice-type: 2\ngeneral: 2\nproject-one: 3\n"+
		"\n\"gone\" is not in the config file -- only general is being sent\n")
}

// The bare command is the listing, with nothing else bolted on.
func TestVocabWithNoArgsIsLs(t *testing.T) {
	writeVocabConfig(t)
	if err := workspaceSave("voice-type"); err != nil {
		t.Fatal(err)
	}

	var bare, ls bytes.Buffer
	if err := runVocab(&bare, nil); err != nil {
		t.Fatalf("runVocab(nil): %v", err)
	}
	if err := runVocab(&ls, []string{"ls"}); err != nil {
		t.Fatalf("runVocab(ls): %v", err)
	}
	if bare.String() != ls.String() {
		t.Errorf("`vocab` and `vocab ls` differ:\n%s\n---\n%s", bare.String(), ls.String())
	}
	if !strings.Contains(bare.String(), "voice-type*: 2") {
		t.Errorf("active list not marked:\n%s", bare.String())
	}
}

func TestVocabSetByNameAndNone(t *testing.T) {
	writeVocabConfig(t)

	var out bytes.Buffer
	if err := runVocab(&out, []string{"set", "voice-type"}); err != nil {
		t.Fatalf("set by name: %v", err)
	}
	if got := workspaceLoad(); got != "voice-type" {
		t.Errorf("workspace = %q, want voice-type", got)
	}
	if !strings.Contains(out.String(), "4 terms on the next dictation (general + voice-type)") {
		t.Errorf("confirmation = %q", out.String())
	}

	out.Reset()
	if err := runVocab(&out, []string{"set", "none"}); err != nil {
		t.Fatalf("set to none: %v", err)
	}
	if got := workspaceLoad(); got != "" {
		t.Errorf("workspace = %q, want cleared", got)
	}
	if !strings.Contains(out.String(), "2 terms on the next dictation (general)") {
		t.Errorf("confirmation = %q", out.String())
	}
}

func TestVocabSetRejectsUnknownGeneralAndNumbers(t *testing.T) {
	writeVocabConfig(t)
	if err := workspaceSave("project-one"); err != nil {
		t.Fatal(err)
	}

	err := runVocab(&bytes.Buffer{}, []string{"set", "nope"})
	if err == nil {
		t.Fatal("an unknown list was accepted")
	}
	// The names come back in file order too, so the error reads like the listing.
	if !strings.Contains(err.Error(), "available: voice-type, project-one, none") {
		t.Errorf("error = %q, want the available names in file order", err)
	}

	if err := runVocab(&bytes.Buffer{}, []string{"set", "general"}); err == nil {
		t.Error("general was accepted as a target")
	}

	// The listing prints no numbers, so a number is not a name.
	if err := runVocab(&bytes.Buffer{}, []string{"set", "1"}); err == nil {
		t.Error("a row number was accepted as a name")
	}

	// A rejected set leaves the active list alone.
	if got := workspaceLoad(); got != "project-one" {
		t.Errorf("workspace = %q, want project-one", got)
	}

	if err := runVocab(&bytes.Buffer{}, []string{"set"}); err == nil {
		t.Error("set without a name was accepted")
	}
	for _, gone := range []string{"status", "list", "switch", "use"} {
		if err := runVocab(&bytes.Buffer{}, []string{gone}); err == nil {
			t.Errorf("retired command %q still works", gone)
		}
	}
}

// A model that drops the prompt has to say so here, not silently bill for it.
func TestVocabLsFlagsAModelThatIgnoresVocabulary(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	config := `{"model": "parakeet-v3"}`
	if err := os.WriteFile(filepath.Join(home, "voice-type.jsonc"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := runVocab(&out, nil); err != nil {
		t.Fatalf("runVocab: %v", err)
	}
	if !strings.Contains(out.String(), "ignores vocabulary") {
		t.Errorf("output lacks the warning:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "no lists yet") {
		t.Errorf("output lacks the empty-list hint:\n%s", out.String())
	}
}

// `voice-type config` counts what will actually be sent, general included.
func TestPrintConfigCountsTheActiveVocabulary(t *testing.T) {
	writeVocabConfig(t)
	if err := workspaceSave("project-one"); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := printConfig(&out); err != nil {
		t.Fatalf("printConfig: %v", err)
	}
	if !strings.Contains(out.String(), "vocabulary: 5 terms (general + project-one)") {
		t.Errorf("output = %s", out.String())
	}
}

func TestPrintConfigShowsXAIKeyAndBatchFallback(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("XAI_API_KEY", "")
	config := `{
    "xai_api_key": "xai-file-secret",
    "model": "grok-voice-transcribe-2.0"
}`
	if err := os.WriteFile(filepath.Join(home, "voice-type.jsonc"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	show := func() string {
		t.Helper()
		var out bytes.Buffer
		if err := printConfig(&out); err != nil {
			t.Fatalf("printConfig: %v", err)
		}
		if strings.Contains(out.String(), "xai-file-secret") {
			t.Errorf("output exposed the xAI key:\n%s", out.String())
		}
		return out.String()
	}

	t.Setenv("OPENROUTER_API_KEY", "")
	got := show()
	for _, want := range []string{
		"api_key:    MISSING\nxai_key:    set (from config file)\nport:",
		"model:      grok-voice-transcribe-2.0  ($0.200/hour, reads vocabulary, streams live)",
		"fallback:   none (no OpenRouter key)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}

	t.Setenv("OPENROUTER_API_KEY", "sk-or-environment-secret")
	t.Setenv("XAI_API_KEY", "xai-environment-secret")
	got = show()
	for _, want := range []string{
		"xai_key:    set (from XAI_API_KEY)",
		"fallback:   " + defaultModelID + " (batch, when the stream fails)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "environment-secret") {
		t.Errorf("output exposed a key:\n%s", got)
	}
}

// A gateway model shows the gateway's key, not xAI's, and says that its
// vocabulary goes nowhere.
func TestPrintConfigShowsVercelKeyForGatewayModel(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("XAI_API_KEY", "xai-environment-secret")
	t.Setenv("AI_GATEWAY_API_KEY", "")
	t.Setenv("OPENROUTER_API_KEY", "")
	config := `{
    "vercel_api_key": "vck-file-secret",
    "model": "mai-transcribe-2-streaming"
}`
	if err := os.WriteFile(filepath.Join(home, "voice-type.jsonc"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	show := func() string {
		t.Helper()
		var out bytes.Buffer
		if err := printConfig(&out); err != nil {
			t.Fatalf("printConfig: %v", err)
		}
		if strings.Contains(out.String(), "secret") {
			t.Errorf("output exposed a key:\n%s", out.String())
		}
		return out.String()
	}

	got := show()
	for _, want := range []string{
		"api_key:    MISSING\nvercel_key: set (from config file)\nport:",
		"model:      mai-transcribe-2-streaming  ($0.540/hour, ignores vocabulary, streams live)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "xai_key") {
		t.Errorf("a gateway model's output mentions the xAI key:\n%s", got)
	}

	t.Setenv("AI_GATEWAY_API_KEY", "vck-environment-secret")
	if got := show(); !strings.Contains(got, "vercel_key: set (from AI_GATEWAY_API_KEY)") {
		t.Errorf("output lacks the environment key:\n%s", got)
	}
}

// A batch model's output is exactly what it was before streaming existed.
func TestPrintConfigBatchModelHasNoXAILine(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("XAI_API_KEY", "xai-environment-secret")
	t.Setenv("AI_GATEWAY_API_KEY", "vck-environment-secret")
	var out bytes.Buffer
	if err := printConfig(&out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "xai_key") || strings.Contains(out.String(), "vercel_key") || strings.Contains(out.String(), "streams live") {
		t.Errorf("batch model output mentions streaming:\n%s", out.String())
	}
}

func TestPrintModelsShowsLiveColumn(t *testing.T) {
	var out bytes.Buffer
	printModels(&out)
	lines := strings.Split(out.String(), "\n")
	header := lines[0]
	live := strings.Index(header, "live")
	if live < 0 || live < strings.Index(header, "vocabulary") || live > strings.Index(header, "fallback") {
		t.Fatalf("header lacks a live column between vocabulary and fallback: %q", header)
	}
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		model, _ := sttLookup(strings.Fields(line)[0])
		want := "no"
		if model.Streams() {
			want = "yes"
		}
		if got := strings.TrimSpace(line[live : live+4]); got != want {
			t.Errorf("%s: live = %q, want %q", model.ID, got, want)
		}
	}
}
