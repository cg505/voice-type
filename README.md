# Voice Type

System-wide speech-to-text for Linux. Press a key, speak, press again — the text
appears wherever your cursor is in the system. Super accurate and cheap.

# Why I built it

I needed high quality transcription, near real time, cheap or free, while using minimal system resources, that works mainly in Arch Linux and Fedora, both x64 and ARM64. Most solutions don't meet these requirements. 

# V4

Uses a hidden chromium instance to take advantage of Google's Speech to Text infrastructure. Install with 

```bash
curl -sSL https://raw.githubusercontent.com/eriknovikov/voice-type/main/v4/install.sh | bash
```

Uninstall with

```bash
curl -sSL https://raw.githubusercontent.com/eriknovikov/voice-type/main/v4/uninstall.sh | bash
```

# V5

A single static Go binary that records audio and sends it to OpenRouter. New
configurations use Microsoft MAI-Transcribe-2 with Azure phrase-list vocabulary
and clean transcription; retryable provider failures fall back to OpenAI GPT
Transcribe for that dictation. MAI-Transcribe-2 is currently a public-preview
service without an SLA, which is why the fallback remains enabled.

Existing configurations keep their explicitly selected model during upgrades.
Run `voice-type models` to see the available models, vocabulary support, prices,
and fallback policy; `grok-voice-transcribe-2.0` and
`mai-transcribe-2-streaming` are the ones that stream live.
Workspace-specific vocabulary can be selected with `voice-type vocab set <name>`.

## Live streaming

With a streaming model, text appears in the focused window while you speak and
is corrected in place as the transcription firms up. There are two:

- **`mai-transcribe-2-streaming`**, Microsoft's model through the Vercel AI
  Gateway. Add your gateway key as `"vercel_api_key"` (or export
  `AI_GATEWAY_API_KEY`); the Vercel account needs a card on file. The last two
  seconds of text are a guess that keeps being corrected; text older than that
  does not change. It takes no vocabulary. Vercel's streaming transcription is
  in beta.
- **`grok-voice-transcribe-2.0`**, straight from xAI. Add your xAI key as
  `"xai_api_key"` (or export `XAI_API_KEY`). Vocabulary lists are sent as xAI
  keyterms (at most 100 terms of 50 characters). Audio is level-corrected
  before it is sent, which recovers most of the sentences a quiet laptop
  microphone would otherwise lose; it does not cope with whispering.

Keep the caret and the focus where they are while a streaming dictation is
open and until the text settles after you stop: corrections are typed as
backspaces, and voice-type cannot see where the caret went. Finish the word
before you stop -- one cut off by the stop press can be lost. Text that arrives
after the stop press is typed in one go when the service has finished, a
fraction of a second later. Use a stop key without modifiers (F9-style) all the
same, so a held Ctrl or Alt cannot turn keystrokes already under way into
shortcuts.

Live text is typed key by key, as plain ASCII on a US keyboard layout: accents
are dropped (café is typed cafe), typographic quotes and dashes become plain
ones, a few symbols are spelled out (€ becomes EUR), and anything else becomes
`?`. Batch dictations are pasted and keep their characters.

If the stream cannot be opened or drops before any text has appeared, the
dictation is still recorded and, when an OpenRouter key is configured, sent to
OpenRouter at stop as a normal batch dictation; without one, voice-type types
`?` and keeps the recording in `/tmp/voice-type/` (or `$TMPDIR/voice-type/`).
If the stream drops after text has appeared, or the service does not confirm
the end of the dictation within a few seconds of the stop, that text stays, the
recording is kept in the same place, and the log says why. Streaming is billed
per second of audio, pauses included; the 10-minute cap still applies.

Install with

```bash
curl -sSL https://raw.githubusercontent.com/eriknovikov/voice-type/main/v5/install.sh | bash
```

Uninstall with

```bash
curl -sSL https://raw.githubusercontent.com/eriknovikov/voice-type/main/v5/uninstall.sh | bash
```

Only one version can be installed at a time: both use the `voice-type` command,
`~/.config/voice-type.jsonc`, and port 3232, so installing one replaces the other.


