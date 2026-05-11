# omi-mobile

Mobile port of the `web-stt-test` page. Same layout, same omi BLE protocol — just runs on iOS/Android via Expo.

## Status

| Feature | Web (working) | Mobile (this repo) |
|---|---|---|
| BLE connect | ✅ | ✅ |
| Photo streaming + grid | ✅ | ✅ |
| Audio packet ingest | ✅ | ✅ |
| Opus decode | ✅ (WASM in browser) | ❌ TODO — see below |
| Deepgram STT | ✅ | ❌ blocked on decode |

## Run on iPhone (first time)

`react-native-ble-plx` needs native code, so this can't run in Expo Go. You need a one-time dev build on the device.

```bash
cd /Users/akshaysunkara/GTC/omi-mobile

# Plug iPhone in, trust the Mac, ensure Xcode is installed
npx expo prebuild --platform ios --clean   # generates ios/ folder
npx expo run:ios --device                  # builds + installs on the connected device
```

If you hit a code-signing prompt: open `ios/omimobile.xcworkspace`, set a Team in Signing & Capabilities, then re-run.

## Subsequent runs

Once the dev build is on the phone, you don't have to rebuild for JS changes:

```bash
npx expo start --dev-client
```

Open the app on the phone, it'll connect to Metro.

## Run on Android (alternative)

```bash
npx expo run:android --device
```

Make sure USB debugging is on.

## Open question — Opus decode

Same problem we had before: pure-JS Opus decoders need WASM, which RN doesn't run by default. iOS native libopus produced PCM Deepgram couldn't transcribe. Three paths:

1. **Tiny Node WS relay** — phone forwards raw Opus bytes over WebSocket; relay decodes with `@discordjs/opus` and pipes PCM to Deepgram. Same shape as the old Python relay but smaller. Most pragmatic.
2. **Custom Expo native module wrapping libopus** — fix the build/version that broke before, or try `opus-tools-c` directly. Risky, time-consuming.
3. **Server-side everything** — phone forwards raw BLE bytes to a backend that does decode + STT + LLM + TTS. Fits a real production architecture; the relay becomes your backend.

Layout already exists for the transcript box; once decode lands, it'll populate.
