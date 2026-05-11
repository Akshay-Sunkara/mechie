import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  ActivityIndicator,
  Animated,
  Easing,
  Image,
  KeyboardAvoidingView,
  PermissionsAndroid,
  Platform,
  Pressable,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from 'react-native';
import { BleManager, Device, Subscription } from 'react-native-ble-plx';
import { Audio } from 'expo-av';
import * as FileSystem from 'expo-file-system/legacy';
import AsyncStorage from '@react-native-async-storage/async-storage';
// omi BLE service / characteristics — same UUIDs the firmware advertises.
// (omi/app/lib/services/devices/models.dart)
const SERVICE_UUID = '19b10000-e8f2-537e-4f6c-d104768a1214';
const AUDIO_CHAR_UUID = '19b10001-e8f2-537e-4f6c-d104768a1214';
const CODEC_CHAR_UUID = '19b10002-e8f2-537e-4f6c-d104768a1214';
const IMAGE_DATA_UUID = '19b10005-e8f2-537e-4f6c-d104768a1214';
const IMAGE_CONTROL_UUID = '19b10006-e8f2-537e-4f6c-d104768a1214';
// Standard GATT Battery Service — firmware exposes percent level (0–100) with notify.
const BATTERY_SERVICE_UUID = '0000180f-0000-1000-8000-00805f9b34fb';
const BATTERY_LEVEL_UUID = '00002a19-0000-1000-8000-00805f9b34fb';
// Photo-upload Wi-Fi extension (firmware/src/config.h)
const PHOTO_UPLOAD_SESSION_UUID = '19b10007-e8f2-537e-4f6c-d104768a1214';
const PHOTO_UPLOAD_STATE_UUID = '19b10008-e8f2-537e-4f6c-d104768a1214';
// Image control byte extensions (in addition to PHOTO_CMD_SINGLE_SHOT 0xFF and STOP 0x00)
const PHOTO_CMD_WIFI_START = 0x10;
const PHOTO_CMD_WIFI_STOP = 0x11;
const PHOTO_CMD_WIFI_POST = 0x12; // followed by [idLen, requestId UTF-8]
// Wi-Fi state values from firmware's photo_upload module
const WIFI_STATE_OFF = 0x00;
const WIFI_STATE_CONNECTING = 0x01;
const WIFI_STATE_READY = 0x02;
const WIFI_STATE_FAILED = 0x03;
const WIFI_STATE_UPLOADING = 0x04;
const ASYNC_KEY_WIFI = 'gtc_wifi_creds';
const ASYNC_KEY_WIFI_LEGACY = 'omi_wifi_creds';

// Firmware control bytes (firmware/src/app.cpp:726):
//   0xFF = single shot (no interval throttle)
//   0x00 = stop
//   5..300 = interval mode (firmware ignores value, uses compile-time constant)
const PHOTO_CMD_SINGLE_SHOT = 0xff;
const PHOTO_END_OF_IMAGE = 0xffff;
const MAX_PHOTOS_KEPT = 24;

// Wire-protocol tag bytes (must match relay/relay.js)
const TAG_PHONE_OPUS = 0x01;
const TAG_PHONE_PHOTO = 0x02;
const TAG_RELAY_TTS = 0x10;            // self-contained WAV of model audio
const TAG_RELAY_WIFI_PHOTO = 0x20;

// Relay endpoint. Hardcoded for now; not user-facing.
const RELAY_URL = 'wss://omi-relay-aksay95.fly.dev';
// Onboarding scan: 60s — covers cold-boot + iPhone bringing the radio up.
const SCAN_TIMEOUT_MS = 60_000;
// Deepgram waits this long for trailing silence before firing is_final.
// Subtract from "is_final → audio" to estimate user-perceived latency.
// Must match the `endpointing` query param in relay/relay.js.
const DG_ENDPOINTING_MS = 300;

type Photo = { id: number; uri: string; rotation: number; size: number; ts: string };
type Mode = 'idle' | 'chat' | 'guidance';
type Phase = 'connect-mechie' | 'connect-wifi' | 'guidance';
type TranscriptEntry =
  | { kind: 'user'; text: string; final: boolean }
  | { kind: 'agent'; text: string };

// react-native-ble-plx encodes characteristic values as base64.
function b64ToBytes(b64: string): Uint8Array {
  const binary = global.atob(b64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}
function bytesToB64(bytes: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return global.btoa(binary);
}

export default function App() {
  // -- UI state ----------------------------------------------------------------
  const [relayUrl] = useState(RELAY_URL);
  const [status, setStatus] = useState('Idle.');
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [connectError, setConnectError] = useState<string | null>(null);
  const [stats, setStats] = useState({
    blePackets: 0,
    opusFrames: 0,
    pcmSamples: 0,
    audioLost: 0,
    photoPackets: 0,
    photosDone: 0,
    photoBytes: 0,
    photoLost: 0,
  });
  const [photos, setPhotos] = useState<Photo[]>([]);
  const [entries, setEntries] = useState<TranscriptEntry[]>([]);
  const [mode, setMode] = useState<Mode>('idle');
  const [batteryLevel, setBatteryLevel] = useState<number | null>(null);
  const [wifiState, setWifiState] = useState<number>(WIFI_STATE_OFF);
  const wifiStateRef = useRef<number>(WIFI_STATE_OFF);
  const [wifiSsid, setWifiSsid] = useState<string>('');
  const [wifiPass, setWifiPass] = useState<string>('');
  const [lastPhotoTransport, setLastPhotoTransport] = useState<'BLE' | 'WIFI' | null>(null);
  // Session config from relay (used to provision the wearable for direct Wi-Fi POST)
  const sessionRef = useRef<{ token: string; uploadUrl: string | null } | null>(null);

  // -- Refs --------------------------------------------------------------------
  const managerRef = useRef<BleManager | null>(null);
  const deviceRef = useRef<Device | null>(null);
  const subsRef = useRef<Subscription[]>([]);
  const relayWsRef = useRef<WebSocket | null>(null);
  const ttsSoundRef = useRef<Audio.Sound | null>(null);
  const ttsCounterRef = useRef(0);
  const ttsQueueRef = useRef<string[]>([]);
  const ttsBusyRef = useRef(false);
  // Mic mute. The breathing orb taps into this; flushOpusFrame consults
  // mutedRef on every BLE callback so we drop frames at the source.
  const [muted, setMuted] = useState(false);
  const mutedRef = useRef(false);
  useEffect(() => { mutedRef.current = muted; }, [muted]);
  // Mirror of ttsBusyRef for the orb's "speaking" breathing pattern.
  // Set/cleared from the TTS queue lifecycle.
  const [agentSpeaking, setAgentSpeaking] = useState(false);
  // Generation counter for the TTS queue. clearTtsQueue() bumps it; any
  // drainTtsQueue() running with a stale gen exits without touching state.
  // Prevents stale audio from a previous session bleeding into a new one.
  const ttsGenRef = useRef(0);
  // Latency probe: wall-clock ms when Deepgram's is_final arrived for the
  // current user turn. Cleared once the corresponding TTS audio shows up.
  // Deepgram's endpointing window adds ~300ms on top — see DG_ENDPOINTING_MS.
  const lastFinalTsRef = useRef<number>(0);
  const modeRef = useRef<Mode>('idle');
  // Distinguishes user-initiated Disconnect (true) from BLE drop (false). The
  // BLE onDisconnected callback consults this to decide whether to auto-retry
  // connect() once before falling back to manual Step 1.
  const userDisconnectingRef = useRef<boolean>(false);

  const counters = useRef({
    blePackets: 0,
    opusFrames: 0,
    pcmSamples: 0,
    audioLost: 0,
    photoPackets: 0,
    photosDone: 0,
    photoBytes: 0,
    photoLost: 0,
  });

  // Audio frame reassembly (mirrors omi/app/lib/utils/audio/wav_bytes.dart:98)
  const audio = useRef({ pending: null as Uint8Array | null, lastPacketIdx: -1, lastFrameId: -1 });

  // Photo frame reassembly (mirrors omi/app/lib/services/devices/omiglass_connection.dart:553)
  const photo = useRef({
    chunks: [] as Uint8Array[],
    len: 0,
    nextFrame: 0,
    transferring: false,
    orientation: 0,
    nextId: 0,
    assemblyStartTs: 0,
  });

  const awaitingPhotoRequestRef = useRef<{ id: string; ts: number } | null>(null);

  // Phase derived from connection + Wi-Fi. Drives which screen renders.
  const phase: Phase = useMemo(() => {
    if (!connected) return 'connect-mechie';
    if (wifiState !== WIFI_STATE_READY && wifiState !== WIFI_STATE_UPLOADING) return 'connect-wifi';
    return 'guidance';
  }, [connected, wifiState]);

  useEffect(() => {
    managerRef.current = new BleManager();
    Audio.setAudioModeAsync({
      playsInSilentModeIOS: true,
      staysActiveInBackground: false,
      shouldDuckAndroid: true,
      allowsRecordingIOS: false,
    }).catch((e) => console.warn('audio mode error', e));
    // Hydrate persisted Wi-Fi creds (check both new and legacy keys).
    (async () => {
      try {
        const raw =
          (await AsyncStorage.getItem(ASYNC_KEY_WIFI)) ??
          (await AsyncStorage.getItem(ASYNC_KEY_WIFI_LEGACY));
        if (!raw) return;
        const j = JSON.parse(raw);
        if (typeof j?.ssid === 'string') setWifiSsid(j.ssid);
        if (typeof j?.pass === 'string') setWifiPass(j.pass);
      } catch { }
    })();
    return () => {
      teardown();
      managerRef.current?.destroy();
      managerRef.current = null;
      ttsSoundRef.current?.unloadAsync().catch(() => { });
      ttsSoundRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Throttled stats flush — BLE notifications fire every ~50ms.
  useEffect(() => {
    if (!connected) return;
    const id = setInterval(() => setStats({ ...counters.current }), 250);
    return () => clearInterval(id);
  }, [connected]);

  useEffect(() => { modeRef.current = mode; }, [mode]);
  useEffect(() => { wifiStateRef.current = wifiState; }, [wifiState]);

  // Auto-enter guidance once wearable + Wi-Fi are both ready.
  useEffect(() => {
    if (phase === 'guidance' && modeRef.current !== 'guidance') {
      applyMode('guidance');
    } else if (phase !== 'guidance' && modeRef.current === 'guidance') {
      applyMode('idle');
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [phase]);

  // Apply a mode locally + tell the relay. Does NOT trigger Wi-Fi or capture writes;
  // those are handled explicitly by startWifi() / chat-mode auto-rearm in onPhoto.
  function applyMode(next: Mode) {
    modeRef.current = next;
    setMode(next);
    setEntries([]);
    flushPhotoState();
    const ws = relayWsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'set_mode', mode: next }));
    }
  }

  // Bring up Wi-Fi photo streaming on the wearable.
  // Provisions session + creds, then writes WIFI_START.
  async function startWifi() {
    await stopFirmwareCapture();
    if (sessionRef.current) {
      await provisionSessionToGlasses();
      // Brief gap so the firmware persists the session before WIFI_START reads it.
      await new Promise((r) => setTimeout(r, 100));
    }
    await writePhotoControl(new Uint8Array([PHOTO_CMD_WIFI_START]));
  }

  function teardown() {
    for (const s of subsRef.current) {
      try { s.remove(); } catch { }
    }
    subsRef.current = [];
    if (deviceRef.current) {
      deviceRef.current.cancelConnection().catch(() => { });
      deviceRef.current = null;
    }
    if (relayWsRef.current) {
      try { relayWsRef.current.close(); } catch { }
      relayWsRef.current = null;
    }
    // Hard-stop any in-flight TTS so a stale reply can't play after reconnect.
    clearTtsQueue();
    setConnected(false);
    setBatteryLevel(null);
    setWifiState(WIFI_STATE_OFF);
    sessionRef.current = null;
    setMode('idle');
    modeRef.current = 'idle';
    setEntries([]);
  }

  async function saveWifiCreds(ssid: string, pass: string) {
    setWifiSsid(ssid);
    setWifiPass(pass);
    try {
      if (!ssid && !pass) {
        await AsyncStorage.removeItem(ASYNC_KEY_WIFI);
        await AsyncStorage.removeItem(ASYNC_KEY_WIFI_LEGACY);
      } else {
        await AsyncStorage.setItem(ASYNC_KEY_WIFI, JSON.stringify({ ssid, pass }));
      }
    } catch (e) {
      console.warn('saving wifi creds failed', e);
    }
  }

  // Provision session config (Wi-Fi creds + URL + session token) to the wearable via BLE.
  // Format: [ssid_len, ssid, pass_len, pass, url_len_hi, url_len_lo, url, token_len, token]
  async function provisionSessionToGlasses() {
    const dev = deviceRef.current;
    const session = sessionRef.current;
    if (!dev || !session?.uploadUrl) return;
    if (!wifiSsid) {
      console.log('[provision] skipping — no Wi-Fi SSID configured');
      return;
    }
    const enc = new TextEncoder();
    const ssid = enc.encode(wifiSsid);
    const pass = enc.encode(wifiPass);
    const url = enc.encode(session.uploadUrl);
    const token = enc.encode(session.token);
    if (ssid.length > 32 || pass.length > 64 || url.length > 256 || token.length > 64) {
      console.warn('[provision] payload too long, aborting');
      return;
    }
    const total = 1 + ssid.length + 1 + pass.length + 2 + url.length + 1 + token.length;
    const buf = new Uint8Array(total);
    let off = 0;
    buf[off++] = ssid.length;
    buf.set(ssid, off); off += ssid.length;
    buf[off++] = pass.length;
    buf.set(pass, off); off += pass.length;
    buf[off++] = (url.length >> 8) & 0xff;
    buf[off++] = url.length & 0xff;
    buf.set(url, off); off += url.length;
    buf[off++] = token.length;
    buf.set(token, off); off += token.length;
    try {
      await dev.writeCharacteristicWithResponseForService(
        SERVICE_UUID,
        PHOTO_UPLOAD_SESSION_UUID,
        bytesToB64(buf),
      );
      console.log('[provision] session sent to wearable');
    } catch (e) {
      console.warn('[provision] write failed', e);
    }
  }

  async function writePhotoControl(payload: Uint8Array) {
    const dev = deviceRef.current;
    if (!dev) return;
    try {
      await dev.writeCharacteristicWithResponseForService(
        SERVICE_UUID,
        IMAGE_CONTROL_UUID,
        bytesToB64(payload),
      );
    } catch (e) {
      console.warn('[photo control] write failed', e);
    }
  }

  function openRelay(url: string): Promise<WebSocket> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      ws.binaryType = 'arraybuffer';
      let settled = false;
      const t = setTimeout(() => {
        if (settled) return;
        settled = true;
        try { ws.close(); } catch { }
        reject(new Error('Relay connect timed out (15s).'));
      }, 15000);

      ws.onopen = () => {
        if (settled) return;
        settled = true;
        clearTimeout(t);
        resolve(ws);
      };
      ws.onerror = (e: any) => {
        console.warn('relay WS error:', e?.message, e);
        if (settled) return;
        settled = true;
        clearTimeout(t);
        reject(new Error('Relay error: ' + (e?.message ?? 'host unreachable, TLS, or DNS failure')));
      };
      ws.onclose = (e) => {
        console.warn('relay WS close:', e.code, e.reason);
        if (!settled) {
          settled = true;
          clearTimeout(t);
          reject(new Error(`Relay closed before open (code=${e.code}${e.reason ? ' "' + e.reason + '"' : ''})`));
          return;
        }
        if (relayWsRef.current === ws) {
          relayWsRef.current = null;
          setStatus(`Relay closed (${e.code}${e.reason ? ' ' + e.reason : ''}).`);
        }
      };
      ws.onmessage = onRelayMessage;
    });
  }

  function appendOrReplaceUserStt(text: string, isFinal: boolean) {
    setEntries((prev) => {
      const last = prev[prev.length - 1];
      // Replace an in-flight interim from the same speaker turn.
      if (last && last.kind === 'user' && !last.final) {
        const next = prev.slice(0, -1);
        return [...next, { kind: 'user', text, final: isFinal }];
      }
      return [...prev, { kind: 'user', text, final: isFinal }];
    });
  }

  function appendAgentText(text: string) {
    if (!text) return;
    setEntries((prev) => [...prev, { kind: 'agent', text }]);
  }

  function onRelayMessage(e: WebSocketMessageEvent) {
    if (typeof e.data !== 'string') {
      const buf = e.data as ArrayBuffer;
      if (!buf || buf.byteLength < 1) return;
      const view = new Uint8Array(buf);
      if (view[0] === TAG_RELAY_TTS) {
        // Latency probe — paired with the most recent is_final transcript.
        if (lastFinalTsRef.current > 0) {
          const finalToAudio = Date.now() - lastFinalTsRef.current;
          const speechToAudio = finalToAudio + DG_ENDPOINTING_MS;
          console.log(
            `[latency] end-of-speech → audio in app: ${speechToAudio}ms ` +
            `(final→audio ${finalToAudio}ms + endpointing ${DG_ENDPOINTING_MS}ms)`,
          );
          lastFinalTsRef.current = 0;
        }
        playTts(view.subarray(1));
      } else if (view[0] === TAG_RELAY_WIFI_PHOTO) {
        // Wi-Fi photo forwarded by relay. Captured for the agent; not displayed.
        const jpeg = new Uint8Array(view.byteLength - 1);
        jpeg.set(view.subarray(1));
        counters.current.photosDone++;
        counters.current.photoBytes += jpeg.length;
        renderPhoto(jpeg, 2);
        setLastPhotoTransport('WIFI');
      }
      return;
    }
    let msg: any;
    try { msg = JSON.parse(e.data); } catch { return; }

    if (msg.type === 'session') {
      sessionRef.current = {
        token: typeof msg.token === 'string' ? msg.token : '',
        uploadUrl: typeof msg.uploadUrl === 'string' ? msg.uploadUrl : null,
      };
      provisionSessionToGlasses();
      return;
    }

    if (msg.type === 'request_photo') {
      const id = typeof msg.id === 'string' && msg.id ? msg.id : 'no-id';
      awaitingPhotoRequestRef.current = { id, ts: Date.now() };
      resetPhoto();
      dispatchPhotoRequest(id);
      return;
    }

    // Optional agent text stream. Renders as a "mechie" bubble alongside spoken TTS.
    // Relay sends one of:
    //   { type: 'agent_text', text: '...' }            — full reply
    //   { type: 'agent_chunk', text: '...', final: bool } — streamed token chunk
    if (msg.type === 'agent_text' && typeof msg.text === 'string') {
      appendAgentText(msg.text);
      return;
    }
    if (msg.type === 'agent_chunk' && typeof msg.text === 'string') {
      setEntries((prev) => {
        const last = prev[prev.length - 1];
        if (last && last.kind === 'agent') {
          const next = prev.slice(0, -1);
          return [...next, { kind: 'agent', text: last.text + msg.text }];
        }
        return [...prev, { kind: 'agent', text: msg.text }];
      });
      return;
    }

    if (msg.type !== 'Results') return;
    const alt = msg.channel?.alternatives?.[0];
    const text = alt?.transcript;
    if (!text) return;
    if (msg.is_final) {
      // Stamp the moment the user's utterance was finalized. Paired with
      // the next TAG_RELAY_TTS arrival to print the round-trip.
      lastFinalTsRef.current = Date.now();
    }
    appendOrReplaceUserStt(text, !!msg.is_final);
  }

  // ---- File-based TTS playback -----------------------------------------
  // The relay sends one self-contained WAV per turn (TAG_RELAY_TTS, tag
  // byte 0x10). We write it to cache and play via expo-av Audio.Sound.
  // One file = no chunk boundaries = guaranteed gapless within a turn.
  // Trade is TTFA = full audio-generation duration, but expo-av's
  // file-based playback is rock-solid where streaming alternatives glitch.
  async function playTts(audio: Uint8Array) {
    try {
      const id = ++ttsCounterRef.current;
      const path = (FileSystem.cacheDirectory ?? '') + `tts-${id}.wav`;
      await FileSystem.writeAsStringAsync(path, bytesToB64(audio), {
        encoding: FileSystem.EncodingType.Base64,
      });
      ttsQueueRef.current.push(path);
      drainTtsQueue();
    } catch (err) {
      console.warn('[tts] queue write failed', err);
    }
  }

  async function drainTtsQueue() {
    if (ttsBusyRef.current) return;
    ttsBusyRef.current = true;
    setAgentSpeaking(true);
    const myGen = ttsGenRef.current;
    try {
      while (ttsQueueRef.current.length > 0 && myGen === ttsGenRef.current) {
        const path = ttsQueueRef.current.shift()!;
        await playSingleTts(path);
      }
    } finally {
      // Only reset state if we're still the active drain — clearTtsQueue
      // increments the generation and resets these synchronously, so we must
      // not stomp on a fresh drain that may have started in the meantime.
      if (myGen === ttsGenRef.current) {
        ttsBusyRef.current = false;
        ttsSoundRef.current = null;
        setAgentSpeaking(false);
        console.log('[tts] queue drained');
      } else {
        console.log('[tts] queue drain superseded by clear');
      }
    }
  }

  // Hard-stop everything TTS-related. Called on disconnect (user-initiated
  // OR involuntary) and on mode change to idle, so a stale reply from the
  // previous session can't bleed into the next one.
  function clearTtsQueue() {
    ttsGenRef.current++;
    ttsQueueRef.current = [];
    const sound = ttsSoundRef.current;
    if (sound) {
      sound.stopAsync().catch(() => { });
      sound.unloadAsync().catch(() => { });
      ttsSoundRef.current = null;
    }
    ttsBusyRef.current = false;
    setAgentSpeaking(false);
  }

  // Plays one WAV to completion. Resolves on didJustFinish OR on a watchdog
  // timeout — never blocks the queue forever.
  async function playSingleTts(path: string): Promise<void> {
    let resolved = false;
    let sound: Audio.Sound | null = null;
    let watchdog: ReturnType<typeof setTimeout> | null = null;
    return new Promise<void>((resolve) => {
      const finish = (reason: string) => {
        if (resolved) return;
        resolved = true;
        if (watchdog) { clearTimeout(watchdog); watchdog = null; }
        if (sound) sound.unloadAsync().catch(() => { });
        FileSystem.deleteAsync(path, { idempotent: true }).catch(() => { });
        if (reason !== 'didJustFinish') {
          console.log(`[tts] segment finished (${reason})`);
        }
        resolve();
      };
      Audio.Sound.createAsync({ uri: path }, { shouldPlay: true })
        .then(({ sound: s, status }) => {
          sound = s;
          ttsSoundRef.current = s;
          const durationMs =
            'isLoaded' in status && status.isLoaded
              ? status.durationMillis ?? 0
              : 0;
          const watchdogMs = durationMs > 0 ? durationMs + 1500 : 20000;
          watchdog = setTimeout(() => finish(`watchdog@${watchdogMs}ms`), watchdogMs);
          s.setOnPlaybackStatusUpdate((st) => {
            if (!('isLoaded' in st) || !st.isLoaded) return;
            if (st.didJustFinish) finish('didJustFinish');
          });
        })
        .catch((err) => {
          console.warn('[tts] segment play failed', err);
          finish('error');
        });
    });
  }

  async function ensureAndroidPerms(): Promise<boolean> {
    if (Platform.OS !== 'android') return true;
    try {
      const apiLevel = Platform.Version as number;
      const perms =
        apiLevel >= 31
          ? [
            PermissionsAndroid.PERMISSIONS.BLUETOOTH_SCAN,
            PermissionsAndroid.PERMISSIONS.BLUETOOTH_CONNECT,
          ]
          : [PermissionsAndroid.PERMISSIONS.ACCESS_FINE_LOCATION];
      const r = await PermissionsAndroid.requestMultiple(perms);
      return Object.values(r).every((v) => v === PermissionsAndroid.RESULTS.GRANTED);
    } catch {
      return false;
    }
  }

  async function connect() {
    setConnectError(null);
    if (!(await ensureAndroidPerms())) {
      setConnectError('Bluetooth permission denied. Open iOS/Android settings and enable Bluetooth for GTC, then try again.');
      return;
    }

    setConnecting(true);
    setStatus('Connecting to server');
    try {
      relayWsRef.current = await openRelay(relayUrl);

      setStatus('Looking for Mechie…');
      const mgr = managerRef.current!;
      const found = await scanForDevice(mgr, SERVICE_UUID, SCAN_TIMEOUT_MS);
      setStatus(`Found ${found.name ?? found.id}...`);

      const dev = await found.connect({ requestMTU: 517 });
      await dev.discoverAllServicesAndCharacteristics();
      deviceRef.current = dev;

      try {
        const chars = await dev.characteristicsForService(SERVICE_UUID);
        console.log('[discovery] service chars:');
        for (const c of chars) {
          console.log(
            `  ${c.uuid}  read=${c.isReadable} write=${c.isWritableWithResponse || c.isWritableWithoutResponse} notify=${c.isNotifiable}`,
          );
        }
      } catch (e) {
        console.warn('[discovery] failed to enumerate chars', e);
      }

      const codecChar = await dev.readCharacteristicForService(SERVICE_UUID, CODEC_CHAR_UUID);
      const codecBytes = b64ToBytes(codecChar.value ?? '');
      const codecId = codecBytes[0] ?? 0;
      const codecName =
        ({ 1: 'PCM8', 20: 'Opus', 21: 'OpusFS320' } as Record<number, string>)[codecId] ??
        `unknown(${codecId})`;

      const audioSub = dev.monitorCharacteristicForService(
        SERVICE_UUID,
        AUDIO_CHAR_UUID,
        (err, char) => {
          if (err) return;
          if (!char?.value) return;
          onAudio(b64ToBytes(char.value));
        },
      );
      subsRef.current.push(audioSub);

      const photoSub = dev.monitorCharacteristicForService(
        SERVICE_UUID,
        IMAGE_DATA_UUID,
        (err, char) => {
          if (err) return;
          if (!char?.value) return;
          onPhoto(b64ToBytes(char.value));
        },
      );
      subsRef.current.push(photoSub);

      const discSub = dev.onDisconnected(() => {
        const wasUserInitiated = userDisconnectingRef.current;
        userDisconnectingRef.current = false;
        setStatus(
          wasUserInitiated
            ? 'mechie disconnected.'
            : 'mechie disconnected. Reconnecting…',
        );
        teardown();
        if (!wasUserInitiated) {
          // Auto-retry once. 800 ms grace lets the BLE stack tear down before
          // we re-scan. If this attempt also fails, connect() falls into its
          // own catch path, surfaces the troubleshooting panel on Step 1, and
          // does NOT loop (no further auto-retry triggers from here).
          setTimeout(() => {
            console.log('[reconnect] auto-retry after BLE drop');
            connect();
          }, 800);
        }
      });
      subsRef.current.push(discSub);

      // Stop the firmware's hardcoded 30s interval capture; we drive captures explicitly.
      await stopFirmwareCapture();

      if (sessionRef.current) {
        await provisionSessionToGlasses();
      }

      try {
        const batChar = await dev.readCharacteristicForService(
          BATTERY_SERVICE_UUID,
          BATTERY_LEVEL_UUID,
        );
        const bytes = b64ToBytes(batChar.value ?? '');
        if (bytes.length > 0) setBatteryLevel(bytes[0]);
      } catch (e) {
        console.warn('battery initial read failed', e);
      }
      const batSub = dev.monitorCharacteristicForService(
        BATTERY_SERVICE_UUID,
        BATTERY_LEVEL_UUID,
        (err, char) => {
          if (err || !char?.value) return;
          const bytes = b64ToBytes(char.value);
          if (bytes.length > 0) setBatteryLevel(bytes[0]);
        },
      );
      subsRef.current.push(batSub);

      try {
        const initial = await dev.readCharacteristicForService(
          SERVICE_UUID,
          PHOTO_UPLOAD_STATE_UUID,
        );
        const bytes = b64ToBytes(initial.value ?? '');
        if (bytes.length > 0) setWifiState(bytes[0]);
      } catch { }
      const wifiSub = dev.monitorCharacteristicForService(
        SERVICE_UUID,
        PHOTO_UPLOAD_STATE_UUID,
        (err, char) => {
          if (err || !char?.value) return;
          const bytes = b64ToBytes(char.value);
          if (bytes.length > 0) setWifiState(bytes[0]);
        },
      );
      subsRef.current.push(wifiSub);

      setConnected(true);
      setStatus(`Connected · codec=${codecName}.`);
    } catch (e: any) {
      console.warn('connect error', e);
      setConnectError(humanizeConnectError(e));
      teardown();
    } finally {
      setConnecting(false);
    }
  }

  async function triggerSinglePhoto() {
    const dev = deviceRef.current;
    if (!dev) return;
    try {
      await dev.writeCharacteristicWithResponseForService(
        SERVICE_UUID,
        IMAGE_CONTROL_UUID,
        bytesToB64(new Uint8Array([PHOTO_CMD_SINGLE_SHOT])),
      );
    } catch (e) {
      console.warn('photo re-arm failed', e);
    }
  }

  // Pick a transport for a photo request.
  //   READY     → Wi-Fi POST (fast path)
  //   CONNECTING → wait up to 8s for it to resolve. Wi-Fi associate competes with BLE
  //                on the shared radio; firing a BLE single-shot during it produces
  //                truncated photos.
  //   else      → BLE single-shot
  async function dispatchPhotoRequest(requestId: string) {
    if (wifiStateRef.current === WIFI_STATE_CONNECTING) {
      const deadline = Date.now() + 8000;
      while (
        wifiStateRef.current === WIFI_STATE_CONNECTING &&
        Date.now() < deadline
      ) {
        await new Promise((r) => setTimeout(r, 200));
      }
    }

    if (wifiStateRef.current === WIFI_STATE_READY) {
      const idBytes = new TextEncoder().encode(requestId);
      const buf = new Uint8Array(2 + idBytes.length);
      buf[0] = PHOTO_CMD_WIFI_POST;
      buf[1] = idBytes.length;
      buf.set(idBytes, 2);
      await writePhotoControl(buf);
      setLastPhotoTransport('WIFI');
    } else {
      await triggerSinglePhoto();
    }
  }

  async function stopFirmwareCapture() {
    const dev = deviceRef.current;
    if (!dev) return;
    try {
      await dev.writeCharacteristicWithResponseForService(
        SERVICE_UUID,
        IMAGE_CONTROL_UUID,
        bytesToB64(new Uint8Array([0x00])),
      );
    } catch (e) {
      console.warn('stop firmware capture failed', e);
    }
  }

  function flushPhotoState() {
    resetPhoto();
    setPhotos([]);
    awaitingPhotoRequestRef.current = null;
  }

  // --- Audio packet handler ---------------------------------------------------
  function onAudio(buf: Uint8Array) {
    counters.current.blePackets++;
    if (buf.length < 4) return;

    const packetIndex = buf[0] | (buf[1] << 8);
    const internal = buf[2];
    const payload = buf.subarray(3);
    const a = audio.current;

    if (a.lastPacketIdx === -1 && internal === 0) {
      a.pending = new Uint8Array(payload);
      a.lastPacketIdx = packetIndex;
      a.lastFrameId = 0;
      return;
    }
    if (a.lastPacketIdx === -1) return;

    if (
      packetIndex !== a.lastPacketIdx + 1 ||
      (internal !== 0 && internal !== a.lastFrameId + 1)
    ) {
      // Estimate frames lost from the BLE packet gap. packetIndex is a
      // uint16 on the wire so it wraps every ~21 minutes at 50 pps;
      // gaps >1000 are almost certainly wrap, treat as a single loss.
      const rawGap = (packetIndex - a.lastPacketIdx - 1 + 65536) % 65536;
      const gap = rawGap > 1000 ? 1 : Math.max(1, rawGap);
      counters.current.audioLost += gap;
      const ws = relayWsRef.current;
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'audio_loss', gap }));
      }
      a.lastPacketIdx = -1;
      a.pending = null;
      return;
    }

    if (internal === 0) {
      if (a.pending && a.pending.length > 0) flushOpusFrame(a.pending);
      a.pending = new Uint8Array(payload);
    } else {
      const merged = new Uint8Array((a.pending?.length ?? 0) + payload.length);
      if (a.pending) merged.set(a.pending, 0);
      merged.set(payload, a.pending?.length ?? 0);
      a.pending = merged;
    }
    a.lastFrameId = internal;
    a.lastPacketIdx = packetIndex;
  }

  function flushOpusFrame(opusFrame: Uint8Array) {
    counters.current.opusFrames++;
    if (mutedRef.current) return;
    const ws = relayWsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const tagged = new Uint8Array(opusFrame.length + 1);
    tagged[0] = TAG_PHONE_OPUS;
    tagged.set(opusFrame, 1);
    ws.send(tagged.buffer);
    counters.current.pcmSamples += 320;
  }

  // --- Photo packet handler ---------------------------------------------------
  function onPhoto(buf: Uint8Array) {
    counters.current.photoPackets++;
    if (buf.length < 2) return;
    const frameIndex = buf[0] | (buf[1] << 8);
    const p = photo.current;

    if (frameIndex === PHOTO_END_OF_IMAGE) {
      if (p.transferring && p.len > 0) {
        const jpeg = concatChunks(p.chunks, p.len);
        counters.current.photosDone++;
        counters.current.photoBytes += jpeg.length;
        renderPhoto(jpeg, p.orientation);

        const req = awaitingPhotoRequestRef.current;
        if (modeRef.current === 'chat') {
          forwardPhotoToRelay(jpeg, null);
          setLastPhotoTransport('BLE');
        } else if (req && p.assemblyStartTs >= req.ts) {
          forwardPhotoToRelay(jpeg, req.id);
          awaitingPhotoRequestRef.current = null;
          setLastPhotoTransport('BLE');
        } else if (req) {
          counters.current.photoLost++;
        }
      }
      resetPhoto();
      if (modeRef.current === 'chat') triggerSinglePhoto();
      return;
    }

    if (frameIndex === 0) {
      resetPhoto();
      p.transferring = true;
      p.assemblyStartTs = Date.now();
      if (buf.length > 2) p.orientation = buf[2];
      if (buf.length > 3) appendChunk(buf.subarray(3));
      p.nextFrame = 1;
      return;
    }

    if (!p.transferring) return;
    if (frameIndex !== p.nextFrame) {
      counters.current.photoLost++;
      resetPhoto();
      return;
    }
    if (buf.length > 2) appendChunk(buf.subarray(2));
    p.nextFrame++;
    if (p.len > 200 * 1024) resetPhoto();
  }

  function appendChunk(view: Uint8Array) {
    const copy = new Uint8Array(view.length);
    copy.set(view);
    photo.current.chunks.push(copy);
    photo.current.len += copy.length;
  }

  function resetPhoto() {
    const p = photo.current;
    p.chunks = [];
    p.len = 0;
    p.nextFrame = 0;
    p.transferring = false;
  }

  function concatChunks(chunks: Uint8Array[], total: number): Uint8Array {
    const out = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) {
      out.set(c, off);
      off += c.length;
    }
    return out;
  }

  function renderPhoto(jpeg: Uint8Array, orientation: number) {
    const uri = 'data:image/jpeg;base64,' + bytesToB64(jpeg);
    const rotation = ([0, 90, 180, 270] as const)[orientation] ?? 0;
    const id = ++photo.current.nextId;
    setPhotos((prev) => {
      const next: Photo = {
        id,
        uri,
        rotation,
        size: jpeg.length,
        ts: new Date().toLocaleTimeString(),
      };
      const merged = [next, ...prev];
      return merged.length > MAX_PHOTOS_KEPT ? merged.slice(0, MAX_PHOTOS_KEPT) : merged;
    });
  }

  function forwardPhotoToRelay(jpeg: Uint8Array, requestId: string | null) {
    const ws = relayWsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const idBytes = requestId
      ? new TextEncoder().encode(requestId)
      : new Uint8Array(0);
    if (idBytes.length > 255) return;
    const out = new Uint8Array(2 + idBytes.length + jpeg.length);
    out[0] = TAG_PHONE_PHOTO;
    out[1] = idBytes.length;
    out.set(idBytes, 2);
    out.set(jpeg, 2 + idBytes.length);
    ws.send(out.buffer);
  }

  // --- Render -----------------------------------------------------------------
  return (
    <View style={styles.root}>
      <StatusBar barStyle="dark-content" />

      <Header
        connected={connected}
        showDisconnect={phase === 'guidance'}
        batteryLevel={batteryLevel}
        onDisconnect={() => {
          // User-initiated Disconnect — flag so the BLE callback skips auto-retry.
          userDisconnectingRef.current = true;
          teardown();
        }}
      />

      <KeyboardAvoidingView
        style={{ flex: 1 }}
        behavior={Platform.OS === 'ios' ? 'padding' : undefined}
        keyboardVerticalOffset={Platform.OS === 'ios' ? 12 : 0}>
        {phase === 'connect-mechie' && (
          <ConnectMechieScreen
            connecting={connecting}
            status={status}
            error={connectError}
            onConnect={connect}
          />
        )}
        {phase === 'connect-wifi' && (
          <WifiSetupScreen
            initialSsid={wifiSsid}
            initialPass={wifiPass}
            wifiState={wifiState}
            onSubmit={async (ssid, pass) => {
              await saveWifiCreds(ssid, pass);
              await startWifi();
            }}
          />
        )}
        {phase === 'guidance' && (
          <GuidanceScreen
            muted={muted}
            agentSpeaking={agentSpeaking}
            onToggleMute={() => setMuted((m) => !m)}
          />
        )}
      </KeyboardAvoidingView>
    </View>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function humanizeConnectError(e: any): string {
  const m = (e?.message ?? String(e)) as string;
  if (/Scan timed out|timed out/i.test(m)) {
    return "Couldn't find mechie within 60 seconds.";
  }
  if (/Bluetooth/i.test(m) || /BLE/i.test(m)) {
    return 'Bluetooth refused the connection.';
  }
  if (/Relay/i.test(m)) {
    return "Couldn't reach the GTC server. Check your phone's internet connection.";
  }
  return m;
}

function scanForDevice(
  mgr: BleManager,
  serviceUuid: string,
  timeoutMs: number,
): Promise<Device> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => {
      mgr.stopDeviceScan();
      reject(new Error('Scan timed out.'));
    }, timeoutMs);
    mgr.startDeviceScan([serviceUuid], null, (err, dev) => {
      if (err) {
        clearTimeout(t);
        mgr.stopDeviceScan();
        reject(err);
        return;
      }
      if (dev) {
        clearTimeout(t);
        mgr.stopDeviceScan();
        resolve(dev);
      }
    });
  });
}

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

function Header({
  connected,
  showDisconnect,
  batteryLevel,
  onDisconnect,
}: {
  connected: boolean;
  showDisconnect: boolean;
  batteryLevel: number | null;
  onDisconnect: () => void;
}) {
  return (
    <View style={styles.header}>
      <View style={styles.headerSide}>
        {connected ? <BatteryIndicator level={batteryLevel} /> : null}
      </View>
      <Text style={styles.headerBrand}>GTC</Text>
      <View style={[styles.headerSide, styles.headerSideRight]}>
        {showDisconnect ? (
          <Pressable
            onPress={onDisconnect}
            style={({ pressed }) => [
              styles.disconnectBtn,
              pressed && styles.pressed,
            ]}>
            <Text style={styles.disconnectBtnText}>Disconnect</Text>
          </Pressable>
        ) : null}
      </View>
    </View>
  );
}

// Minimalist battery — neutral pill outline with a charcoal fill that turns
// red only when critically low. Drawn from primitives (no emoji).
function BatteryIndicator({ level }: { level: number | null }) {
  if (level === null) return null;
  const pct = Math.max(0, Math.min(100, level));
  const low = pct <= 15;
  const fill = low ? '#dc2626' : '#3f3f46';
  return (
    <View style={styles.battery}>
      <View style={styles.batteryBody}>
        <View
          style={[
            styles.batteryFill,
            { width: `${pct}%`, backgroundColor: fill },
          ]}
        />
      </View>
      <View style={styles.batteryNub} />
      <Text style={[styles.batteryPct, { color: fill }]}>{pct}%</Text>
    </View>
  );
}

function ConnectMechieScreen({
  connecting,
  status,
  error,
  onConnect,
}: {
  connecting: boolean;
  status: string;
  error: string | null;
  onConnect: () => void;
}) {
  return (
    <ScrollView
      contentContainerStyle={styles.connectScroll}
      keyboardShouldPersistTaps="handled">
      <Text style={[styles.eyebrow, styles.eyebrowTopLeft]}>Step 1 of 2</Text>

      <View style={styles.flexSpacer} />

      <View style={styles.connectMiddle}>
        <Text style={[styles.heroTitle, styles.heroTitleCenter]}>
          Connect to Mechie
        </Text>
        <Text style={[styles.heroBody, styles.heroBodyCenter]}>
          Power on your mechie wearable and keep it within a few feet of this
          phone.
        </Text>
        <Pressable
          disabled={connecting}
          onPress={onConnect}
          style={({ pressed }) => [
            styles.cta,
            styles.ctaCentered,
            connecting && styles.ctaConnecting,
            pressed && !connecting && styles.pressed,
          ]}>
          {connecting ? (
            <View style={styles.ctaInline}>
              <ActivityIndicator color="#fff" />
              <Text style={[styles.ctaText, styles.ctaStatusText]} numberOfLines={1}>
                {status}
              </Text>
            </View>
          ) : (
            <Text style={styles.ctaText}>{error ? 'Retry' : 'Connect'}</Text>
          )}
        </Pressable>
      </View>

      <View style={styles.flexSpacer} />

      {error && !connecting ? <TroubleshootingPanel error={error} /> : null}
    </ScrollView>
  );
}

function TroubleshootingPanel({ error }: { error: string }) {
  return (
    <View style={styles.panel}>
      <Text style={styles.panelTitle}>Couldn't reach mechie</Text>
      <Text style={styles.panelError}>{error}</Text>
      <View style={styles.divider} />
      <Text style={styles.panelStepTitle}>Try this</Text>
      <Step
        n="1"
        text="Confirm mechie is powered on (status LED visible)."
      />
      <Step
        n="2"
        text="Keep mechie within 6 feet of this phone, line of sight."
      />
      <Step
        n="3"
        text="Close any other app paired to mechie (the desktop or web demo)."
      />
      <Step
        n="4"
        text="Toggle this phone's Bluetooth off and back on."
      />
      <Step
        n="5"
        text="If still nothing, hold mechie's side button for 5 seconds to restart it."
      />
    </View>
  );
}

function Step({ n, text }: { n: string; text: string }) {
  return (
    <View style={styles.step}>
      <View style={styles.stepBullet}>
        <Text style={styles.stepBulletText}>{n}</Text>
      </View>
      <Text style={styles.stepText}>{text}</Text>
    </View>
  );
}

function WifiSetupScreen({
  initialSsid,
  initialPass,
  wifiState,
  onSubmit,
}: {
  initialSsid: string;
  initialPass: string;
  wifiState: number;
  onSubmit: (ssid: string, pass: string) => Promise<void>;
}) {
  const [ssid, setSsid] = useState(initialSsid);
  const [pass, setPass] = useState(initialPass);
  useEffect(() => setSsid(initialSsid), [initialSsid]);
  useEffect(() => setPass(initialPass), [initialPass]);

  const isConnecting = wifiState === WIFI_STATE_CONNECTING;
  const failed = wifiState === WIFI_STATE_FAILED;
  const canSubmit = !!ssid && !isConnecting;

  return (
    <ScrollView
      contentContainerStyle={styles.phaseRoot}
      keyboardShouldPersistTaps="handled">
      <Text style={[styles.eyebrow, styles.eyebrowTopLeft]}>Step 2 of 2</Text>
      <View style={styles.wifiHeroBlock}>
        <Text style={styles.heroTitle}>Connect Mechie to Wi-Fi</Text>
        <Text style={styles.heroBody}>
          Mechie streams images over Wi-Fi for fast guidance.
        </Text>
      </View>

      <View style={styles.inputBlock}>
        <Text style={styles.inputLabel}>Network</Text>
        <TextInput
          value={ssid}
          onChangeText={setSsid}
          placeholder="e.g. Akshay's iPhone"
          placeholderTextColor="#9ca3af"
          autoCapitalize="none"
          autoCorrect={false}
          spellCheck={false}
          editable={!isConnecting}
          style={styles.input}
        />
      </View>
      <View style={styles.inputBlock}>
        <Text style={styles.inputLabel}>Password</Text>
        <TextInput
          value={pass}
          onChangeText={setPass}
          placeholder="Network password"
          placeholderTextColor="#9ca3af"
          autoCapitalize="none"
          autoCorrect={false}
          spellCheck={false}
          secureTextEntry
          editable={!isConnecting}
          style={styles.input}
        />
      </View>

      {failed ? (
        <View style={styles.errorPanel}>
          <Text style={styles.errorPanelTitle}>Wi-Fi didn't connect</Text>
          <Text style={styles.errorPanelBody}>
            Mechie couldn't reach this network. Check the password, confirm the
            network is online, and try again.
          </Text>
        </View>
      ) : null}

      <Pressable
        disabled={!canSubmit}
        onPress={() => onSubmit(ssid.trim(), pass)}
        style={({ pressed }) => [
          styles.cta,
          !canSubmit && styles.ctaDisabled,
          pressed && canSubmit && styles.pressed,
        ]}>
        {isConnecting ? (
          <View style={styles.ctaInline}>
            <ActivityIndicator color="#fff" />
            <Text style={styles.ctaText}>Connecting…</Text>
          </View>
        ) : (
          <Text style={styles.ctaText}>{failed ? 'Retry Wi-Fi' : 'Connect'}</Text>
        )}
      </Pressable>
    </ScrollView>
  );
}

function GuidanceScreen({
  muted,
  agentSpeaking,
  onToggleMute,
}: {
  muted: boolean;
  agentSpeaking: boolean;
  onToggleMute: () => void;
}) {
  return (
    <View style={[styles.phaseRootGuidance, styles.orbScreen]}>
      <BreathingOrb
        muted={muted}
        agentSpeaking={agentSpeaking}
        onPress={onToggleMute}
      />
    </View>
  );
}

// Breathing orb: dark circle in center that scales rhythmically. Three sonar
// halo rings expand outward and fade. Slow, near-imperceptible breath while
// listening (~2.8s/cycle); faster, larger swell while the model is speaking
// (~1.5s/cycle). Muted state freezes everything and dims the orb.
//
// Performance: every animation drives `transform` or `opacity` and runs with
// useNativeDriver, so the JS thread is never woken to step the loop.
const BreathingOrb = React.memo(function BreathingOrb({
  muted,
  agentSpeaking,
  onPress,
}: {
  muted: boolean;
  agentSpeaking: boolean;
  onPress: () => void;
}) {
  const scale = useRef(new Animated.Value(1)).current;
  const halo1 = useRef(new Animated.Value(0)).current;
  const halo2 = useRef(new Animated.Value(0)).current;
  const halo3 = useRef(new Animated.Value(0)).current;
  const press = useRef(new Animated.Value(1)).current;

  useEffect(() => {
    if (muted) {
      Animated.parallel([
        Animated.timing(scale, { toValue: 0.94, duration: 240, easing: Easing.out(Easing.quad), useNativeDriver: true }),
        Animated.timing(halo1, { toValue: 0, duration: 240, useNativeDriver: true }),
        Animated.timing(halo2, { toValue: 0, duration: 240, useNativeDriver: true }),
        Animated.timing(halo3, { toValue: 0, duration: 240, useNativeDriver: true }),
      ]).start();
      return;
    }

    // Breathing parameters by state — model speaking is bigger + faster.
    const peak = agentSpeaking ? 1.16 : 1.06;
    const dur = agentSpeaking ? 1500 : 2800;

    const orbLoop = Animated.loop(
      Animated.sequence([
        Animated.timing(scale, {
          toValue: peak,
          duration: dur / 2,
          easing: Easing.inOut(Easing.quad),
          useNativeDriver: true,
        }),
        Animated.timing(scale, {
          toValue: 1,
          duration: dur / 2,
          easing: Easing.inOut(Easing.quad),
          useNativeDriver: true,
        }),
      ]),
    );

    // Each halo ring expands outward and fades; rings are offset by 1/3 of
    // the cycle so a new wave starts before the previous fades, giving a
    // continuous sonar feel without overlapping starts.
    const ringDur = agentSpeaking ? 1800 : 2700;
    const ringLoop = (val: Animated.Value, delay: number) =>
      Animated.loop(
        Animated.sequence([
          Animated.delay(delay),
          Animated.timing(val, {
            toValue: 1,
            duration: ringDur,
            easing: Easing.out(Easing.quad),
            useNativeDriver: true,
          }),
          Animated.timing(val, {
            toValue: 0,
            duration: 0,
            useNativeDriver: true,
          }),
        ]),
      );

    orbLoop.start();
    const r1 = ringLoop(halo1, 0);
    const r2 = ringLoop(halo2, ringDur / 3);
    const r3 = ringLoop(halo3, (ringDur / 3) * 2);
    r1.start(); r2.start(); r3.start();

    return () => {
      orbLoop.stop();
      r1.stop(); r2.stop(); r3.stop();
    };
  }, [muted, agentSpeaking, scale, halo1, halo2, halo3]);

  const haloStyle = (val: Animated.Value, baseOpacity: number) => ({
    transform: [
      { scale: val.interpolate({ inputRange: [0, 1], outputRange: [1, 2.6] }) },
    ],
    opacity: val.interpolate({ inputRange: [0, 1], outputRange: [baseOpacity, 0] }),
  });

  // Halo intensity: brighter when model is speaking, dim when listening.
  const haloOpacity = agentSpeaking ? 0.22 : 0.1;

  return (
    <Pressable
      onPress={onPress}
      onPressIn={() => Animated.spring(press, { toValue: 0.94, useNativeDriver: true, stiffness: 400, damping: 22 }).start()}
      onPressOut={() => Animated.spring(press, { toValue: 1, useNativeDriver: true, stiffness: 300, damping: 18 }).start()}
      style={styles.orbHit}
      accessibilityRole="button"
      accessibilityLabel={muted ? 'Unmute microphone' : 'Mute microphone'}
    >
      {!muted && (
        <>
          <Animated.View pointerEvents="none" style={[styles.orbHalo, haloStyle(halo1, haloOpacity)]} />
          <Animated.View pointerEvents="none" style={[styles.orbHalo, haloStyle(halo2, haloOpacity)]} />
          <Animated.View pointerEvents="none" style={[styles.orbHalo, haloStyle(halo3, haloOpacity)]} />
        </>
      )}
      <Animated.View
        style={[
          styles.orb,
          muted && styles.orbMuted,
          agentSpeaking && styles.orbSpeaking,
          { transform: [{ scale: Animated.multiply(scale, press) }] },
        ]}
      >
        {muted && <View style={styles.orbMuteSlash} />}
      </Animated.View>
    </Pressable>
  );
});

// Dev tab is commented out for the onboarding flow. To bring back diagnostics
// (BLE packet counters, raw transcript, photo grid, relay URL override),
// reintroduce a screen component here and add a route into it from the header.

const styles = StyleSheet.create({
  // -- Root + header ---------------------------------------------------------
  root: { flex: 1, backgroundColor: '#fafafa' },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingTop: 56,
    paddingHorizontal: 20,
    paddingBottom: 16,
    backgroundColor: '#fafafa',
    borderBottomWidth: 1,
    borderBottomColor: '#e5e7eb',
  },
  headerSide: { flex: 1, flexDirection: 'row', alignItems: 'center' },
  headerSideRight: { justifyContent: 'flex-end' },
  headerBrand: {
    fontSize: 16,
    fontWeight: '700',
    color: '#111',
    letterSpacing: 1.5,
  },

  // Battery indicator — neutral outline + charcoal fill, monochrome until low.
  battery: { flexDirection: 'row', alignItems: 'center' },
  batteryBody: {
    width: 26,
    height: 11,
    borderWidth: 1,
    borderRadius: 3,
    borderColor: '#d4d4d8',
    paddingHorizontal: 1.5,
    paddingVertical: 1,
    justifyContent: 'center',
  },
  batteryFill: { height: '100%', borderRadius: 1.5 },
  batteryNub: {
    width: 2,
    height: 5,
    marginLeft: 1.5,
    borderRadius: 1,
    backgroundColor: '#d4d4d8',
  },
  batteryPct: {
    marginLeft: 8,
    fontSize: 11,
    fontWeight: '500',
    letterSpacing: 0.2,
    fontVariant: ['tabular-nums'],
  },

  disconnectBtn: {
    paddingVertical: 7,
    paddingHorizontal: 14,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: '#e5e7eb',
    backgroundColor: '#fff',
  },
  disconnectBtnText: {
    fontSize: 12,
    fontWeight: '600',
    color: '#374151',
    letterSpacing: 0.3,
  },

  // -- Generic phase content -------------------------------------------------
  phaseRoot: {
    paddingTop: 24,
    paddingHorizontal: 24,
    paddingBottom: 48,
  },
  phaseRootGuidance: { flex: 1, paddingHorizontal: 0 },

  heroBlock: { marginBottom: 28 },
  eyebrow: {
    fontSize: 11,
    fontWeight: '700',
    color: '#6b7280',
    letterSpacing: 1.4,
    textTransform: 'uppercase',
    marginBottom: 10,
  },
  heroTitle: {
    fontSize: 30,
    fontWeight: '700',
    color: '#111',
    letterSpacing: -0.6,
    lineHeight: 34,
  },
  heroSub: {
    fontSize: 14,
    color: '#6b7280',
    marginTop: 6,
    lineHeight: 20,
  },
  heroBody: {
    fontSize: 15,
    color: '#4b5563',
    marginTop: 10,
    lineHeight: 22,
  },

  // CTA button — single accent (deep neutral), tactile press.
  cta: {
    marginTop: 28,
    backgroundColor: '#111',
    paddingVertical: 16,
    paddingHorizontal: 28,
    borderRadius: 14,
    alignItems: 'center',
    justifyContent: 'center',
  },
  ctaDisabled: { backgroundColor: '#9ca3af' },
  // While connecting, keep the dark fill (status text reads white-on-dark) but
  // dim slightly so it reads as a non-interactive state.
  ctaConnecting: { opacity: 0.85 },
  // Centered button gets a comfortable min-width so it doesn't shrink to "Connect"
  // and balloon to the full status string between renders.
  ctaCentered: { alignSelf: 'stretch', minHeight: 56 },
  ctaText: { color: '#fff', fontWeight: '600', fontSize: 15, letterSpacing: 0.2 },
  ctaStatusText: { fontSize: 14, fontWeight: '500' },
  ctaInline: { flexDirection: 'row', alignItems: 'center', gap: 10 },
  pressed: { transform: [{ translateY: 1 }], opacity: 0.92 },

  // -- Step 1: connect screen -----------------------------------------------
  // Eyebrow top-left at the same Y as Step 2 of 2 (paddingTop: 24, matching
  // phaseRoot). Title + body + button float vertically centered as one block,
  // with equal flex spacers above and below — that keeps the button close to
  // the body text instead of pinned to the bottom.
  connectScroll: {
    flexGrow: 1,
    paddingHorizontal: 28,
    paddingTop: 24,
    paddingBottom: 32,
  },
  eyebrowTopLeft: { alignSelf: 'flex-start', marginBottom: 0 },
  flexSpacer: { flex: 1 },
  connectMiddle: {
    alignItems: 'center',
    width: '100%',
  },
  heroTitleCenter: { textAlign: 'center' },
  heroBodyCenter: { textAlign: 'center', maxWidth: 320 },

  // -- Step 2: Wi-Fi screen --------------------------------------------------
  // Push the title and the rest of the form down so it reads as separate
  // from "Step 2 of 2" sitting at the top.
  wifiHeroBlock: { marginTop: 64, marginBottom: 28 },

  // -- Troubleshooting panel -------------------------------------------------
  panel: {
    marginTop: 8,
    paddingVertical: 18,
    paddingHorizontal: 18,
    borderRadius: 14,
    backgroundColor: '#fff',
    borderWidth: 1,
    borderColor: '#e5e7eb',
  },
  panelTitle: { fontSize: 15, fontWeight: '700', color: '#111' },
  panelError: { fontSize: 13, color: '#6b7280', marginTop: 4, lineHeight: 19 },
  divider: {
    height: 1,
    backgroundColor: '#f1f5f9',
    marginVertical: 14,
  },
  panelStepTitle: {
    fontSize: 11,
    fontWeight: '700',
    color: '#374151',
    letterSpacing: 1.2,
    textTransform: 'uppercase',
    marginBottom: 10,
  },
  step: { flexDirection: 'row', alignItems: 'flex-start', marginBottom: 10 },
  stepBullet: {
    width: 22,
    height: 22,
    borderRadius: 11,
    backgroundColor: '#f3f4f6',
    alignItems: 'center',
    justifyContent: 'center',
    marginRight: 10,
    marginTop: 1,
  },
  stepBulletText: { fontSize: 11, fontWeight: '700', color: '#4b5563' },
  stepText: { flex: 1, fontSize: 13, color: '#374151', lineHeight: 19 },

  // -- WiFi inputs -----------------------------------------------------------
  inputBlock: { marginBottom: 16 },
  inputLabel: {
    fontSize: 12,
    fontWeight: '600',
    color: '#374151',
    marginBottom: 8,
    letterSpacing: 0.3,
  },
  input: {
    paddingVertical: 14,
    paddingHorizontal: 16,
    borderWidth: 1,
    borderColor: '#e5e7eb',
    borderRadius: 12,
    fontSize: 15,
    color: '#111',
    backgroundColor: '#fff',
  },

  errorPanel: {
    marginTop: 4,
    paddingVertical: 14,
    paddingHorizontal: 16,
    borderRadius: 12,
    backgroundColor: '#fef2f2',
    borderWidth: 1,
    borderColor: '#fecaca',
  },
  errorPanelTitle: { fontSize: 13, fontWeight: '700', color: '#991b1b' },
  errorPanelBody: { fontSize: 12, color: '#b91c1c', marginTop: 4, lineHeight: 18 },

  footnote: {
    marginTop: 20,
    fontSize: 12,
    color: '#9ca3af',
    lineHeight: 18,
  },

  // -- Guidance --------------------------------------------------------------
  guidanceHeader: {
    paddingTop: 24,
    paddingHorizontal: 24,
    paddingBottom: 16,
  },
  liveDotRow: { flexDirection: 'row', alignItems: 'center', gap: 8, marginBottom: 8 },
  liveDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
    backgroundColor: '#16a34a',
  },
  // -- Photo strip (verification) -------------------------------------------
  photoStripWrap: {
    paddingHorizontal: 24,
    paddingBottom: 12,
  },
  photoStripLabel: {
    fontSize: 10,
    fontWeight: '700',
    color: '#9ca3af',
    letterSpacing: 1.2,
    textTransform: 'uppercase',
    marginBottom: 8,
  },
  photoStrip: { gap: 8 },
  photoThumb: {
    width: 64,
    height: 64,
    borderRadius: 10,
    overflow: 'hidden',
    backgroundColor: '#e5e7eb',
    borderWidth: 1,
    borderColor: '#e5e7eb',
  },
  photoThumbImg: { width: '100%', height: '100%' },
  transcriptScrollOuter: { flex: 1 },
  transcriptScroll: {
    paddingHorizontal: 24,
    paddingTop: 8,
    paddingBottom: 32,
  },
  transcriptEmpty: { paddingVertical: 24 },
  transcriptEmptyText: {
    fontSize: 14,
    color: '#9ca3af',
    lineHeight: 21,
  },
  bubble: {
    marginBottom: 14,
    paddingVertical: 12,
    paddingHorizontal: 16,
    borderRadius: 14,
    maxWidth: '92%',
  },
  bubbleUser: {
    alignSelf: 'flex-end',
    backgroundColor: '#111',
  },
  bubbleAgent: {
    alignSelf: 'flex-start',
    backgroundColor: '#fff',
    borderWidth: 1,
    borderColor: '#e5e7eb',
  },
  bubbleLabel: {
    fontSize: 10,
    fontWeight: '700',
    color: '#9ca3af',
    letterSpacing: 1.2,
    textTransform: 'uppercase',
    marginBottom: 4,
  },
  bubbleLabelAgent: { color: '#16a34a' },
  bubbleText: {
    fontSize: 15,
    color: '#fff',
    lineHeight: 21,
  },
  bubbleTextAgent: { color: '#111' },
  bubbleInterim: { opacity: 0.6 },

  // -- Breathing orb (guidance center) ---------------------------------------
  orbScreen: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    paddingHorizontal: 24,
    paddingTop: 32,
    paddingBottom: 80,
  },
  orbStatusLabel: {
    fontSize: 11,
    fontWeight: '700',
    color: '#9ca3af',
    letterSpacing: 3.2,
    textTransform: 'uppercase',
    marginBottom: 64,
    fontVariant: ['tabular-nums'],
  },
  orbHit: {
    width: 280,
    height: 280,
    alignItems: 'center',
    justifyContent: 'center',
  },
  orb: {
    width: 140,
    height: 140,
    borderRadius: 70,
    backgroundColor: '#111',
    alignItems: 'center',
    justifyContent: 'center',
    // Diffusion shadow tinted to the off-white background, no neon glow.
    shadowColor: '#000',
    shadowOpacity: 0.16,
    shadowRadius: 28,
    shadowOffset: { width: 0, height: 14 },
    elevation: 10,
  },
  orbSpeaking: {
    // Slightly lifted black to read as "active" without changing hue family.
    backgroundColor: '#0a0a0a',
  },
  orbMuted: {
    backgroundColor: '#e5e7eb',
    shadowOpacity: 0.04,
    shadowRadius: 12,
  },
  orbHalo: {
    position: 'absolute',
    width: 140,
    height: 140,
    borderRadius: 70,
    borderWidth: 1,
    borderColor: '#111',
  },
  // Diagonal slash through the muted orb. Centered by parent flex (the orb
  // uses alignItems/justifyContent: 'center'); 45deg rotation on the line.
  orbMuteSlash: {
    width: 96,
    height: 3,
    borderRadius: 2,
    backgroundColor: '#525252',
    transform: [{ rotate: '45deg' }],
  },
  orbHint: {
    marginTop: 64,
    fontSize: 13,
    color: '#9ca3af',
    letterSpacing: 0.4,
  },
});
