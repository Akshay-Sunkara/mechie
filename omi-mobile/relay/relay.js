// Relay: phone ↔ OpenAI Realtime gateway. The phone uploads Opus + JPEGs
// over a single WS; the relay decodes audio to 24 kHz PCM, opens an OpenAI
// Realtime WS that handles STT + reasoning + tool calls + native audio
// output, buffers the full PCM reply, and ships it as one self-contained
// 24 kHz WAV per turn for gapless file-based playback on the phone.
//
// Wire protocol:
//   Phone → Relay binary:
//     0x01 [opus frame]            one reassembled Opus frame
//     0x02 [idLen][reqId][jpeg]    a JPEG (response to request_photo, or
//                                  passive cache when reqId empty)
//   Phone → Relay text JSON:
//     {type: "set_mode", mode: "idle"|"chat"|"guidance"}
//   Relay → Phone:
//     text JSON                    Deepgram-shaped Results envelopes (relay
//                                  re-emits the realtime transcript in that
//                                  shape), plus {type:"request_photo", id}
//                                  and {type:"agent_chunk", text}.
//     binary: 0x10 [WAV bytes]     full 24 kHz mono S16LE WAV of model audio
//     binary: 0x20 [JPEG bytes]    Wi-Fi-photo dev mirror
//
// Glasses → Relay HTTP: POST /upload-photo with x-session-token + x-request-id.

import 'dotenv/config';
import { WebSocketServer, WebSocket } from 'ws';
import { OpusDecoder } from 'opus-decoder';
import { Buffer } from 'node:buffer';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import * as fs from 'node:fs';
import * as fsp from 'node:fs/promises';
import * as path from 'node:path';

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || 'gpt-realtime-2';
// Realtime input transcription model. Defaults to gpt-4o-transcribe; flip
// via Fly secret to gpt-4o-transcribe-diarize once your org has access.
const OPENAI_TRANSCRIBE_MODEL =
  process.env.OPENAI_TRANSCRIBE_MODEL || 'gpt-realtime-whisper';

if (!OPENAI_API_KEY) {
  console.warn('[warn] OPENAI_API_KEY missing — realtime connection will fail.');
}
console.log(
  `[realtime] model=${OPENAI_REALTIME_MODEL} transcribe=${OPENAI_TRANSCRIBE_MODEL}`,
);

// Log a fingerprint of the OpenAI key so deploys can verify which key is
// loaded, then probe /v1/models to confirm the configured transcription
// model is in the account's allow-list.
if (OPENAI_API_KEY) {
  const fp =
    OPENAI_API_KEY.slice(0, 7) +
    '…' +
    OPENAI_API_KEY.slice(-4) +
    ` (len=${OPENAI_API_KEY.length})`;
  console.log(`[openai] key fingerprint: ${fp}`);
  (async () => {
    try {
      const res = await fetch('https://api.openai.com/v1/models', {
        headers: { Authorization: `Bearer ${OPENAI_API_KEY}` },
      });
      if (!res.ok) {
        console.warn(
          `[openai] /v1/models probe failed: ${res.status} ${(await res.text()).slice(0, 200)}`,
        );
        return;
      }
      const data = await res.json();
      const ids = (data.data || []).map((m) => m.id);
      const targetOk = ids.includes(OPENAI_TRANSCRIBE_MODEL);
      console.log(
        `[openai] transcribe model "${OPENAI_TRANSCRIBE_MODEL}" ${targetOk ? '✓ available' : '✗ NOT in account models list'
        }`,
      );
    } catch (e) {
      console.warn(`[openai] probe error: ${e.message}`);
    }
  })();
}

const PORT = Number(process.env.PORT || 8765);

// Audio debug recording: when RECORD_AUDIO is truthy, write a 16 kHz mono
// WAV per phone connection to /tmp. Browse via GET /audio (HTML index with
// inline players) or GET /audio/<file> (raw WAV). /tmp is ephemeral on Fly
// so files vanish on restart — meant for debugging audio quality / mic
// gain / background noise per session, not long-term archive.
// Default ON; set RECORD_AUDIO=0 to disable.
const RECORD_AUDIO =
  process.env.RECORD_AUDIO !== '0' && process.env.RECORD_AUDIO !== 'false';
const AUDIO_DIR = '/tmp/mechie-audio';
if (RECORD_AUDIO) {
  try { fs.mkdirSync(AUDIO_DIR, { recursive: true }); } catch { /* ignore */ }
  console.log(`[audio-rec] enabled — writing to ${AUDIO_DIR}`);
}

const PHOTO_CACHE_SIZE = 5;
// Photo request timeout. The timer starts when the request is FIRED, not
// when the realtime path awaits it — speculative pre-capture fires on
// speech_started, often seconds before the model decides it needs the
// photo. First-turn captures can take 8-12s (Wi-Fi associate + camera
// warmup). 20s leaves enough headroom.
const PHOTO_REQUEST_TIMEOUT_MS = 20000;

// Wire-protocol tag bytes
const TAG_PHONE_OPUS = 0x01;
const TAG_PHONE_PHOTO = 0x02;
const TAG_RELAY_TTS = 0x10;        // self-contained WAV of model audio
const TAG_RELAY_WIFI_PHOTO = 0x20; // mirror of Wi-Fi-uploaded JPEG (dev grid)

const GUIDANCE_SYSTEM_PROMPT =
  'You are an interactive hands-on task coach for someone wearing smart glasses with a camera. ' +
  'Your replies are spoken aloud.\n\n' +
  'RESPONSE LENGTH:\n' +
  'Default to 1-2 sentences. You CAN go longer when the situation truly requires it — explaining ' +
  'a multi-step procedure that can\'t reasonably split, defining a complex term the user asked ' +
  'about — but in normal conversation 1-2 sentences is what you do. Don\'t pad. Don\'t recap what ' +
  'the user just said. Don\'t add "let me know if you need anything else" filler. Speak like a ' +
  'concise colleague.\n\n' +
  'VISION — IMPORTANT:\n' +
  'You DO NOT see the user\'s environment by default. You have a `take_picture` tool that ' +
  'captures a fresh photo from their glasses. Call it EAGERLY — err on the side of taking ' +
  'a picture. The latency is hidden by speculative pre-capture, so calling it costs almost nothing.\n\n' +
  'CALL `take_picture` whenever the user:\n' +
  '- Refers to a physical object with "this", "that", "it", "here", "there" ("how do I turn this off", ' +
  '"what is this", "is this right", "where\'s the button on this")\n' +
  '- Asks you to look, see, check, watch, or verify ("look at this", "check my work", "see the screen")\n' +
  '- Asks any question whose answer depends on the physical scene in front of them\n' +
  '- Reports completing a step in a hands-on task (verify before moving on)\n' +
  '- Asks for help with anything physical and you don\'t already have a recent photo\n\n' +
  'DO NOT call `take_picture` for:\n' +
  '- Pure greetings ("hi", "can you hear me", "are you there")\n' +
  '- Abstract / conceptual questions with no physical referent ("what\'s the capital of France")\n' +
  '- Direct follow-ups about something you JUST saw in this turn\n\n' +
  'NEVER tell the user "I can\'t see" or "I don\'t have a camera" — you have one, you just have ' +
  'to call the tool. If you need to see, call `take_picture`. Do not refuse a visual question; ' +
  'fulfill it by capturing a photo.\n\n' +
  'HONEST ABOUT WHAT YOU SEE — DO NOT HALLUCINATE:\n' +
  'After calling `take_picture`, only describe things that are clearly, unambiguously visible in ' +
  'the actual image. NEVER invent objects, animals, people, text, or scenes that aren\'t there.\n' +
  '- If the photo is dark, black, or empty, say "the camera looks covered" or "it\'s too dark to ' +
  'see anything" and ask the user to uncover the lens or move into better light.\n' +
  '- If the photo is blurry or you can only make out vague shapes, say so honestly ("I see ' +
  'something blurry, can you describe what you\'re looking at?").\n' +
  '- If you\'re uncertain what an object is, ASK rather than guess.\n' +
  '- The tool may return a message saying the lens appears covered — when it does, trust that and ' +
  'tell the user; do not try to invent what might be there.\n' +
  'Inventing visual content the user didn\'t actually show you is the worst failure mode of this ' +
  'system. Always prefer "I can\'t tell" over a confident guess.\n\n' +
  'RULES:\n' +
  '- For hands-on tasks (cooking, electronics, repair, assembly, art), walk them through it ONE ' +
  'STEP AT A TIME. Give exactly one step per reply, end with a brief check-in like "Let me know ' +
  'when you\'re done."\n' +
  '- When they say they\'re done with a step, call `take_picture` and VERIFY before moving on. ' +
  'Praise good work; gently correct mistakes.\n' +
  '- If a captured photo is unclear or doesn\'t show what you need, ASK them to adjust ("tilt down ' +
  'a bit", "can you describe what I should be seeing") — never guess.\n' +
  '- For greetings or connection tests, reply briefly and ask what they want help with — do NOT ' +
  'invent a task from a photo.\n' +
  '- For pure casual questions, answer conversationally without forcing a task flow.';

// Soft-diarization addendum. The Realtime API has no native speaker labels;
// near-field noise reduction + a top-tier transcription model + this prompt
// guidance yields equivalent behavior — model only responds to the wearer.
const REALTIME_DIARIZATION_INSTRUCTIONS =
  '\n\nIMPORTANT — speaker awareness:\n' +
  'The user is wearing smart glasses with a near-field microphone. ' +
  'The PRIMARY speaker is the wearer; their voice is the only one you ' +
  'should respond to. If you hear other voices in the background ' +
  '(other people in the room, conversation across the room, audio from ' +
  "a TV/radio, music, or anything that isn't the wearer speaking " +
  'directly to you), IGNORE them completely. Only acknowledge and ' +
  'respond to what the wearer says directly. If a non-wearer voice ' +
  "interrupts mid-utterance, treat it as background noise and don't " +
  'react to it.';

// Wake-word gating: the Realtime API auto-fires response.create on every
// user turn by default, which means the model talks back to *anything* it
// hears. We disable that (create_response: false in turn_detection) and
// manually trigger response.create only when the transcribed turn contains
// "hey mechie" or a close mishearing. Transcription still runs on every
// turn — that's what gives us the trigger signal — but the model only
// responds when summoned.
//
// Mishearings: the transcription model (gpt-4o-transcribe / Whisper) routinely
// renders "mechie" as several other tokens. Accept the common variants so a
// pilot user doesn't get stranded by a single mistranscription.
const WAKE_WORD_VARIANTS = [
  'hey mechie', 'hey mechi', 'hey mechy', 'hey mech',
  'hey machine', 'hey machi', 'hey machy',
  'hey mochi', 'hey mocchi', 'hey michi', 'hey miche',
  'hi mechie', 'hi mechi', 'hi mech',
];
function transcriptInvokesWake(transcript) {
  if (!transcript) return false;
  const normalized = transcript
    .toLowerCase()
    .replace(/[^\w\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return WAKE_WORD_VARIANTS.some((w) => normalized.includes(w));
}

const TAKE_PICTURE_TOOL = {
  type: 'function',
  name: 'take_picture',
  description:
    "Capture a fresh photo from the user's smart glasses camera. " +
    "You have NO visual context by default — call this tool to see anything in the user's environment. " +
    "Call EAGERLY whenever the user references a physical object, scene, or asks any visual/spatial " +
    "question. Examples that REQUIRE this tool: 'how do I turn this off', 'what is this', 'look at this', " +
    "'check my work', 'is this right', 'where's the button', or any use of this/that/it/here/there to " +
    "refer to something physical. Latency is hidden by speculative pre-capture, so calling this is cheap. " +
    "Never tell the user you can't see — call this tool instead.",
  parameters: { type: 'object', properties: {}, required: [] },
};

// Global lookup of active sessions, used by HTTP /upload-photo to find the
// right per-connection state when glasses POST a photo directly over Wi-Fi.
const sessions = new Map(); // sessionToken (string) → state

// Live audio monitor: a parallel WS endpoint /monitor that re-broadcasts
// the exact 24 kHz S16LE PCM bytes we send to OpenAI Realtime. Open
// /monitor in a browser to listen to what the model is "hearing" — useful
// for debugging hallucinations that look like audio quality issues.
const monitorClients = new Set();
function broadcastToMonitors(pcm24k) {
  if (monitorClients.size === 0) return;
  for (const ws of monitorClients) {
    if (ws.readyState !== WebSocket.OPEN) continue;
    try { ws.send(pcm24k); } catch { /* ignore */ }
  }
}

const MONITOR_HTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>mechie audio monitor</title>
<style>
body { font: 14px/1.5 -apple-system, sans-serif; max-width: 640px;
       margin: 2rem auto; padding: 0 1rem; }
button { font-size: 1rem; padding: .5rem 1rem; cursor: pointer; }
#status { margin-top: 1rem; color: #666; font-variant-numeric: tabular-nums; }
.bar { height: 6px; background: #eee; border-radius: 3px; overflow: hidden; margin-top: .5rem; }
.bar > div { height: 100%; background: #06f; width: 0%; transition: width .05s linear; }
</style></head><body>
<h2>mechie audio monitor</h2>
<p>Live stream of the 24 kHz mono PCM the relay forwards to OpenAI Realtime.
   Click <b>Start</b> (browser requires a user gesture) to begin.</p>
<button id="start">Start listening</button>
<div id="status">disconnected</div>
<div class="bar"><div id="level"></div></div>
<script>
(() => {
  const status = document.getElementById('status');
  const level = document.getElementById('level');
  const btn = document.getElementById('start');
  let ctx, ws, nextStart = 0, totalSamples = 0, peakRecent = 0;
  btn.onclick = async () => {
    if (ctx) return;
    ctx = new AudioContext();
    if (ctx.state === 'suspended') await ctx.resume();
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(proto + '//' + location.host + '/monitor');
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => { status.textContent = 'connected • waiting for audio…'; btn.disabled = true; };
    ws.onclose = () => { status.textContent = 'disconnected'; btn.disabled = false; };
    ws.onerror = () => { status.textContent = 'error'; };
    ws.onmessage = (e) => {
      const i16 = new Int16Array(e.data);
      const f32 = new Float32Array(i16.length);
      let peak = 0;
      for (let i = 0; i < i16.length; i++) {
        const v = i16[i] / 32768;
        f32[i] = v;
        const a = v < 0 ? -v : v;
        if (a > peak) peak = a;
      }
      // Buffer at native 24 kHz; Web Audio will resample to ctx rate.
      const buf = ctx.createBuffer(1, f32.length, 24000);
      buf.copyToChannel(f32, 0);
      const src = ctx.createBufferSource();
      src.buffer = buf;
      src.connect(ctx.destination);
      const startAt = Math.max(nextStart, ctx.currentTime + 0.05);
      src.start(startAt);
      nextStart = startAt + buf.duration;
      totalSamples += i16.length;
      peakRecent = Math.max(peakRecent * 0.85, peak);
      level.style.width = (peakRecent * 100).toFixed(1) + '%';
      status.textContent = 'streaming • ' +
        (totalSamples / 24000).toFixed(1) + 's received • peak ' +
        (20 * Math.log10(Math.max(peakRecent, 1e-4))).toFixed(0) + ' dBFS';
    };
  };
})();
</script></body></html>`;

// =====================================================================
// WAV writer (debug audio capture). 24kHz mono signed-16 little-endian.
// =====================================================================
function buildWavHeader(dataBytes) {
  // RIFF header — sizes can be patched later; we only call this with the
  // correct dataBytes once we're closing the file.
  const h = Buffer.alloc(44);
  h.write('RIFF', 0);
  h.writeUInt32LE(36 + dataBytes, 4);   // file size - 8
  h.write('WAVE', 8);
  h.write('fmt ', 12);
  h.writeUInt32LE(16, 16);              // fmt chunk size
  h.writeUInt16LE(1, 20);               // PCM format
  h.writeUInt16LE(1, 22);               // channels
  h.writeUInt32LE(24000, 24);           // sample rate
  h.writeUInt32LE(24000 * 2, 28);       // byte rate
  h.writeUInt16LE(2, 32);               // block align (2 bytes per sample)
  h.writeUInt16LE(16, 34);              // bits per sample
  h.write('data', 36);
  h.writeUInt32LE(dataBytes, 40);       // data chunk size
  return h;
}

async function startAudioRecording(state) {
  if (!RECORD_AUDIO || state.audioRec) return;
  const fileName = `mechie-${new Date().toISOString().replace(/[:.]/g, '-')}-${state.sessionToken.slice(0, 8)}.wav`;
  const filePath = path.join(AUDIO_DIR, fileName);
  try {
    const fd = await fsp.open(filePath, 'w');
    // Write placeholder header — patched on close.
    await fd.write(buildWavHeader(0));
    state.audioRec = { fd, filePath, fileName, dataBytes: 0 };
    console.log(`  [audio-rec] writing ${fileName}`);
  } catch (e) {
    console.warn('  [audio-rec] open failed:', e.message);
  }
}

async function appendAudioRecording(state, pcmBuffer) {
  const rec = state.audioRec;
  if (!rec) return;
  try {
    await rec.fd.write(pcmBuffer);
    rec.dataBytes += pcmBuffer.length;
  } catch (e) {
    console.warn('  [audio-rec] write failed:', e.message);
  }
}

async function closeAudioRecording(state) {
  const rec = state.audioRec;
  if (!rec) return;
  state.audioRec = null;
  try {
    // Patch the header in place with the actual sizes.
    const header = buildWavHeader(rec.dataBytes);
    await rec.fd.write(header, 0, header.length, 0);
    await rec.fd.close();
    console.log(`  [audio-rec] closed ${rec.fileName} (${rec.dataBytes}B PCM, ${(rec.dataBytes / 48000).toFixed(1)}s)`);
  } catch (e) {
    console.warn('  [audio-rec] close failed:', e.message);
  }
}

async function handleAudioList(req, res) {
  try {
    const files = await fsp.readdir(AUDIO_DIR).catch(() => []);
    const wavs = files.filter((f) => f.endsWith('.wav')).sort().reverse();
    const stats = await Promise.all(
      wavs.map(async (f) => {
        try {
          const s = await fsp.stat(path.join(AUDIO_DIR, f));
          return { f, size: s.size, mtime: s.mtimeMs };
        } catch { return null; }
      }),
    );
    const fmtTime = (ms) => new Date(ms).toLocaleString();
    const rows = stats
      .filter(Boolean)
      .map((s) => {
        const sizeKb = (s.size / 1024).toFixed(1);
        const dur = ((s.size - 44) / 48000).toFixed(1);
        const url = `/audio/${encodeURIComponent(s.f)}`;
        return `<li>
  <div class="meta"><b>${s.f}</b> — ${fmtTime(s.mtime)} • ${sizeKb} KB • ~${dur}s</div>
  <audio controls preload="none" src="${url}"></audio>
  <a class="dl" href="${url}" download>download</a>
</li>`;
      })
      .join('\n');
    const html = `<!doctype html><html><head>
<meta charset="utf-8">
<title>mechie audio</title>
<meta http-equiv="refresh" content="30">
<style>
  body { font: 14px/1.5 -apple-system, sans-serif; max-width: 720px;
         margin: 2rem auto; padding: 0 1rem; }
  h2 { margin-bottom: .25rem; }
  .sub { color: #666; margin-top: 0; font-size: 13px; }
  ul { list-style: none; padding: 0; }
  li { border-top: 1px solid #eee; padding: .85rem 0; }
  .meta { margin-bottom: .35rem; }
  audio { display: block; width: 100%; }
  .dl { display: inline-block; margin-top: .35rem; color: #06f;
        font-size: 12px; }
  .empty { color: #888; font-style: italic; }
  a.monitor { float: right; font-size: 13px; }
</style></head><body>
<a class="monitor" href="/monitor">live monitor →</a>
<h2>mechie audio captures</h2>
<p class="sub">One WAV per phone session, 24 kHz mono. Auto-refresh every 30s.
Newest first. Files live in /tmp on Fly — they reset on every relay deploy.</p>
${rows ? `<ul>${rows}</ul>` : '<p class="empty">No recordings yet — connect mechie and speak.</p>'}
</body></html>`;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
  } catch (e) {
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('list failed: ' + e.message);
  }
}

async function handleAudioGet(req, res, fileName) {
  // Reject path traversal attempts.
  if (!/^[A-Za-z0-9._-]+\.wav$/.test(fileName)) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('bad filename');
    return;
  }
  const filePath = path.join(AUDIO_DIR, fileName);
  try {
    const stat = await fsp.stat(filePath);
    res.writeHead(200, {
      'Content-Type': 'audio/wav',
      'Content-Length': stat.size,
      'Content-Disposition': `inline; filename="${fileName}"`,
    });
    fs.createReadStream(filePath).pipe(res);
  } catch (e) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('not found');
  }
}

const httpServer = createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/audio') {
    return handleAudioList(req, res);
  }
  if (req.method === 'GET' && req.url?.startsWith('/audio/')) {
    const fname = decodeURIComponent(req.url.slice('/audio/'.length).split('?')[0]);
    if (fname === 'list') return handleAudioList(req, res);
    return handleAudioGet(req, res, fname);
  }
  if (req.method === 'POST' && (req.url === '/upload-photo' || req.url?.startsWith('/upload-photo?'))) {
    // mechie can drop the upload mid-stream when its Wi-Fi flickers, which
    // makes the `for await` inside handleUploadPhoto reject with ECONNRESET.
    // Catch here so the rejection doesn't bubble out of the createServer
    // callback as an uncaught promise — Node 22 would kill the process.
    handleUploadPhoto(req, res).catch((e) => {
      console.warn('  [upload-photo] handler error:', e?.message || e);
      try {
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'text/plain' });
          res.end('upload aborted');
        } else {
          res.end();
        }
      } catch { }
    });
    return;
  }
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }
  if (req.method === 'GET' && req.url === '/monitor') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(MONITOR_HTML);
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

// Last-line defense: log unhandled rejections instead of crashing the
// process. Node 22 defaults to terminating on unhandledRejection.
process.on('unhandledRejection', (reason) => {
  const msg = reason && reason.message ? reason.message : String(reason);
  console.warn('[unhandledRejection]', msg);
});
process.on('uncaughtException', (err) => {
  console.warn('[uncaughtException]', err?.message || err);
});

const wss = new WebSocketServer({ server: httpServer });
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`Relay listening on http://0.0.0.0:${PORT} (ws + POST /upload-photo)`);
});

wss.on('connection', async (phoneWs, req) => {
  const peer = req.socket.remoteAddress;
  // Live audio monitor clients connect to /monitor — they receive
  // re-broadcast PCM and never enter the per-session phone state machine.
  if (req.url === '/monitor') {
    monitorClients.add(phoneWs);
    console.log(`[monitor] client connected from ${peer} (total=${monitorClients.size})`);
    phoneWs.on('close', () => {
      monitorClients.delete(phoneWs);
      console.log(`[monitor] client disconnected (total=${monitorClients.size})`);
    });
    phoneWs.on('error', () => monitorClients.delete(phoneWs));
    return;
  }
  console.log(`[+] phone connected from ${peer}`);

  // Decode at 24 kHz (OpenAI's input rate). Avoids the libopus-internal
  // anti-aliasing decimation that decoding at 16 kHz forced — preserving
  // 6-10 kHz consonant energy that the transcriber needs to disambiguate
  // sibilants and stop releases.
  const decoder = new OpusDecoder({ sampleRate: 24000, channels: 1 });
  await decoder.ready;

  // Per-connection state (closed-over, passed by reference into handlers)
  const sessionToken = randomUUID();
  const state = {
    phoneWs,
    sessionToken,
    mode: 'idle', // 'idle' | 'chat' | 'guidance'
    photoCache: [], // {buffer, ts}[] — passive cache for unsolicited photos
    // requestId → { resolve, reject, timer }. Only the photo whose ID matches
    // a pending entry is allowed to fulfill it. Stale photos with mismatched/
    // empty IDs go to the cache.
    pendingPhotoRequests: new Map(),
    // Speculative pre-capture: when the realtime WS emits speech_started in
    // guidance mode, fire a photo request immediately so it overlaps with
    // the rest of the user's speech. handleRealtimeToolCall consumes the
    // promise instead of issuing a fresh request, often resolving instantly.
    // { promise, startedAt } | null
    speculativePhoto: null,
    llmInFlight: false,
    counters: {
      opusFrames: 0,
      pcmSamples: 0,
      bytesToOpenAI: 0,
      photosReceived: 0,
      blePhotos: 0,
      wifiPhotos: 0,
      // Phone reports BLE packet gaps via {type:'audio_loss', gap}. audioLossFrames
      // sums the gaps (≈ Opus frames lost since 1 frame ≈ 1 BLE packet at our
      // bitrate). audioLossEvents counts how many times the phone hit the loss
      // branch. Loss rate displayed in the 5s stats line.
      audioLossFrames: 0,
      audioLossEvents: 0,
    },
    // Realtime: streaming function-call argument buffer + ready flag.
    toolCallBuffer: {},
    realtimeReady: false,
  };

  sessions.set(sessionToken, state);

  // Tell the phone its session token + upload URL so it can hand them to
  // the glasses (phone provisions both over BLE; glasses use them for the
  // direct Wi-Fi POST).
  const uploadUrl = process.env.PUBLIC_BASE_URL
    ? `${process.env.PUBLIC_BASE_URL.replace(/\/$/, '')}/upload-photo`
    : null;
  phoneWs.send(JSON.stringify({ type: 'session', token: sessionToken, uploadUrl }));

  setupRealtimeConnection(state);

  phoneWs.on('message', (msg, isBinary) => {
    if (isBinary) return handleBinaryFromPhone(state, msg, decoder);
    return handleTextFromPhone(state, msg);
  });

  const stats = setInterval(() => {
    const c = state.counters;
    if (c.opusFrames === 0 && c.photosReceived === 0) return;
    const totalFrames = c.opusFrames + c.audioLossFrames;
    const lossRate = totalFrames > 0 ? (c.audioLossFrames / totalFrames) * 100 : 0;
    console.log(
      `  stats: mode=${state.mode} opus=${c.opusFrames} pcm=${c.pcmSamples} ` +
      `oai=${c.bytesToOpenAI}B audio_lost=${c.audioLossFrames}f/${c.audioLossEvents}ev (${lossRate.toFixed(1)}%) ` +
      `ble_photos=${c.blePhotos} wifi_photos=${c.wifiPhotos} cached=${state.photoCache.length}`,
    );
  }, 5000);

  phoneWs.on('close', () => {
    clearInterval(stats);
    const c = state.counters;
    const totalFrames = c.opusFrames + c.audioLossFrames;
    const lossRate = totalFrames > 0 ? (c.audioLossFrames / totalFrames) * 100 : 0;
    console.log(
      `[-] phone disconnected (${peer}). final: opus=${c.opusFrames} ` +
      `audio_lost=${c.audioLossFrames}f/${c.audioLossEvents}ev (${lossRate.toFixed(1)}%) ` +
      `ble_photos=${c.blePhotos} wifi_photos=${c.wifiPhotos}`,
    );
    sessions.delete(sessionToken);
    if (
      state.realtimeWs &&
      (state.realtimeWs.readyState === WebSocket.OPEN ||
        state.realtimeWs.readyState === WebSocket.CONNECTING)
    ) {
      try { state.realtimeWs.close(); } catch { }
    }
    try { decoder.free(); } catch { }
    // Finalize any debug audio recording for this session.
    if (state.audioRec) closeAudioRecording(state);
    // Reject any outstanding photo requests so awaiting promises unblock
    for (const [id, req] of state.pendingPhotoRequests.entries()) {
      try { clearTimeout(req.timer); } catch { }
      try { req.reject(new Error('phone disconnected')); } catch { }
    }
    state.pendingPhotoRequests.clear();
  });

  phoneWs.on('error', (e) => console.warn('  phone ws error:', e.message));
});

// -----------------------------------------------------------------------------
// Phone → Relay handlers
// -----------------------------------------------------------------------------

function handleTextFromPhone(state, msg) {
  let parsed;
  try { parsed = JSON.parse(msg.toString()); } catch { return; }
  if (parsed.type === 'audio_loss') {
    const gap = Number(parsed.gap) || 1;
    state.counters.audioLossFrames += gap;
    state.counters.audioLossEvents += 1;
    return;
  }
  if (parsed.type === 'set_mode') {
    const newMode = parsed.mode;
    if (!['idle', 'chat', 'guidance'].includes(newMode)) return;
    state.mode = newMode;
    state.photoCache = [];
    state.speculativePhoto = null;
    // Cancel outstanding photo requests so a stale photo from before the
    // mode switch can't accidentally resolve them under the new mode.
    for (const [id, req] of state.pendingPhotoRequests.entries()) {
      try { clearTimeout(req.timer); } catch { }
      try { req.reject(new Error('mode switched')); } catch { }
    }
    state.pendingPhotoRequests.clear();
    console.log(`  mode → ${newMode}`);
  }
}

function handleBinaryFromPhone(state, msg, decoder) {
  if (msg.length < 1) return;
  const tag = msg[0];

  if (tag === TAG_PHONE_OPUS) {
    forwardOpusToRealtime(state, msg.subarray(1), decoder);
    return;
  } else if (tag === TAG_PHONE_PHOTO) {
    // Wire: [TAG][idLen byte][idLen bytes UTF-8 requestId][JPEG bytes]
    if (msg.length < 2) return;
    const idLen = msg[1];
    if (msg.length < 2 + idLen) return;
    const requestId = idLen > 0 ? msg.subarray(2, 2 + idLen).toString('utf8') : '';
    const jpeg = Buffer.from(msg.subarray(2 + idLen));
    state.counters.photosReceived++;

    if (requestId) {
      const req = state.pendingPhotoRequests.get(requestId);
      if (req) {
        try { clearTimeout(req.timer); } catch { }
        state.pendingPhotoRequests.delete(requestId);
        state.counters.blePhotos++;
        req.resolve(jpeg);
        console.log(`  [ble] photo received id=${requestId.slice(0, 8)}… ${jpeg.length}B`);
      } else {
        console.log(`  [ble] stale photo discarded id=${requestId.slice(0, 8)}… ${jpeg.length}B`);
      }
    } else {
      // Untagged photo → chat-mode passive cache
      state.photoCache.push({ buffer: jpeg, ts: Date.now() });
      if (state.photoCache.length > PHOTO_CACHE_SIZE) state.photoCache.shift();
    }
  } else {
    console.warn('  unknown tag:', tag);
  }
}

// -----------------------------------------------------------------------------
// Phone-bound helpers
// -----------------------------------------------------------------------------

// Per-token text delta. Phone's agent_chunk handler appends to the running
// mechie bubble (or starts one if there's no agent bubble yet).
function sendAgentChunk(state, text) {
  if (!text) return;
  if (state.phoneWs.readyState !== WebSocket.OPEN) return;
  state.phoneWs.send(JSON.stringify({ type: 'agent_chunk', text }));
}


// -----------------------------------------------------------------------------
// Photo request: tell phone to take a fresh single-shot, await the next photo binary
// -----------------------------------------------------------------------------

function requestPhoto(state, timeoutMs) {
  return new Promise((resolve, reject) => {
    if (state.phoneWs.readyState !== WebSocket.OPEN) {
      return reject(new Error('phone WS not open'));
    }
    const id = randomUUID();
    const timer = setTimeout(() => {
      if (state.pendingPhotoRequests.has(id)) {
        state.pendingPhotoRequests.delete(id);
        reject(new Error(`photo request timed out after ${timeoutMs}ms (id=${id.slice(0, 8)}…)`));
      }
    }, timeoutMs);
    state.pendingPhotoRequests.set(id, { resolve, reject, timer });
    state.phoneWs.send(JSON.stringify({ type: 'request_photo', id }));
  });
}

// Fire an early photo request that overlaps with the user's speech.
// handleRealtimeToolCall awaits this promise instead of issuing a fresh
// request, so the photo is often already in hand when the model decides
// it needs one.
function fireSpeculativePhoto(state) {
  if (state.phoneWs.readyState !== WebSocket.OPEN) return;
  const startedAt = Date.now();
  const promise = requestPhoto(state, PHOTO_REQUEST_TIMEOUT_MS);
  const entry = { promise, startedAt };
  state.speculativePhoto = entry;
  console.log('  [spec-photo] fired on first interim');
  promise
    .then((photo) => {
      if (state.speculativePhoto === entry) {
        console.log(
          `  [spec-photo] arrived ${photo.length}B in ${Date.now() - startedAt}ms`,
        );
      } // else it was already consumed by the tool-call handler.
    })
    .catch((e) => {
      if (state.speculativePhoto === entry) {
        console.warn(
          `  [spec-photo] failed in ${Date.now() - startedAt}ms: ${e.message}`,
        );
        state.speculativePhoto = null;
      }
    });
}


// -----------------------------------------------------------------------------
// HTTP /upload-photo — glasses POST a JPEG directly via Wi-Fi
// -----------------------------------------------------------------------------
//
// Headers:
//   x-session-token: <token>   — session ID (same as ws "session" message)
//   x-request-id:    <uuid>    — matches a pendingPhotoRequest entry on the state
// Body: raw JPEG bytes (Content-Type: image/jpeg). Keep wire format dead simple — no
// multipart parsing required on the firmware side.

async function handleUploadPhoto(req, res) {
  const sessionToken = (req.headers['x-session-token'] || '').toString();
  const requestId = (req.headers['x-request-id'] || '').toString();

  if (!sessionToken || !requestId) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('missing x-session-token or x-request-id');
    return;
  }
  const state = sessions.get(sessionToken);
  if (!state) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('session not found');
    return;
  }

  // Accumulate request body into a single Buffer.
  const chunks = [];
  let total = 0;
  const MAX_BYTES = 1_000_000; // 1MB cap for safety
  let aborted = false;
  for await (const chunk of req) {
    total += chunk.length;
    if (total > MAX_BYTES) {
      aborted = true;
      res.writeHead(413, { 'Content-Type': 'text/plain' });
      res.end('payload too large');
      return;
    }
    chunks.push(chunk);
  }
  if (aborted) return;

  const jpeg = Buffer.concat(chunks, total);
  state.counters.photosReceived++;
  state.counters.wifiPhotos++;

  // Always forward to the phone for dev visibility — without this, Wi-Fi photos never
  // touch the phone (they go glasses → relay direct), so there's no way to verify the
  // upload visually. Tagged so the phone can route it to the dev-grid renderer.
  if (state.phoneWs.readyState === WebSocket.OPEN) {
    const tagged = Buffer.concat([Buffer.from([TAG_RELAY_WIFI_PHOTO]), jpeg]);
    state.phoneWs.send(tagged);
  }

  const reqEntry = state.pendingPhotoRequests.get(requestId);
  if (reqEntry) {
    try { clearTimeout(reqEntry.timer); } catch { }
    state.pendingPhotoRequests.delete(requestId);
    reqEntry.resolve(jpeg);
    console.log(`  [wifi] photo received session=${sessionToken.slice(0, 8)}… id=${requestId.slice(0, 8)}… ${jpeg.length}B`);
    res.writeHead(204);
    res.end();
  } else {
    // No pending request matched — could be a chat-mode/dev stream photo. Cache it for
    // chat-mode context and acknowledge anyway. Photo is still displayed via the WS
    // forward above.
    state.photoCache.push({ buffer: jpeg, ts: Date.now() });
    if (state.photoCache.length > PHOTO_CACHE_SIZE) state.photoCache.shift();
    console.log(`  [wifi] photo cached (no pending req) session=${sessionToken.slice(0, 8)}… id=${requestId.slice(0, 8)}… ${jpeg.length}B`);
    res.writeHead(202);
    res.end();
  }
}


// =====================================================================
// OpenAI Realtime mode — single WS handles STT + reasoning + tools + vision.
// =====================================================================

const REALTIME_URL = 'wss://api.openai.com/v1/realtime';

// Wrap accumulated 16-bit LE PCM chunks in a 44-byte WAV header so the
// phone can play it directly via expo-av's file-based path.
function buildWavBuffer(pcmChunks, sampleRate) {
  let dataLen = 0;
  for (const c of pcmChunks) dataLen += c.length;
  const wav = Buffer.alloc(44 + dataLen);
  wav.write('RIFF', 0);
  wav.writeUInt32LE(36 + dataLen, 4);
  wav.write('WAVE', 8);
  wav.write('fmt ', 12);
  wav.writeUInt32LE(16, 16);             // fmt chunk size (PCM)
  wav.writeUInt16LE(1, 20);              // PCM format
  wav.writeUInt16LE(1, 22);              // mono
  wav.writeUInt32LE(sampleRate, 24);
  wav.writeUInt32LE(sampleRate * 2, 28); // byte rate
  wav.writeUInt16LE(2, 32);              // block align
  wav.writeUInt16LE(16, 34);             // bits per sample
  wav.write('data', 36);
  wav.writeUInt32LE(dataLen, 40);
  let o = 44;
  for (const c of pcmChunks) { c.copy(wav, o); o += c.length; }
  return wav;
}

function flushTurnWav(state) {
  if (state.phoneWs.readyState !== WebSocket.OPEN) return;
  const chunks = state.audioOutPcm || [];
  state.audioOutPcm = [];
  state.audioOutPcmBytes = 0;
  if (chunks.length === 0) return;
  const wav = buildWavBuffer(chunks, 24000);
  const frame = Buffer.alloc(1 + wav.length);
  frame.writeUInt8(TAG_RELAY_TTS, 0);
  wav.copy(frame, 1);
  state.phoneWs.send(frame);
}

function setupRealtimeConnection(state) {
  const url = `${REALTIME_URL}?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`;
  // GA Realtime API: drop the `OpenAI-Beta: realtime=v1` header. That header
  // forces the legacy beta protocol which doesn't accept gpt-realtime-2.
  const ws = new WebSocket(url, {
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
    },
  });
  state.realtimeWs = ws;

  ws.on('open', () => {
    console.log('  [realtime] WS open');
    // GA session config — nested audio.input config, output_modalities
    // replaces the flat `modalities` field, and most legacy top-level
    // fields (input_audio_format, input_audio_transcription, etc.) move
    // under audio.input. The audio_transcript.* events run alongside the
    // audio stream and populate the "mechie" text bubble in parallel.
    ws.send(
      JSON.stringify({
        type: 'session.update',
        session: {
          type: 'realtime',
          model: OPENAI_REALTIME_MODEL,
          instructions: GUIDANCE_SYSTEM_PROMPT + REALTIME_DIARIZATION_INSTRUCTIONS,
          output_modalities: ['audio'],
          tools: [TAKE_PICTURE_TOOL],
          tool_choice: 'auto',
          max_output_tokens: 500,
          // Higher reasoning effort: model thinks harder before replying.
          // Trades a small amount of latency for better intent inference
          // (especially for ambiguous visual / multi-step requests).
          reasoning: { effort: 'high' },
          audio: {
            input: {
              format: { type: 'audio/pcm', rate: 24000 },
              transcription: {
                model: OPENAI_TRANSCRIBE_MODEL,
                language: 'en',
              },
              noise_reduction: { type: 'near_field' },
              // Semantic (dynamic) VAD: a small model decides end-of-turn from
              // *what* was said, not just energy/silence. Tolerates natural mid-
              // sentence pauses (especially good for hands-on tasks where the
              // user thinks while working). `auto` ≈ medium eagerness — switch
              // to 'low' if the model interrupts too eagerly, 'high' if it lags.
              //
              // create_response: false → suppress the API's auto-response on
              // every turn. We manually trigger response.create only when the
              // transcript contains the "hey mechie" wake phrase (see the
              // input_audio_transcription.completed handler).
              //
              // interrupt_response stays true so the user can still talk over
              // an in-progress reply without re-saying the wake word.
              turn_detection: {
                type: 'semantic_vad',
                eagerness: 'auto',
                create_response: false,
                interrupt_response: true,
              },
            },
            output: {
              voice: process.env.OPENAI_VOICE || 'alloy',
              format: { type: 'audio/pcm', rate: 24000 },
            },
          },
        },
      }),
    );
  });

  ws.on('message', (data) => {
    handleRealtimeEvent(state, data);
  });

  ws.on('error', (e) => console.error('  [realtime] error:', e.message));
  ws.on('close', (code, reason) => {
    console.log('  [realtime] WS closed', code, reason?.toString() || '');
    state.realtimeReady = false;
  });
}

function handleRealtimeEvent(state, raw) {
  let msg;
  try { msg = JSON.parse(raw.toString()); } catch { return; }

  // Diagnostic: log every event type once per response. Helps catch cases
  // where OpenAI emits events under names we don't have handlers for.
  if (
    msg.type &&
    !msg.type.includes('.delta') &&
    msg.type !== 'rate_limits.updated'
  ) {
    console.log(`  [realtime-evt] ${msg.type}`);
  }

  switch (msg.type) {
    case 'session.created':
    case 'session.updated':
      state.realtimeReady = true;
      console.log(`  [realtime] ${msg.type}`);
      break;

    case 'input_audio_buffer.speech_started':
      // Every new utterance gets a FRESH photo. If a previous turn left an
      // unconsumed speculative in the slot (model decided no tool call was
      // needed and we never cleared it), discard it now — using a stale
      // photo on the next visual question is worse than nothing.
      if (state.mode === 'guidance' && !state.llmInFlight) {
        if (state.speculativePhoto) {
          console.log('  [spec-photo] discarding stale (previous turn unused)');
          state.speculativePhoto = null;
        }
        fireSpeculativePhoto(state);
      }
      break;

    case 'input_audio_buffer.speech_stopped':
      // Server VAD detected end of utterance. Response auto-fires next.
      break;

    case 'conversation.item.input_audio_transcription.completed': {
      // Forward to phone as Deepgram-style Results envelope so the
      // existing user-bubble code path lights up unchanged.
      const transcript = msg.transcript || '';
      if (transcript && state.phoneWs.readyState === WebSocket.OPEN) {
        state.phoneWs.send(
          JSON.stringify({
            type: 'Results',
            channel: { alternatives: [{ transcript }] },
            is_final: true,
          }),
        );
      }
      if (transcript) {
        const wake = transcriptInvokesWake(transcript);
        console.log(`  [realtime] user said: "${transcript}" wake=${wake}`);
        if (wake && state.realtimeWs && state.realtimeWs.readyState === WebSocket.OPEN) {
          // Wake phrase matched — manually fire the response. The model sees
          // the full transcript including the wake phrase; that's fine, it's
          // good at filtering "hey mechie, ..." down to the actual request.
          state.realtimeWs.send(JSON.stringify({ type: 'response.create' }));
        }
      } else {
        // VAD committed a buffer but the transcriber found no speech.
        // Usually noise (cough, click, bump) — non-fatal, just logged.
        console.log('  [realtime] empty transcript (vad over-triggered or noise)');
      }
      break;
    }

    case 'conversation.item.input_audio_transcription.failed': {
      const err = msg.error || {};
      console.warn(
        `  [realtime] transcription failed: ${err.code || '?'} ${err.message || JSON.stringify(err).slice(0, 200)}`,
      );
      break;
    }

    case 'response.created':
      state.llmInFlight = true;
      state.responseStartedAt = Date.now();
      state.audioOutPcm = [];
      state.audioOutPcmBytes = 0;
      break;

    // Model emits 24 kHz signed 16-bit LE PCM. Accumulate the whole turn
    // and ship as one self-contained 24 kHz mono WAV at audio.done. One
    // file per turn = no chunk boundaries = guaranteed gapless within a
    // turn. Trade is TTFA = full audio-generation duration (~1.5-2.5s
    // for typical replies, since gpt-realtime generates faster than
    // realtime). GA renamed response.audio.* → response.output_audio.*.
    case 'response.output_audio.delta':
    case 'response.audio.delta': {
      const audioB64 = msg.delta;
      if (!audioB64) break;
      if (!state.audioOutPcm) state.audioOutPcm = [];
      const buf = Buffer.from(audioB64, 'base64');
      state.audioOutPcm.push(buf);
      state.audioOutPcmBytes = (state.audioOutPcmBytes || 0) + buf.length;
      break;
    }

    case 'response.output_audio.done':
    case 'response.audio.done': {
      const bytes = state.audioOutPcmBytes || 0;
      flushTurnWav(state);
      const elapsedMs = state.responseStartedAt
        ? Date.now() - state.responseStartedAt
        : -1;
      console.log(
        `  [audio] turn done bytes=${bytes} elapsed_ms=${elapsedMs}`,
      );
      break;
    }

    // Audio transcript runs alongside the audio stream — populates the
    // mechie text bubble on the phone via the existing agent_chunk path.
    // GA renamed these to response.output_audio_transcript.*
    case 'response.output_audio_transcript.delta':
    case 'response.audio_transcript.delta': {
      const tok = msg.delta || '';
      if (tok) sendAgentChunk(state, tok);
      break;
    }

    case 'response.output_audio_transcript.done':
    case 'response.audio_transcript.done':
      // Nothing to do — bubble was built incrementally via the deltas.
      break;

    // GA renamed text events from response.text.* → response.output_text.*.
    // Handle both for safety. When the model produces text-only output (no
    // audio), still pipe deltas to the phone bubble — audio output is the
    // norm but the model can fall back to text on rare configurations.
    case 'response.text.delta':
    case 'response.output_text.delta': {
      const tok = msg.delta || '';
      if (tok) sendAgentChunk(state, tok);
      break;
    }

    case 'response.text.done':
    case 'response.output_text.done':
      // Bubble already built via deltas; native audio is the response itself.
      break;

    case 'response.function_call_arguments.delta': {
      // Accumulate streamed tool-call arguments by call_id.
      const id = msg.call_id;
      if (!id) break;
      if (!state.toolCallBuffer[id]) state.toolCallBuffer[id] = { name: 'take_picture', args: '' };
      state.toolCallBuffer[id].args += msg.delta || '';
      break;
    }

    case 'response.function_call_arguments.done': {
      const id = msg.call_id;
      const argsStr = msg.arguments || (state.toolCallBuffer[id] && state.toolCallBuffer[id].args) || '{}';
      delete state.toolCallBuffer[id];
      handleRealtimeToolCall(state, id, msg.name || 'take_picture', argsStr);
      break;
    }

    case 'response.done':
      state.llmInFlight = false;
      // Turn is fully complete. If a speculative photo is still sitting in
      // the slot, the model didn't need it — discard so the next utterance
      // starts clean. (speech_started also clears, this is defensive.)
      if (state.speculativePhoto) {
        console.log('  [spec-photo] discarding unused (response.done)');
        state.speculativePhoto = null;
      }
      // Surface response status for debugging (errors, content_filter, etc).
      if (msg.response?.status && msg.response.status !== 'completed') {
        console.warn(
          `  [realtime] response status=${msg.response.status}` +
          (msg.response.status_details
            ? ` details=${JSON.stringify(msg.response.status_details).slice(0, 200)}`
            : ''),
        );
      }
      break;

    case 'rate_limits.updated':
      // Ignore for now — could log if hitting limits.
      break;

    case 'error':
      console.warn('  [realtime] API error:', JSON.stringify(msg.error || msg).slice(0, 300));
      break;

    default:
      // Many other event types (response.output_item.added, content_part.*,
      // conversation.item.created, etc.) — none require action here.
      break;
  }
}

async function handleRealtimeToolCall(state, callId, name, _argsStr) {
  if (name !== 'take_picture') {
    console.warn('  [realtime] unknown tool:', name);
    return;
  }

  // Silent capture — no interim text or audio. The model's next response
  // (after we hand back the photo) is the only thing the user hears.

  // Fetch photo: prefer the speculative if it's still alive.
  let photo = null;
  const tp = Date.now();
  let photoPromise;
  if (state.speculativePhoto) {
    photoPromise = state.speculativePhoto.promise;
    state.speculativePhoto = null;
  } else {
    photoPromise = requestPhoto(state, PHOTO_REQUEST_TIMEOUT_MS);
  }
  try {
    photo = await photoPromise;
    console.log(`  [realtime] photo ${photo.length}B in ${Date.now() - tp}ms`);
  } catch (e) {
    console.warn(`  [realtime] photo fetch failed in ${Date.now() - tp}ms: ${e.message}`);
  }

  // Send tool output + (if photo) image as a follow-up user message,
  // then trigger the next response.
  const ws = state.realtimeWs;
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    console.warn('  [realtime] WS dead before tool response');
    return;
  }

  // Heuristic: a covered or pitch-black lens compresses to a tiny JPEG.
  // Normal scenes from this camera land at 10-50KB; under ~5KB is almost
  // always a covered lens or completely dark room. Annotate the tool output
  // so the model is told explicitly rather than left to invent content.
  const COVERED_LENS_BYTES = 5000;
  const photoLooksCovered = photo && photo.length < COVERED_LENS_BYTES;
  if (photoLooksCovered) {
    console.log(`  [realtime] photo looks covered/dark (${photo.length}B < ${COVERED_LENS_BYTES})`);
  }

  ws.send(
    JSON.stringify({
      type: 'conversation.item.create',
      item: {
        type: 'function_call_output',
        call_id: callId,
        output: photo
          ? (photoLooksCovered
            ? 'Photo captured but the image is nearly black or empty (only ' + photo.length + ' bytes — typical of a covered lens or pitch-dark room). Tell the user honestly that the camera appears covered or it is too dark to see anything. DO NOT invent content. Ask them to uncover the camera or move into better light.'
            : 'Photo captured. The image is attached in the next user message.')
          : "Photo capture failed; the user's camera isn't responding.",
      },
    }),
  );

  // Only attach the image when it actually contains something. Sending a
  // near-black covered-lens image lets the model "see" pixels and invent
  // content (gpt-realtime-2 has hallucinated objects from dark frames).
  // The function_call_output above already told it the lens is covered.
  if (photo && !photoLooksCovered) {
    ws.send(
      JSON.stringify({
        type: 'conversation.item.create',
        item: {
          type: 'message',
          role: 'user',
          content: [
            {
              type: 'input_image',
              image_url: `data:image/jpeg;base64,${photo.toString('base64')}`,
            },
          ],
        },
      }),
    );
  }

  ws.send(JSON.stringify({ type: 'response.create' }));
}

// Forward Opus from phone → realtime WS as base64 24 kHz PCM. Decoder is
// configured to render at 24 kHz natively (see setup), so libopus does
// the proper polyphase resampling internally and the speech band passes
// through cleanly — no JS-side resample, no aliasing.
function forwardOpusToRealtime(state, opus, decoder) {
  const ws = state.realtimeWs;
  if (!ws || ws.readyState !== WebSocket.OPEN || !state.realtimeReady) return;
  try {
    const u8 = new Uint8Array(opus.buffer, opus.byteOffset, opus.byteLength);
    const { channelData, samplesDecoded } = decoder.decodeFrame(u8);
    if (!samplesDecoded) return;
    state.counters.opusFrames++;
    const f32 = channelData[0];
    state.counters.pcmSamples += f32.length;
    const i16 = new Int16Array(f32.length);
    for (let i = 0; i < f32.length; i++) {
      const s = Math.max(-1, Math.min(1, f32[i]));
      i16[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
    }
    const buf = Buffer.from(i16.buffer, i16.byteOffset, i16.byteLength);
    if (RECORD_AUDIO) {
      if (!state.audioRec) startAudioRecording(state);
      appendAudioRecording(state, buf);
    }
    // Re-broadcast to any connected /monitor clients — exact same bytes
    // that go to OpenAI Realtime, for live audio-quality debugging.
    broadcastToMonitors(buf);
    ws.send(
      JSON.stringify({
        type: 'input_audio_buffer.append',
        audio: buf.toString('base64'),
      }),
    );
    state.counters.bytesToOpenAI += buf.length;
  } catch (e) {
    console.warn('  [realtime] opus → pcm failed:', e.message);
  }
}

