/* ═══════════════════════════════════════════════════════════════
   RAILOPS AI — app.js
   Thessaloniki Metro Control Center
   Pure vanilla JS — no dependencies
═══════════════════════════════════════════════════════════════ */

'use strict';

/* ─── STATION DATA ─── */
const STATIONS = [
  { id: 'new_railway', name: 'New Railway Station', lat: 40.64280, lon: 22.92380 },
  { id: 'dimokratias', name: 'Dimokratias',          lat: 40.64220, lon: 22.93580 },
  { id: 'venizelou',   name: 'Venizelou',             lat: 40.63580, lon: 22.94194 },
  { id: 'agias_sofias',name: 'Agias Sofias',          lat: 40.63444, lon: 22.94639 },
  { id: 'sintrivani',  name: 'Sintrivani',             lat: 40.63190, lon: 22.94940 },
  { id: 'panepistimio',name: 'Panepistimio',           lat: 40.63040, lon: 22.95280 },
  { id: 'papafi',      name: 'Papafi',                 lat: 40.62280, lon: 22.96490 },
  { id: 'efkleidis',   name: 'Efkleidis',              lat: 40.62080, lon: 22.95840 },
  { id: 'fleming',     name: 'Fleming',                lat: 40.61740, lon: 22.96240 },
  { id: 'analipsi',    name: 'Analipsi',               lat: 40.61390, lon: 22.95940 },
  { id: '25_martiou',  name: '25 Martiou',             lat: 40.60690, lon: 22.97210 },
  { id: 'voulgari',    name: 'Voulgari',               lat: 40.60320, lon: 22.98050 },
  { id: 'nea_elvetia', name: 'Nea Elvetia',            lat: 40.59890, lon: 22.97290 },
];

/* Route order (west → east / north → south for map) */
const ROUTE_ORDER = [
  'new_railway','dimokratias','venizelou','agias_sofias','sintrivani',
  'panepistimio','papafi','efkleidis','fleming','analipsi',
  '25_martiou','voulgari','nea_elvetia'
];

/* ─── GLOBAL STATE ─── */
let groqApiKey = '';
let aggregatesData = null;
let lastPrediction = null;
let selectedStation = null;
let alertStationIds = [];

/* ═══════════════════════════════════════════════════════
   INIT
═══════════════════════════════════════════════════════ */
window.addEventListener('DOMContentLoaded', () => {
  populateStations();
  setDateTimeNow();
  startClock();
  loadKeyFromStorage();
  initCanvas();
  drawTrajectory(null, []);
});

function populateStations() {
  const sel = document.getElementById('station-select');
  STATIONS.forEach(s => {
    const o = document.createElement('option');
    o.value = s.id;
    o.textContent = s.name;
    sel.appendChild(o);
  });
  sel.addEventListener('change', () => {
    selectedStation = sel.value || null;
    drawTrajectory(selectedStation, alertStationIds);
  });
}

function setDateTimeNow() {
  const now = new Date();
  const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
  document.getElementById('datetime-input').value = local.toISOString().slice(0, 16);
}

function startClock() {
  const el = document.getElementById('clock');
  const tick = () => {
    const now = new Date();
    el.textContent = now.toTimeString().slice(0, 8);
  };
  tick();
  setInterval(tick, 1000);
}

function loadKeyFromStorage() {
  const lsKey = localStorage.getItem('railops_groq_key');
  const ssKey = sessionStorage.getItem('railops_groq_key');
  if (lsKey) { groqApiKey = lsKey; updateApiStatus(true); }
  else if (ssKey) { groqApiKey = ssKey; updateApiStatus(true); }
}

function updateApiStatus(ok) {
  const el = document.getElementById('api-status');
  if (ok) {
    el.textContent = '● API READY';
    el.className = 'status-badge status-ok';
  } else {
    el.textContent = '● NO KEY';
    el.className = 'status-badge status-err';
  }
}

/* ═══════════════════════════════════════════════════════
   LOGIN / AUTH
═══════════════════════════════════════════════════════ */
function doLogin() {
  const user = document.getElementById('login-user').value.trim();
  const pass = document.getElementById('login-pass').value.trim();
  if (!user || !pass) {
    shakeElement(document.querySelector('.login-box'));
    return;
  }
  document.getElementById('user-badge').textContent = user.toUpperCase().slice(0, 8);
  const ls = document.getElementById('login-screen');
  ls.classList.add('fade-out');
  setTimeout(() => {
    ls.style.display = 'none';
    document.getElementById('app').classList.remove('hidden');
    if (!groqApiKey) showApiModal();
    else updateApiStatus(true);
  }, 400);
}

function doLogout() {
  location.reload();
}

document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !document.getElementById('login-screen').classList.contains('fade-out')) {
    doLogin();
  }
});

function shakeElement(el) {
  el.style.animation = 'none';
  el.offsetHeight;
  el.style.animation = 'shake 0.4s ease';
}
const style = document.createElement('style');
style.textContent = `
@keyframes shake {
  0%,100%{transform:translateX(0)}
  20%{transform:translateX(-8px)}
  40%{transform:translateX(8px)}
  60%{transform:translateX(-5px)}
  80%{transform:translateX(5px)}
}`;
document.head.appendChild(style);

/* ═══════════════════════════════════════════════════════
   API KEY MODAL
═══════════════════════════════════════════════════════ */
function showApiModal() {
  document.getElementById('api-modal').classList.remove('hidden');
}
function saveApiKey() {
  const key = document.getElementById('api-key-input').value.trim();
  if (!key.startsWith('gsk_') && !key.startsWith('groq_')) {
    alert('Invalid Groq key format. Should start with gsk_ or groq_');
    return;
  }
  groqApiKey = key;
  const remember = document.getElementById('remember-key').checked;
  if (remember) localStorage.setItem('railops_groq_key', key);
  else sessionStorage.setItem('railops_groq_key', key);
  document.getElementById('api-modal').classList.add('hidden');
  updateApiStatus(true);
}
function skipApiKey() {
  groqApiKey = '';
  document.getElementById('api-modal').classList.add('hidden');
  updateApiStatus(false);
}

/* ═══════════════════════════════════════════════════════
   AGGREGATES / DATA LOADING
═══════════════════════════════════════════════════════ */
function loadAggregates(input) {
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = e => {
    try {
      aggregatesData = JSON.parse(e.target.result);
      document.getElementById('ats-status').textContent  = 'LOADED';
      document.getElementById('ats-status').className    = 'ds-badge ds-loaded';
      document.getElementById('ticket-status').textContent = 'LOADED';
      document.getElementById('ticket-status').className   = 'ds-badge ds-loaded';
      console.log('[RailOps] Aggregates loaded:', Object.keys(aggregatesData));
    } catch(err) {
      alert('Failed to parse aggregates.json — check file format.');
    }
  };
  reader.readAsText(file);
}

/* ═══════════════════════════════════════════════════════
   PREDICTION — MAIN AI CALL
═══════════════════════════════════════════════════════ */
async function runPrediction() {
  if (!groqApiKey) {
    showApiModal();
    return;
  }

  const stationId   = document.getElementById('station-select').value;
  const datetime    = document.getElementById('datetime-input').value;
  const headway     = document.getElementById('headway-input').value;
  const load        = document.getElementById('load-input').value;
  const context     = document.getElementById('context-input').value.trim();
  const stationName = STATIONS.find(s => s.id === stationId)?.name || 'All stations';

  setLoading(true);

  /* Build aggregates context snippet */
  let aggContext = '';
  if (aggregatesData) {
    const hour = datetime ? new Date(datetime).getHours() : new Date().getHours();
    const stKey = stationId || 'all';
    const hourData = aggregatesData?.[stKey]?.[hour] || aggregatesData?.['all']?.[hour];
    if (hourData) {
      aggContext = `\nHistorical context for ${stationName} at hour ${hour}:00: ${JSON.stringify(hourData)}`;
    } else {
      aggContext = '\nHistorical aggregates loaded but no match for this station/hour.';
    }
  }

  const userMessage = `
Metro Controller Query:
- Station: ${stationName} (id: ${stationId || 'none'})
- Datetime: ${datetime}
- Current headway: ${headway} min
- Passenger load: ${load}%
- Operational context: ${context || 'None provided'}
- Route: New Railway Station → Dimokratias → Venizelou → Agias Sofias → Sintrivani → Panepistimio → Papafi → Efkleidis → Fleming → Analipsi → 25 Martiou → Voulgari → Nea Elvetia
${aggContext}

Provide your prediction analysis. Return ONLY valid JSON matching the specified schema.
`.trim();

  const systemPrompt = buildSystemPrompt();

  try {
    const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${groqApiKey}`
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        temperature: 0.2,
        max_tokens: 1500,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userMessage }
        ]
      })
    });

    if (!response.ok) {
      const err = await response.json().catch(() => ({}));
      throw new Error(err?.error?.message || `HTTP ${response.status}`);
    }

    const data = await response.json();
    const raw  = data.choices?.[0]?.message?.content || '';
    const parsed = extractJson(raw);

    if (!parsed) throw new Error('Model returned invalid JSON');

    lastPrediction = parsed;
    renderPrediction(parsed, stationId);

  } catch (err) {
    console.error('[RailOps] API error:', err);
    showError(err.message);
  } finally {
    setLoading(false);
  }
}

function buildSystemPrompt() {
  return `You are RailOps AI, an expert metro operations advisor for Thessaloniki Metro (Greece).
You analyze rail controller inputs and return STRICT JSON predictions — no prose, no markdown fences.

Station IDs and coordinates:
${STATIONS.map(s => `  ${s.id}: ${s.name} [${s.lat}, ${s.lon}]`).join('\n')}

Route order (west→east): ${ROUTE_ORDER.join(' → ')}

RETURN ONLY this JSON object (no extra text, no markdown, no backticks):
{
  "predicted_delay_min": <number, realistic minutes 0-30>,
  "risk_level": "<low|medium|high>",
  "recommended_actions": ["<string>", ...],
  "alerts": ["<string>", ...],
  "rationale": "<2-3 sentence operational explanation>",
  "trajectory": {
    "route_station_ids": ["<id>", ...],
    "polyline": [[lat,lon], ...],
    "selected_station_id": "<id or empty string>",
    "alert_station_ids": ["<id>", ...]
  }
}

Rules:
- predicted_delay_min must be a real number (0 if no delay expected)
- If load > 130% or headway < 2.5 min, risk is at least medium
- If context mentions incidents/faults, elevate risk_level and generate specific alerts
- trajectory.polyline must be the full route in lat/lon pairs matching route_station_ids
- alert_station_ids = stations most affected by current issue
- Never hallucinate; if uncertain set risk_level to medium and add clarifying alert
- recommended_actions must be specific operational actions (3-5 items)
- alerts must be time-critical signals (0 if none, up to 4)`;
}

/* Extract JSON even if model wraps in fences */
function extractJson(raw) {
  if (!raw) return null;
  // Strip markdown fences
  let cleaned = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();
  // Find first { ... }
  const start = cleaned.indexOf('{');
  const end   = cleaned.lastIndexOf('}');
  if (start === -1 || end === -1) return null;
  try {
    return JSON.parse(cleaned.slice(start, end + 1));
  } catch {
    return null;
  }
}

/* ═══════════════════════════════════════════════════════
   RENDER PREDICTION → CARDS
═══════════════════════════════════════════════════════ */
function renderPrediction(data, stationId) {
  /* Delay */
  const delayEl = document.getElementById('delay-value');
  const delay   = typeof data.predicted_delay_min === 'number'
    ? Math.round(data.predicted_delay_min) : '?';
  delayEl.textContent = delay;
  delayEl.className   = 'delay-num';
  if (data.risk_level === 'high')   delayEl.classList.add('risk-high-num');
  if (data.risk_level === 'medium') delayEl.classList.add('risk-medium-num');

  /* Risk badge */
  const riskEl = document.getElementById('risk-badge');
  riskEl.textContent = (data.risk_level || 'unknown').toUpperCase();
  riskEl.className   = `risk-badge risk-${data.risk_level || 'none'}`;

  /* Rationale */
  document.getElementById('rationale-text').textContent = data.rationale || '—';

  /* Actions */
  const actList = document.getElementById('actions-list');
  actList.innerHTML = '';
  const actions = data.recommended_actions || [];
  if (actions.length === 0) {
    actList.innerHTML = '<li class="placeholder-item">No actions required at this time.</li>';
  } else {
    actions.forEach((a, i) => {
      const li = document.createElement('li');
      li.textContent = `${i+1}. ${a}`;
      actList.appendChild(li);
    });
  }

  /* Alerts */
  const alertList  = document.getElementById('alerts-list');
  const alertCount = document.getElementById('alert-count');
  alertList.innerHTML = '';
  const alerts = data.alerts || [];
  alertCount.textContent = alerts.length;
  alertCount.className   = `alert-count${alerts.length === 0 ? ' zero' : ''}`;

  if (alerts.length === 0) {
    alertList.innerHTML = '<li class="placeholder-item">System nominal — no active alerts.</li>';
  } else {
    alerts.forEach(a => {
      const li = document.createElement('li');
      li.textContent = `⚠ ${a}`;
      alertList.appendChild(li);
    });
  }

  /* Trajectory */
  const traj = data.trajectory || {};
  alertStationIds = traj.alert_station_ids || [];
  const sel = traj.selected_station_id || stationId || null;
  drawTrajectory(sel, alertStationIds);
}

/* ═══════════════════════════════════════════════════════
   TRAJECTORY CANVAS
═══════════════════════════════════════════════════════ */
let canvas, ctx;
const CANVAS_W = 800, CANVAS_H = 220;

function initCanvas() {
  canvas = document.getElementById('traj-canvas');
  canvas.width  = CANVAS_W;
  canvas.height = CANVAS_H;
  ctx = canvas.getContext('2d');

  canvas.addEventListener('mousemove', onCanvasHover);
  canvas.addEventListener('mouseleave', () => {
    document.getElementById('traj-tooltip').classList.add('hidden');
  });
}

/* Convert geo lat/lon → canvas x,y */
function geoToCanvas(lat, lon, minLat, maxLat, minLon, maxLon) {
  const PAD = 40;
  const x = PAD + (lon - minLon) / (maxLon - minLon) * (CANVAS_W - PAD * 2);
  const y = PAD + (1 - (lat - minLat) / (maxLat - minLat)) * (CANVAS_H - PAD * 2);
  return { x, y };
}

let stationPoints = []; // [{id, name, x, y}]

function drawTrajectory(selectedId, alertIds = []) {
  if (!ctx) return;

  /* Compute bounding box */
  const lats = STATIONS.map(s => s.lat);
  const lons = STATIONS.map(s => s.lon);
  const minLat = Math.min(...lats) - 0.002;
  const maxLat = Math.max(...lats) + 0.002;
  const minLon = Math.min(...lons) - 0.002;
  const maxLon = Math.max(...lons) + 0.002;

  const byId = {};
  STATIONS.forEach(s => { byId[s.id] = s; });

  /* Build ordered points */
  const ordered = ROUTE_ORDER.map(id => byId[id]).filter(Boolean);
  stationPoints = ordered.map(s => ({
    ...s,
    ...geoToCanvas(s.lat, s.lon, minLat, maxLat, minLon, maxLon)
  }));

  /* Clear */
  ctx.clearRect(0, 0, CANVAS_W, CANVAS_H);

  /* Background grid */
  ctx.strokeStyle = 'rgba(30,45,61,0.4)';
  ctx.lineWidth = 0.5;
  for (let x = 0; x < CANVAS_W; x += 60) {
    ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, CANVAS_H); ctx.stroke();
  }
  for (let y = 0; y < CANVAS_H; y += 40) {
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(CANVAS_W, y); ctx.stroke();
  }

  /* Draw main line */
  if (stationPoints.length > 1) {
    ctx.beginPath();
    ctx.moveTo(stationPoints[0].x, stationPoints[0].y);
    for (let i = 1; i < stationPoints.length; i++) {
      ctx.lineTo(stationPoints[i].x, stationPoints[i].y);
    }
    ctx.strokeStyle = 'rgba(0,183,204,0.25)';
    ctx.lineWidth = 8;
    ctx.lineCap = 'round';
    ctx.lineJoin = 'round';
    ctx.stroke();

    /* Solid inner line */
    ctx.beginPath();
    ctx.moveTo(stationPoints[0].x, stationPoints[0].y);
    for (let i = 1; i < stationPoints.length; i++) {
      ctx.lineTo(stationPoints[i].x, stationPoints[i].y);
    }
    ctx.strokeStyle = '#00b8cc';
    ctx.lineWidth = 2.5;
    ctx.stroke();
  }

  /* Draw alert segments (red overlay) */
  if (alertIds.length > 0) {
    const alertSet = new Set(alertIds);
    for (let i = 0; i < stationPoints.length - 1; i++) {
      if (alertSet.has(stationPoints[i].id) || alertSet.has(stationPoints[i+1].id)) {
        ctx.beginPath();
        ctx.moveTo(stationPoints[i].x, stationPoints[i].y);
        ctx.lineTo(stationPoints[i+1].x, stationPoints[i+1].y);
        ctx.strokeStyle = 'rgba(255,51,102,0.7)';
        ctx.lineWidth = 3;
        ctx.stroke();
      }
    }
  }

  /* Draw station dots */
  stationPoints.forEach(pt => {
    const isSelected = pt.id === selectedId;
    const isAlert    = alertIds.includes(pt.id);

    /* Glow */
    if (isSelected || isAlert) {
      ctx.beginPath();
      ctx.arc(pt.x, pt.y, isSelected ? 12 : 9, 0, Math.PI * 2);
      ctx.fillStyle = isAlert
        ? 'rgba(255,51,102,0.2)'
        : 'rgba(0,255,136,0.2)';
      ctx.fill();
    }

    /* Outer ring */
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, isSelected ? 7 : 5, 0, Math.PI * 2);
    ctx.fillStyle = isSelected ? '#00ff88' : (isAlert ? '#ff3366' : '#00e5ff');
    ctx.fill();

    /* Inner dot */
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, isSelected ? 3 : 2, 0, Math.PI * 2);
    ctx.fillStyle = '#080c10';
    ctx.fill();

    /* Station name labels (small) */
    ctx.fillStyle = isSelected ? '#00ff88' : (isAlert ? '#ff6688' : '#4a7a99');
    ctx.font = isSelected ? `bold 9px 'Share Tech Mono'` : `9px 'Share Tech Mono'`;
    ctx.textAlign = 'center';
    /* Alternate label above/below to avoid overlap */
    const idx = stationPoints.indexOf(pt);
    const yOff = (idx % 2 === 0) ? -14 : 18;
    ctx.fillText(pt.name.length > 10 ? pt.name.slice(0, 10) + '…' : pt.name, pt.x, pt.y + yOff);
  });

  /* Direction arrow at end */
  const last = stationPoints[stationPoints.length - 1];
  const prev = stationPoints[stationPoints.length - 2];
  if (last && prev) {
    const dx = last.x - prev.x;
    const dy = last.y - prev.y;
    const angle = Math.atan2(dy, dx);
    ctx.save();
    ctx.translate(last.x, last.y);
    ctx.rotate(angle);
    ctx.beginPath();
    ctx.moveTo(10, 0);
    ctx.lineTo(-5, -5);
    ctx.lineTo(-5, 5);
    ctx.closePath();
    ctx.fillStyle = '#00b8cc';
    ctx.fill();
    ctx.restore();
  }
}

/* Canvas hover tooltip */
function onCanvasHover(e) {
  const rect = canvas.getBoundingClientRect();
  const scaleX = CANVAS_W / rect.width;
  const scaleY = CANVAS_H / rect.height;
  const mx = (e.clientX - rect.left) * scaleX;
  const my = (e.clientY - rect.top) * scaleY;

  const HIT_R = 14;
  const tip  = document.getElementById('traj-tooltip');
  let found = null;
  for (const pt of stationPoints) {
    const dist = Math.hypot(mx - pt.x, my - pt.y);
    if (dist < HIT_R) { found = pt; break; }
  }

  if (found) {
    tip.textContent = `${found.name}  [${found.lat.toFixed(5)}, ${found.lon.toFixed(5)}]`;
    tip.style.left  = (e.clientX - rect.left + 12) + 'px';
    tip.style.top   = (e.clientY - rect.top - 10) + 'px';
    tip.classList.remove('hidden');
  } else {
    tip.classList.add('hidden');
  }
}

/* ═══════════════════════════════════════════════════════
   CHAT / WHAT-IF
═══════════════════════════════════════════════════════ */
function chatKeydown(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    sendChat();
  }
}

async function sendChat() {
  const input = document.getElementById('chat-input');
  const text  = input.value.trim();
  if (!text) return;
  input.value = '';

  appendChatMsg('user', 'YOU', text);
  const thinking = appendChatMsg('ai', 'RAILOPS AI', '…thinking…', true);

  if (!groqApiKey) {
    thinking.remove();
    appendChatMsg('ai', 'RAILOPS AI', 'No API key configured. Click ⚙ to add your Groq key.');
    return;
  }

  /* Build context from last prediction */
  let predCtx = '';
  if (lastPrediction) {
    predCtx = `\nCurrent prediction context: delay=${lastPrediction.predicted_delay_min} min, risk=${lastPrediction.risk_level}`;
  }

  try {
    const resp = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${groqApiKey}`
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        temperature: 0.3,
        max_tokens: 250,
        messages: [
          {
            role: 'system',
            content: `You are RailOps AI, a rail operations assistant for Thessaloniki Metro.
Answer what-if operational questions concisely in bullet points, max 120 words.
Focus on practical controller actions and realistic operational implications.
${predCtx}`
          },
          { role: 'user', content: text }
        ]
      })
    });

    const data = await resp.json();
    const answer = data.choices?.[0]?.message?.content || 'No response received.';
    thinking.remove();
    appendChatMsg('ai', 'RAILOPS AI', answer);

  } catch (err) {
    thinking.remove();
    appendChatMsg('ai', 'RAILOPS AI', `Error: ${err.message}`);
  }
}

function appendChatMsg(type, sender, text, isThinking = false) {
  const msgs = document.getElementById('chat-messages');
  const div  = document.createElement('div');
  div.className = `chat-msg chat-${type}${isThinking ? ' chat-thinking' : ''}`;
  div.innerHTML = `<span class="chat-sender">${sender}</span><p>${escapeHtml(text)}</p>`;
  msgs.appendChild(div);
  msgs.scrollTop = msgs.scrollHeight;
  return div;
}

function escapeHtml(s) {
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
          .replace(/\n/g, '<br>');
}

/* ═══════════════════════════════════════════════════════
   UI HELPERS
═══════════════════════════════════════════════════════ */
function setLoading(on) {
  const btn  = document.getElementById('predict-btn');
  const load = document.getElementById('loading-indicator');
  btn.disabled = on;
  load.classList.toggle('hidden', !on);
}

function showError(msg) {
  const riskEl = document.getElementById('risk-badge');
  riskEl.textContent = 'API ERROR';
  riskEl.className   = 'risk-badge risk-high';
  document.getElementById('rationale-text').textContent = `Error: ${msg}. Check your Groq API key and network connection.`;
  updateApiStatus(false);
}
