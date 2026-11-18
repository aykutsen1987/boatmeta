// BoatMeta server: (1) realtime room relay over WebSocket, (2) leaderboard over HTTPS backed by Neon PostgreSQL.
// - Positions live in memory only while connected (never stored).
// - Scores/nickname are stored in Postgres until the player deletes them (/api/delete) or is inactive for 365 days.
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

// ------------------------------------------------------------------ database (optional)
let pool = null;
if (process.env.DATABASE_URL) {
  const { Pool } = require('pg');
  pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 4, idleTimeoutMillis: 20000 });
  pool.on('error', (e) => console.log('pg pool error', e.message));
}
const REGIONS = new Set(['lagoon', 'venice', 'river', 'fjord', 'city', 'ocean', 'coast', 'harbor', 'lighthouse', 'arctic', 'village', 'atoll']);
const BOARDS = { stunt: { min: 1, max: 300000, order: 'DESC', agg: 'GREATEST' }, trial: { min: 8, max: 3600, order: 'ASC', agg: 'LEAST' } };

async function initDb() {
  if (!pool) { console.log('DATABASE_URL not set -> leaderboard disabled'); return; }
  await pool.query(`CREATE TABLE IF NOT EXISTS players (id text PRIMARY KEY, nick text NOT NULL, secret_hash text NOT NULL, created timestamptz DEFAULT now(), last_seen timestamptz DEFAULT now())`);
  await pool.query(`CREATE TABLE IF NOT EXISTS scores (player_id text REFERENCES players(id) ON DELETE CASCADE, board text NOT NULL, region text NOT NULL, value double precision NOT NULL, updated timestamptz DEFAULT now(), PRIMARY KEY (player_id, board, region))`);
  await pool.query(`CREATE INDEX IF NOT EXISTS scores_board_idx ON scores (board, region, value)`);
  await purge();
  setInterval(() => purge().catch(() => { }), 24 * 3600 * 1000);
  console.log('Neon/Postgres ready');
}
async function purge() { // 365-day retention for inactive players (scores cascade)
  const r = await pool.query(`DELETE FROM players WHERE last_seen < now() - interval '365 days'`);
  if (r.rowCount) console.log('purged inactive players:', r.rowCount);
}
// ------------------------------------------------------------------ run validation (anti-cheat)
// The client sends x,z,y samples every 0.5 s. We re-check duration, speed, path adherence, start/finish and plausibility.
// It raises the bar a lot, but a determined cheater who forges samples is not fully stopped - keep an eye on the board.
const TRACKS = {
  lagoon: { w: 70, c: [[0,-60],[0,200],[20,380],[70,540],[40,720],[-30,880],[-40,1040],[0,1180]] },
  venice: { w: 26, c: [[0,-60],[0,160],[0,230],[50,280],[130,280],[180,330],[180,470],[130,520],[50,520],[0,570],[0,700],[50,750],[150,750],[200,800],[200,960]] },
  river: { w: 34, c: [[0,-60],[0,100],[40,220],[80,340],[40,470],[-30,590],[-60,720],[-10,850],[50,960],[60,1100],[20,1200]] },
  fjord: { w: 60, c: [[0,-60],[0,200],[-40,380],[-90,540],[-70,700],[0,830],[70,960],[60,1120],[0,1250]] },
  city: { w: 34, c: [[0,-60],[0,200],[0,300],[40,360],[120,360],[160,420],[160,600],[120,660],[40,660],[0,720],[0,900],[40,960],[140,960],[180,1020],[180,1160]] },
  ocean: { w: 80, c: [[0,-60],[0,150],[-60,330],[-100,520],[-30,700],[60,850],[90,1050],[40,1200]] },
  coast: { w: 90, c: [[0,-60],[0,200],[30,380],[80,540],[60,720],[0,880],[-40,1040],[-30,1200]] },
  harbor: { w: 90, c: [[0,-60],[0,220],[-20,400],[0,580],[50,740],[60,920],[20,1080],[0,1200]] },
  lighthouse: { w: 100, c: [[0,-60],[0,180],[40,340],[110,500],[130,680],[80,860],[0,1000],[-40,1160]] },
  arctic: { w: 110, c: [[0,-60],[0,200],[-50,360],[-60,540],[0,700],[70,860],[60,1040],[0,1200]] },
  village: { w: 90, c: [[0,-60],[0,200],[-30,380],[-70,560],[-50,740],[10,900],[60,1060],[50,1200]] },
  atoll: { w: 100, c: [[0,-60],[0,180],[50,340],[0,520],[-60,700],[-40,880],[30,1040],[60,1200]] }
};
for (const t of Object.values(TRACKS)) { t.len = 0; t.cum = [0]; for (let i = 1; i < t.c.length; i++) { t.len += Math.hypot(t.c[i][0] - t.c[i - 1][0], t.c[i][1] - t.c[i - 1][1]); t.cum.push(t.len); } }
function distSeg(px, pz, a, b) { const dx = b[0] - a[0], dz = b[1] - a[1]; const l2 = dx * dx + dz * dz || 1; let t = ((px - a[0]) * dx + (pz - a[1]) * dz) / l2; t = Math.max(0, Math.min(1, t)); return Math.hypot(px - (a[0] + t * dx), pz - (a[1] + t * dz)); }
function distPoly(t, x, z) { let m = 1e9; for (let i = 1; i < t.c.length; i++) m = Math.min(m, distSeg(x, z, t.c[i - 1], t.c[i])); return m; }
function pointAlong(t, d) { d = Math.max(0, Math.min(t.len, d)); for (let i = 1; i < t.c.length; i++) if (t.cum[i] >= d) { const k = (d - t.cum[i - 1]) / ((t.cum[i] - t.cum[i - 1]) || 1); return [t.c[i - 1][0] + (t.c[i][0] - t.c[i - 1][0]) * k, t.c[i - 1][1] + (t.c[i][1] - t.c[i - 1][1]) * k]; } return t.c[t.c.length - 1]; }
function validateRun(m, board, region, value) {
  if (process.env.VALIDATE === 'off') return null;
  const T = Number(m.t);
  if (!(T >= 5 && T <= 3600)) return 'time';
  if (typeof m.run !== 'string' || m.run.length < 16 || m.run.length > 16000) return 'run';
  const buf = Buffer.from(m.run, 'base64');
  if (buf.length % 12 !== 0) return 'run';
  const n = buf.length / 12; if (n < 3 || n > 2400) return 'run';
  const P = [];
  for (let i = 0; i < n; i++) {
    const x = buf.readFloatLE(i * 12), z = buf.readFloatLE(i * 12 + 4), y = buf.readFloatLE(i * 12 + 8);
    if (!Number.isFinite(x) || !Number.isFinite(z) || !Number.isFinite(y) || Math.abs(x) > 3000 || Math.abs(z) > 3000) return 'values';
    P.push([x, z, y]);
  }
  if (Math.abs((n - 1) * 0.5 - T) > 3) return 'duration';
  for (let i = 1; i < n; i++) if (Math.hypot(P[i][0] - P[i - 1][0], P[i][1] - P[i - 1][1]) > 0.5 * 95 * 1.25) return 'teleport';
  const tr = TRACKS[region]; if (!tr) return 'region';
  const tol = tr.w * 0.5 + 90;
  for (const p of P) if (distPoly(tr, p[0], p[1]) > tol) return 'offtrack';
  const st = pointAlong(tr, 14), en = pointAlong(tr, tr.len - 60);
  if (Math.hypot(P[0][0] - st[0], P[0][1] - st[1]) > 100) return 'start';
  if (Math.hypot(P[n - 1][0] - en[0], P[n - 1][1] - en[1]) > 140) return 'finish';
  if (board === 'trial') {
    if (Math.abs(value - T) > 1.0) return 'mismatch';
    if (T < tr.len / 90) return 'too fast';
  } else {
    const air = P.filter((p) => p[2] > 1.6).length * 0.5;
    if (value > 1500 + air * (25 + 5 * 90) + 6000) return 'score';
  }
  return null;
}

const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
const safeEq = (a, b) => { const x = Buffer.from(a), y = Buffer.from(b); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const okId = (v) => /^[a-f0-9]{8,32}$/.test(v || '');
const okSecret = (v) => typeof v === 'string' && v.length >= 16 && v.length <= 80;
const okNick = (v) => /^[A-Za-z0-9_]{3,14}$/.test(v || '');

// ------------------------------------------------------------------ http api
const hits = new Map(); // ip -> {n, t}
function limited(ip) { const now = Date.now(); let h = hits.get(ip); if (!h || now - h.t > 60000) { h = { n: 0, t: now }; hits.set(ip, h); } return ++h.n > 40; }
const send = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
function body(req, max = 2048) {
  return new Promise((resolve, reject) => {
    let d = ''; req.on('data', (c) => { d += c; if (d.length > max) { reject(new Error('big')); req.destroy(); } });
    req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch (e) { reject(e); } });
  });
}
async function auth(m) { // trust-on-first-use: the first secret seen for an id owns it
  if (!okId(m.id) || !okSecret(m.secret)) return null;
  const h = sha(m.secret);
  const r = await pool.query('SELECT secret_hash FROM players WHERE id=$1', [m.id]);
  if (r.rowCount === 0) return { fresh: true, hash: h };
  return safeEq(r.rows[0].secret_hash, h) ? { fresh: false, hash: h } : null;
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x');
    if (url.pathname === '/') { res.writeHead(200); return res.end('BoatMeta server OK'); }
    if (!url.pathname.startsWith('/api/')) { res.writeHead(404); return res.end(); }
    if (!pool) return send(res, 503, { error: 'leaderboard disabled' });
    if (limited(req.socket.remoteAddress)) return send(res, 429, { error: 'slow down' });

    if (req.method === 'GET' && url.pathname === '/api/top') {
      const board = url.searchParams.get('board'), region = url.searchParams.get('region');
      const limit = Math.max(1, Math.min(50, parseInt(url.searchParams.get('limit') || '15', 10)));
      if (!BOARDS[board] || !REGIONS.has(region)) return send(res, 400, { error: 'bad params' });
      const r = await pool.query(`SELECT p.nick AS nick, s.value AS value FROM scores s JOIN players p ON p.id = s.player_id WHERE s.board=$1 AND s.region=$2 ORDER BY s.value ${BOARDS[board].order} LIMIT $3`, [board, region, limit]);
      return send(res, 200, { rows: r.rows });
    }

    if (req.method === 'POST' && url.pathname === '/api/score') {
      const m = await body(req, 16384);
      const B = BOARDS[m.board]; const v = Number(m.value);
      if (!B || !REGIONS.has(m.region) || !okNick(m.nick) || !Number.isFinite(v) || v < B.min || v > B.max) return send(res, 400, { error: 'bad data' });
      const bad = validateRun(m, m.board, m.region, v);
      if (bad) return send(res, 422, { error: 'invalid run: ' + bad });
      const a = await auth(m); if (!a) return send(res, 403, { error: 'auth' });
      if (a.fresh) await pool.query('INSERT INTO players(id,nick,secret_hash) VALUES($1,$2,$3)', [m.id, m.nick, a.hash]);
      else await pool.query('UPDATE players SET nick=$2, last_seen=now() WHERE id=$1', [m.id, m.nick]);
      await pool.query(`INSERT INTO scores(player_id,board,region,value) VALUES($1,$2,$3,$4) ON CONFLICT (player_id,board,region) DO UPDATE SET value=${B.agg}(scores.value, EXCLUDED.value), updated=now()`, [m.id, m.board, m.region, v]);
      return send(res, 200, { ok: true });
    }

    if (req.method === 'POST' && url.pathname === '/api/delete') { // player reset -> remove everything about this player
      const m = await body(req);
      const a = await auth(m); if (!a) return send(res, 403, { error: 'auth' });
      await pool.query('DELETE FROM players WHERE id=$1', [m.id]);
      return send(res, 200, { ok: true });
    }
    send(res, 404, { error: 'not found' });
  } catch (e) { send(res, 500, { error: 'server' }); }
});

// ------------------------------------------------------------------ websocket rooms (in-memory only)
const MAX_PER_ROOM = 12;
const rooms = new Map();
const wss = new WebSocketServer({ server, maxPayload: 2048 });
const clean = (v, n) => String(v || '').replace(/[^\w\- ]/g, '').slice(0, n);
const num = (v, lim) => { v = Number(v); return Number.isFinite(v) ? Math.max(-lim, Math.min(lim, v)) : 0; };

function leave(ws) {
  if (!ws.room) return;
  const r = rooms.get(ws.room);
  if (r) { r.delete(ws.pid); if (r.size === 0) rooms.delete(ws.room); }
  ws.room = null;
}

wss.on('connection', (ws) => {
  ws.alive = true; ws.msgs = 0;
  ws.on('pong', () => { ws.alive = true; });
  const rl = setInterval(() => { ws.msgs = 0; }, 1000);
  ws.on('message', (data) => {
    if (++ws.msgs > 30) return;
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    if (m.t === 'join') {
      leave(ws);
      const room = clean(m.room, 40); if (!room) return;
      let r = rooms.get(room); if (!r) { r = new Map(); rooms.set(room, r); }
      const mc = room.match(/-(\d+)$/);
      const cap = Math.max(2, Math.min(MAX_PER_ROOM, mc ? parseInt(mc[1], 10) : MAX_PER_ROOM));
      if (r.size >= cap) { ws.close(); return; }
      ws.pid = clean(m.id, 16) || Math.random().toString(36).slice(2, 10);
      ws.room = room;
      r.set(ws.pid, { ws, st: { id: ws.pid, name: clean(m.name, 16) || 'Pilot', boat: Math.floor(num(m.boat, 40)), x: 0, y: 0, z: 0, ry: 0 } });
    } else if (m.t === 'pos' && ws.room) {
      const r = rooms.get(ws.room); const p = r && r.get(ws.pid); if (!p) return;
      p.st.boat = Math.floor(num(m.boat, 40)); p.st.x = num(m.x, 20000); p.st.y = num(m.y, 500); p.st.z = num(m.z, 20000); p.st.ry = num(m.ry, 360);
    }
  });
  ws.on('close', () => { clearInterval(rl); leave(ws); });
  ws.on('error', () => { });
});

setInterval(() => {
  for (const r of rooms.values()) {
    const list = Array.from(r.values()).map((v) => v.st);
    const msg = JSON.stringify({ t: 'state', n: list.length, p: list });
    for (const v of r.values()) if (v.ws.readyState === 1) v.ws.send(msg);
  }
}, 100);
setInterval(() => { wss.clients.forEach((ws) => { if (!ws.alive) { leave(ws); return ws.terminate(); } ws.alive = false; ws.ping(); }); }, 30000);

initDb().catch((e) => console.log('db init failed:', e.message)).finally(() => {
  server.listen(process.env.PORT || 8080, () => console.log('BoatMeta server listening'));
});
