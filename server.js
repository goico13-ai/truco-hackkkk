const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

// Estado por sesión
const sessions = {};

// Msgpack decoder mínimo
function mpDec(buf, o) {
  o = o || 0;
  const b = buf[o];
  if (b === undefined) return { v: null, n: o };
  if ((b & 0x80) === 0) return { v: b, n: o + 1 };
  if ((b & 0xe0) === 0xe0) return { v: b - 256, n: o + 1 };
  if ((b & 0xf0) === 0x80) {
    const len = b & 0x0f; let p = o + 1; const m = {};
    for (let i = 0; i < len; i++) { const k = mpDec(buf, p); p = k.n; const v = mpDec(buf, p); p = v.n; m[k.v] = v.v; }
    return { v: m, n: p };
  }
  if ((b & 0xf0) === 0x90) {
    const len = b & 0x0f; let p = o + 1; const a = [];
    for (let i = 0; i < len; i++) { const r = mpDec(buf, p); a.push(r.v); p = r.n; }
    return { v: a, n: p };
  }
  if ((b & 0xe0) === 0xa0) {
    const len = b & 0x1f; let s = '';
    for (let i = 0; i < len; i++) s += String.fromCharCode(buf[o + 1 + i]);
    return { v: s, n: o + 1 + len };
  }
  if (b === 0xc0) return { v: null, n: o + 1 };
  if (b === 0xc2) return { v: false, n: o + 1 };
  if (b === 0xc3) return { v: true, n: o + 1 };
  if (b === 0xcc) return { v: buf[o + 1], n: o + 2 };
  if (b === 0xcd) return { v: (buf[o + 1] << 8) | buf[o + 2], n: o + 3 };
  if (b === 0xce) return { v: ((buf[o+1]<<24)|(buf[o+2]<<16)|(buf[o+3]<<8)|buf[o+4])>>>0, n: o + 5 };
  if (b === 0xd0) return { v: buf[o+1] > 127 ? buf[o+1]-256 : buf[o+1], n: o + 2 };
  if (b === 0xd1) { const v = (buf[o+1]<<8)|buf[o+2]; return { v: v>32767?v-65536:v, n: o+3 }; }
  if (b === 0xd9) { const l=buf[o+1]; let s=''; for(let i=0;i<l;i++) s+=String.fromCharCode(buf[o+2+i]); return {v:s,n:o+2+l}; }
  if (b === 0xda) { const l=(buf[o+1]<<8)|buf[o+2]; let s=''; for(let i=0;i<l;i++) s+=String.fromCharCode(buf[o+3+i]); return {v:s,n:o+3+l}; }
  if (b === 0xdc) { const l=(buf[o+1]<<8)|buf[o+2]; let p=o+3; const a=[]; for(let i=0;i<l;i++){const r=mpDec(buf,p);a.push(r.v);p=r.n;} return {v:a,n:p}; }
  if (b === 0xde) { const l=(buf[o+1]<<8)|buf[o+2]; let p=o+3; const m={}; for(let i=0;i<l;i++){const k=mpDec(buf,p);p=k.n;const v=mpDec(buf,p);p=v.n;m[k.v]=v.v;} return {v:m,n:p}; }
  if (b === 0xd4) return { v: null, n: o+3 };
  if (b === 0xd5) return { v: null, n: o+4 };
  if (b === 0xd6) return { v: null, n: o+6 };
  if (b === 0xd7) return { v: null, n: o+10 };
  if (b === 0xc4) { const l=buf[o+1]; return {v:null,n:o+2+l}; }
  if (b === 0xc7) { const l=buf[o+1]; return {v:null,n:o+3+l}; }
  return { v: null, n: o + 1 };
}

function decodeMsgpack(buf) {
  try {
    for (let off = 0; off <= 4 && off < buf.length; off++) {
      try { const r = mpDec(buf, off); if (r && r.n > off) return r.v; } catch(e) {}
    }
  } catch(e) {}
  return null;
}

// Buscar arrays de cartas en un objeto
const VALID_RANKS = [1,2,3,4,5,6,7,10,11,12];
const RANK_MAP = [1,2,3,4,5,6,7,10,11,12];
const SUIT_NAMES = ['espada','basto','oro','copa'];

function decodeCardId(id, calIdx) {
  const SUIT_ORDERS = [[0,1,2,3],[0,1,3,2],[0,2,1,3],[1,0,2,3],[2,0,1,3],[3,0,1,2],[0,3,2,1],[1,2,0,3]];
  const decoders = [];
  SUIT_ORDERS.forEach(so => {
    decoders.push(id => { if(id<0||id>=40) return null; return {suit:so[Math.floor(id/10)],rank:RANK_MAP[id%10]}; });
    decoders.push(id => { if(id<1||id>40) return null; const n=id-1; return {suit:so[Math.floor(n/10)],rank:RANK_MAP[n%10]}; });
    decoders.push(id => { if(id<0||id>=40) return null; return {suit:so[id%4],rank:RANK_MAP[Math.floor(id/4)]}; });
    decoders.push(id => { if(id<1||id>40) return null; const n=id-1; return {suit:so[n%4],rank:RANK_MAP[Math.floor(n/4)]}; });
  });
  const fn = decoders[calIdx >= 0 && calIdx < decoders.length ? calIdx : 0];
  return fn ? fn(id) : null;
}

function findCards(obj, path, depth, results, seen) {
  if (depth > 12 || !obj || typeof obj !== 'object') return;
  if (seen.has(obj)) return;
  seen.add(obj);
  if (Array.isArray(obj)) {
    if (obj.length >= 1 && obj.length <= 40) {
      const cards = obj.map(x => {
        if (typeof x === 'number') return decodeCardId(x, 0);
        if (typeof x === 'string') {
          const m = x.match(/(\d+)/); if (m) return { rank: parseInt(m[1]), suit: 0 };
        }
        return null;
      }).filter(Boolean);
      if (cards.length === obj.length && cards.length > 0) {
        results.push({ path, cards, raw: obj });
        return;
      }
    }
    obj.forEach((item, i) => findCards(item, `${path}[${i}]`, depth+1, results, seen));
  } else {
    Object.keys(obj).forEach(k => findCards(obj[k], `${path}.${k}`, depth+1, results, seen));
  }
}

function extractCards(obj) {
  const results = [];
  findCards(obj, 'root', 0, results, new WeakSet());
  return results;
}

// Conectar al WS de Colyseus como segundo cliente
async function connectToRoom(accessToken, roomId) {
  const sessionKey = `${accessToken}_${roomId}`;
  
  if (sessions[sessionKey] && sessions[sessionKey].ws && sessions[sessionKey].ws.readyState < 2) {
    return { ok: true, cached: true, data: sessions[sessionKey].data };
  }

  // Paso 1: joinById
  const joinRes = await fetch(`https://www.trucox.com/game/matchmake/joinById/${roomId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${accessToken}` },
    body: JSON.stringify({})
  });
  
  const joinData = await joinRes.json();
  if (!joinData.token) return { ok: false, error: joinData.error || 'No token', joinData };

  const colyToken = joinData.token;
  const sessionId = joinData.sessionId;
  const wsHost = 'fh-118116076e9a4c2a96a99fbb70bea2a0.ecs.us-west-2.on.aws';
  const wsUrl = `wss://${wsHost}/${roomId}?sessionId=${sessionId}&token=${encodeURIComponent(colyToken)}`;

  console.log(`[WS] Conectando a ${wsUrl.slice(0, 80)}...`);

  return new Promise((resolve) => {
    const ws = new WebSocket(wsUrl);
    ws.binaryType = 'arraybuffer';
    
    const sessionData = {
      ws, roomId, sessionId,
      messages: [],
      candidates: [],
      hexLog: [],
      data: { myCards: [], rivalCards: [], playedCards: [], raw: [] }
    };
    sessions[sessionKey] = sessionData;

    const timeout = setTimeout(() => {
      resolve({ ok: false, error: 'timeout', candidates: sessionData.candidates, hexLog: sessionData.hexLog });
    }, 8000);

    ws.on('open', () => console.log('[WS] Conectado'));
    
    ws.on('message', (data) => {
      try {
        const u8 = Buffer.from(data);
        const hex = u8.slice(0, 80).toString('hex').match(/.{2}/g).join(' ');
        sessionData.hexLog.push({ len: u8.length, hex });
        console.log(`[MSG] len=${u8.length} ${hex.slice(0, 60)}`);

        // Intentar decodificar
        const decoded = decodeMsgpack(u8);
        if (decoded) {
          sessionData.messages.push(decoded);
          const cands = extractCards(decoded);
          if (cands.length > 0) {
            sessionData.candidates.push(...cands);
            console.log('[CARDS]', JSON.stringify(cands.map(c => ({ path: c.path, cards: c.cards }))));
          }
        }

        // Si ya tenemos candidatos, resolver
        if (sessionData.candidates.length >= 2) {
          clearTimeout(timeout);
          resolve({
            ok: true,
            candidates: sessionData.candidates,
            hexLog: sessionData.hexLog.slice(0, 10),
            messages: sessionData.messages.slice(0, 5)
          });
        }
      } catch(e) { console.error('[MSG ERR]', e.message); }
    });

    ws.on('error', (e) => {
      console.error('[WS ERR]', e.message);
      clearTimeout(timeout);
      resolve({ ok: false, error: e.message, hexLog: sessionData.hexLog });
    });

    ws.on('close', (code) => {
      console.log('[WS] Cerrado code='+code);
      clearTimeout(timeout);
      if (sessionData.hexLog.length > 0 && !resolve._called) {
        resolve({ ok: true, candidates: sessionData.candidates, hexLog: sessionData.hexLog });
      }
    });
  });
}

// API endpoints
app.get('/health', (req, res) => res.json({ ok: true, version: '1.0.0' }));

app.post('/join', async (req, res) => {
  const { accessToken, roomId } = req.body;
  if (!accessToken || !roomId) return res.json({ ok: false, error: 'accessToken y roomId requeridos' });
  
  try {
    const result = await connectToRoom(accessToken, roomId);
    res.json(result);
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

app.get('/status/:key', (req, res) => {
  const s = sessions[req.params.key];
  if (!s) return res.json({ ok: false, error: 'no session' });
  res.json({ ok: true, candidates: s.candidates, hexLog: s.hexLog.slice(0, 5) });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor en puerto ${PORT}`));
