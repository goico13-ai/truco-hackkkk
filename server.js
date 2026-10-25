const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const SUIT_NAMES = ['espada','basto','oro','copa'];
const RANK_MAP = [1,2,3,4,5,6,7,10,11,12];

// Parsear carta formato "1_espada", "7_oro" etc
function parseCardStr(s) {
  if (!s || typeof s !== 'string') return null;
  const low = s.toLowerCase();
  const suit = SUIT_NAMES.findIndex(n => low.includes(n));
  const m = low.match(/\b([1-9]|1[012])\b/);
  if (suit >= 0 && m) return { suit, rank: parseInt(m[1]) };
  return null;
}

function findCards(obj, path, depth, results, seen) {
  if (depth > 15 || !obj || typeof obj !== 'object') return;
  if (seen.has(obj)) return;
  seen.add(obj);
  if (Array.isArray(obj)) {
    // Array de strings tipo ["1_espada","7_oro","3_copa"]
    if (obj.length >= 1 && obj.length <= 10) {
      const cards = obj.map(x => {
        if (typeof x === 'string') return parseCardStr(x);
        if (typeof x === 'object' && x) {
          // objeto con campos carta/palo/numero
          const keys = Object.keys(x).join(',').toLowerCase();
          if (keys.includes('suit') || keys.includes('rank') || keys.includes('palo') || keys.includes('numero') || keys.includes('carta')) {
            const suit = x.suit ?? x.palo ?? x.s;
            const rank = x.rank ?? x.numero ?? x.n ?? x.valor;
            if (suit !== undefined && rank !== undefined) return { suit: typeof suit === 'number' ? suit : SUIT_NAMES.indexOf(String(suit).toLowerCase()), rank: parseInt(rank) };
          }
          // objeto con campo id tipo "7_espada"
          const id = x.id ?? x.carta ?? x.card ?? x.name;
          if (typeof id === 'string') return parseCardStr(id);
        }
        return null;
      }).filter(Boolean);
      if (cards.length === obj.length && cards.length > 0) {
        results.push({ path, cards });
        return;
      }
    }
    obj.forEach((item, i) => findCards(item, `${path}[${i}]`, depth + 1, results, seen));
  } else {
    // Buscar campos específicos de cartas
    const cardFields = ['hand', 'mano', 'cartas', 'cards', 'myCards', 'opponentCards', 
                        'playerCards', 'rivalCards', 'carta', 'naipes', 'mazo'];
    Object.keys(obj).forEach(k => {
      const priority = cardFields.some(f => k.toLowerCase().includes(f)) ? 0 : 1;
      findCards(obj[k], `${path}.${k}`, depth + priority, results, seen);
    });
  }
}

function extractCards(obj) {
  const results = [];
  try { findCards(obj, 'root', 0, results, new WeakSet()); } catch(e) {}
  return results;
}

/* ══ GRAN TRUCO - CONECTAR AL WS FAYE ══ */
async function connectGranTruco(connectSid, wsAuthToken) {
  const BASE = 'https://trucogame.com';
  const cookieStr = `connect.sid=${connectSid}`;

  // Paso 1: obtener ws-token si no lo tenemos
  let wsToken = wsAuthToken;
  if (!wsToken) {
    console.log('[GT] Obteniendo ws-token...');
    try {
      const res = await fetch(`${BASE}/api/auth/ws-token`, {
        method: 'POST',
        headers: {
          'Cookie': cookieStr,
          'Content-Type': 'application/json',
          'Origin': BASE,
          'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 GranTrucoArgentino 1.1.0 GTABuild/54'
        }
      });
      const data = await res.json();
      console.log('[GT] ws-token response:', JSON.stringify(data).slice(0, 200));
      wsToken = data.wsAuthToken || data.token;
    } catch(e) { console.error('[GT] Error obteniendo ws-token:', e.message); }
  }

  if (!wsToken) return { ok: false, error: 'No se pudo obtener wsAuthToken' };

  // Paso 2: obtener sala activa
  console.log('[GT] Buscando sala activa...');
  let roomHost = null, attemptId = null;
  try {
    const res = await fetch(`${BASE}/api/users-info`, {
      method: 'POST',
      headers: {
        'Cookie': cookieStr,
        'Content-Type': 'application/json',
        'Origin': BASE,
        'User-Agent': 'Mozilla/5.0 GranTrucoArgentino 1.1.0'
      }
    });
    const data = await res.json();
    console.log('[GT] users-info:', JSON.stringify(data).slice(0, 300));
    // Buscar sala en la respuesta
    roomHost = data.roomHost || data.host || data.server || data.room;
    attemptId = data.attemptId || data.attempt || require('crypto').randomUUID();
  } catch(e) { console.error('[GT] Error users-info:', e.message); }

  // Si no tenemos roomHost, usar el conocido
  if (!roomHost) {
    roomHost = 'pr-room2-16-f4f45cf7aa.trucogame.com';
    attemptId = require('crypto').randomUUID();
    console.log('[GT] Usando roomHost conocido:', roomHost);
  }

  // Paso 3: conectar WebSocket Faye
  const wsUrl = `wss://${roomHost}/faye?attemptId=${attemptId}`;
  console.log('[GT] Conectando WS:', wsUrl);

  return new Promise((resolve) => {
    const ws = new WebSocket(wsUrl, {
      headers: {
        'Cookie': cookieStr,
        'Origin': 'https://trucogame.com',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 GranTrucoArgentino 1.1.0 GTABuild/54'
      }
    });

    const data = { messages: [], candidates: [], rawLog: [] };
    const timeout = setTimeout(() => {
      ws.close();
      resolve({ ok: data.candidates.length > 0, candidates: data.candidates, rawLog: data.rawLog.slice(0, 10), timeout: true });
    }, 12000);

    ws.on('open', () => {
      console.log('[GT WS] Conectado');
      // Handshake Faye
      ws.send(JSON.stringify([{
        channel: '/meta/handshake',
        version: '1.0',
        minimumVersion: '1.0',
        supportedConnectionTypes: ['websocket'],
        id: '1'
      }]));
    });

    ws.on('message', (raw) => {
      try {
        const str = raw.toString();
        console.log('[GT MSG]', str.slice(0, 200));
        data.rawLog.push(str.slice(0, 300));

        const msgs = JSON.parse(str);
        if (!Array.isArray(msgs)) return;

        msgs.forEach(msg => {
          // Handshake response → subscribe
          if (msg.channel === '/meta/handshake' && msg.successful && msg.clientId) {
            console.log('[GT] clientId:', msg.clientId);
            // Suscribirse a todos los canales relevantes
            ['/game/state', '/game/cards', '/game/hand', '/room/state', '/game', '/mesa', '/jugada'].forEach((ch, i) => {
              ws.send(JSON.stringify([{
                channel: '/meta/subscribe',
                subscription: ch,
                clientId: msg.clientId,
                id: String(i + 2),
                ext: { authToken: wsToken }
              }]));
            });
            // Connect
            ws.send(JSON.stringify([{
              channel: '/meta/connect',
              connectionType: 'websocket',
              clientId: msg.clientId,
              id: '10'
            }]));
          }

          // Mensajes de datos
          if (msg.data) {
            data.messages.push(msg);
            const cands = extractCards(msg.data);
            if (cands.length > 0) {
              data.candidates.push(...cands);
              console.log('[GT CARDS]', JSON.stringify(cands.map(c => ({ path: c.path, n: c.cards.length, cards: c.cards }))));
            }
            // También buscar en el mensaje completo
            const allCands = extractCards(msg);
            allCands.forEach(c => {
              if (!data.candidates.find(x => x.path === c.path)) {
                data.candidates.push(c);
              }
            });
          }
        });

        if (data.candidates.length >= 2) {
          clearTimeout(timeout);
          ws.close();
          resolve({ ok: true, candidates: data.candidates, rawLog: data.rawLog.slice(0, 10) });
        }
      } catch(e) { console.error('[GT MSG ERR]', e.message); }
    });

    ws.on('error', (e) => {
      console.error('[GT WS ERR]', e.message);
      clearTimeout(timeout);
      resolve({ ok: false, error: e.message, rawLog: data.rawLog });
    });

    ws.on('close', (code) => {
      console.log('[GT WS] Cerrado code=' + code);
      clearTimeout(timeout);
      resolve({ ok: data.candidates.length > 0, candidates: data.candidates, rawLog: data.rawLog, closeCode: code });
    });
  });
}

/* ══ TRUCOX - COLYSEUS ══ */
async function connectTrucox(accessToken, credential, roomId) {
  const BASE = 'https://www.trucox.com';
  let joinToken = null, sessionId = null;

  if (accessToken) {
    try {
      const res = await fetch(`${BASE}/game/matchmake/joinById/${roomId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${accessToken}` },
        body: JSON.stringify({})
      });
      const data = await res.json();
      if (data.token) { joinToken = data.token; sessionId = data.sessionId; }
    } catch(e) {}
  }

  if (!joinToken && credential) {
    for (const url of [`${BASE}/game/matchmake/reconnect/${roomId}`, `${BASE}/game/reconnect`]) {
      try {
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ reconnectionToken: credential })
        });
        const data = await res.json();
        if (data.token) { joinToken = data.token; sessionId = data.sessionId; break; }
      } catch(e) {}
    }
  }

  if (!joinToken) return { ok: false, error: 'No token' };

  const wsUrl = `wss://fh-118116076e9a4c2a96a99fbb70bea2a0.ecs.us-west-2.on.aws/${roomId}?sessionId=${sessionId}&token=${encodeURIComponent(joinToken)}`;
  return connectWS(wsUrl);
}

async function connectWS(wsUrl) {
  return new Promise((resolve) => {
    const ws = new WebSocket(wsUrl);
    ws.binaryType = 'arraybuffer';
    const data = { hexLog: [], candidates: [] };
    const timeout = setTimeout(() => { ws.close(); resolve({ ok: data.candidates.length > 0, candidates: data.candidates, hexLog: data.hexLog.slice(0,5) }); }, 10000);
    ws.on('message', (msg) => {
      try {
        const u8 = Buffer.from(msg);
        data.hexLog.push({ len: u8.length, hex: u8.slice(0,40).toString('hex').match(/.{2}/g).join(' ') });
      } catch(e) {}
    });
    ws.on('error', (e) => { clearTimeout(timeout); resolve({ ok: false, error: e.message }); });
    ws.on('close', () => { clearTimeout(timeout); resolve({ ok: data.candidates.length > 0, candidates: data.candidates, hexLog: data.hexLog }); });
  });
}

/* ══ ENDPOINTS ══ */
app.get('/health', (req, res) => res.json({ ok: true, version: '4.0.0' }));

// Gran Truco Argentino
app.post('/gran-truco', async (req, res) => {
  const { connectSid, wsAuthToken } = req.body;
  if (!connectSid) return res.json({ ok: false, error: 'connectSid requerido' });
  console.log('[GT] Nueva conexión, sid=' + connectSid.slice(0, 20));
  try {
    const result = await connectGranTruco(connectSid, wsAuthToken);
    res.json(result);
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

// TrucoX
app.post('/join', async (req, res) => {
  const { accessToken, credential, roomId } = req.body;
  if (!roomId) return res.json({ ok: false, error: 'roomId requerido' });
  try {
    const result = await connectTrucox(accessToken, credential, roomId);
    res.json(result);
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[SERVER] v4.0.0 puerto ${PORT}`));
