const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const { io } = require('socket.io-client');

const app = express();
app.use(cors());
app.use(express.json());

const SUIT_NAMES = ['espada','basto','oro','copa'];
const SUIT_SYMS = ['♠','♣','♦','♥'];

function parseCard(s) {
  if (!s || typeof s !== 'string') return null;
  const low = s.toLowerCase();
  const suit = SUIT_NAMES.findIndex(n => low.includes(n));
  const m = low.match(/\b([1-9]|1[012])\b/);
  if (suit >= 0 && m) return { suit, rank: parseInt(m[1]), str: s };
  // formato numérico tipo "1" "7" etc con palo separado
  return null;
}

function findCards(obj, path, depth, results, seen) {
  if (depth > 12 || !obj || typeof obj !== 'object') return;
  if (seen.has(obj)) return; seen.add(obj);
  if (Array.isArray(obj)) {
    if (obj.length >= 1 && obj.length <= 10) {
      const cards = obj.map(x => {
        if (typeof x === 'string') return parseCard(x);
        if (typeof x === 'object' && x) {
          const id = x.id ?? x.carta ?? x.card ?? x.name ?? x.value;
          if (typeof id === 'string') return parseCard(id);
          if (typeof x.suit !== 'undefined' && typeof x.rank !== 'undefined')
            return { suit: x.suit, rank: x.rank, str: `${x.rank}_${SUIT_NAMES[x.suit]}` };
        }
        return null;
      }).filter(Boolean);
      if (cards.length === obj.length && cards.length > 0) {
        results.push({ path, cards }); return;
      }
    }
    obj.forEach((item, i) => findCards(item, `${path}[${i}]`, depth+1, results, seen));
  } else {
    Object.keys(obj).forEach(k => findCards(obj[k], `${path}.${k}`, depth+1, results, seen));
  }
}

function extractCards(obj) {
  const results = [];
  try { findCards(obj, 'root', 0, results, new WeakSet()); } catch(e) {}
  return results;
}

/* ══ TRUCOBITS SOCKET.IO ══ */
async function connectTrucobits(accessToken, gameId, ioCookie, refreshToken) {
  const BASE = 'https://trucobits.com';

  // Obtener nuevo accessToken si expiró
  let token = accessToken;
  if (!token && refreshToken) {
    try {
      const res = await fetch(`${BASE}/api/auth/refresh`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Cookie': `refresh_token=${refreshToken}; has_session=1`,
          'Origin': BASE
        },
        body: '{}'
      });
      const data = await res.json();
      token = data.accessToken;
      console.log('[TB] Token refrescado:', token ? 'OK' : 'FAIL');
    } catch(e) { console.error('[TB] Error refresh:', e.message); }
  }

  if (!token) return { ok: false, error: 'No accessToken' };

  console.log('[TB] Conectando Socket.IO, gameId:', gameId);

  return new Promise((resolve) => {
    const cookieStr = ioCookie ? `io=${ioCookie}` : '';
    
    const socket = io('https://trucobits.com', {
      path: '/socket.io/',
      transports: ['websocket'],
      auth: { token: `Bearer ${token}` },
      extraHeaders: {
        'Authorization': `Bearer ${token}`,
        'Cookie': cookieStr,
        'Origin': 'https://trucobits.com',
        'Referer': `https://trucobits.com/game/play/${gameId}`
      }
    });

    const data = { events: [], candidates: [], rawLog: [] };
    
    const timeout = setTimeout(() => {
      socket.disconnect();
      resolve({ ok: data.candidates.length > 0, candidates: data.candidates, rawLog: data.rawLog.slice(0,10), timeout: true });
    }, 12000);

    socket.on('connect', () => {
      console.log('[TB] Conectado! id:', socket.id);
      data.rawLog.push('CONNECTED: ' + socket.id);
      
      // Unirse a la partida
      if (gameId) {
        socket.emit('joinGame', { gameId }, (res) => {
          console.log('[TB] joinGame response:', JSON.stringify(res).slice(0, 200));
          data.rawLog.push('joinGame: ' + JSON.stringify(res).slice(0, 200));
        });
        socket.emit('join', { gameId });
        socket.emit('game:join', { gameId });
        socket.emit('room:join', { roomId: gameId });
      }
    });

    socket.on('connect_error', (e) => {
      console.error('[TB] connect_error:', e.message);
      data.rawLog.push('CONNECT_ERROR: ' + e.message);
    });

    // Escuchar todos los eventos
    const origOn = socket.onevent?.bind(socket);
    socket.onAny((event, ...args) => {
      const str = JSON.stringify(args).slice(0, 300);
      console.log(`[TB EVENT] ${event}:`, str);
      data.rawLog.push(`${event}: ${str}`);
      data.events.push({ event, args });
      
      // Buscar cartas en cualquier evento
      args.forEach(arg => {
        if (arg && typeof arg === 'object') {
          const cands = extractCards(arg);
          if (cands.length > 0) {
            data.candidates.push(...cands);
            console.log('[TB CARDS]', JSON.stringify(cands.map(c => ({ path: c.path, cards: c.cards }))));
          }
        }
      });

      if (data.candidates.length >= 2) {
        clearTimeout(timeout);
        socket.disconnect();
        resolve({ ok: true, candidates: data.candidates, rawLog: data.rawLog.slice(0, 15), events: data.events.map(e => e.event) });
      }
    });

    socket.on('disconnect', (reason) => {
      console.log('[TB] Desconectado:', reason);
      clearTimeout(timeout);
      resolve({ ok: data.candidates.length > 0, candidates: data.candidates, rawLog: data.rawLog, events: data.events.map(e => e.event) });
    });
  });
}

/* ══ ENDPOINTS ══ */
app.get('/health', (req, res) => res.json({ ok: true, version: '5.0.0' }));

// Trucobits
app.post('/trucobits', async (req, res) => {
  const { accessToken, gameId, ioCookie, refreshToken } = req.body;
  if (!gameId) return res.json({ ok: false, error: 'gameId requerido' });
  console.log(`[TB] gameId=${gameId}`);
  try {
    const result = await connectTrucobits(accessToken, gameId, ioCookie, refreshToken);
    res.json(result);
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

// Gran Truco (Faye)
app.post('/gran-truco', async (req, res) => {
  res.json({ ok: false, error: 'Usar /trucobits para trucobits.com' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[SERVER] v5.0.0 puerto ${PORT}`));
