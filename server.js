const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');

const app = express();
app.use(express.static('public'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const MAX_POR_SALA = 2;
const rooms = new Map(); // sala -> Map(id -> { ws, name, lat, lng, acc, ts })

function broadcast(room) {
  const r = rooms.get(room);
  if (!r) return;
  const users = [...r.entries()].map(([id, u]) => ({
    id, name: u.name, lat: u.lat, lng: u.lng, acc: u.acc, ts: u.ts
  }));
  const msg = JSON.stringify({ type: 'state', users });
  for (const u of r.values()) if (u.ws.readyState === 1) u.ws.send(msg);
}

wss.on('connection', (ws) => {
  let room = null, id = null;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join') {
      room = String(m.room || '').trim().toLowerCase().slice(0, 40);
      id = String(m.id || '').slice(0, 64);
      if (!room || !id) return;
      if (!rooms.has(room)) rooms.set(room, new Map());
      const r = rooms.get(room);
      if (!r.has(id) && r.size >= MAX_POR_SALA) {
        ws.send(JSON.stringify({ type: 'full' }));
        room = null;
        return;
      }
      const old = r.get(id);
      if (old && old.ws !== ws) old.ws.close();
      r.set(id, { ws, name: String(m.name || 'Alguém').slice(0, 20), lat: null, lng: null, acc: null, ts: null });
      broadcast(room);
    }

    if (m.type === 'pos' && room) {
      const u = rooms.get(room)?.get(id);
      if (!u || typeof m.lat !== 'number' || typeof m.lng !== 'number') return;
      Object.assign(u, { lat: m.lat, lng: m.lng, acc: m.acc, ts: Date.now() });
      broadcast(room);
    }
  });

  ws.on('close', () => {
    if (!room) return;
    const r = rooms.get(room);
    if (r && r.get(id)?.ws === ws) {
      r.delete(id);
      if (r.size === 0) rooms.delete(room); else broadcast(room);
    }
  });
});

// derruba conexões mortas
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false; ws.ping();
  });
}, 25000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Rodando em http://localhost:' + PORT));
