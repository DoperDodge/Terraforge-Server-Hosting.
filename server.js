// ═══════════════════════════════════════════════════════════════════
// TerraForge Relay Server
// Always-on WebSocket server that replaces the browser-based PeerJS host.
// Clients connect via WebSocket instead of PeerJS when joining a relay room.
// The server holds world state, relays player positions, block changes,
// chat, entity syncs, and PvP hits — exactly like TerraForge's host does.
// ═══════════════════════════════════════════════════════════════════

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

// ─── Room Storage ──────────────────────────────────────────────────
const rooms = new Map(); // code -> Room

class Room {
  constructor(code, config) {
    this.code = code;
    this.name = config.name || 'TerraForge World';
    this.host = config.host || 'Server';
    this.worldSize = config.worldSize || 'small';
    this.gameMode = config.gameMode || 'survival';
    this.maxPlayers = config.maxPlayers || 8;
    this.seed = config.seed || Math.floor(Math.random() * 999999) + 1;
    this.pvp = config.pvp || false;
    this.createdAt = Date.now();

    // World dimensions from TerraForge's WORLD_SIZES
    const sizes = { small: [600, 1400], medium: [1200, 1400], large: [2400, 1400] };
    const [w, h] = sizes[this.worldSize] || sizes.small;
    this.worldW = w;
    this.worldH = h;
    this.surfaceY = Math.floor(h * 0.12); // matches TerraForge's SURFACE_Y ratio

    // World state: we don't generate the world server-side.
    // Instead, the first client to connect becomes the "world source" —
    // they generate from the seed and upload their pristine world.
    // After that, we track block changes (diffs) on top of that seed.
    this.worldReady = false;
    this.worldSeed = this.seed;
    this.blockDiffs = []; // [bx, by, blockId, bx, by, blockId, ...]
    this.templeX = 0;
    this.templeGuardianDefeated = false;

    // Game state — gameTime is a float where fractional part is time-of-day (0.0–1.0)
    // dayLength=900 means one full day cycle = 900 real seconds
    this.gameTime = 0.3; // start at ~7:12 AM, matching client default
    this.dayLength = 900;
    this.entities = [];

    // Connected clients
    this.clients = new Map(); // clientId -> { ws, name, lastPos, skinData }

    // Tick: advance game time (TerraForge runs at ~60fps, time increments 1/frame)
    this.tickInterval = setInterval(() => this.tick(), 1000);
    this.entitySyncInterval = setInterval(() => this.broadcastEntitySync(), 333); // ~3x/sec
    this.timeSyncInterval = setInterval(() => {
      this.broadcast({ type: 'time_sync', t: this.gameTime });
    }, 1000);

    console.log(`[Room ${code}] Created: "${this.name}" (${this.worldSize}, seed: ${this.seed})`);
  }

  tick() {
    // Advance game time: 1 tick = 1 real second, full day = dayLength seconds
    this.gameTime += 1 / this.dayLength;
  }

  addClient(clientId, ws, name) {
    if (this.clients.size >= this.maxPlayers) return false;

    this.clients.set(clientId, {
      ws, name: name || 'Player',
      lastPos: null, skinData: null,
    });

    // Notify existing players
    this.broadcast({
      type: 'chat',
      text: `${name || 'Player'} joined the game!`
    }, clientId);

    // Send world sync to the new client
    this.sendWorldSync(clientId);

    console.log(`[Room ${this.code}] ${name} joined (${this.clients.size}/${this.maxPlayers})`);
    return true;
  }

  removeClient(clientId) {
    const client = this.clients.get(clientId);
    if (!client) return;

    this.clients.delete(clientId);

    // Notify remaining players
    this.broadcast({
      type: 'chat',
      text: `${client.name} left the game.`
    });

    // Send updated player list (remove this player's position)
    // Other clients will stop rendering them when they stop receiving pos updates

    console.log(`[Room ${this.code}] ${client.name} left (${this.clients.size}/${this.maxPlayers})`);

    // Auto-cleanup: destroy room if empty for 5 minutes
    if (this.clients.size === 0) {
      this._emptyTimer = setTimeout(() => {
        if (this.clients.size === 0) {
          this.destroy();
        }
      }, 5 * 60 * 1000);
    }
  }

  sendWorldSync(clientId) {
    const client = this.clients.get(clientId);
    if (!client) return;

    // Send the seed-based world sync (client generates terrain from seed)
    client.ws.send(JSON.stringify({
      type: 'world_sync',
      worldSeed: this.worldSeed,
      worldSizeKey: this.worldSize,
      worldW: this.worldW,
      worldH: this.worldH,
      surfaceY: this.surfaceY,
      gameTime: this.gameTime,
      templeX: this.templeX,
      templeGuardianDefeated: this.templeGuardianDefeated,
    }));

    // Send accumulated block diffs in chunks
    if (this.blockDiffs.length > 0) {
      const CHUNK = 3000; // 1000 blocks per chunk (3 ints each)
      for (let i = 0; i < this.blockDiffs.length; i += CHUNK) {
        const chunk = this.blockDiffs.slice(i, i + CHUNK);
        client.ws.send(JSON.stringify({
          type: 'world_diffs',
          diffs: chunk,
          done: i + CHUNK >= this.blockDiffs.length,
        }));
      }
    } else {
      client.ws.send(JSON.stringify({
        type: 'world_diffs',
        diffs: [],
        done: true,
      }));
    }

    // Send current entity state
    if (this.entities.length > 0) {
      client.ws.send(JSON.stringify({
        type: 'entity_sync',
        entities: this.entities,
      }));
    }
  }

  handleMessage(clientId, data) {
    if (!data || !data.type) return;

    const client = this.clients.get(clientId);
    if (!client) return;

    switch (data.type) {
      case 'player_pos':
        // Store latest position
        client.lastPos = data;
        if (data.name) client.name = data.name;
        if (data.skin) client.skinData = data.skin;

        // Relay to all other clients with _from tag (like TerraForge host does)
        data._from = clientId;
        this.broadcast(data, clientId);
        break;

      case 'block_change':
        // Validate bounds
        if (data.bx >= 0 && data.bx < this.worldW &&
            data.by >= 0 && data.by < this.worldH) {
          // Store the diff
          // Check if this block was already changed — update existing or add new
          let found = false;
          for (let i = 0; i < this.blockDiffs.length; i += 3) {
            if (this.blockDiffs[i] === data.bx && this.blockDiffs[i + 1] === data.by) {
              this.blockDiffs[i + 2] = data.blockId;
              found = true;
              break;
            }
          }
          if (!found) {
            this.blockDiffs.push(data.bx, data.by, data.blockId);
          }

          // Relay to all other clients
          this.broadcast(data, clientId);
        }
        break;

      case 'chat':
        // Relay chat to all other clients
        this.broadcast(data, clientId);
        break;

      case 'entity_sync':
        // Accept entity updates from any client (first connected acts as entity authority)
        if (data.entities) {
          this.entities = data.entities;
          this.broadcast(data, clientId);
        }
        break;

      case 'pvp_hit':
        // Relay PvP hits to all other clients
        this.broadcast(data, clientId);
        break;

      case 'request_world':
        // Re-send world to requesting client
        this.sendWorldSync(clientId);
        break;

      case 'time_sync':
        // Allow host (first client / entity authority) to set time via /time commands
        if (data.t !== undefined) {
          this.gameTime = data.t;
          this.broadcast({ type: 'time_sync', t: this.gameTime });
        }
        break;

      default:
        // Forward unknown message types to all other clients
        this.broadcast(data, clientId);
        break;
    }
  }

  broadcast(data, excludeClientId = null) {
    const msg = typeof data === 'string' ? data : JSON.stringify(data);
    for (const [id, client] of this.clients) {
      if (id !== excludeClientId && client.ws.readyState === 1) {
        try { client.ws.send(msg); } catch (e) { /* ignore */ }
      }
    }
  }

  broadcastEntitySync() {
    if (this.entities.length > 0 && this.clients.size > 0) {
      this.broadcast({ type: 'entity_sync', entities: this.entities });
    }
  }

  destroy() {
    clearInterval(this.tickInterval);
    clearInterval(this.entitySyncInterval);
    clearInterval(this.timeSyncInterval);
    if (this._emptyTimer) clearTimeout(this._emptyTimer);

    // Disconnect all clients
    for (const [id, client] of this.clients) {
      try { client.ws.close(1000, 'Room closed'); } catch (e) { /* ignore */ }
    }
    this.clients.clear();

    rooms.delete(this.code);
    console.log(`[Room ${this.code}] Destroyed`);
  }

  toJSON() {
    return {
      code: this.code,
      name: this.name,
      host: this.host,
      worldSize: this.worldSize,
      gameMode: this.gameMode,
      maxPlayers: this.maxPlayers,
      seed: this.seed,
      pvp: this.pvp,
      players: this.clients.size,
      createdAt: this.createdAt,
      status: 'online',
    };
  }
}

// ─── Room Code Generation ──────────────────────────────────────────
function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do {
    code = '';
    for (let i = 0; i < 6; i++) {
      code += chars[Math.floor(Math.random() * chars.length)];
    }
  } while (rooms.has(code));
  return code;
}

function generateClientId() {
  return 'c_' + crypto.randomBytes(8).toString('hex');
}

// ─── REST API (for the website / server browser) ───────────────────
app.use(express.json());

// CORS — allow the website to reach us
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// Health check
app.get('/', (req, res) => {
  res.json({
    service: 'TerraForge Relay Server',
    rooms: rooms.size,
    uptime: process.uptime(),
  });
});

// List all active rooms
app.get('/api/rooms', (req, res) => {
  const list = [];
  for (const room of rooms.values()) {
    list.push(room.toJSON());
  }
  res.json(list);
});

// Get a specific room
app.get('/api/rooms/:code', (req, res) => {
  const room = rooms.get(req.params.code.toUpperCase());
  if (!room) return res.status(404).json({ error: 'Room not found' });
  res.json(room.toJSON());
});

// Create a new room
app.post('/api/rooms', (req, res) => {
  const { name, host, worldSize, gameMode, maxPlayers, seed, pvp } = req.body || {};
  const code = generateRoomCode();
  const room = new Room(code, {
    name, host, worldSize, gameMode,
    maxPlayers: Math.min(maxPlayers || 8, 20),
    seed: seed || 0,
    pvp: !!pvp,
  });
  rooms.set(code, room);
  res.json(room.toJSON());
});

// Delete a room
app.delete('/api/rooms/:code', (req, res) => {
  const room = rooms.get(req.params.code.toUpperCase());
  if (!room) return res.status(404).json({ error: 'Room not found' });
  room.destroy();
  res.json({ ok: true });
});

// ─── WebSocket Handling ────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const clientId = generateClientId();
  let currentRoom = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // ── Connection-level messages ──
    if (data.type === 'join_room') {
      const code = (data.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room not found' }));
        return;
      }
      if (room.clients.size >= room.maxPlayers) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room is full' }));
        return;
      }

      // Leave previous room if any
      if (currentRoom) {
        currentRoom.removeClient(clientId);
      }

      currentRoom = room;
      const name = data.name || 'Player';
      room.addClient(clientId, ws, name);

      ws.send(JSON.stringify({
        type: 'joined',
        code: room.code,
        clientId: clientId,
        roomInfo: room.toJSON(),
      }));
      return;
    }

    if (data.type === 'create_room') {
      const code = generateRoomCode();
      const room = new Room(code, {
        name: data.name,
        host: data.host,
        worldSize: data.worldSize,
        gameMode: data.gameMode,
        maxPlayers: data.maxPlayers,
        seed: data.seed,
        pvp: data.pvp,
      });
      rooms.set(code, room);

      // Auto-join the creator
      currentRoom = room;
      room.addClient(clientId, ws, data.host || 'Host');

      ws.send(JSON.stringify({
        type: 'room_created',
        code: room.code,
        clientId: clientId,
        roomInfo: room.toJSON(),
      }));
      return;
    }

    if (data.type === 'list_rooms') {
      const list = [];
      for (const room of rooms.values()) {
        list.push(room.toJSON());
      }
      ws.send(JSON.stringify({ type: 'room_list', rooms: list }));
      return;
    }

    // ── Game messages — forward to room ──
    if (currentRoom) {
      currentRoom.handleMessage(clientId, data);
    }
  });

  ws.on('close', () => {
    if (currentRoom) {
      currentRoom.removeClient(clientId);
    }
  });

  ws.on('error', () => {
    if (currentRoom) {
      currentRoom.removeClient(clientId);
    }
  });

  // Send welcome
  ws.send(JSON.stringify({
    type: 'welcome',
    clientId: clientId,
    message: 'TerraForge Relay Server',
  }));
});

// ─── Periodic cleanup of empty rooms ───────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    // Auto-destroy rooms empty for over 30 minutes
    if (room.clients.size === 0 && now - room.createdAt > 30 * 60 * 1000) {
      room.destroy();
    }
    // Auto-destroy rooms older than 24 hours
    if (now - room.createdAt > 24 * 60 * 60 * 1000) {
      room.destroy();
    }
  }
}, 60 * 1000);

// ─── Start ─────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`TerraForge Relay Server running on port ${PORT}`);
  console.log(`REST API: http://localhost:${PORT}/api/rooms`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
});
