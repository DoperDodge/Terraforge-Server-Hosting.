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
const wss = new WebSocketServer({ server, maxPayload: 50 * 1024 * 1024 }); // 50MB for world uploads

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
    this.chestData = {}; // key -> array of 20 slots (shared chest contents)
    this.templeX = 0;
    this.templeGuardianDefeated = false;

    // Game state — gameTime is a float where fractional part is time-of-day (0.0–1.0)
    // dayLength=900 means one full day cycle = 900 real seconds
    this.gameTime = 0.3; // start at ~7:12 AM, matching client default
    this.dayLength = 900;
    this.entities = [];

    // Player data persistence — keyed by player name (case-insensitive)
    // Stores: inventory, armorSlots, wingSlot, playerMana, playerMaxMana, playerGold,
    //         hasDashShield, position, hp, maxHp, gameMode, selectedSlot, skin
    this.playerData = new Map(); // lowercase name -> { ... }

    // Full world upload support (when host loads a save file)
    this.fullWorldData = null; // if set, this replaces seed-based world generation

    // Connected clients
    this.clients = new Map(); // clientId -> { ws, name, lastPos, skinData }
    this.hostClientId = null; // set when room creator joins
    this.admins = new Set(); // clientIds with admin privileges
    this.adminToken = crypto.randomBytes(16).toString('hex'); // secret token for website admin auth

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

    // Check for saved player data (by name, case-insensitive)
    const nameKey = (name || 'Player').toLowerCase();
    const saved = this.playerData.get(nameKey);
    if (saved) {
      // Send stored player data back so client can restore inventory/stats
      try {
        ws.send(JSON.stringify({
          type: 'restore_player_data',
          data: saved,
        }));
      } catch (e) { /* ignore */ }
      console.log(`[Room ${this.code}] Restored player data for "${name}"`);
    }

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

    // NOTE: We do NOT auto-destroy empty rooms.
    // Rooms persist until manually closed by admin or server restart.
    // Player data is retained so players can rejoin and get their items back.
  }

  sendWorldSync(clientId) {
    const client = this.clients.get(clientId);
    if (!client) return;

    // If a full world was uploaded (from a save file), send that instead of seed
    if (this.fullWorldData) {
      client.ws.send(JSON.stringify({
        type: 'world_sync',
        worldSizeKey: this.fullWorldData.worldSizeKey || this.worldSize,
        worldW: this.fullWorldData.worldW || this.worldW,
        worldH: this.fullWorldData.worldH || this.worldH,
        surfaceY: this.fullWorldData.surfaceY || this.surfaceY,
        gameTime: this.gameTime,
        templeX: this.fullWorldData.templeX || this.templeX,
        templeGuardianDefeated: this.fullWorldData.templeGuardianDefeated || this.templeGuardianDefeated,
        chestData: this.chestData,
        worldSeed: this.fullWorldData.worldSeed || this.worldSeed,
        fullWorld: true, // flag telling client this is a full world upload
      }));

      // Send the full world arrays in chunks
      const worldRLE = this.fullWorldData.world;
      const bgRLE = this.fullWorldData.bgWorld;
      const wallRLE = this.fullWorldData.wallWorld;
      const CHUNK = 50000;

      // Send world data
      if (worldRLE && worldRLE.length > 0) {
        for (let i = 0; i < worldRLE.length; i += CHUNK) {
          const chunk = worldRLE.slice(i, i + CHUNK);
          client.ws.send(JSON.stringify({
            type: 'full_world_chunk',
            layer: 'world',
            data: chunk,
            done: i + CHUNK >= worldRLE.length,
          }));
        }
      }
      if (bgRLE && bgRLE.length > 0) {
        for (let i = 0; i < bgRLE.length; i += CHUNK) {
          const chunk = bgRLE.slice(i, i + CHUNK);
          client.ws.send(JSON.stringify({
            type: 'full_world_chunk',
            layer: 'bgWorld',
            data: chunk,
            done: i + CHUNK >= bgRLE.length,
          }));
        }
      }
      if (wallRLE && wallRLE.length > 0) {
        for (let i = 0; i < wallRLE.length; i += CHUNK) {
          const chunk = wallRLE.slice(i, i + CHUNK);
          client.ws.send(JSON.stringify({
            type: 'full_world_chunk',
            layer: 'wallWorld',
            data: chunk,
            done: i + CHUNK >= wallRLE.length,
          }));
        }
      }

      // Send block diffs accumulated on top of the loaded world
      if (this.blockDiffs.length > 0) {
        const DC = 3000;
        for (let i = 0; i < this.blockDiffs.length; i += DC) {
          const chunk = this.blockDiffs.slice(i, i + DC);
          client.ws.send(JSON.stringify({
            type: 'world_diffs',
            diffs: chunk,
            done: i + DC >= this.blockDiffs.length,
          }));
        }
      } else {
        client.ws.send(JSON.stringify({
          type: 'world_diffs',
          diffs: [],
          done: true,
        }));
      }

    } else {
      // Seed-based world sync (original behavior)
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
        chestData: this.chestData,
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

          // If a chest block was destroyed, clean up its stored contents
          if (data.blockId === 0) { // B.AIR
            const chestKey = data.bx + ',' + data.by;
            if (this.chestData[chestKey]) {
              delete this.chestData[chestKey];
            }
          }
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

      case 'chest_sync':
        // Store chest contents server-side and relay to other players
        if (data.key && data.slots) {
          this.chestData[data.key] = data.slots;
          this.broadcast(data, clientId);
        }
        break;

      case 'player_data_sync':
        // Client sends their full player data for persistence
        // Keyed by player name (case-insensitive)
        if (data.playerData && client.name) {
          const nameKey = client.name.toLowerCase();
          this.playerData.set(nameKey, data.playerData);
          // Confirm save
          try {
            client.ws.send(JSON.stringify({ type: 'player_data_saved' }));
          } catch (e) { /* ignore */ }
        }
        break;

      case 'world_upload':
        // Host uploads a full world (from a save file) to replace seed-based generation
        // Only admins/host can do this
        if (!this.admins.has(clientId)) {
          client.ws.send(JSON.stringify({ type: 'chat', text: '[Server] Only admins can upload worlds.' }));
          break;
        }
        if (data.worldData) {
          this.fullWorldData = data.worldData;
          // Update room dimensions from the uploaded world
          if (data.worldData.worldW) this.worldW = data.worldData.worldW;
          if (data.worldData.worldH) this.worldH = data.worldData.worldH;
          if (data.worldData.surfaceY) this.surfaceY = data.worldData.surfaceY;
          if (data.worldData.worldSeed) this.worldSeed = data.worldData.worldSeed;
          if (data.worldData.worldSizeKey) this.worldSize = data.worldData.worldSizeKey;
          if (data.worldData.templeX !== undefined) this.templeX = data.worldData.templeX;
          if (data.worldData.templeGuardianDefeated) this.templeGuardianDefeated = data.worldData.templeGuardianDefeated;
          if (data.worldData.chestData) this.chestData = data.worldData.chestData;
          if (data.worldData.gameTime) this.gameTime = data.worldData.gameTime;
          // Reset block diffs since the full world is now the baseline
          this.blockDiffs = [];
          this.broadcast({ type: 'chat', text: '[Server] World loaded from save file!' });
          client.ws.send(JSON.stringify({ type: 'world_uploaded' }));
          console.log(`[Room ${this.code}] Full world uploaded by ${client.name}`);
        }
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

      case 'admin_cmd':
        // Only admins (including host) can issue admin commands
        if (!this.admins.has(clientId)) {
          client.ws.send(JSON.stringify({ type: 'chat', text: '[Server] You are not an admin.' }));
          break;
        }
        this.handleAdminCmd(clientId, data);
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

  handleAdminCmd(hostId, data) {
    const cmd = data.cmd;
    const hostClient = this.clients.get(hostId);
    if (!hostClient) return;

    const sendToHost = (msg) => {
      try { hostClient.ws.send(JSON.stringify({ type: 'chat', text: msg })); } catch(e) {}
    };

    // Find target client by name (case-insensitive)
    const findClient = (name) => {
      if (!name) return null;
      const lower = name.toLowerCase();
      for (const [id, c] of this.clients) {
        if (c.name && c.name.toLowerCase() === lower) return { id, client: c };
      }
      return null;
    };

    switch (cmd) {
      case 'tp_player': {
        // Teleport a player to coordinates
        const target = findClient(data.target);
        if (!target) { sendToHost('[Admin] Player "' + data.target + '" not found.'); break; }
        target.client.ws.send(JSON.stringify({
          type: 'admin_action', action: 'teleport', x: data.x, y: data.y
        }));
        this.broadcast({ type: 'chat', text: '[Admin] ' + target.client.name + ' was teleported.' });
        break;
      }
      case 'tp_to_player': {
        // Teleport one player to another
        const src = findClient(data.target);
        const dest = findClient(data.destination);
        if (!src) { sendToHost('[Admin] Player "' + data.target + '" not found.'); break; }
        if (!dest) { sendToHost('[Admin] Player "' + data.destination + '" not found.'); break; }
        if (!dest.client.lastPos) { sendToHost('[Admin] Destination player has no position data.'); break; }
        src.client.ws.send(JSON.stringify({
          type: 'admin_action', action: 'teleport', x: dest.client.lastPos.x, y: dest.client.lastPos.y
        }));
        this.broadcast({ type: 'chat', text: '[Admin] ' + src.client.name + ' was teleported to ' + dest.client.name + '.' });
        break;
      }
      case 'kill_player': {
        const target = findClient(data.target);
        if (!target) { sendToHost('[Admin] Player "' + data.target + '" not found.'); break; }
        target.client.ws.send(JSON.stringify({ type: 'admin_action', action: 'kill' }));
        this.broadcast({ type: 'chat', text: '[Admin] ' + target.client.name + ' was slain by the server.' });
        break;
      }
      case 'heal_player': {
        const target = findClient(data.target);
        if (!target) { sendToHost('[Admin] Player "' + data.target + '" not found.'); break; }
        target.client.ws.send(JSON.stringify({ type: 'admin_action', action: 'heal' }));
        this.broadcast({ type: 'chat', text: '[Admin] ' + target.client.name + ' was healed.' });
        break;
      }
      case 'give_item': {
        const target = findClient(data.target);
        if (!target) { sendToHost('[Admin] Player "' + data.target + '" not found.'); break; }
        target.client.ws.send(JSON.stringify({
          type: 'admin_action', action: 'give', itemId: data.itemId, count: data.count || 1
        }));
        this.broadcast({ type: 'chat', text: '[Admin] Gave items to ' + target.client.name + '.' });
        break;
      }
      case 'spawn_entity': {
        // Relay spawn to entity authority (host client)
        hostClient.ws.send(JSON.stringify({
          type: 'admin_action', action: 'spawn_entity',
          entityType: data.entityType, x: data.x, y: data.y, count: data.count || 1
        }));
        sendToHost('[Admin] Spawning ' + (data.count || 1) + 'x ' + data.entityType + '.');
        break;
      }
      case 'kill_entities': {
        // Kill all entities or specific type
        this.entities = data.entityType
          ? this.entities.filter(e => e.type !== data.entityType)
          : [];
        this.broadcast({ type: 'entity_sync', entities: this.entities });
        this.broadcast({ type: 'chat', text: '[Admin] Cleared ' + (data.entityType || 'all') + ' entities.' });
        break;
      }
      case 'kick_player': {
        const target = findClient(data.target);
        if (!target) { sendToHost('[Admin] Player "' + data.target + '" not found.'); break; }
        if (target.id === hostId) { sendToHost('[Admin] You cannot kick yourself.'); break; }
        target.client.ws.send(JSON.stringify({ type: 'admin_action', action: 'kicked', reason: data.reason || 'Kicked by host' }));
        this.broadcast({ type: 'chat', text: '[Admin] ' + target.client.name + ' was kicked.' });
        // Delay to let the message arrive, then close
        setTimeout(() => { try { target.client.ws.close(1000, 'Kicked'); } catch(e) {} }, 200);
        break;
      }
      case 'set_time': {
        if (data.t !== undefined) {
          this.gameTime = data.t;
          this.broadcast({ type: 'time_sync', t: this.gameTime });
          sendToHost('[Admin] Time set.');
        }
        break;
      }
      case 'set_pvp': {
        this.pvp = !!data.enabled;
        this.broadcast({ type: 'admin_action', action: 'set_pvp', enabled: this.pvp });
        this.broadcast({ type: 'chat', text: '[Admin] PvP ' + (this.pvp ? 'enabled' : 'disabled') + ' server-wide.' });
        break;
      }
      case 'broadcast_msg': {
        if (data.text) {
          this.broadcast({ type: 'chat', text: '[Server] ' + data.text });
        }
        break;
      }
      case 'save_world': {
        // Request all connected clients to send their player data,
        // then tell the requesting admin to trigger a client-side save download
        // First, broadcast a save request to all clients
        this.broadcast({ type: 'admin_action', action: 'save_player_data' });
        // Tell the admin client to initiate a world save download after a short delay
        // (to let player data arrive first)
        setTimeout(() => {
          const adminClient = this.clients.get(hostId);
          if (adminClient) {
            adminClient.ws.send(JSON.stringify({
              type: 'admin_action',
              action: 'download_world_save',
              roomData: {
                seed: this.worldSeed,
                worldSize: this.worldSize,
                worldW: this.worldW,
                worldH: this.worldH,
                surfaceY: this.surfaceY,
                gameTime: this.gameTime,
                chestData: this.chestData,
                blockDiffs: this.blockDiffs,
                templeX: this.templeX,
                templeGuardianDefeated: this.templeGuardianDefeated,
                hasFullWorld: !!this.fullWorldData,
                fullWorldData: this.fullWorldData || null,
                playerDataEntries: Array.from(this.playerData.entries()),
              }
            }));
          }
        }, 500);
        sendToHost('[Admin] Saving world and player data...');
        break;
      }
      case 'force_save_players': {
        // Force all connected players to send their data to server
        this.broadcast({ type: 'admin_action', action: 'save_player_data' });
        sendToHost('[Admin] Requested all players to save their data.');
        break;
      }
      case 'list_players': {
        const players = [];
        for (const [id, c] of this.clients) {
          if (c.isWebAdmin) continue; // don't list web admin connections as players
          const pos = c.lastPos ? `(${Math.floor(c.lastPos.x/8)}, ${Math.floor(c.lastPos.y/8)})` : '(?)';
          players.push({ name: c.name, id: id, pos: pos, isHost: id === this.hostClientId, isAdmin: this.admins.has(id) });
        }
        hostClient.ws.send(JSON.stringify({
          type: 'admin_player_list', players: players
        }));
        break;
      }
      case 'promote_admin': {
        // Only the host can promote/demote
        if (clientId !== this.hostClientId) { sendToHost('[Admin] Only the host can promote admins.'); break; }
        const target = findClient(data.target);
        if (!target) { sendToHost('[Admin] Player "' + data.target + '" not found.'); break; }
        this.admins.add(target.id);
        target.client.ws.send(JSON.stringify({ type: 'admin_action', action: 'promoted' }));
        this.broadcast({ type: 'chat', text: '[Server] ' + target.client.name + ' is now an admin.' });
        break;
      }
      case 'demote_admin': {
        if (clientId !== this.hostClientId) { sendToHost('[Admin] Only the host can demote admins.'); break; }
        const target = findClient(data.target);
        if (!target) { sendToHost('[Admin] Player "' + data.target + '" not found.'); break; }
        if (target.id === this.hostClientId) { sendToHost('[Admin] Cannot demote the host.'); break; }
        this.admins.delete(target.id);
        target.client.ws.send(JSON.stringify({ type: 'admin_action', action: 'demoted' }));
        this.broadcast({ type: 'chat', text: '[Server] ' + target.client.name + ' is no longer an admin.' });
        break;
      }
      default:
        sendToHost('[Admin] Unknown command: ' + cmd);
    }
  }

  destroy() {
    clearInterval(this.tickInterval);
    clearInterval(this.entitySyncInterval);
    clearInterval(this.timeSyncInterval);

    // Disconnect all clients
    for (const [id, client] of this.clients) {
      try { client.ws.close(1000, 'Room closed'); } catch (e) { /* ignore */ }
    }
    this.clients.clear();

    rooms.delete(this.code);
    console.log(`[Room ${this.code}] Destroyed`);
  }

  toJSON() {
    let playerCount = 0;
    for (const [, c] of this.clients) { if (!c.isWebAdmin) playerCount++; }
    return {
      code: this.code,
      name: this.name,
      host: this.host,
      worldSize: this.worldSize,
      gameMode: this.gameMode,
      maxPlayers: this.maxPlayers,
      seed: this.seed,
      pvp: this.pvp,
      players: playerCount,
      savedPlayers: this.playerData.size,
      createdAt: this.createdAt,
      uptimeMinutes: Math.floor((Date.now() - this.createdAt) / 60000),
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
  const resp = room.toJSON();
  resp.adminToken = room.adminToken; // only returned at creation
  res.json(resp);
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
      room.hostClientId = clientId;
      room.admins.add(clientId); // host is always admin
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

    // Admin join — website admin panel connects with token auth
    if (data.type === 'admin_join') {
      const code = (data.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room not found' }));
        return;
      }
      if (data.token !== room.adminToken) {
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid admin token' }));
        return;
      }
      currentRoom = room;
      room.admins.add(clientId);
      // Store a lightweight admin-only "client" for message routing
      room.clients.set(clientId, {
        ws, name: '🛡 WebAdmin', lastPos: null, skinData: null, isWebAdmin: true,
      });
      ws.send(JSON.stringify({
        type: 'admin_joined', code: room.code, clientId: clientId,
        roomInfo: room.toJSON(),
      }));
      console.log(`[Room ${code}] Web admin connected`);
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
// Rooms are NOT auto-destroyed based on time.
// They persist until manually closed by an admin or server restart.
// This interval only logs stats for monitoring.
setInterval(() => {
  const now = Date.now();
  let activeRooms = 0;
  let totalPlayers = 0;
  for (const [code, room] of rooms) {
    activeRooms++;
    totalPlayers += room.clients.size;
  }
  if (activeRooms > 0) {
    console.log(`[Stats] ${activeRooms} room(s), ${totalPlayers} player(s) connected`);
  }
}, 5 * 60 * 1000); // Log every 5 minutes

// ─── Start ─────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`TerraForge Relay Server running on port ${PORT}`);
  console.log(`REST API: http://localhost:${PORT}/api/rooms`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
});
