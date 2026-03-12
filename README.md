# TerraForge Relay Server

An always-on WebSocket relay server for TerraForge multiplayer. Replaces the browser-based PeerJS host so that game servers persist independently — no one needs to keep a tab open.

## How It Works

```
┌──────────────┐     WebSocket      ┌─────────────────┐     WebSocket      ┌──────────────┐
│  Player A    │ ◄─────────────────► │  Relay Server   │ ◄─────────────────► │  Player B    │
│ (TerraForge) │                     │  (always-on)    │                     │ (TerraForge) │
└──────────────┘                     │                  │                     └──────────────┘
                                     │ - Holds world    │
┌──────────────┐     HTTP REST       │ - Tracks diffs   │
│  Website     │ ◄─────────────────► │ - Relays msgs    │
│ (browser UI) │                     │ - Manages rooms   │
└──────────────┘                     └─────────────────┘
```

**The server:**
- Holds world state (seed + block diffs) so new players can sync
- Relays player positions, block changes, chat, entity syncs, PvP hits
- Manages room creation/listing/joining
- Runs 24/7 — rooms survive even when all players disconnect (for 30 min)
- Speaks TerraForge's exact multiplayer protocol

**No one needs to "host" anymore.** Players create a room on the website, the server spins it up, and anyone can join anytime using the room code.

## API

### REST (for the website / server browser)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Health check |
| `GET` | `/api/rooms` | List all active rooms |
| `GET` | `/api/rooms/:code` | Get specific room info |
| `POST` | `/api/rooms` | Create a new room |
| `DELETE` | `/api/rooms/:code` | Delete a room |

**Create room body:**
```json
{
  "name": "My World",
  "host": "PlayerName",
  "worldSize": "small",
  "gameMode": "survival",
  "maxPlayers": 8,
  "seed": 12345,
  "pvp": false
}
```

### WebSocket (for the game client)

Connect to `ws://your-server.com` (or `wss://` for HTTPS).

**Client → Server messages:**
```js
// Join a room
{ type: 'join_room', code: 'ABC123', name: 'Player' }

// Create a room (via WS instead of REST)
{ type: 'create_room', name: 'My World', host: 'Player', worldSize: 'small', ... }

// List rooms
{ type: 'list_rooms' }

// After joining — send normal TerraForge messages:
{ type: 'player_pos', x, y, vx, vy, facing, wf, hp, name, ... }
{ type: 'block_change', bx, by, blockId }
{ type: 'chat', text: 'hello' }
{ type: 'entity_sync', entities: [...] }
{ type: 'pvp_hit', dmg, fromName }
```

**Server → Client messages:**
```js
{ type: 'welcome', clientId: '...' }
{ type: 'joined', code: '...', clientId: '...', roomInfo: {...} }
{ type: 'room_created', code: '...', clientId: '...', roomInfo: {...} }
{ type: 'room_list', rooms: [...] }
{ type: 'error', message: '...' }

// Plus all relayed TerraForge messages (player_pos, block_change, etc.)
// The server also sends world_sync + world_diffs on join.
```

## Deploy

### Railway (recommended free option)

1. Push this folder to a GitHub repo
2. Go to [railway.app](https://railway.app), sign in with GitHub
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your repo — Railway auto-detects Node.js
5. It will run `npm start` automatically
6. You'll get a public URL like `your-app.up.railway.app`

### Render

1. Push to GitHub
2. Go to [render.com](https://render.com), create a "Web Service"
3. Connect your repo
4. Build command: `npm install`
5. Start command: `node server.js`
6. Free tier gives you a public URL

### Fly.io

1. Install flyctl: `curl -L https://fly.io/install.sh | sh`
2. `cd terraforge-relay`
3. `fly launch` (follow prompts)
4. `fly deploy`

### Local testing

```bash
cd terraforge-relay
npm install
npm start
# Server runs on http://localhost:3000
# WebSocket on ws://localhost:3000
```

## Connecting TerraForge to the Relay

TerraForge needs a small code change to support WebSocket connections alongside PeerJS. The game's `joinGame()` function currently connects via PeerJS — you'll need to add a WebSocket mode that:

1. Connects to `wss://your-relay-server.com`
2. Sends `{ type: 'join_room', code: 'ABCD12', name: 'Player' }`
3. Handles incoming messages with the existing `handleNetMessage()` function
4. Sends outgoing messages via `ws.send(JSON.stringify(data))` instead of `conn.send(data)`

The relay server speaks the same protocol, so `handleNetMessage` works unchanged — only the transport layer switches from PeerJS to WebSocket.

## Room Lifecycle

- Rooms are created via REST API or WebSocket
- Players join/leave freely
- When all players leave, room stays alive for **30 minutes** (so players can reconnect)
- After 30 min empty, room is auto-destroyed
- All rooms auto-destroy after **24 hours** max
- Block diffs accumulate so late-joining players get the full modified world
