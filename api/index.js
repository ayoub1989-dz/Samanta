import { createServer } from 'net';
import { WebSocketServer } from 'ws';

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const UUID  = process.env.UUID  || '2bd47a2d-4b8d-47ea-8888-10fa77e95aa1';   // set in Vercel env vars
const PROXY = process.env.PROXY || '';                  // optional CDN/relay host
// ─── VLESS CONSTANTS ──────────────────────────────────────────────────────────
const VLESS_VERSION = 0;

function parseUUID(uuid) {
  return Buffer.from(uuid.replace(/-/g, ''), 'hex');
}

function processVlessHeader(data, userUUID) {
  if (data.length < 24) return { hasError: true, message: 'Invalid header' };

  const version = data[0];
  const clientUUID = data.slice(1, 17);
  const expectedUUID = parseUUID(userUUID);

  if (!clientUUID.equals(expectedUUID)) {
    return { hasError: true, message: 'UUID mismatch' };
  }

  const optLength = data[17];
  const command   = data[18 + optLength];  // 1=TCP, 2=UDP

  if (command !== 1) {
    return { hasError: true, message: 'Only TCP supported' };
  }

  const portIndex  = 18 + optLength + 1;
  const port       = data.readUInt16BE(portIndex);
  const addrType   = data[portIndex + 2];
  let   address    = '';
  let   headerLen  = portIndex + 3;

  if (addrType === 1) {           // IPv4
    address  = data.slice(headerLen, headerLen + 4).join('.');
    headerLen += 4;
  } else if (addrType === 2) {   // Domain
    const domainLen = data[headerLen];
    address  = data.slice(headerLen + 1, headerLen + 1 + domainLen).toString();
    headerLen += 1 + domainLen;
  } else if (addrType === 3) {   // IPv6
    const ipv6 = [];
    for (let i = 0; i < 8; i++) {
      ipv6.push(data.readUInt16BE(headerLen + i * 2).toString(16));
    }
    address   = ipv6.join(':');
    headerLen += 16;
  } else {
    return { hasError: true, message: 'Unknown address type' };
  }

  return {
    hasError:        false,
    addressRemote:   address,
    portRemote:      port,
    rawDataIndex:    headerLen,
    vlessVersion:    new Uint8Array([version, 0]),
  };
}

// ─── MAIN HANDLER ─────────────────────────────────────────────────────────────
export default function handler(req, res) {
  // Upgrade check
  if (req.headers['upgrade'] !== 'websocket') {
    res.setHeader('Content-Type', 'text/plain');
    return res.end('V2Ray WebSocket proxy is running.');
  }

  const wss = new WebSocketServer({ noServer: true });

  wss.handleUpgrade(req, req.socket, Buffer.alloc(0), (ws) => {
    ws.once('message', (data) => {
      const header = processVlessHeader(data, UUID);

      if (header.hasError) {
        console.error('VLESS error:', header.message);
        return ws.close();
      }

      // Send VLESS response header
      ws.send(Buffer.from(header.vlessVersion));

      const { addressRemote, portRemote, rawDataIndex } = header;
      const remainder = data.slice(rawDataIndex);

      // Open TCP connection to target
      const tcp = createServer().listen(0);

      const socket = require('net').createConnection(
        { host: addressRemote, port: portRemote },
        () => {
          if (remainder.length) socket.write(remainder);

          // WebSocket → TCP
          ws.on('message', (chunk) => socket.write(chunk));

          // TCP → WebSocket
          socket.on('data', (chunk) => {
            if (ws.readyState === ws.OPEN) ws.send(chunk);
          });

          socket.on('end',   () => ws.close());
          socket.on('error', (e) => { console.error('TCP error', e); ws.close(); });
          ws.on('close', () => socket.destroy());
        }
      );

      socket.on('error', (e) => {
        console.error('Connect error:', e.message);
        ws.close();
      });
    });
  });
}
