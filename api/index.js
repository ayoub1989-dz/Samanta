const { WebSocketServer } = require('ws');
const net = require('net');

const UUID = process.env.UUID || 'ded02f85-3716-46da-b1b4-40fe89583901';

module.exports = function handler(req, res) {
  // Health check
  if (req.headers['upgrade'] !== 'websocket') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK');
    return;
  }

  const wss = new WebSocketServer({ noServer: true });

  wss.handleUpgrade(req, req.socket, Buffer.alloc(0), (ws) => {
    req.socket.setTimeout(0);
    req.socket.setNoDelay(true);
    req.socket.setKeepAlive(true, 0);

    ws.once('message', (raw) => {
      const data = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
      const parsed = parseVless(data, UUID);

      if (parsed.error) {
        console.error('[VLESS] parse error:', parsed.error);
        ws.close(1003, parsed.error);
        return;
      }

      // Send VLESS response header
      ws.send(Buffer.from([parsed.version, 0]));

      const { host, port, dataOffset } = parsed;
      const remaining = data.slice(dataOffset);

      console.log(`[VLESS] connecting → ${host}:${port}`);

      // ✅ Fixed: use createConnection not createServer
      const tcp = net.createConnection({ host, port }, () => {
        if (remaining.length) tcp.write(remaining);

        // WS → TCP
        ws.on('message', (chunk) => {
          if (!tcp.destroyed) {
            tcp.write(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
          }
        });

        // TCP → WS
        tcp.on('data', (chunk) => {
          if (ws.readyState === ws.OPEN) ws.send(chunk);
        });
      });

      tcp.on('error', (e) => {
        console.error('[TCP] error:', e.message);
        ws.close();
      });

      tcp.on('close', () => ws.readyState === ws.OPEN && ws.close());
      ws.on('close', () => !tcp.destroyed && tcp.destroy());
      ws.on('error', () => !tcp.destroyed && tcp.destroy());
    });
  });
};

// ── VLESS header parser ──────────────────────────────────────────────────────
function parseVless(data, uuid) {
  if (data.length < 24) return { error: 'Too short' };

  const version    = data[0];
  const clientUUID = data.slice(1, 17).toString('hex');
  const expected   = uuid.replace(/-/g, '').toLowerCase();

  if (clientUUID !== expected) return { error: 'UUID mismatch' };

  const optLen  = data[17];
  const cmd     = data[18 + optLen];
  if (cmd !== 1) return { error: 'Only TCP supported' };

  let offset = 18 + optLen + 1;
  const port = data.readUInt16BE(offset); offset += 2;
  const addrType = data[offset]; offset++;

  let host = '';

  if (addrType === 1) {           // IPv4
    host = [data[offset], data[offset+1], data[offset+2], data[offset+3]].join('.');
    offset += 4;

  } else if (addrType === 2) {   // Domain
    const len = data[offset]; offset++;
    host = data.slice(offset, offset + len).toString('utf8');
    offset += len;

  } else if (addrType === 3) {   // IPv6
    const parts = [];
    for (let i = 0; i < 8; i++) {
      parts.push(data.readUInt16BE(offset).toString(16));
      offset += 2;
    }
    host = parts.join(':');

  } else {
    return { error: 'Unknown address type' };
  }

  return { version, host, port, dataOffset: offset };
}
