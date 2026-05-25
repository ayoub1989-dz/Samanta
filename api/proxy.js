const WebSocket = require('ws');

const UUID = process.env.UUID || 'ded02f85-3716-46da-b1b4-40fe89583901';
const WS_PATH = process.env.WS_PATH || '/ws/vless';

module.exports = async (req, res) => {
  if (req.url.includes(WS_PATH) && req.headers.upgrade === 'websocket') {
    // Handle WebSocket upgrade for VLESS
    const wsServer = new WebSocket.Server({ noServer: true });
    
    req.wsServer = wsServer;
    
    wsServer.handleUpgrade(req, req.socket, Buffer.alloc(0), (ws) => {
      ws.on('message', (message) => {
        // Basic VLESS handshake simulation (for demo - production use full Xray)
        console.log('VLESS WS data received');
        ws.send(message);
      });
      
      ws.on('close', () => console.log('Client disconnected'));
    });
  } else {
    // Fallback HTTP response
    res.status(200).json({
      status: "VLESS WebSocket endpoint active",
      path: WS_PATH,
      usage: "Use with V2Ray/Xray client - VLESS + WS"
    });
  }
};
