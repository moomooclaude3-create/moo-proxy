/**
 * proxy-server.js — Render.com WebSocket relay
 *
 * FIX: Node.js `ws` library sends NO Origin header on outbound connections by default.
 * moomoo.io validates Origin: https://moomoo.io on the WS upgrade handshake → 403.
 * Adding it here is likely all that's needed.
 *
 * Usage (client → this relay):
 *   wss://moo-proxy.onrender.com?target=<encodeURIComponent("wss://sgs-xxx.moomoo.io/?token=...")>
 */

'use strict';

const { WebSocketServer, WebSocket } = require('ws');
const http  = require('http');
const { URL } = require('url');

const PORT = parseInt(process.env.PORT || '10000', 10);

// ── HTTP server (Render health checks + proxy wake-up pings) ─────────────────
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('moo-relay OK\n');
});

// ── WebSocket relay ───────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (clientWs, req) => {
    const connId = Date.now().toString(36);

    // Parse ?target= from the request URL
    const qsStart  = req.url.indexOf('?');
    const qs       = qsStart >= 0 ? req.url.slice(qsStart + 1) : '';
    const rawTarget = new URLSearchParams(qs).get('target');

    if (!rawTarget) {
        console.warn(`[${connId}] no ?target= param — closing`);
        clientWs.close(1008, 'Missing ?target=');
        return;
    }

    let targetUrl;
    try {
        targetUrl = decodeURIComponent(rawTarget);
    } catch {
        console.warn(`[${connId}] bad target encoding`);
        clientWs.close(1008, 'Bad target encoding');
        return;
    }

    // Extract host for the Host header
    let host;
    try {
        host = new URL(targetUrl).host;
    } catch {
        host = targetUrl.replace(/^wss?:\/\//, '').split(/[/?]/)[0];
    }

    console.log(`[${connId}] relay → ${targetUrl.slice(0, 80)}`);

    // ── 🔑 THE FIX ────────────────────────────────────────────────────────────
    // Origin + User-Agent make the upstream handshake look like it comes from
    // a real browser tab on moomoo.io — the server stops returning 403.
    const upstream = new WebSocket(targetUrl, {
        headers: {
            'Origin':     'https://moomoo.io',
            'Host':       host,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        },
        handshakeTimeout: 10_000,
    });

    // Buffer client messages that arrive before upstream is ready
    let upstreamOpen = false;
    const queue = [];

    clientWs.on('message', (data, isBinary) => {
        if (!upstreamOpen) {
            queue.push({ data, isBinary });
        } else if (upstream.readyState === WebSocket.OPEN) {
            upstream.send(data, { binary: isBinary });
        }
    });

    upstream.on('open', () => {
        upstreamOpen = true;
        console.log(`[${connId}] upstream open — flushing ${queue.length} buffered msg(s)`);
        for (const { data, isBinary } of queue) {
            if (upstream.readyState === WebSocket.OPEN) {
                upstream.send(data, { binary: isBinary });
            }
        }
        queue.length = 0;
    });

    upstream.on('message', (data, isBinary) => {
        if (clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(data, { binary: isBinary });
        }
    });

    upstream.on('close', (code, reason) => {
        console.log(`[${connId}] upstream closed — code=${code}`);
        if (clientWs.readyState < 2) clientWs.close(code, reason);
    });

    upstream.on('error', err => {
        console.error(`[${connId}] upstream error: ${err.message}`);
        if (clientWs.readyState < 2) clientWs.close(1011, 'upstream-error');
    });

    clientWs.on('close', (code, reason) => {
        if (upstream.readyState < 2) upstream.close(code, reason);
    });

    clientWs.on('error', err => {
        console.error(`[${connId}] client error: ${err.message}`);
        if (upstream.readyState < 2) upstream.terminate();
    });
});

httpServer.listen(PORT, () => {
    console.log(`[relay] listening on :${PORT}`);
});
