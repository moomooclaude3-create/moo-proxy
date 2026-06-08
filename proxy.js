/**
 * moo-proxy — WebSocket + HTTP relay for ParmaBots
 *
 * Each deployed instance gives the bots a separate IP address,
 * bypassing the moomoo.io per-IP connection limit.
 *
 * Deploy N copies of this for N different IPs.
 * Then in the proxy field of the bot UI:
 *   wss://your-service.onrender.com
 *
 * The HTTP endpoint (?url=...) is used automatically by the bot
 * engine for Altcha challenge fetches — no extra config needed.
 *
 * Install:  npm install
 * Run:      node proxy.js
 *           PORT=8080 node proxy.js
 */

'use strict';

const http      = require('http');
const https     = require('https');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

// Headers the proxy sends when connecting to moomoo game servers.
// moomoo rejects WebSocket connections that don't look like they come from
// the moomoo.io page — the Origin header is the key check.
const UPSTREAM_HEADERS = {
    'Origin':     'https://moomoo.io',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer':    'https://moomoo.io/',
};

// ── HTTP server (also hosts WebSocket upgrade) ────────────────────
const server = http.createServer((req, res) => {
    // CORS preflight
    res.setHeader('Access-Control-Allow-Origin',  '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    // Health check
    if (req.url === '/' || req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('moo-proxy ok\n');
        return;
    }

    // HTTP relay — ?url=<encoded target URL>  (used by BotAltcha for challenge fetch)
    let target;
    try {
        const params = new URL(req.url, 'http://x').searchParams;
        target = params.get('url') || params.get('target');
        if (!target) throw new Error('missing url param');
        new URL(target); // validate
    } catch (e) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Bad request: missing or invalid ?url= parameter\n');
        return;
    }

    const mod = target.startsWith('https') ? https : http;
    const reqOptions = {
        timeout: 8000,
        headers: {
            'Origin':  'https://moomoo.io',
            'Referer': 'https://moomoo.io/',
            'User-Agent': UPSTREAM_HEADERS['User-Agent'],
        },
    };
    const proxyReq = mod.get(target, reqOptions, proxyRes => {
        res.writeHead(proxyRes.statusCode, {
            'Content-Type':                proxyRes.headers['content-type'] || 'application/json',
            'Access-Control-Allow-Origin': '*',
        });
        proxyRes.pipe(res);
    });
    proxyReq.on('error', err => {
        console.error('[HTTP relay error]', err.message);
        if (!res.headersSent) { res.writeHead(502); }
        res.end();
    });
    proxyReq.on('timeout', () => {
        proxyReq.destroy();
        if (!res.headersSent) { res.writeHead(504); }
        res.end();
    });
});

// ── WebSocket relay ───────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on('connection', (client, req) => {
    let targetUrl;
    try {
        const params = new URL(req.url, 'http://x').searchParams;
        const raw = params.get('target');
        if (!raw) throw new Error('missing target');
        targetUrl = decodeURIComponent(raw);
        new URL(targetUrl); // validate
    } catch (e) {
        console.warn('[WS] rejected — missing/invalid ?target=:', e.message);
        client.close(1008, 'Missing or invalid ?target= parameter');
        return;
    }

    console.log(`[WS] relay → ${targetUrl.slice(0, 100)}`);

    let upstream;
    try {
        // Pass UPSTREAM_HEADERS so moomoo accepts the connection
        upstream = new WebSocket(targetUrl, { headers: UPSTREAM_HEADERS });
    } catch (e) {
        console.error('[WS] failed to create upstream socket:', e.message);
        client.close(1011, 'Cannot connect to target');
        return;
    }

    upstream.on('open', () => {
        // Bot → moomoo
        client.on('message', (data, isBinary) => {
            if (upstream.readyState === WebSocket.OPEN)
                upstream.send(data, { binary: isBinary });
        });
        // moomoo → bot
        upstream.on('message', (data, isBinary) => {
            if (client.readyState === WebSocket.OPEN)
                client.send(data, { binary: isBinary });
        });
    });

    upstream.on('close', (code, reason) => {
        console.log(`[WS] upstream closed code=${code} reason=${reason}`);
        if (client.readyState < 2) client.close(code, reason);
    });
    upstream.on('error', err => {
        console.error('[WS] upstream error:', err.message);
        if (client.readyState < 2) client.close(1011, 'Upstream error');
    });
    client.on('close',  () => { if (upstream.readyState < 2) upstream.close(); });
    client.on('error',  ()  => { if (upstream.readyState < 2) upstream.close(); });
});

server.listen(PORT, () => {
    console.log(`moo-proxy listening on port ${PORT}`);
});
