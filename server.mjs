import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { bearerMatches, createRemoteAuth, createWorkerClient, inboundEnvelope, isAllowedChat, QR_LIFETIME_MS } from './core.mjs';
import { createAgentGateway } from './api.mjs';

export const CONTROL_POLL_MS = 10_000;

// The Worker owns the encrypted human command and its expiry. This consumer
// never exposes its service Bearer to a browser or calls this service's HTTP URL.
export function createControlPoller({ callWorker, start, stop, now = Date.now, schedule = setInterval, cancel = clearInterval }) {
  const completed = new Set();
  let pendingAck = null, polling = false, closed = false;
  const acknowledge = async () => {
    if (pendingAck === null || closed) return;
    await callWorker({ op: 'ack', command_id: pendingAck });
    pendingAck = null;
  };
  const poll = async () => {
    if (closed || polling) return;
    polling = true;
    try {
      // A failed ACK is retried before fetching another command. The action has
      // already completed, so a lost response must never execute it twice.
      await acknowledge();
      if (closed) return;
      const { command } = await callWorker({ op: 'control' });
      if (closed || !command || typeof command.id !== 'string' || !command.id.trim() || command.id.length > 200) return;
      if (completed.has(command.id)) {
        pendingAck = command.id;
        await acknowledge();
        return;
      }
      const expires = typeof command.expires_at === 'string' ? Date.parse(command.expires_at) : NaN;
      if (!Number.isFinite(expires) || expires <= now() || !['start', 'stop'].includes(command.action)) return;
      await (command.action === 'start' ? start() : stop());
      completed.add(command.id);
      pendingAck = command.id;
      await acknowledge();
    } catch {
      // Offline reads/actions/ACKs are retried on later ticks. Do not log private
      // payloads or let a control-network failure stop the HTTP API or its timer.
    } finally { polling = false; }
  };
  const timer = schedule(() => { void poll(); }, CONTROL_POLL_MS);
  timer?.unref?.();
  return {
    poll,
    close() { closed = true; cancel(timer); },
  };
}

export async function runBridge(env = process.env) {
  const agentGateway = createAgentGateway();
  let callWorker, baileys, makeWASocket, DisconnectReason, logger, preparing;
  const workerClient = () => {
    if (!callWorker) callWorker = createWorkerClient({ endpoint: env.HIDRO_WA_ENDPOINT, token: env.HIDRO_WA_BRIDGE_TOKEN });
    return callWorker;
  };
  const prepareWhatsApp = () => {
    if (!preparing) preparing = (async () => {
      workerClient();
      baileys = await import('@whiskeysockets/baileys');
      ({ default: makeWASocket, DisconnectReason } = baileys);
      const { default: pino } = await import('pino');
      // Baileys logs may contain private protocol/auth data. Never send them to platform logs.
      logger = pino({ level: 'silent' });
    })().catch(error => { preparing = undefined; throw error; });
    return preparing;
  };
  let socket, auth, starting, generation = 0, retry = 0, stopped = true;
  let qrTimer, reconnectTimer, heartbeatTimer;
  let state = 'ready', allowedChats = [], lastDetail = null;
  let statusTail = Promise.resolve();
  const sendStatus = (next, { qr = null, expires_at = null, detail = null } = {}) => {
    state = next; lastDetail = detail;
    const payload = { op: 'status', state: next, qr, expires_at, detail };
    statusTail = statusTail.catch(() => {}).then(() => callWorker(payload));
    return statusTail;
  };
  const closeSocket = () => {
    clearTimeout(qrTimer); clearTimeout(reconnectTimer); clearInterval(heartbeatTimer);
    const closing = socket; socket = undefined;
    // end() disconnects this client; it does not unlink the user's device.
    for (const event of ['connection.update', 'creds.update', 'messages.upsert']) closing?.ev.removeAllListeners(event);
    try { closing?.end(new Error('HIDRO64_BRIDGE_STOP')); } catch {}
  };
  const fail = async code => {
    stopped = true; generation++; closeSocket();
    await sendStatus('error', { detail: code }).catch(() => {});
  };
  const start = async () => {
    if (starting) return starting;
    if (socket) return;
    starting = (async () => {
      const current = ++generation;
      stopped = false;
      await prepareWhatsApp();
      if (current !== generation) return;
      const loaded = await callWorker({ op: 'load' });
      if (current !== generation) return;
      allowedChats = Array.isArray(loaded.allowed_chats) ? loaded.allowed_chats : [];
      auth = createRemoteAuth({ initial: loaded.auth, revision: loaded.revision, library: baileys, callWorker });
      await auth.saveCreds();
      if (current !== generation) return;
      await sendStatus('connecting');
      if (current !== generation) return;
      const client = makeWASocket({
        auth: auth.state, logger, printQRInTerminal: false, markOnlineOnConnect: false,
        syncFullHistory: false, shouldSyncHistoryMessage: () => false,
        shouldIgnoreJid: jid => !isAllowedChat(jid, allowedChats),
        qrTimeout: QR_LIFETIME_MS, connectTimeoutMs: 30_000,
        getMessage: async () => undefined,
      });
      socket = client;
      client.ev.on('creds.update', update => {
        if (current === generation) auth.saveCreds(update).catch(() => fail('AUTH_PERSISTENCE_FAILED'));
      });
      client.ev.on('connection.update', update => {
        if (current !== generation) return;
        (async () => {
          if (update.qr) {
            clearTimeout(qrTimer);
            const expires_at = new Date(Date.now() + QR_LIFETIME_MS).toISOString();
            await sendStatus('needs_qr', { qr: update.qr, expires_at });
            qrTimer = setTimeout(() => {
              if (current === generation && state === 'needs_qr') sendStatus('disconnected', { detail: 'QR_EXPIRED' }).catch(() => fail('STATUS_UNAVAILABLE'));
            }, QR_LIFETIME_MS);
          }
          if (update.connection === 'open') {
            clearTimeout(qrTimer); retry = 0;
            await auth.flush();
            await sendStatus('connected');
            heartbeatTimer = setInterval(() => {
              if (current !== generation) return;
              (async () => {
                const latest = await callWorker({ op: 'load' });
                allowedChats = Array.isArray(latest.allowed_chats) ? latest.allowed_chats : [];
                if (current === generation) await sendStatus('connected');
              })().catch(() => fail('STATUS_UNAVAILABLE'));
            }, 30_000);
          }
          if (update.connection === 'close') {
            closeSocket();
            const code = update.lastDisconnect?.error?.output?.statusCode;
            if (code === DisconnectReason.loggedOut) {
              stopped = true;
              await sendStatus('logged_out', { detail: 'RELINK_REQUIRED' });
              // Replace revoked credentials with a fresh identity, durably, before another start.
              await auth.flush();
              const fresh = createRemoteAuth({ revision: (await callWorker({ op: 'load' })).revision, library: baileys, callWorker });
              await fresh.saveCreds();
            } else {
              await auth.flush();
              await sendStatus('disconnected', { detail: 'CONNECTION_CLOSED' });
              if (!stopped && retry++ < 8) reconnectTimer = setTimeout(() => start().catch(() => fail('RECONNECT_FAILED')), Math.min(30_000, 1000 * 2 ** retry));
            }
          }
        })().catch(() => fail('SESSION_FAILED'));
      });
      client.ev.on('messages.upsert', event => {
        if (current !== generation || event.type !== 'notify' || event.requestId) return;
        (async () => {
          // Recheck authorization for every batch; the Worker repeats the allowlist check.
          const loaded = await callWorker({ op: 'load' });
          allowedChats = Array.isArray(loaded.allowed_chats) ? loaded.allowed_chats : [];
          if (current !== generation) return;
          for (const message of event.messages || []) {
            const payload = inboundEnvelope(message, allowedChats);
            if (payload) await callWorker(payload);
          }
        })().catch(error => fail(error.code === 'MESSAGE_TEXT_TOO_LONG' ? 'MESSAGE_TEXT_TOO_LONG' : 'INBOUND_STORAGE_FAILED'));
      });
    })().finally(() => { starting = undefined; });
    return starting;
  };
  const stop = async () => {
    stopped = true; generation++; closeSocket();
    if (auth) await auth.flush();
    if (!callWorker) { state = 'disconnected'; lastDetail = 'STOPPED_BY_OPERATOR'; return; }
    await sendStatus('disconnected', { detail: 'STOPPED_BY_OPERATOR' });
  };
  const server = createServer(async (request, response) => {
    if (await agentGateway(request, response)) return;
    response.setHeader('Content-Type', 'application/json');
    response.setHeader('Cache-Control', 'no-store');
    response.setHeader('X-Content-Type-Options', 'nosniff');
    const reply = (code, body) => { response.writeHead(code); response.end(JSON.stringify(body)); };
    if (request.method === 'GET' && request.url === '/health') return reply(200, { ok: true, service: 'hidro64-whatsapp-bridge' });
    if (!bearerMatches(request.headers.authorization, env.HIDRO_WA_BRIDGE_TOKEN)) return reply(401, { error: 'UNAUTHORIZED' });
    try {
      if (request.method === 'GET' && request.url === '/session') return reply(200, { state, detail: lastDetail });
      if (request.method === 'POST' && request.url === '/session/start') { await start(); return reply(202, { state }); }
      if (request.method === 'POST' && request.url === '/session/stop') { await stop(); return reply(200, { state }); }
      return reply(404, { error: 'NOT_FOUND' });
    } catch { await fail('CONTROL_FAILED'); return reply(503, { error: 'BRIDGE_UNAVAILABLE' }); }
  });
  server.requestTimeout = 35_000;
  await new Promise(resolveListen => server.listen(Number(env.PORT || 3000), '0.0.0.0', resolveListen));
  const control = createControlPoller({
    callWorker: payload => workerClient()(payload),
    start: async () => {
      try { await start(); }
      catch (error) { await fail('CONTROL_FAILED'); throw error; }
    },
    stop: async () => {
      try { await stop(); }
      catch (error) { await fail('CONTROL_FAILED'); throw error; }
    },
  });
  const shutdown = () => { control.close(); stop().catch(() => {}).finally(() => server.close()); };
  process.once('SIGTERM', shutdown); process.once('SIGINT', shutdown);
  server.once('close', () => { control.close(); process.off('SIGTERM', shutdown); process.off('SIGINT', shutdown); });
  const bootstrapGeneration = generation;
  try {
  await prepareWhatsApp();
  const existing = await callWorker({ op: 'load' });
  if (bootstrapGeneration === generation) {
    if (existing.auth) {
      const restored = JSON.parse(existing.auth, baileys.BufferJSON.reviver);
      if (restored.creds?.registered) await start().catch(() => fail('WHATSAPP_UNAVAILABLE'));
      else await sendStatus('ready');
    } else await sendStatus('ready');
  }
  } catch {
    // The official agent API does not require a WhatsApp QR, session, or secret.
    // Preserve HTTP availability if WhatsApp configuration/dependencies/core fail.
    if (bootstrapGeneration === generation) {
      stopped = true; generation++; closeSocket();
      state = 'error'; lastDetail = 'WHATSAPP_UNAVAILABLE';
    }
  }
  return { server, start, stop, pollControl: control.poll };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runBridge().catch(() => { console.error('HIDRO64 WhatsApp bridge could not start; verify endpoint, secrets and dependencies.'); process.exitCode = 1; });
}
