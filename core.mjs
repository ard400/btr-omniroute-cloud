import { timingSafeEqual } from 'node:crypto';

export const AUTH_FORMAT = 'hidro64-baileys-v1';
export const QR_LIFETIME_MS = 55_000;
const own = (object, key) => Object.prototype.hasOwnProperty.call(object, key);

export function bearerMatches(header, token) {
  if (!token || token.length < 32 || typeof header !== 'string') return false;
  const supplied = Buffer.from(header);
  const expected = Buffer.from(`Bearer ${token}`);
  return supplied.length === expected.length && timingSafeEqual(supplied, expected);
}

export function createWorkerClient({ endpoint, token, fetchImpl = fetch }) {
  const url = new URL(endpoint);
  if (url.protocol !== 'https:' || url.username || url.password || url.pathname !== '/api/whatsapp-bridge' || url.search || url.hash) {
    throw new Error('HIDRO_WA_ENDPOINT must be an HTTPS /api/whatsapp-bridge endpoint');
  }
  if (!token || token.length < 32) throw new Error('HIDRO_WA_BRIDGE_TOKEN must contain at least 32 characters');
  return async payload => {
    const response = await fetchImpl(url, {
      method: 'POST', redirect: 'error', signal: AbortSignal.timeout(12_000),
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = new Error(response.status === 409 ? 'AUTH_REVISION_CONFLICT' : `WORKER_HTTP_${response.status}`);
      error.status = response.status;
      throw error;
    }
    return response.json();
  };
}

// Persist both credentials and every Signal-key mutation before keys.set resolves.
// Auth is opaque to the Worker, which encrypts it before durable storage.
export function createRemoteAuth({ initial, revision = 0, library, callWorker }) {
  const { BufferJSON, initAuthCreds, proto } = library;
  const loaded = initial ? JSON.parse(initial, BufferJSON.reviver) : null;
  if (loaded && (loaded.format !== AUTH_FORMAT || !loaded.creds || !loaded.keys)) throw new Error('INVALID_AUTH_FORMAT');
  const creds = loaded?.creds || initAuthCreds();
  const data = Object.assign(Object.create(null), loaded?.keys || {});
  let currentRevision = revision;
  let tail = Promise.resolve();
  let failure;
  const persist = () => {
    const snapshot = JSON.stringify({ format: AUTH_FORMAT, creds, keys: data }, BufferJSON.replacer);
    tail = tail.then(async () => {
      if (failure) throw failure;
      const result = await callWorker({ op: 'save', auth: snapshot, expected_revision: currentRevision });
      if (!Number.isSafeInteger(result.revision) || result.revision <= currentRevision) throw new Error('INVALID_AUTH_REVISION');
      currentRevision = result.revision;
    }).catch(error => { failure = error; throw error; });
    return tail;
  };
  return {
    state: {
      creds,
      keys: {
        async get(type, ids) {
          const result = {};
          for (const id of ids) {
            let value = own(data, type) && own(data[type], id) ? data[type][id] : undefined;
            if (type === 'app-state-sync-key' && value) value = proto.Message.AppStateSyncKeyData.fromObject(value);
            if (value != null) Object.defineProperty(result, id, { value, enumerable: true, configurable: true });
          }
          return result;
        },
        async set(updates) {
          for (const [type, entries] of Object.entries(updates)) {
            if (!own(data, type)) Object.defineProperty(data, type, { value: Object.create(null), enumerable: true });
            for (const [id, value] of Object.entries(entries)) {
              if (value == null) delete data[type][id];
              else Object.defineProperty(data[type], id, { value, enumerable: true, configurable: true, writable: true });
            }
          }
          await persist();
        },
      },
    },
    async saveCreds(update = {}) { Object.assign(creds, update); await persist(); },
    flush: () => tail,
  };
}

export function isAllowedChat(jid, allowedChats) {
  return typeof jid === 'string' && /^\d+(?:-\d+)?@(?:s\.whatsapp\.net|g\.us)$/.test(jid) && Array.isArray(allowedChats) && allowedChats.includes(jid);
}

export function inboundEnvelope(message, allowedChats) {
  const key = message?.key;
  if (!key?.id || key.fromMe || !isAllowedChat(key.remoteJid, allowedChats)) return null;
  let body = message.message;
  for (let i = 0; body && i < 4; i++) {
    const wrapped = body.ephemeralMessage || body.viewOnceMessage || body.viewOnceMessageV2;
    if (!wrapped?.message) break;
    body = wrapped.message;
  }
  if (!body) return null;
  // Only text/captions enter HIDRO64. No contacts, history, media bytes or location are collected.
  const text = body.conversation || body.extendedTextMessage?.text || body.imageMessage?.caption || body.videoMessage?.caption || body.documentMessage?.caption;
  if (typeof text !== 'string' || !text.trim()) return null;
  if (text.length > 64_000) {
    const error = new RangeError('MESSAGE_TEXT_TOO_LONG');
    error.code = 'MESSAGE_TEXT_TOO_LONG';
    throw error;
  }
  const seconds = Number(message.messageTimestamp);
  if (!Number.isFinite(seconds) || seconds <= 0 || !Number.isFinite(new Date(seconds * 1000).valueOf())) return null;
  return {
    op: 'message', chat_jid: key.remoteJid, sender_jid: key.participant || key.remoteJid,
    message_id: String(key.id).slice(0, 200), text,
    timestamp: new Date(seconds * 1000).toISOString(),
  };
}
