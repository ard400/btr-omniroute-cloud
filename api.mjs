// Official HIDRO64 transport. Authentication and permissions remain in the Worker.
// Incoming URLs and headers never select the upstream host or an arbitrary route.
export const CORE_ORIGIN = 'https://hidro64.btr.pe';
export const MAX_REQUEST_BYTES = 1_200_000;
export const MAX_RESPONSE_BYTES = 5_000_000;
export const CORE_TIMEOUT_MS = 60_000;

const core = new URL(CORE_ORIGIN);
if (core.protocol !== 'https:' || core.origin !== CORE_ORIGIN || core.username || core.password || core.port || core.pathname !== '/' || core.search || core.hash) {
  throw new Error('INVALID_HIDRO64_CORE_ORIGIN');
}
const destinations = new Map([
  ['/api/agent', `${CORE_ORIGIN}/api/agent`],
  ['/mcp', `${CORE_ORIGIN}/mcp`],
]);
const methods = new Set(['GET', 'POST', 'OPTIONS']);
const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization, Content-Type, Accept, MCP-Protocol-Version, MCP-Session-Id, Last-Event-ID',
  'Access-Control-Expose-Headers': 'Content-Disposition, Content-Type, MCP-Protocol-Version, MCP-Session-Id, WWW-Authenticate',
};
const requestHeaders = ['content-type', 'accept', 'mcp-protocol-version', 'mcp-session-id', 'last-event-id'];
const responseHeaders = new Set([
  'content-type', 'content-disposition', 'allow', 'www-authenticate', 'vary',
  'mcp-protocol-version', 'mcp-session-id',
]);

function defaults(response) {
  for (const [name, value] of Object.entries(cors)) response.setHeader(name, value);
  response.setHeader('Cache-Control', 'no-store');
  response.setHeader('X-Content-Type-Options', 'nosniff');
}

function json(response, status, body) {
  if (response.destroyed || response.writableEnded) return;
  response.setHeader('Content-Type', 'application/json; charset=utf-8');
  response.writeHead(status);
  response.end(JSON.stringify(body));
}

function fault(status, code) {
  return Object.assign(new Error(code), { status, code });
}

// Do not use request.json()/an unbounded read: enforce the limit while receiving
// chunks and finish checking the entire input before any upstream fetch starts.
function readRequest(request) {
  const declared = request.headers['content-length'];
  if (declared && Number(declared) > MAX_REQUEST_BYTES) {
    request.resume();
    return Promise.reject(fault(413, 'REQUEST_TOO_LARGE'));
  }
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    const clean = () => {
      request.off('data', onData); request.off('end', onEnd);
      request.off('aborted', onAborted); request.off('error', onError);
    };
    const onData = chunk => {
      size += chunk.length;
      if (size > MAX_REQUEST_BYTES) {
        clean(); request.resume(); reject(fault(413, 'REQUEST_TOO_LARGE'));
      } else chunks.push(chunk);
    };
    const onEnd = () => { clean(); resolve(Buffer.concat(chunks, size)); };
    const onAborted = () => { clean(); reject(fault(400, 'INCOMPLETE_REQUEST')); };
    const onError = () => { clean(); reject(fault(400, 'INCOMPLETE_REQUEST')); };
    request.on('data', onData); request.once('end', onEnd);
    request.once('aborted', onAborted); request.once('error', onError);
  });
}

async function readResponse(upstream) {
  const declared = upstream.headers.get('content-length');
  if (declared && Number(declared) > MAX_RESPONSE_BYTES) {
    await upstream.body?.cancel().catch(() => {});
    throw fault(502, 'CORE_RESPONSE_TOO_LARGE');
  }
  if (!upstream.body) return Buffer.alloc(0);
  const reader = upstream.body.getReader();
  const chunks = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return Buffer.concat(chunks, size);
      size += value.byteLength;
      if (size > MAX_RESPONSE_BYTES) throw fault(502, 'CORE_RESPONSE_TOO_LARGE');
      chunks.push(Buffer.from(value));
    }
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error;
  } finally { reader.releaseLock(); }
}

export function createAgentGateway({ fetchImpl = globalThis.fetch, timeoutMs = CORE_TIMEOUT_MS } = {}) {
  // The timeout override is only a dependency seam for local HTTP checks; no
  // request, environment variable or deployment setting can choose an upstream.
  if (typeof fetchImpl !== 'function' || !Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > CORE_TIMEOUT_MS) {
    throw new Error('INVALID_AGENT_TRANSPORT');
  }
  return async (request, response) => {
    const target = request.url;
    if (target === '/') {
      defaults(response);
      if (request.method !== 'GET') {
        response.setHeader('Allow', 'GET');
        json(response, 405, { error: 'METHOD_NOT_ALLOWED' });
      } else json(response, 200, {
        name: 'HIDRO 64 · conexión oficial de IA',
        application: CORE_ORIGIN,
        endpoints: { agent: '/api/agent', mcp: '/mcp' },
        guide: `${CORE_ORIGIN}/conectar-ia`,
        authentication: {
          authority: CORE_ORIGIN, scheme: 'Bearer',
          login: { endpoint: '/api/agent', operation: 'login', credentials_in_json_body: true },
          browser_cookie_accepted: false,
        },
        limits: { request_bytes: MAX_REQUEST_BYTES, response_bytes: MAX_RESPONSE_BYTES, timeout_ms: CORE_TIMEOUT_MS },
        whatsapp_session_required: false,
      });
      return true;
    }
    if (!destinations.has(target)) {
      // URL parameters are unnecessary on either protocol endpoint, and can leak
      // credentials into infrastructure logs. Never forward them or echo them.
      if (destinations.has(String(target).split(/[?#]/, 1)[0])) {
        defaults(response);
        json(response, 400, { error: 'URL_PARAMETERS_NOT_SUPPORTED' });
        return true;
      }
      return false;
    }
    defaults(response);
    if (!methods.has(request.method)) {
      response.setHeader('Allow', 'GET, POST, OPTIONS');
      json(response, 405, { error: 'METHOD_NOT_ALLOWED' });
      return true;
    }
    const authorization = request.headers.authorization;
    if (authorization !== undefined && (typeof authorization !== 'string' || !/^Bearer [^\s,]+$/i.test(authorization))) {
      json(response, 401, { error: 'BEARER_REQUIRED' });
      return true;
    }
    let controller, timer, timedOut = false;
    const abort = () => { if (!response.writableEnded) controller?.abort(); };
    try {
      const body = await readRequest(request);
      if (request.method !== 'POST' && body.length) throw fault(400, 'BODY_NOT_ALLOWED');
      if (request.aborted || response.destroyed) return true;
      const headers = new Headers();
      for (const name of requestHeaders) {
        const value = request.headers[name];
        if (typeof value === 'string') headers.set(name, value);
      }
      if (authorization) headers.set('Authorization', authorization);
      // Cookies, Host, Forwarded, CF/IP headers, and all other incoming headers
      // are deliberately absent. Only the user's Bearer authenticates upstream.
      controller = new AbortController();
      timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeoutMs);
      timer.unref?.();
      request.once('aborted', abort); response.once('close', abort);
      const upstream = await fetchImpl(destinations.get(target), {
        method: request.method, headers, redirect: 'error', cache: 'no-store', signal: controller.signal,
        ...(request.method === 'POST' ? { body } : {}),
      });
      // Buffer at most 5 MB before writing status/headers, so oversize or timed
      // out replies never expose a partial authenticated document as success.
      const output = await readResponse(upstream);
      if (response.destroyed || response.writableEnded) return true;
      for (const [name, value] of upstream.headers) {
        if (responseHeaders.has(name) || (name.startsWith('access-control-') && name !== 'access-control-allow-credentials')) {
          response.setHeader(name, value);
        }
      }
      response.writeHead(upstream.status);
      response.end(output);
    } catch (error) {
      if (response.destroyed || response.writableEnded) return true;
      if (error.code === 'REQUEST_TOO_LARGE') response.setHeader('Connection', 'close');
      if (timedOut) json(response, 504, { error: 'CORE_TIMEOUT', message: 'HIDRO64 no respondió dentro del límite de 60 segundos.' });
      else if (error.status) json(response, error.status, { error: error.code });
      else json(response, 502, {
        error: 'CORE_UNAVAILABLE',
        message: 'No se pudo alcanzar el núcleo de HIDRO64. Comprueba su disponibilidad y la política de red del alojamiento.',
      });
    } finally {
      clearTimeout(timer);
      request.off('aborted', abort); response.off('close', abort);
    }
    return true;
  };
}
