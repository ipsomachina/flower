// The member's own API: the admin token, JSON requests and the
// activity stream.
//
// The token lives in this tab's sessionStorage only and is sent only
// as an Authorization header to same-origin /v1 paths: never in a URL,
// never to another origin (`mode: 'same-origin'` refuses one), never
// with cookies.

/** Every endpoint the dashboard calls. Tests extract these paths and
 *  check each one routes, so renaming an endpoint fails the build. */
export const ENDPOINTS = Object.freeze({
  status: '/v1/status',
  ready: '/health/ready',
  metrics: '/v1/metrics',
  raft: '/v1/admin/raft',
  membership: '/v1/admin/membership',
  leadership: '/v1/admin/leadership',
  modules: '/v1/admin/modules',
  deployments: '/v1/admin/deployments',
  keys: '/v1/admin/keys',
  backup: '/v1/admin/backup',
  retention: '/v1/admin/retention',
  activity: '/v1/admin/activity',
  activityStream: '/v1/admin/activity/stream',
  adminStream: '/v1/admin/stream',
  timerStats: '/v1/timer/stats',
  queueStats: '/v1/queue/stats',
  partitions: '/v1/admin/partitions',
  transactions: '/v1/admin/transactions',
});

/** Every event of the admin stream the page handles: tests check it is
 *  the list the server sends. */
export const STREAM_EVENTS = Object.freeze([
  'snapshot', 'raft', 'readiness', 'replication', 'metrics', 'catalog', 'backup', 'work', 'activity',
  'group_raft', 'groups', 'partitions', 'transactions',
]);

const TOKEN_KEY = 'flower.admin.token';

/** The admin token, kept for this tab only. */
export const token = {
  get() {
    try {
      return sessionStorage.getItem(TOKEN_KEY) || null;
    } catch {
      return null;
    }
  },
  set(value) {
    try {
      sessionStorage.setItem(TOKEN_KEY, value);
    } catch {
      // Storage refused: the token lasts until the page reloads.
    }
    memory = value;
  },
  clear() {
    try {
      sessionStorage.removeItem(TOKEN_KEY);
    } catch {
      // Nothing stored.
    }
    memory = null;
  },
};
// When sessionStorage is unavailable, the token lives in this module.
let memory = null;
function currentToken() {
  return token.get() || memory;
}

/** A failed request: HTTP status, the server's code and message. */
export class ApiError extends Error {
  constructor(status, code, message) {
    super(message || code || `HTTP ${status}`);
    this.status = status;
    this.code = code;
  }

  /** One line for a panel. */
  describe() {
    const what = this.status ? `HTTP ${this.status}` : 'network';
    return this.code ? `${what} ${this.code}: ${this.message}` : `${what}: ${this.message}`;
  }
}

const listeners = { forbidden: () => {}, leader: () => {} };

/** Called on any 403 (the token was refused), and with the leader's
 *  advertised API URL whenever a response names it. */
export function onForbidden(callback) {
  listeners.forbidden = callback;
}
export function onLeaderUrl(callback) {
  listeners.leader = callback;
}

function headers(path, json) {
  const result = { Accept: json ? 'application/json' : 'text/event-stream' };
  const bearer = currentToken();
  if (bearer && path.startsWith('/v1/')) result.Authorization = `Bearer ${bearer}`;
  return result;
}

function check(path) {
  if (!/^\/(v1|health)\/[a-z0-9_/]+(\?.*)?$/.test(path)) throw new Error(`not an API path: ${path}`);
}

function noticeLeader(response) {
  const leader = response.headers.get('x-flower-leader-url');
  if (leader) listeners.leader(leader);
}

/**
 * One JSON request to this member. Resolves with the parsed body, the
 * HTTP status and the round trip; rejects with an ApiError on a
 * network failure or a status outside 2xx and `accept`.
 */
export async function request(path, { method = 'GET', body, accept = [], signal } = {}) {
  check(path);
  const init = {
    method,
    headers: headers(path, true),
    mode: 'same-origin',
    credentials: 'omit',
    cache: 'no-store',
    redirect: 'error',
    referrerPolicy: 'no-referrer',
    signal,
  };
  if (body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const started = performance.now();
  let response;
  try {
    response = await fetch(path, init);
  } catch (error) {
    throw new ApiError(0, null, error && error.message ? error.message : 'request failed');
  }
  const elapsed = performance.now() - started;
  noticeLeader(response);
  const text = await response.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = null;
  }
  if (response.status === 403) {
    listeners.forbidden();
  }
  if (!response.ok && !accept.includes(response.status)) {
    const code = parsed && typeof parsed.code === 'string' ? parsed.code : null;
    const message = parsed && typeof parsed.message === 'string' ? parsed.message : text.slice(0, 300);
    throw new ApiError(response.status, code, message || response.statusText);
  }
  return { status: response.status, body: parsed, elapsed };
}

/**
 * Read a server-sent event stream with fetch, since EventSource cannot
 * send the Authorization header. Calls `onEvent(name, data)` per event
 * until the stream ends or `signal` aborts.
 */
export async function stream(path, onEvent, signal) {
  check(path);
  let response;
  try {
    response = await fetch(path, {
      headers: headers(path, false),
      mode: 'same-origin',
      credentials: 'omit',
      cache: 'no-store',
      redirect: 'error',
      referrerPolicy: 'no-referrer',
      signal,
    });
  } catch (error) {
    if (signal && signal.aborted) return;
    throw new ApiError(0, null, error && error.message ? error.message : 'stream failed');
  }
  noticeLeader(response);
  if (!response.ok || !response.body) {
    let parsed = null;
    try {
      parsed = await response.json();
    } catch {
      parsed = null;
    }
    if (response.status === 403) listeners.forbidden();
    throw new ApiError(response.status, parsed && parsed.code, parsed && parsed.message);
  }
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = '';
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buffer = (buffer + value).replace(/\r\n?/g, '\n');
      let end = buffer.indexOf('\n\n');
      while (end >= 0) {
        dispatch(buffer.slice(0, end), onEvent);
        buffer = buffer.slice(end + 2);
        end = buffer.indexOf('\n\n');
      }
    }
  } catch (error) {
    if (signal && signal.aborted) return;
    throw new ApiError(0, null, error && error.message ? error.message : 'stream broke');
  } finally {
    reader.releaseLock();
  }
}

/** One SSE dispatch: `event:` and `data:` lines; comments keep alive. */
function dispatch(block, onEvent) {
  let name = 'message';
  const data = [];
  for (const line of block.split('\n')) {
    if (line.startsWith(':')) {
      onEvent('comment', line.slice(1).trim());
      continue;
    }
    const colon = line.indexOf(':');
    const field = colon < 0 ? line : line.slice(0, colon);
    const value = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '');
    if (field === 'event') name = value;
    if (field === 'data') data.push(value);
  }
  if (data.length) onEvent(name, data.join('\n'));
}
