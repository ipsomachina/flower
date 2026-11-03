// The dashboard: follows this member's admin stream (a snapshot, then
// Raft transitions, readiness, catalog changes, metrics samples and the
// rest as they happen), keeps a rolling window of samples for the charts
// (rates from counter deltas between the member's own timestamps), and
// runs operator actions behind a confirmation. When the stream is
// refused, or no snapshot arrives (a buffering proxy, say), it falls back
// to polling the same endpoints until the stream comes back.

import { ENDPOINTS, STREAM_EVENTS, request, stream, token, onForbidden, onLeaderUrl } from './api.js';
import { el, fill, byId, chip } from './dom.js';
import { TimeChart } from './charts.js';
import { bytes, clock, count, duration, isNumber, percent, rate, shortHex, exact } from './format.js';
import {
  Panel, describe, renderActivity, renderBackup, renderCluster, renderDatabases, renderGroups, renderKeys, renderModules,
  renderRetention, renderStorage, renderTiles, renderWork,
} from './panels.js';

const MAX_WINDOW = 15 * 60 * 1000;
const SLOW_EVERY = 10_000;
const SNAPSHOT_WITHIN = 5_000;
const STREAM_RETRY = 30_000;
const PREFS_KEY = 'flower.admin.prefs';
const PROCESS_KEY = 'flower.admin.process';
const INTERVALS = [1000, 2000, 5000, 10000, 60000];

// ------------------------------------------------------------ settings

/** Per-viewer conveniences: interval, window, theme, tracked queues. */
const prefs = (() => {
  const defaults = { interval: 2000, window: 300_000, theme: 'auto', queues: [] };
  try {
    const stored = JSON.parse(localStorage.getItem(PREFS_KEY) || '{}');
    const merged = { ...defaults, ...stored, queues: Array.isArray(stored.queues) ? stored.queues.filter(validQueue) : [] };
    if (!INTERVALS.includes(merged.interval)) merged.interval = merged.interval === 0 ? 60000 : 2000;
    return merged;
  } catch {
    return defaults;
  }
})();

function savePrefs() {
  try {
    localStorage.setItem(PREFS_KEY, JSON.stringify(prefs));
  } catch {
    // Not kept: the page still works.
  }
}

function validQueue(entry) {
  return Array.isArray(entry) && entry.length === 2 && entry.every((part) => typeof part === 'string' && part.length > 0 && part.length <= 192);
}

// `?queue=db/name` adds a tracked queue (the demo links with one).
for (const value of new URLSearchParams(location.search).getAll('queue')) {
  const slash = value.indexOf('/');
  const entry = slash > 0 ? [value.slice(0, slash), value.slice(slash + 1)] : null;
  if (entry && validQueue(entry) && !prefs.queues.some(([db, queue]) => db === entry[0] && queue === entry[1])) {
    prefs.queues.push(entry);
    savePrefs();
  }
}

// --------------------------------------------------------------- state

const state = {
  samples: [],
  // Server time minus this browser's, from the stream's samples: the
  // charts' window ends at the member's now.
  offset: 0,
  status: null,
  ready: null,
  raft: null,
  membership: null,
  transitions: [],
  // The other groups this process hosts, the partition catalog and the
  // transactions in doubt: from the stream only (undefined until a
  // snapshot, null on a member serving one group).
  hostedGroups: undefined,
  partitionCatalog: undefined,
  inDoubt: undefined,
  activity: null,
  activityAt: 0,
  processId: null,
  restarts: 0,
  restartTimes: [],
  leaderUrl: null,
  paused: false,
  // live | connecting | reconnecting | fallback | idle
  transport: 'idle',
  transportDetail: '',
  streamAbort: null,
  streamFailures: 0,
  retryTimer: null,
  snapshotTimer: null,
  fastTimer: null,
  lastSlow: 0,
  ticking: false,
  activityAbort: null,
  activityLive: false,
  lastRefresh: null,
  latency: null,
  unreachable: null,
  bannerKey: '',
};

try {
  const stored = JSON.parse(sessionStorage.getItem(PROCESS_KEY) || 'null');
  if (stored && typeof stored.id === 'string') {
    state.processId = stored.id;
    state.restarts = Number(stored.restarts) || 0;
  }
} catch {
  // A fresh count.
}

// ---------------------------------------------------------------- page

const charts = {
  writes: new TimeChart({ title: 'Writes applied', subtitle: 'Per second on this member, from database revisions', series: [{ key: 'writes', label: 'Writes' }], format: rate }),
  inUse: new TimeChart({
    title: 'Admission slots in use',
    subtitle: 'Running requests by class',
    series: [{ key: 'activeQuery', label: 'Query' }, { key: 'activeRefresh', label: 'Refresh' }, { key: 'activeWriter', label: 'Writer' }],
    format: count,
    integer: true,
  }),
  queued: new TimeChart({
    title: 'Admission queue depth',
    subtitle: 'Waiting requests by class',
    series: [{ key: 'queuedQuery', label: 'Query' }, { key: 'queuedRefresh', label: 'Refresh' }, { key: 'queuedWriter', label: 'Writer' }],
    format: count,
    integer: true,
  }),
  refusals: new TimeChart({
    title: 'Refusals',
    subtitle: 'Per second: shed by CoDel, over the byte budget, past the wait bound',
    series: [{ key: 'shed', label: 'Shed (CoDel)' }, { key: 'rejected', label: 'Over budget' }, { key: 'timedOut', label: 'Timed out' }],
    format: rate,
  }),
  reads: new TimeChart({
    title: 'Fresh-read proofs',
    subtitle: 'Per second: lease reads against quorum rounds',
    series: [{ key: 'lease', label: 'Lease reads' }, { key: 'quorum', label: 'Quorum rounds' }, { key: 'fences', label: 'Remote fences' }],
    format: rate,
  }),
  hitRate: new TimeChart({ title: 'Query cache hit rate', subtitle: 'Hits of lookups in each interval', series: [{ key: 'hitRate', label: 'Hit rate' }], format: percent, max: 100 }),
  belowPurge: new TimeChart({
    title: 'Log kept below the purge point',
    subtitle: 'Entries the purge passed that the log still stores',
    series: [{ key: 'belowPurge', label: 'Entries' }],
    format: count,
    integer: true,
  }),
  held: new TimeChart({ title: 'Log bytes held for backup', subtitle: 'Purged entries kept until the archive has them', series: [{ key: 'held', label: 'Held' }], format: bytes, binary: true }),
  unsnapshotted: new TimeChart({
    title: 'Unsnapshotted bytes',
    subtitle: 'Applied since the last snapshot',
    series: [{ key: 'unsnapshotted', label: 'Unsnapshotted' }],
    format: bytes,
    binary: true,
    threshold: () => {
      const after = latestMetrics() && latestMetrics().snapshots && latestMetrics().snapshots.after_bytes;
      return after ? { value: after, label: `snapshot trigger ${bytes(after)}` } : null;
    },
  }),
};
byId('charts').append(...Object.values(charts).map((chart) => chart.root));

const panels = {
  cluster: new Panel(byId('cluster-panel')),
  groups: new Panel(byId('groups-panel')),
  databases: new Panel(byId('databases-panel')),
  activity: new Panel(byId('activity-panel')),
  modules: new Panel(byId('modules-panel')),
  keys: new Panel(byId('keys-panel')),
  backup: new Panel(byId('backup-panel')),
  retention: new Panel(byId('retention-panel')),
  work: new Panel(byId('work-panel')),
};
const storage = byId('storage-grid');
const storagePanels = {
  compaction: new Panel(storage, { title: 'Compaction' }),
  snapshots: new Panel(storage, { title: 'Snapshots' }),
  log: new Panel(storage, { title: 'Raft log and purge' }),
  wasm: new Panel(storage, { title: 'Wasm runtime' }),
  cache: new Panel(storage, { title: 'Query cache' }),
  admission: new Panel(storage, { title: 'Admission' }),
  writer: new Panel(storage, { title: 'Writer and retention' }),
};

// ------------------------------------------------------------ samples

function latestMetrics() {
  const last = state.samples[state.samples.length - 1];
  return last ? last.metrics : null;
}

function restartedBetween(from, to) {
  return state.restartTimes.some((at) => at > from && at <= to);
}

/** Gauges straight from one metrics answer, rates from the change since
 *  the previous one. A counter that went down (a restart) gives a gap. */
function derive(previous, metrics, t) {
  const classes = Object.fromEntries((metrics.admission && metrics.admission.classes) || []);
  const gauge = (name, field) => (classes[name] ? classes[name][field] : null);
  const purge = metrics.log_purge || {};
  const d = {
    activeQuery: gauge('query', 'active'),
    activeRefresh: gauge('refresh', 'active'),
    activeWriter: gauge('writer', 'active'),
    queuedQuery: gauge('query', 'queued'),
    queuedRefresh: gauge('refresh', 'queued'),
    queuedWriter: gauge('writer', 'queued'),
    belowPurge: isNumber(purge.target) ? Math.max(0, purge.target - (purge.first_retained ?? purge.target + 1) + 1) : 0,
    held: purge.hold ? purge.hold.held_bytes : null,
    unsnapshotted: metrics.snapshots ? metrics.snapshots.unsnapshotted_bytes : null,
    dbRates: {},
  };
  if (!previous || restartedBetween(previous.t, t)) return d;
  const seconds = (t - previous.t) / 1000;
  if (seconds <= 0) return d;
  const delta = (get) => {
    const before = get(previous.metrics);
    const after = get(metrics);
    return isNumber(before) && isNumber(after) && after >= before ? after - before : null;
  };
  const per = (get) => {
    const change = delta(get);
    return change === null ? null : change / seconds;
  };
  const before = Object.fromEntries((previous.metrics.dbs || []).map((db) => [db.db, db.revision]));
  let writes = 0;
  let reset = false;
  for (const db of metrics.dbs || []) {
    if (!(db.db in before)) continue;
    const change = db.revision - before[db.db];
    if (change < 0) {
      reset = true;
      continue;
    }
    writes += change;
    d.dbRates[db.db] = change / seconds;
  }
  d.writes = reset ? null : writes / seconds;
  d.shed = per((m) => m.admission && m.admission.shed && m.admission.shed.arrival + m.admission.shed.queued + m.admission.shed.deadline);
  d.rejected = per((m) => m.admission && m.admission.rejected);
  d.timedOut = per((m) => m.admission && m.admission.timed_out);
  d.lease = per((m) => m.reads && m.reads.lease_reads);
  d.quorum = per((m) => m.reads && m.reads.quorum_rounds);
  d.fences = per((m) => m.reads && m.reads.remote_fences);
  const hits = delta((m) => m.queries && m.queries.cache.hits);
  const misses = delta((m) => m.queries && m.queries.cache.misses);
  d.hitRate = hits !== null && misses !== null && hits + misses > 0 ? (100 * hits) / (hits + misses) : null;
  return d;
}

/** Add one metrics sample taken at `t` (the member's clock when the
 *  stream stamped it, this browser's when polled). */
function addSample(metrics, t) {
  const previous = state.samples[state.samples.length - 1];
  if (previous && t <= previous.t) return;
  state.samples.push({ t, metrics, d: derive(previous, metrics, t) });
  while (state.samples.length && state.samples[0].t < t - MAX_WINDOW) state.samples.shift();
  state.status = metrics.node || state.status;
}

function now() {
  return Date.now() + state.offset;
}

function windowed() {
  const to = now();
  const from = to - prefs.window;
  return { from, to, samples: state.samples.filter((sample) => sample.t >= from) };
}

function trend(key, samples) {
  return samples.slice(-30).map((sample) => sample.d[key] ?? null);
}

/** Leader and term changes as chart markers: from the stream's
 *  transitions (those a reconnect recovered included), or from polls. */
function markers() {
  const marks = [];
  let previous = null;
  for (const transition of state.transitions) {
    if (previous && (previous.term !== transition.term || previous.leader !== transition.leader)) {
      const leader = transition.leader == null ? 'no leader' : `leader ${transition.leader}`;
      const mark = {
        t: transition.at_ms,
        label: `T${transition.term}${transition.leader == null ? '' : `·L${transition.leader}`}`,
        title: `${clock(transition.at_ms)}: term ${transition.term}, ${leader} (${transition.state})`,
      };
      // An election's steps a moment apart draw as one line, labelled
      // with where they ended; its title lists every step.
      const last = marks[marks.length - 1];
      if (last && mark.t - last.t < 1500) {
        last.label = mark.label;
        last.title = `${last.title}\n${mark.title}`;
      } else {
        marks.push(mark);
      }
    }
    previous = transition;
  }
  return marks;
}

/** Keep a transition, by sequence number when the stream sends one. */
function noteTransition(transition) {
  const last = state.transitions[state.transitions.length - 1];
  if (isNumber(transition.seq) && last && isNumber(last.seq) && transition.seq <= last.seq) return;
  state.transitions.push(transition);
  while (state.transitions.length > 256) state.transitions.shift();
}

// ----------------------------------------------------------- rendering

const TRANSPORT = {
  live: ['good', 'live'],
  connecting: ['info', 'connecting'],
  reconnecting: ['warning', 'reconnecting'],
  fallback: ['warning', 'fallback polling'],
  idle: ['neutral', 'idle'],
};

function transportChip() {
  const [status, label] = state.paused ? ['neutral', 'paused'] : TRANSPORT[state.transport] || TRANSPORT.idle;
  return el('span', { title: state.transportDetail || undefined }, chip(status, label));
}

function renderHeader() {
  const status = state.status;
  const identity = byId('node-identity');
  if (status) {
    fill(identity,
      el('strong', {}, `Node ${status.id}`), ' ',
      chip(status.state === 'Leader' ? 'good' : status.leader == null ? 'warning' : 'neutral', status.state),
      el('span', { class: 'muted' }, ` term ${exact(status.term)} · leader ${status.leader ?? 'unknown'}`));
  } else {
    fill(identity, el('span', { class: 'muted' }, state.unreachable ? 'unreachable' : 'connecting…'));
  }
  const link = byId('leader-link');
  fill(link);
  if (status && status.leader != null && status.leader !== status.id && state.leaderUrl) {
    try {
      const target = new URL('/admin', state.leaderUrl);
      if ((target.protocol === 'http:' || target.protocol === 'https:') && target.origin !== location.origin) {
        link.append(el('a', { href: target.href, rel: 'noopener noreferrer' }, `Leader’s dashboard (node ${status.leader}) ↗`));
      }
    } catch {
      // Not a URL: no link.
    }
  }
  fill(byId('transport'), transportChip());
  const refreshed = byId('last-refresh');
  refreshed.textContent = state.lastRefresh
    ? `Updated ${clock(state.lastRefresh)}${state.transport !== 'live' && isNumber(state.latency) ? ` · ${Math.round(state.latency)} ms` : ''}`
    : '';
  byId('forget').textContent = token.get() ? 'Forget token' : 'Enter token';
  byId('pause').textContent = state.paused ? 'Resume' : 'Pause';
  byId('pause').setAttribute('aria-pressed', String(state.paused));
}

function renderBanners() {
  const banners = [];
  const metrics = latestMetrics();
  const failure = (state.ready && state.ready.storage) || (metrics && metrics.storage_failure);
  if (state.unreachable) {
    banners.push(banner('critical', 'Cannot reach this member', `${state.unreachable}. Panels show the last data received.`));
  }
  if (failure) {
    banners.push(banner('critical', `Storage failed: ${failure.component}`,
      `${failure.error} (since ${clock(failure.since_ms)}). This member refuses work until it restarts with healthy storage.`));
  }
  if (state.ready && !state.ready.ok && !failure) {
    banners.push(banner('warning', 'Not ready', state.ready.reason || 'This member is not ready to serve.'));
  }
  if (state.ready && state.ready.rejoining) {
    banners.push(banner('warning', 'Rejoining', 'This member opened without Raft state and is catching up with its group: it neither votes nor serves sync until then.'));
  }
  const recent = state.restartTimes.filter((at) => Date.now() - at < 10 * 60 * 1000);
  if (recent.length) {
    banners.push(banner('serious', 'Process restarted', `At ${clock(recent[recent.length - 1])}: counters started again from zero; the charts show a gap there.`));
  }
  if (!token.get()) {
    const enter = el('button', { type: 'button', class: 'action primary' }, 'Enter admin token');
    enter.addEventListener('click', () => askToken());
    banners.push(banner('info', 'Admin token needed', 'Only status and readiness show without it.', enter));
  }
  if (state.transport === 'fallback' && !state.paused) {
    banners.push(banner('warning', 'Polling', `The admin stream is unavailable (${state.transportDetail || 'no reason given'}): polling every ${duration(prefs.interval)} until it comes back.`));
  }
  if (state.paused) banners.push(banner('info', 'Paused', 'Nothing updates until you resume.'));
  // Unchanged banners stay in place: no flicker, no repeated announcement.
  const key = banners.map((node) => node.textContent).join('|');
  if (key === state.bannerKey) return;
  state.bannerKey = key;
  fill(byId('banners'), banners);
}

function banner(status, title, text, extra) {
  return el('div', { class: `banner ${status}`, role: status === 'critical' ? 'alert' : 'status' },
    chip(status, title), el('span', { class: 'banner-text' }, text), extra || null);
}

function renderSamples() {
  const { from, to, samples } = windowed();
  const points = samples.map((sample) => ({ t: sample.t, v: sample.d }));
  const marks = markers();
  for (const chart of Object.values(charts)) chart.update(points, from, to, marks);
  const latest = samples.length ? samples[samples.length - 1].d : {};
  renderTiles(byId('tiles'), {
    status: state.status,
    ready: state.ready,
    latest: { writes: latest.writes, reads: proofs(latest), hitRate: latest.hitRate },
    trend: {
      writes: trend('writes', samples),
      reads: samples.slice(-30).map((sample) => proofs(sample.d)),
      hitRate: trend('hitRate', samples),
    },
    activity: state.activity,
    uptime: state.activity
      ? state.activity.sampled_at_ms - state.activity.started_at_ms + (Date.now() - state.activityAt)
      : null,
    restarts: state.restarts,
  });
  const metrics = latestMetrics();
  if (metrics) {
    renderDatabases(panels.databases, metrics, (db) => samples.map((sample) => sample.d.dbRates[db] ?? null));
    renderStorage(storagePanels, metrics);
  }
}

/** Fresh reads proven per second: by lease, by quorum round, or by a
 *  fence from the leader (followers). */
function proofs(d) {
  const parts = [d.lease, d.quorum, d.fences];
  return parts.every(isNumber) ? parts.reduce((a, b) => a + b, 0) : null;
}

function renderClusterPanel() {
  if (!state.status && !state.raft && !state.membership) return;
  renderCluster(panels.cluster, { status: state.status, raft: state.raft, membership: state.membership, ready: state.ready }, actions);
}

function renderGroupsPanel() {
  if (state.partitionCatalog === undefined) return;
  renderGroups(panels.groups, {
    hosted: state.hostedGroups,
    catalog: state.partitionCatalog,
    transactions: state.inDoubt,
    polling: state.transport === 'fallback',
    offset: state.offset,
  }, actions);
}

function renderActivityPanel() {
  if (!state.activity) return;
  const connection = state.transport === 'live' || !state.activityLive ? transportChip() : chip('good', 'activity stream');
  renderActivity(panels.activity, state.activity, state.activityAt, connection);
}

function renderAll() {
  state.lastRefresh = Date.now();
  renderHeader();
  renderBanners();
  renderSamples();
  renderClusterPanel();
}

// ------------------------------------------------------ what arrives

function applyReadiness(ready) {
  state.ready = ready;
}

function applyActivity(snapshot) {
  if (!snapshot || typeof snapshot.process_id !== 'string') return;
  if (state.processId && snapshot.process_id !== state.processId) {
    state.restarts += 1;
    state.restartTimes.push(now());
  }
  state.processId = snapshot.process_id;
  try {
    sessionStorage.setItem(PROCESS_KEY, JSON.stringify({ id: state.processId, restarts: state.restarts }));
  } catch {
    // Counted for this page only.
  }
  state.activity = snapshot;
  state.activityAt = Date.now();
  renderActivityPanel();
}

/** The catalog event: modules and staging, keys, retention, membership. */
function applyCatalog(catalog) {
  if (!catalog || catalog.error) {
    const error = new Error(catalog ? catalog.error : 'no catalog');
    for (const panel of [panels.modules, panels.keys, panels.retention]) panel.fail(error);
    return;
  }
  const databases = catalog.databases || [];
  const staging = new Map();
  for (const database of databases) {
    for (const module of database.modules || []) {
      if (module.staging) {
        staging.set(`${database.db}\u0000${module.name}`, { staging: module.staging, identity: module.staged_identity });
      }
    }
  }
  const note = catalog.leads
    ? null
    : 'A staged deployment replicates and shows on every member; a one-call deployment in progress shows on the leader’s dashboard only.';
  renderModules(panels.modules, { databases }, staging, actions, note);
  if (catalog.keys && catalog.keys.error) panels.keys.fail(new Error(catalog.keys.error));
  else renderKeys(panels.keys, catalog.keys);
  state.catalog = catalog;
  renderRetentionPanel();
  if (catalog.membership) state.membership = { membership: catalog.membership };
  state.catalogDbs = databases.map((database) => database.db);
}

/** Retention per database from the last catalog, with the receipt
 *  clock from the last work sample and the leader's counters from the
 *  last metrics sample. */
function renderRetentionPanel() {
  const catalog = state.catalog;
  if (!catalog || catalog.error) return;
  const stats = latestMetrics() && latestMetrics().retention;
  renderRetention(panels.retention, (catalog.databases || []).map((database) => [
    database.db,
    database.retention && database.retention.error
      ? new Error(database.retention.error)
      : { ...database.retention, receipts: state.receipts || {}, stats },
  ]), actions);
}

/** Timer and queue counts, as the stream sends them. */
function applyWork(work) {
  if (!work || work.error) {
    panels.work.fail(new Error(work ? work.error : 'no counts'));
    return;
  }
  if (work.receipts && !work.receipts.error) {
    state.receipts = work.receipts;
    renderRetentionPanel();
  }
  const failed = (entry) => (entry.error ? new Error(entry.error) : entry);
  renderWork(panels.work, {
    timers: (work.timers || []).map((entry) => [entry.db, failed(entry)]),
    queues: (work.queues || []).map((entry) => [entry.db, entry.queue, failed(entry)]),
    dbs: state.catalogDbs || (work.timers || []).map((entry) => entry.db),
  }, actions);
}

function applyBackup(backup) {
  if (backup && !backup.error) renderBackup(panels.backup, backup, actions);
  else panels.backup.fail(new Error(backup ? backup.error : 'no backup status'));
}

/** Replication progress merged into the Raft view. */
function applyReplication(progress) {
  if (state.ready) {
    // The tile's positions move with replication; whether and why
    // ready comes with readiness events.
    if (isNumber(progress.applied)) state.ready.applied = progress.applied;
    if (isNumber(progress.committed)) state.ready.committed = progress.committed;
  }
  if (!state.raft || !state.raft.raft) return;
  const raft = state.raft.raft;
  raft.last_log_index = progress.last_log;
  if (raft.last_applied && isNumber(progress.applied)) raft.last_applied = { ...raft.last_applied, index: progress.applied };
  raft.snapshot = progress.snapshot;
  raft.purged = progress.purged;
  raft.replication = progress.replication;
  raft.millis_since_quorum_ack = progress.millis_since_quorum_ack;
}

// ---------------------------------------------------------- the stream

const handlers = {
  snapshot(snapshot) {
    clearTimeout(state.snapshotTimer);
    state.streamFailures = 0;
    setTransport('live', snapshot.reason === 'resync' ? 'resynced after falling behind' : '');
    stopFallback();
    state.status = snapshot.node;
    applyReadiness(snapshot.readiness);
    state.raft = snapshot.raft && !snapshot.raft.error ? { raft: snapshot.raft } : state.raft;
    state.membership = { membership: snapshot.membership };
    for (const transition of snapshot.transitions || []) noteTransition(transition);
    if (snapshot.metrics && !snapshot.metrics.error) {
      state.offset = snapshot.metrics.at_ms - Date.now();
      addSample(snapshot.metrics, snapshot.metrics.at_ms);
    }
    applyCatalog(snapshot.catalog);
    applyBackup(snapshot.backup);
    applyWork(snapshot.work);
    if (snapshot.activity) applyActivity(snapshot.activity);
    // Older members send no group sections: as one serving one group.
    state.hostedGroups = snapshot.groups ?? null;
    state.partitionCatalog = snapshot.partitions ?? null;
    state.inDoubt = snapshot.transactions ?? null;
    renderGroupsPanel();
    renderAll();
  },
  raft(transition) {
    noteTransition(transition);
    if (state.status) {
      state.status = { ...state.status, term: transition.term, state: transition.state, leader: transition.leader };
    }
    if (state.raft && state.raft.raft) {
      Object.assign(state.raft.raft, { current_term: transition.term, current_leader: transition.leader, state: transition.state });
    }
    renderAll();
  },
  readiness(ready) {
    applyReadiness(ready);
    renderAll();
  },
  replication(progress) {
    applyReplication(progress);
    renderClusterPanel();
  },
  metrics(sample) {
    if (sample.error) {
      for (const chart of Object.values(charts)) chart.setError(`metrics: ${sample.error}`);
      return;
    }
    for (const chart of Object.values(charts)) chart.setError(null);
    state.offset = sample.at_ms - Date.now();
    addSample(sample, sample.at_ms);
    renderAll();
  },
  catalog(catalog) {
    applyCatalog(catalog);
    renderClusterPanel();
  },
  backup: applyBackup,
  work: applyWork,
  activity: applyActivity,
  group_raft(transition) {
    const group = (state.hostedGroups || []).find((entry) => entry.name === transition.group && entry.uid === transition.uid);
    if (!group) return;
    group.node = { ...group.node, term: transition.term, state: transition.state, leader: transition.leader };
    renderGroupsPanel();
  },
  groups(update) {
    state.hostedGroups = update.groups || [];
    renderGroupsPanel();
  },
  partitions(catalog) {
    state.partitionCatalog = catalog;
    renderGroupsPanel();
  },
  transactions(inDoubt) {
    state.inDoubt = inDoubt;
    renderGroupsPanel();
  },
};

// Every event the page consumes is one the server sends (tests compare
// this list with the server's).
if (STREAM_EVENTS.some((name) => !(name in handlers))) throw new Error('an admin stream event has no handler');

function setTransport(transport, detail) {
  if (Math.abs(state.offset) > 500 && (transport === 'fallback') !== (state.transport === 'fallback')) {
    // Server and browser clocks disagree: samples on both would misorder.
    state.samples = [];
  }
  if (transport === 'fallback') state.offset = 0;
  state.transport = transport;
  state.transportDetail = detail || '';
  renderHeader();
  renderBanners();
  renderActivityPanel();
  renderGroupsPanel();
}

function streamUrl() {
  const params = new URLSearchParams([['interval_ms', String(prefs.interval)]]);
  for (const [db, queue] of prefs.queues) params.append('queue', `${db}/${queue}`);
  return `${ENDPOINTS.adminStream}?${params}`;
}

/** Open the admin stream; fall back to polling when it is refused or
 *  sends no snapshot in time, and try it again later. */
async function connect() {
  clearTimeout(state.retryTimer);
  clearTimeout(state.snapshotTimer);
  if (state.streamAbort) state.streamAbort.abort();
  if (!token.get() || state.paused) {
    setTransport('idle');
    return;
  }
  const controller = new AbortController();
  state.streamAbort = controller;
  if (state.transport !== 'fallback') setTransport(state.transport === 'live' ? 'reconnecting' : 'connecting');
  state.snapshotTimer = setTimeout(() => {
    if (state.streamAbort !== controller) return;
    state.streamAbort = null;
    controller.abort();
    startFallback('no snapshot within 5 s: a proxy may be buffering the stream');
    retryLater(STREAM_RETRY);
  }, SNAPSHOT_WITHIN);
  try {
    await stream(streamUrl(), (name, data) => {
      const handle = handlers[name];
      if (!handle) return;
      let value;
      try {
        value = JSON.parse(data);
      } catch {
        return;
      }
      handle(value);
    }, controller.signal);
  } catch (error) {
    clearTimeout(state.snapshotTimer);
    if (controller.signal.aborted && state.streamAbort !== controller) return;
    if (error.status === 403) {
      setTransport('idle');
      return;
    }
    state.streamFailures += 1;
    if (error.status === 429 || state.streamFailures >= 2) {
      startFallback(error.status === 429 ? 'stream capacity reached' : describe(error));
      retryLater(STREAM_RETRY);
      return;
    }
    setTransport('reconnecting', describe(error));
    retryLater(1000 * 2 ** Math.min(state.streamFailures, 4));
    return;
  }
  clearTimeout(state.snapshotTimer);
  if (controller.signal.aborted) return;
  // The member ended it (shutdown): reconnect, polling meanwhile.
  state.streamFailures += 1;
  setTransport('reconnecting', 'the stream ended');
  retryLater(1000 * 2 ** Math.min(state.streamFailures, 4));
}

function retryLater(ms) {
  clearTimeout(state.retryTimer);
  state.retryTimer = setTimeout(() => {
    if (!state.paused) connect();
  }, ms);
}

// ------------------------------------------------------------ fallback

function startFallback(reason) {
  const already = state.transport === 'fallback';
  setTransport('fallback', reason);
  if (already) return;
  schedule();
  connectActivity();
  state.lastSlow = 0;
  tick();
}

function stopFallback() {
  clearTimeout(state.fastTimer);
  state.fastTimer = null;
  if (state.activityAbort) state.activityAbort.abort();
  state.activityAbort = null;
  state.activityLive = false;
}

/** One polling round: status, readiness, metrics and Raft. */
async function tick() {
  if (state.ticking) return;
  state.ticking = true;
  const authorized = Boolean(token.get());
  const polling = state.transport === 'fallback';
  const [status, ready, metrics, raft, activity] = await Promise.allSettled([
    request(ENDPOINTS.status),
    request(ENDPOINTS.ready, { accept: [503] }),
    authorized && polling ? request(ENDPOINTS.metrics) : Promise.resolve(null),
    authorized && polling ? request(ENDPOINTS.raft) : Promise.resolve(null),
    authorized && polling && !state.activityLive ? request(ENDPOINTS.activity) : Promise.resolve(null),
  ]);
  state.ticking = false;
  const at = Date.now();
  if (status.status === 'fulfilled') {
    const previous = state.status;
    state.status = status.value.body;
    state.unreachable = null;
    if (polling && previous && (previous.term !== state.status.term || previous.leader !== state.status.leader)) {
      noteTransition({ at_ms: at, term: state.status.term, leader: state.status.leader, state: state.status.state });
    }
  } else {
    state.unreachable = describe(status.reason);
  }
  if (ready.status === 'fulfilled') applyReadiness(ready.value.body);
  if (metrics.status === 'fulfilled' && metrics.value) {
    addSample(metrics.value.body, at);
    state.latency = metrics.value.elapsed;
    for (const chart of Object.values(charts)) chart.setError(null);
  } else if (metrics.status === 'rejected') {
    for (const chart of Object.values(charts)) chart.setError(`metrics: ${describe(metrics.reason)}`);
    panels.databases.fail(metrics.reason);
    for (const panel of Object.values(storagePanels)) panel.fail(metrics.reason);
  }
  if (raft.status === 'fulfilled' && raft.value) state.raft = raft.value.body;
  else if (raft.status === 'rejected') panels.cluster.fail(raft.reason);
  if (activity.status === 'fulfilled' && activity.value) applyActivity(activity.value.body);
  else if (activity.status === 'rejected') panels.activity.fail(activity.reason);
  renderAll();
  if (authorized && polling && at - state.lastSlow >= SLOW_EVERY) slowTick();
}

/** One slow polling round: membership, modules and staging, keys,
 *  backup, retention, timers and tracked queues. */
async function slowTick() {
  if (!token.get()) return;
  state.lastSlow = Date.now();
  const dbs = ((latestMetrics() && latestMetrics().dbs) || []).map((db) => db.db);
  const settle = (promise) => promise.then((response) => response.body, (error) => (error instanceof Error ? error : new Error(String(error))));
  const [membership, modules, keys, backup] = await Promise.all([
    settle(request(ENDPOINTS.membership)),
    settle(request(ENDPOINTS.modules)),
    settle(request(ENDPOINTS.keys)),
    settle(request(ENDPOINTS.backup)),
  ]);
  const query = (path, params) => `${path}?${new URLSearchParams(params)}`;
  const [retention, timers, queues] = await Promise.all([
    Promise.all(dbs.map(async (db) => [db, await settle(request(query(ENDPOINTS.retention, { db })))])),
    Promise.all(dbs.map(async (db) => [db, await settle(request(query(ENDPOINTS.timerStats, { db })))])),
    Promise.all(prefs.queues.map(async ([db, queue]) => [db, queue, await settle(request(query(ENDPOINTS.queueStats, { db, queue })))])),
  ]);
  if (state.transport !== 'fallback') return;
  if (membership instanceof Error) panels.cluster.fail(membership);
  else {
    state.membership = membership;
    renderClusterPanel();
  }
  if (modules instanceof Error) panels.modules.fail(modules);
  else {
    // Ask the leader (followers forward) per module: a one-call
    // deployment's staging is its own.
    const pairs = (modules.databases || []).flatMap((database) => (database.modules || []).map((module) => [database.db, module.name]));
    const statuses = await Promise.all(pairs.map(async ([db, module]) => [
      `${db}\u0000${module}`,
      await settle(request(ENDPOINTS.deployments, { method: 'POST', body: { operation: 'status', db, module } })),
    ]));
    renderModules(panels.modules, modules, new Map(statuses), actions);
  }
  if (keys instanceof Error) panels.keys.fail(keys);
  else renderKeys(panels.keys, keys);
  if (backup instanceof Error) panels.backup.fail(backup);
  else renderBackup(panels.backup, backup, actions);
  renderRetention(panels.retention, retention, actions);
  renderWork(panels.work, { timers, queues, dbs }, actions);
}

function schedule() {
  clearTimeout(state.fastTimer);
  state.fastTimer = null;
  if (state.paused || state.transport !== 'fallback') return;
  const loop = async () => {
    await tick();
    if (!state.paused && state.transport === 'fallback') state.fastTimer = setTimeout(loop, prefs.interval);
  };
  state.fastTimer = setTimeout(loop, prefs.interval);
}

/** While polling, activity still streams from its own endpoint. */
async function connectActivity() {
  if (state.activityAbort) state.activityAbort.abort();
  if (!token.get() || state.paused || state.transport !== 'fallback') return;
  const controller = new AbortController();
  state.activityAbort = controller;
  try {
    await stream(ENDPOINTS.activityStream, (name, data) => {
      if (name !== 'snapshot') return;
      state.activityLive = true;
      try {
        applyActivity(JSON.parse(data));
      } catch {
        // A malformed snapshot: wait for the next.
      }
    }, controller.signal);
  } catch {
    // Polled instead.
  }
  state.activityLive = false;
}

// --------------------------------------------------------------- token

const tokenDialog = byId('token-dialog');
const tokenForm = byId('token-form');
const tokenInput = byId('token-input');

function askToken(message) {
  byId('token-error').textContent = message || '';
  if (!tokenDialog.open) tokenDialog.showModal();
  tokenInput.focus();
}

tokenForm.addEventListener('submit', (event) => {
  event.preventDefault();
  const value = tokenInput.value.trim();
  if (!value) return;
  token.set(value);
  tokenInput.value = '';
  tokenDialog.close();
  restart();
});
byId('token-later').addEventListener('click', () => tokenDialog.close());

onForbidden(() => {
  token.clear();
  if (state.streamAbort) state.streamAbort.abort();
  stopFallback();
  setTransport('idle');
  askToken('The admin token was refused (HTTP 403). Enter it again.');
});

onLeaderUrl((url) => {
  state.leaderUrl = url;
});

// ------------------------------------------------------------- actions

const confirmDialog = byId('confirm-dialog');
const confirmForm = byId('confirm-form');
let confirmResolve = null;

/** Ask before an action, saying exactly what it does and sends. */
function confirmAction({ title, lines, method, path, body, verb }) {
  fill(byId('confirm-title'), title);
  fill(byId('confirm-body'), lines.map((line) => el('p', {}, line)));
  byId('confirm-request').textContent = `${method} ${path}\n${JSON.stringify(body, null, 2)}`;
  byId('confirm-ok').textContent = verb || 'Confirm';
  confirmDialog.showModal();
  byId('confirm-cancel').focus();
  return new Promise((resolve) => {
    confirmResolve = resolve;
  });
}

function closeConfirm(ok) {
  if (confirmDialog.open) confirmDialog.close();
  if (confirmResolve) confirmResolve(ok);
  confirmResolve = null;
}

confirmForm.addEventListener('submit', (event) => {
  event.preventDefault();
  closeConfirm(true);
});
byId('confirm-cancel').addEventListener('click', () => closeConfirm(false));
confirmDialog.addEventListener('cancel', () => closeConfirm(false));

/** Run a confirmed POST and report its outcome in `panel`. The stream
 *  then pushes what it changed; only polling looks again. */
async function perform(panel, spec, summarize) {
  if (!(await confirmAction({ ...spec, method: 'POST' }))) return;
  panel.report(null, `${spec.title.replace(/\?$/, '')}…`);
  try {
    const { body } = await request(spec.path, { method: 'POST', body: spec.body });
    if (body && body.ok === false) panel.report(false, `${body.code || 'refused'}: ${body.message || 'no message'}`);
    else panel.report(true, summarize(body || {}));
  } catch (error) {
    panel.report(false, describe(error));
  }
  if (state.transport === 'fallback') {
    state.lastSlow = 0;
    tick();
  }
}

const actions = {
  transfer(to, leader) {
    perform(panels.cluster, {
      title: `Transfer leadership to node ${to}?`,
      verb: 'Transfer leadership',
      path: ENDPOINTS.leadership,
      body: { to },
      lines: [
        `Node ${leader ?? '(unknown)'} hands leadership to voter ${to}. This member sends the request to the leader, forwarding it if it does not lead.`,
        'While leadership moves, writes are refused with a retryable 503 and clients retry. A staged deployment the old leader holds is dropped.',
        'If the transfer does not happen, the answer is 409 transfer_failed and leadership stays where it was.',
      ],
    }, (body) => {
      const moved = body.transfer || {};
      if (!isNumber(moved.to)) return `Leadership transfer: ${JSON.stringify(body)}`;
      const what = moved.unchanged ? `Node ${moved.to} already led` : `Node ${moved.from} handed leadership to node ${moved.to}`;
      return `${what} (term ${exact(moved.term)}${isNumber(moved.elapsed_ms) ? `, ${duration(moved.elapsed_ms)}` : ''})`;
    });
  },
  backup() {
    perform(panels.backup, {
      title: 'Take a full backup now?',
      verb: 'Back up now',
      path: ENDPOINTS.backup,
      body: {},
      lines: [
        `Member ${state.status ? state.status.id : ''} encrypts every database’s current view and uploads it to its backup store as a new backup.`,
        'Continuous backup and the log hold carry on unchanged. Large databases take a while; the request waits for the upload.',
      ],
    }, (body) => `Backup ${shortHex(body.backup_id || '')}: ${count(body.objects)} objects, covering index ${exact(body.covered_index)}`);
  },
  deployment(operation, db, module, staged) {
    const target = staged && staged.identity ? shortHex(staged.identity) : 'the staged target';
    const specs = {
      advance: {
        title: `Advance the staged deployment of ${module}?`,
        verb: 'Advance backfill',
        body: { operation, db, module, max_records: 4096 },
        lines: [
          `The leader builds up to 4,096 more records of the backfill for module “${module}” in database “${db}” (target ${target}), in one bounded transaction.`,
          'The active code keeps serving; nothing switches until activation.',
        ],
      },
      activate: {
        title: `Activate the staged deployment of ${module}?`,
        verb: 'Activate',
        body: { operation, db, module },
        lines: [
          `The leader checks the links again and proposes the activation of ${target} for module “${module}” in database “${db}” through consensus.`,
          'Every member switches code, indexes, HTTP aliases and access policies at once. Retired structures wait for collection.',
          'A refusal answers 409 deployment and leaves the old code active.',
        ],
      },
      cancel: {
        title: `Cancel the staged deployment of ${module}?`,
        verb: 'Cancel staging',
        body: { operation, db, module },
        lines: [
          `The leader abandons the staging of ${target} for module “${module}” in database “${db}”: its backfill is purged and the names only it interned are released.`,
          'The active code keeps serving. Staging again starts from scratch.',
        ],
      },
    };
    perform(panels.modules, { ...specs[operation], path: ENDPOINTS.deployments }, (body) => {
      if (operation === 'cancel') return body.canceled ? 'Staging canceled' : 'Nothing was staged';
      if (operation === 'activate') return `Activated at revision ${exact(body.revision)}, log position ${exact(body.position)}`;
      if (!body.staging) return 'Advanced';
      if (body.rebuilt || body.staging.phase === 'rebuilding') {
        return `Graph at ${body.staging.graph_position} of ${body.staging.graph}${body.staging.ready ? ', ready to activate' : ''} (${count(body.rebuilt || 0)} roots rebuilt)`;
      }
      return `Backfill at ${body.staging.position} of ${body.staging.planned}${body.staging.ready ? ', ready to activate' : ''} (${count(body.scanned)} records scanned)`;
    });
  },
  retention(db, operation, values, current) {
    const specs = {
      rotate: {
        title: `Change retention rotation of ${db}?`,
        verb: 'Rotate',
        body: { db, operation, epoch_ms: values.epoch_ms, keep_epochs: values.keep_epochs },
        lines: [
          values.epoch_ms
            ? `The leader starts a new retry epoch of database “${db}” every ${duration(values.epoch_ms)}, keeping ${values.keep_epochs} epochs (the current one included).`
            : `The leader stops rotating retry epochs of database “${db}” (no epoch length), keeping ${values.keep_epochs} epochs.`,
          `Currently: ${current.epoch_ms ? `every ${duration(current.epoch_ms)}, keeping ${current.keep_epochs}` : 'not rotating'}.`,
        ],
      },
      advance: {
        title: `Advance retention epochs of ${db}?`,
        verb: 'Advance',
        body: { db, operation, from_epoch: values.from_epoch, current_epoch: values.current_epoch, min_epoch: values.min_epoch },
        lines: [
          `Database “${db}” moves from epoch ${values.from_epoch} to current epoch ${values.current_epoch}, with floor ${values.min_epoch}.`,
          'Scoped request IDs from epochs below the floor are refused afterwards, and their receipts become collectable. Refused if the current epoch is no longer the one it was planned against.',
        ],
      },
      collect: {
        title: `Collect retry receipts of ${db}?`,
        verb: 'Collect',
        body: { db, operation, limit: values.limit },
        lines: [
          `The leader inspects up to ${values.limit ?? 'a page of'} receipts of database “${db}” and deletes those below the floor or past their lifetime.`,
          'Deleted receipts no longer deduplicate retries of their requests.',
        ],
      },
    };
    perform(panels.retention, { ...specs[operation], path: ENDPOINTS.retention }, (body) => `Retention ${operation}: ${JSON.stringify(body.retention ?? body)}`);
  },
  abortMove(partition, movement) {
    perform(panels.groups, {
      title: `Abort the move of ${partition}?`,
      verb: 'Abort move',
      path: ENDPOINTS.partitions,
      body: { operation: 'abort', partition, op: movement.op, reason: 'aborted from the dashboard' },
      lines: [
        `The root’s leader records that move “${movement.op}” of partition “${partition}” from ${movement.source} to ${movement.destination} is given up. It is now ${movement.phase}.`,
        `The driver then thaws ${partition} in ${movement.source} at a new epoch, where it serves again, and abandons the copy in ${movement.destination}.`,
        'A move past its cutover cannot be aborted: the answer is then 409 and the move completes.',
      ],
    }, (body) => `Move ${movement.op}: ${JSON.stringify(body.value)}`);
  },
  abortTransaction(group, record) {
    perform(panels.groups, {
      title: 'Abort this transaction?',
      verb: 'Abort transaction',
      path: ENDPOINTS.transactions,
      body: { operation: 'abort', transaction: record.id, group },
      lines: [
        `Group ${group} coordinates transaction ${record.id} across ${(record.participants || []).map((participant) => participant.group).join(', ')}; it has not decided yet.`,
        'Its leader records an abort decision: every participant drops what it prepared and releases its databases. If a commit was decided first, that decision stands.',
      ],
    }, (body) => `Transaction ${shortHex(record.id, 10)}: ${(body.value && body.value.decision) || 'decided'}`);
  },
  // Tracked queues are stream parameters: reconnect with the new list.
  track(db, queue) {
    if (!validQueue([db, queue]) || prefs.queues.some(([a, b]) => a === db && b === queue)) return;
    prefs.queues.push([db, queue]);
    savePrefs();
    refreshQueues();
  },
  untrack(db, queue) {
    prefs.queues = prefs.queues.filter(([a, b]) => !(a === db && b === queue));
    savePrefs();
    refreshQueues();
  },
};

function refreshQueues() {
  if (state.transport === 'fallback') {
    state.lastSlow = 0;
    slowTick();
  } else {
    connect();
  }
}

// ------------------------------------------------------------ controls

function applyTheme() {
  if (prefs.theme === 'auto') delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = prefs.theme;
  byId('theme').textContent = `Theme: ${prefs.theme}`;
}

const intervalSelect = byId('interval');
intervalSelect.value = String(prefs.interval);
intervalSelect.addEventListener('change', () => {
  prefs.interval = Number(intervalSelect.value);
  savePrefs();
  // The stream samples at the interval it was opened with.
  if (state.transport === 'fallback') schedule();
  else connect();
});
const windowSelect = byId('window');
windowSelect.value = String(prefs.window);
if (windowSelect.value !== String(prefs.window)) windowSelect.value = '300000';
windowSelect.addEventListener('change', () => {
  prefs.window = Number(windowSelect.value);
  savePrefs();
  renderSamples();
});
byId('pause').addEventListener('click', () => {
  state.paused = !state.paused;
  if (state.paused) {
    clearTimeout(state.retryTimer);
    clearTimeout(state.snapshotTimer);
    if (state.streamAbort) state.streamAbort.abort();
    state.streamAbort = null;
    stopFallback();
    setTransport('idle');
  } else {
    restart();
  }
  renderHeader();
  renderBanners();
});
byId('refresh').addEventListener('click', () => {
  if (state.transport === 'fallback') {
    state.lastSlow = 0;
    tick();
  } else {
    // A fresh snapshot.
    connect();
  }
});
byId('forget').addEventListener('click', () => {
  if (token.get()) {
    token.clear();
    if (state.streamAbort) state.streamAbort.abort();
    stopFallback();
    setTransport('idle');
  }
  askToken();
});
byId('theme').addEventListener('click', () => {
  prefs.theme = { auto: 'light', light: 'dark', dark: 'auto' }[prefs.theme] || 'auto';
  savePrefs();
  applyTheme();
});

function restart() {
  state.streamFailures = 0;
  connect();
  // Status and readiness need no token: show them at once.
  tick();
}

// Charts' window ends at the member's now: move it between samples too.
setInterval(() => {
  if (!state.paused) renderSamples();
}, 5000);

applyTheme();
renderHeader();
renderBanners();
if (!token.get()) askToken();
restart();
