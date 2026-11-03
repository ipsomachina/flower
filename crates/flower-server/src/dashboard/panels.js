// Panels: each renders one part of the page from the endpoint data it
// is given, and degrades on its own when that endpoint fails.

import { el, fill, chip, facts, table } from './dom.js';
import { sparkline, meter, nestedBars } from './charts.js';
import {
  ago, bytes, clock, count, datetime, duration, exact, isNumber, logId, micros, percent, rate, shortHex, yesNo,
} from './format.js';

/**
 * A panel: a card whose body is replaced on success and kept, dimmed,
 * with the error beside it on failure. Actions report into `result`.
 */
export class Panel {
  constructor(container, { title, wide } = {}) {
    this.body = el('div', { class: 'panel-body' }, el('p', { class: 'muted' }, 'Waiting for data…'));
    this.error = el('p', { class: 'panel-error', hidden: true });
    this.result = el('div', { class: 'action-result', 'aria-live': 'polite' });
    this.root = el('div', { class: `card panel${wide ? ' wide' : ''}` },
      title ? el('h3', { class: 'panel-title' }, title) : null,
      this.error, this.body, this.result);
    container.append(this.root);
  }

  set(...children) {
    this.error.hidden = true;
    this.body.classList.remove('stale');
    fill(this.body, children);
  }

  fail(error) {
    this.error.hidden = false;
    this.error.textContent = describe(error);
    this.body.classList.add('stale');
  }

  /** Show an action's outcome: `ok` true, false, or null (running). */
  report(ok, text) {
    const status = ok === null ? 'info' : ok ? 'good' : 'critical';
    const label = ok === null ? 'Running' : ok ? 'Done' : 'Failed';
    fill(this.result, el('div', { class: `action-outcome ${status}` },
      chip(status, label), el('span', { class: 'action-text' }, text),
      el('span', { class: 'muted' }, ` at ${clock(Date.now())}`)));
  }
}

/** An error as one line. */
export function describe(error) {
  if (!error) return '';
  if (typeof error.describe === 'function') return error.describe();
  return error.message || String(error);
}

/** A button for an operator action. */
function action(label, onClick, { disabled, title, kind } = {}) {
  const button = el('button', { type: 'button', class: `action ${kind || ''}`, disabled: Boolean(disabled), title }, label);
  button.addEventListener('click', onClick);
  return button;
}

// ------------------------------------------------------------- health

/** One stat tile: label, value, optional sparkline and detail. */
function tile(label, value, { detail, trend, status } = {}) {
  return el('div', { class: 'tile' },
    el('div', { class: 'tile-label' }, label),
    el('div', { class: 'tile-value' }, status || value),
    trend || null,
    detail ? el('div', { class: 'tile-detail' }, detail) : null);
}

/** The KPI row. */
export function renderTiles(container, view) {
  const { status, ready, latest, trend, activity, restarts } = view;
  const tiles = [];
  if (ready) {
    tiles.push(tile('Readiness', null, {
      status: ready.ok ? chip('good', 'Ready') : chip(ready.storage ? 'critical' : 'warning', 'Not ready'),
      detail: ready.ok ? `leader ${ready.leader ?? '—'}` : (ready.reason || 'no reason given'),
    }));
  } else {
    tiles.push(tile('Readiness', '—', { detail: 'unknown' }));
  }
  if (status) {
    tiles.push(tile('Role', status.state, { detail: `term ${exact(status.term)} · leader ${status.leader ?? 'unknown'}` }));
  }
  if (ready && isNumber(ready.committed)) {
    // Committed may lag applied by a check: never a negative backlog.
    const lag = isNumber(ready.applied) ? Math.max(0, ready.committed - ready.applied) : null;
    tiles.push(tile('Committed / applied', `${exact(ready.committed)}`, {
      detail: `applied ${exact(ready.applied)} · ${lag === 0 ? 'caught up' : `${exact(lag)} behind`}`,
    }));
  }
  tiles.push(tile('Writes applied', rate(latest.writes), { trend: sparkline(trend.writes), detail: 'per second, this member' }));
  tiles.push(tile('Fresh reads', rate(latest.reads), { trend: sparkline(trend.reads), detail: 'proven per second: lease, quorum or fence' }));
  tiles.push(tile('Query cache hits', percent(latest.hitRate), { trend: sparkline(trend.hitRate), detail: 'of lookups, last interval' }));
  if (activity) {
    tiles.push(tile('Live watches', count(activity.watches.active), { detail: `${count(activity.watches.updates)} snapshots sent` }));
    // Uptime is measured on the server's clock: client skew does not show.
    tiles.push(tile('Process up', duration(view.uptime), {
      detail: `${shortHex(activity.process_id, 8)} · ${restarts === 0 ? 'no restart seen' : `${restarts} restart${restarts === 1 ? '' : 's'} seen`}`,
    }));
  }
  fill(container, tiles);
}

// ------------------------------------------------------------ cluster

/** Members, Raft positions and replication, with leadership transfer. */
export function renderCluster(panel, { status, raft, membership, ready }, act) {
  const view = membership && membership.membership;
  const metrics = raft && raft.raft;
  const self = status ? status.id : metrics ? metrics.id : null;
  const leader = status ? status.leader : view ? view.leader : null;
  const voters = new Set();
  for (const config of (view && view.voter_configs) || []) for (const id of config) voters.add(id);
  const nodes = view ? Object.entries(view.nodes || {}).map(([id, addr]) => [Number(id), addr]) : [];
  const replication = metrics && metrics.replication;
  const lastLog = metrics ? metrics.last_log_index : null;
  const rows = nodes.sort(([a], [b]) => a - b).map(([id, addr]) => {
    const roles = [];
    roles.push(id === leader ? chip('good', 'leader') : voters.has(id) ? chip('neutral', 'voter') : chip('info', 'learner'));
    if (id === self) roles.push(el('span', { class: 'tag' }, 'this member'));
    let matched = '—';
    let lag = '—';
    if (replication && id in replication) {
      const index = replication[id] ? replication[id].index : null;
      matched = exact(index);
      const behind = isNumber(lastLog) && isNumber(index) ? lastLog - index : null;
      lag = behind === null ? 'not matched yet' : behind === 0 ? 'caught up' : `${exact(behind)} entries`;
    }
    const move = voters.has(id) && id !== leader
      ? action('Make leader…', () => act.transfer(id, leader), { title: `Hand leadership to node ${id}` })
      : null;
    return [exact(id), roles, addr || '(in-process)', matched, lag, move];
  });
  const joint = view && view.voter_configs && view.voter_configs.length > 1;
  const quorumAck = metrics && metrics.millis_since_quorum_ack;
  panel.set(
    el('div', { class: 'split aside' },
      el('div', {},
        table(['Node', 'Role', 'Raft address', 'Matched', 'Replication lag', ''], rows, {
          caption: 'Cluster members',
          empty: 'No membership known',
          numeric: [0, 3],
        }),
        el('p', { class: 'muted small' },
          replication ? 'Replication is this leader’s view.' : 'Replication lag is reported by the leader; open its dashboard to see it.',
          joint ? ' Membership is in joint consensus.' : '')),
      facts([
        ['Term', exact(metrics ? metrics.current_term : status && status.term)],
        ['Vote', metrics && metrics.vote ? `node ${metrics.vote.leader_id.node_id}${metrics.vote.committed ? ', committed' : ''}` : '—'],
        ['Last log', exact(lastLog ?? (status && status.last_log))],
        ['Committed', exact(ready && ready.committed)],
        ['Applied', exact(metrics && metrics.last_applied ? metrics.last_applied.index : status && status.last_applied)],
        ['Snapshot', logId(metrics && metrics.snapshot)],
        ['Purged through', logId(metrics && metrics.purged)],
        isNumber(quorumAck) ? ['Quorum ack', `${duration(quorumAck)} ago`] : null,
        ['Membership', logId(view && view.log_id)],
        view && view.compatibility ? ['Build', `${view.compatibility.build} · raft wire ${view.compatibility.raft_wire}`] : null,
      ])));
}

// --------------------------------------------- groups and partitions

/** Move phases before the cutover: a move there can still be aborted. */
const ABORTABLE = new Set(['copying', 'freezing', 'importing']);

function phaseChip(phase) {
  const status = phase === 'complete' ? 'good'
    : phase === 'aborted' ? 'warning'
      : ['aborting', 'abandoning'].includes(phase) ? 'serious' : 'info';
  return chip(status, phase);
}

/**
 * Every group the partition catalog lists, with this process's replicas;
 * every partition with its owner, epoch and move in flight; the last
 * finished moves; and the cross-group transactions in doubt here.
 * `view.catalog` is null on a member that serves one group.
 */
export function renderGroups(panel, view, act) {
  const { hosted, catalog, transactions, polling, offset } = view;
  if (catalog === null) {
    panel.set(el('p', { class: 'muted' }, 'This member serves one group: there is no partition catalog.'));
    return;
  }
  if (!catalog) return;
  if (catalog.error) {
    panel.fail(new Error(catalog.error));
    return;
  }
  // The member's clock: ages read the same whatever this browser's says.
  const now = Date.now() + (offset || 0);
  const local = new Map((hosted || []).map((group) => [group.name, group]));
  const groups = (catalog.groups || []).map((group) => {
    const here = local.get(group.name);
    const node = here && here.node;
    const ready = here && here.readiness;
    const replica = group.name === 'root'
      ? el('span', { class: 'muted' }, 'the root (above)')
      : here
        ? [
          node && node.leader === node.id ? chip('good', 'leader') : chip('neutral', node ? node.state : 'replica'),
          el('span', { class: 'muted small' }, ` term ${exact(node && node.term)} · leader ${node && node.leader !== null ? node.leader : '—'}`),
        ]
        : el('span', { class: 'muted' }, group.hosted ? 'opening' : 'not here');
    const readiness = group.name === 'root' || !here ? '—'
      : ready && ready.ok ? chip('good', 'ready') : chip(ready && ready.storage ? 'critical' : 'warning', 'not ready', ready ? ready.reason || '' : '');
    return [
      group.name,
      el('code', { title: group.uid }, shortHex(group.uid, 8)),
      chip(group.status === 'active' ? 'good' : group.status === 'removed' ? 'neutral' : 'info', group.status),
      (group.members || []).join(', '),
      replica,
      readiness,
    ];
  });
  const partitions = (catalog.partitions || []).map((entry) => {
    const movement = entry.movement;
    const move = movement
      ? el('div', {},
        phaseChip(movement.phase),
        el('span', { class: 'muted small' }, ` ${movement.source} → ${movement.destination} at epoch ${exact(movement.epoch)} · ${duration(Math.max(0, now - movement.started_ms))}`))
      : el('span', { class: 'muted' }, 'none');
    const abort = movement && ABORTABLE.has(movement.phase)
      ? action('Abort move…', () => act.abortMove(entry.name, movement), { kind: 'danger', title: `Give up moving ${entry.name} before its cutover` })
      : null;
    const status = entry.status === 'active' ? 'good' : entry.status === 'moving' ? 'info' : entry.status === 'retired' ? 'neutral' : 'warning';
    return [entry.name, entry.owner, exact(entry.epoch), chip(status, entry.status), move, abort];
  });
  const moves = (catalog.moves || []).slice(-10).reverse().map((done) => [
    done.partition,
    `${done.source} → ${done.destination}`,
    phaseChip(done.phase),
    `epoch ${exact(done.source_epoch)} → ${exact(done.phase === 'complete' ? done.epoch : done.source_epoch + 2)}`,
    done.reason || el('span', { class: 'muted' }, '—'),
    duration(Math.max(0, done.updated_ms - done.started_ms)),
  ]);
  const inDoubt = [];
  if (transactions && !transactions.error) {
    for (const [group, records] of Object.entries(transactions.groups || {})) {
      if (records.error) {
        inDoubt.push([group, el('span', { class: 'panel-error' }, records.error), '', '', '', '']);
        continue;
      }
      for (const record of records.coordinated || []) {
        const decided = record.decision;
        inDoubt.push([
          group,
          el('code', { title: record.id }, shortHex(record.id, 10)),
          decided ? chip(decided === 'commit' ? 'good' : 'warning', `${decided}, finishing`) : chip('info', 'coordinating'),
          `${(record.finished || []).length} of ${(record.participants || []).length} groups finished`,
          `${duration(Math.max(0, now - record.created_ms))}`,
          decided ? null : action('Abort…', () => act.abortTransaction(group, record), { kind: 'danger', title: 'Abort this undecided transaction at its coordinator' }),
        ]);
      }
      for (const record of records.prepared || []) {
        inDoubt.push([
          group,
          el('code', { title: record.id }, shortHex(record.id, 10)),
          chip('serious', 'prepared'),
          `${(record.dbs || []).map((db) => `${db.db}@${db.epoch}`).join(', ')} locked · coordinator ${record.coordinator}`,
          `${duration(Math.max(0, now - record.prepared_ms))}`,
          null,
        ]);
      }
    }
  }
  panel.set(
    polling ? el('p', { class: 'muted small' }, 'Groups, partitions and transactions come from the admin stream: shown as last streamed while polling.') : null,
    el('h4', {}, 'Groups'),
    table(['Group', 'UID', 'Status', 'Founding members', 'Replica here', 'Readiness'], groups, { caption: 'Groups in the partition catalog', empty: 'No groups' }),
    el('h4', {}, 'Partitions'),
    table(['Partition', 'Owner', 'Epoch', 'Status', 'Move', ''], partitions, { caption: 'Named partitions', empty: 'No named partitions: every database is the root’s', numeric: [2] }),
    el('div', { class: 'split even' },
      el('div', {},
        el('h4', {}, 'Recent moves'),
        table(['Partition', 'Route', 'Outcome', 'Epochs', 'Reason', 'Took'], moves, { caption: 'Finished moves, newest first', empty: 'No finished moves' })),
      el('div', {},
        el('h4', {}, 'Transactions in doubt'),
        table(['Group', 'Transaction', 'State', 'Progress', 'Age', ''], inDoubt, { caption: 'Cross-group transactions in doubt in the groups this process hosts', empty: 'None in doubt' }),
        el('p', { class: 'muted small' }, 'Each group’s leader resolves these on its own: undecided ones commit when every participant prepared, else abort.'))),
    el('p', { class: 'muted small' }, `Catalog revision ${exact(catalog.revision)}, as this member applied it.`));
}

// ---------------------------------------------------------- databases

/** Per-database revisions, applied positions and write rates. */
export function renderDatabases(panel, metrics, perDb) {
  const rows = (metrics.dbs || []).map((db) => {
    const series = perDb(db.db);
    const latest = series.length ? series[series.length - 1] : null;
    return [db.db, exact(db.revision), exact(db.applied), rate(latest), sparkline(series)];
  });
  panel.set(table(['Database', 'Revision', 'Applied position', 'Writes', 'Trend'], rows, {
    caption: 'Databases on this member',
    empty: 'No databases yet',
    numeric: [1, 2, 3],
  }));
}

// ----------------------------------------------------------- activity

/** Recent applied writes and live watches from the activity snapshot. */
export function renderActivity(panel, snapshot, receivedAt, connection) {
  const now = (at) => snapshot.sampled_at_ms - at + (Date.now() - receivedAt);
  const writes = snapshot.writes.entries.slice(-15).reverse().map((write) => [
    write.db, exact(write.revision), exact(write.position), `${duration(Math.max(0, now(write.at_ms)))} ago`,
  ]);
  const watches = snapshot.watches.entries.slice().sort((a, b) => b.updated_at_ms - a.updated_at_ms).map((watch) => [
    watch.db, watch.module, watch.name, count(watch.updates), exact(watch.revision),
    `${duration(Math.max(0, now(watch.started_at_ms)))}`, `${duration(Math.max(0, now(watch.updated_at_ms)))} ago`,
  ]);
  panel.set(
    el('div', { class: 'panel-meta' }, connection,
      el('span', { class: 'muted' }, ` ${count(snapshot.writes.total)} writes applied since start, ${count(snapshot.writes.evicted)} out of the window`,
        snapshot.drops.writes ? `, ${count(snapshot.drops.writes)} without detail` : '')),
    el('div', { class: 'split even' },
      el('div', {},
        el('h4', {}, 'Recent writes'),
        table(['Database', 'Revision', 'Position', 'Age'], writes, { caption: 'Recent applied writes, newest first', empty: 'No writes recorded', numeric: [1, 2] })),
      el('div', {},
        el('h4', {}, `Live watches (${count(snapshot.watches.active)})`),
        table(['Database', 'Module', 'Alias', 'Updates', 'Revision', 'Open for', 'Last update'], watches, {
          caption: 'Live watches on this member',
          empty: 'No live watches',
          numeric: [3, 4],
        }),
        el('p', { class: 'muted small' },
          `${count(snapshot.watches.started)} started, ${count(snapshot.watches.ended)} ended`,
          snapshot.watches.omitted ? `, ${count(snapshot.watches.omitted)} not detailed` : ''))));
}

// ---------------------------------------------------- storage/runtime

/** Compaction, snapshots, the log, the Wasm runtime, caches, admission
 *  and the writer, as cards. */
export function renderStorage(panels, metrics) {
  const compaction = metrics.compaction || [];
  panels.compaction.set(
    nestedBars(compaction.map((file) => ({
      label: file.file === 'raft_log' ? 'Raft log' : file.file === 'group' ? 'Group (data)' : file.file,
      parts: [
        { name: 'file', value: file.file_bytes },
        { name: 'occupied', value: file.occupied_bytes },
        { name: 'allocated', value: file.allocated_bytes },
      ],
    })), ['File', 'Occupied', 'Allocated'], bytes),
    table(['File', 'State', 'Checks', 'Steps', 'Step time', 'Longest'], compaction.map((file) => [
      file.file,
      file.active ? chip('info', 'compacting') : isNumber(file.waiting_ms) ? chip('warning', `waiting ${duration(file.waiting_ms)}`) : chip('neutral', 'idle'),
      count(file.checks), count(file.steps), micros(file.step_micros), micros(file.max_step_micros),
    ]), { caption: 'Online compaction', numeric: [2, 3, 4, 5] }));

  const snap = metrics.snapshots || {};
  panels.snapshots.set(
    meter(snap.unsnapshotted_bytes, snap.after_bytes, bytes, { label: 'Applied since the last snapshot' }),
    facts([
      ['Last snapshot', `index ${exact(snap.snapshot)} · ${duration(snap.snapshot_age_ms)} old`],
      ['Builds', `${count(snap.builds)} done${snap.active ? `, ${snap.active} running` : ''}`],
      ['Last build took', duration(snap.last_build_ms)],
      ['Cooldown', duration(snap.cooldown_ms)],
      ['Due', snap.due ? String(snap.due) : 'no'],
      ['Triggers', `${snap.after_bytes ? bytes(snap.after_bytes) : 'bytes off'} · ${snap.max_age_ms ? duration(snap.max_age_ms) : 'age off'}`],
    ]));

  const purge = metrics.log_purge || {};
  const hold = purge.hold || {};
  panels.log.set(
    hold.enabled ? meter(hold.held_bytes, hold.max_bytes, bytes, { label: 'Held for backup' }) : null,
    facts([
      ['First retained', exact(purge.first_retained)],
      ['Purge target', exact(purge.target)],
      ['Purge', purge.behind ? chip('warning', 'deleting') : chip('good', 'caught up')],
      ['Deleted', `${count(purge.deleted)} entries in ${count(purge.transactions)} transactions`],
      ['Backup hold', hold.enabled ? `released through ${exact(hold.released)}` : 'off'],
      hold.enabled ? ['Hold caps', `${hold.max_entries ? `${count(hold.max_entries)} entries` : 'no entry cap'} · ${hold.max_bytes ? bytes(hold.max_bytes) : 'no byte cap'}`] : null,
    ]));

  const wasm = metrics.wasm;
  if (wasm) {
    panels.wasm.set(
      meter(wasm.images.bytes, wasm.images.budget_bytes, bytes, { label: 'Image cache' }),
      facts([
        ['Instances', `${count(wasm.instances)} live · ${count(wasm.warm_slots)} warm slots`],
        ['Allocator', wasm.pooling ? 'pooling' : 'on demand'],
        ['Images', `${count(wasm.images.images)} cached · ${count(wasm.images.prepared)} prepared · ${count(wasm.images.evicted)} evicted`],
        ['Page scan', yesNo(wasm.pagemap_scan)],
      ]));
  } else {
    panels.wasm.set(el('p', { class: 'muted' }, 'No module has loaded in this process yet.'));
  }

  const cache = (metrics.queries && metrics.queries.cache) || {};
  const lookups = (cache.hits || 0) + (cache.misses || 0);
  panels.cache.set(
    cache.budget_bytes ? meter(cache.bytes, cache.budget_bytes, bytes, { label: 'Query results kept' }) : el('p', { class: 'muted' }, 'The query cache is off (budget 0).'),
    facts([
      ['Entries', count(cache.entries)],
      ['Hits / misses', `${count(cache.hits)} / ${count(cache.misses)}`],
      ['Hit rate', lookups ? percent((100 * cache.hits) / lookups) : '—', 'since the process started'],
      ['Watch evaluations', count(metrics.queries && metrics.queries.watch_evaluations)],
    ]));

  const admission = metrics.admission || {};
  const queueing = admission.queueing || {};
  const shed = admission.shed || {};
  panels.admission.set(
    meter(admission.retained_input_bytes, admission.input_budget_bytes, bytes, { label: 'Input budget' }),
    meter(admission.retained_control_bytes, admission.control_budget_bytes, bytes, { label: 'Control budget' }),
    meter(admission.reserved_evaluation_bytes, admission.evaluation_memory_budget_bytes, bytes, { label: 'Evaluation memory' }),
    el('div', { class: 'chips' },
      queueing.queries_overloaded ? chip('serious', 'query queue standing') : chip('good', 'queries flowing'),
      queueing.writers_overloaded ? chip('serious', 'writer queue standing') : chip('good', 'writers flowing')),
    table(['Class', 'Running', 'Queued', 'Lanes', 'Oldest wait'], (admission.classes || []).map(([name, gauges]) => [
      name, count(gauges.active), count(gauges.queued), count(gauges.lanes), duration(gauges.oldest_queued_ms),
    ]), { caption: 'Admission by class', numeric: [1, 2, 3, 4] }),
    facts([
      ['Query workers', count(admission.query_workers)],
      ['Admitted', count(admission.admitted)],
      ['Refused', `${count(admission.rejected)} over budget · ${count(admission.timed_out)} timed out · ${count(admission.canceled)} canceled`],
      ['Shed', `${count(shed.arrival)} at arrival · ${count(shed.queued)} at turn · ${count(shed.deadline)} at deadline`],
      ['CoDel', `target ${duration(queueing.target_ms)} · interval ${duration(queueing.interval_ms)} · max wait ${duration(queueing.max_wait_ms)}`],
    ]));

  const writer = metrics.writer || {};
  panels.writer.set(facts([
    ['Batches', count(writer.batches)],
    ['Writes', `${count(writer.writes)} · ${writer.batches ? (writer.writes / writer.batches).toFixed(1) : '—'} per batch`],
    ['Largest batch', count(writer.largest)],
    ['Stale batches', count(writer.stale)],
    ['Decided unproposed', count(writer.decided), 'replays, refusals and failures answered at preparation'],
    ['Retention', `${count(metrics.retention && metrics.retention.advances)} advances · ${count(metrics.retention && metrics.retention.collections)} collections`],
  ]), el('p', { class: 'muted small' }, 'The writer runs on the leader; followers show zeros.'));
}

// ------------------------------------------------------------ modules

/** Active modules per database, and staged deployments with actions;
 *  `note` says where staging lives when this member holds none. */
export function renderModules(panel, modules, staging, act, note) {
  const databases = (modules && modules.databases) || [];
  const rows = [];
  for (const database of databases) {
    for (const module of database.modules || []) {
      const kinds = {};
      for (const kind of Object.values(module.definitions || {})) kinds[kind] = (kinds[kind] || 0) + 1;
      const aliases = Object.entries(module.http || {});
      const staged = staging.get(`${database.db}\u0000${module.name}`);
      rows.push([
        database.db,
        module.name,
        el('code', { title: module.identity || '' }, shortHex(module.identity)),
        aliases.length
          ? el('span', { title: aliases.map(([alias, definition]) => `${alias} → ${definition}`).join('\n') }, aliases.map(([alias]) => alias).join(', '))
          : el('span', { class: 'muted' }, 'none'),
        module.authorize ? chip('info', 'hook') : el('span', { class: 'muted' }, 'none'),
        Object.entries(kinds).map(([kind, total]) => `${total} ${kind}${total === 1 ? '' : 's'}`).join(', ') || '—',
        stagingCell(database.db, module.name, staged, act),
      ]);
    }
  }
  panel.set(
    table(['Database', 'Module', 'Identity', 'HTTP aliases', 'Auth', 'Definitions', 'Staged deployment'], rows, {
      caption: 'Active modules',
      empty: 'No modules deployed',
    }),
    note ? el('p', { class: 'muted small' }, note) : null);
}

function stagingCell(db, module, staged, act) {
  if (staged instanceof Error) return el('span', { class: 'panel-error' }, describe(staged));
  if (!staged || !staged.staging) return el('span', { class: 'muted' }, 'none');
  const status = staged.staging;
  if (status.phase === 'collecting') {
    return el('div', { class: 'staging' },
      chip('info', 'collecting'),
      el('div', { class: 'muted small' }, `${status.retired} retired, collected by the leader's maintenance`));
  }
  const rebuilding = status.phase === 'rebuilding';
  const phase = status.ready
    ? chip('good', 'ready to activate')
    : chip('info', rebuilding ? 'rebuilding graph' : 'backfilling');
  return el('div', { class: 'staging' },
    phase,
    meter(status.position, status.planned, count, { label: 'Collections backfilled', progress: true }),
    status.graph
      ? meter(status.graph_position, status.graph, count, { label: 'Derived definitions rebuilt', progress: true })
      : null,
    el('div', { class: 'muted small' }, `target ${shortHex(staged.identity)}${status.rebuilt ? ` · ${count(status.rebuilt)} roots rebuilt` : ''}${status.retired ? ` · ${status.retired} retired` : ''}`),
    el('div', { class: 'actions' },
      action('Advance…', () => act.deployment('advance', db, module, staged), { disabled: status.ready }),
      action('Activate…', () => act.deployment('activate', db, module, staged), { disabled: !status.ready, kind: 'primary' }),
      action('Cancel…', () => act.deployment('cancel', db, module, staged), { kind: 'danger' })));
}

// --------------------------------------------------------------- keys

/** The managed-key catalog's public listing: no material ever. */
/** Every database's managed-key catalog: `{databases: {db: {revision, value}}}`. */
export function renderKeys(panel, listing) {
  const databases = Object.entries((listing && listing.databases) || {});
  const sections = databases.flatMap(([db, entry]) => {
    const catalog = (entry && entry.value) || {};
    const rows = Object.entries(catalog.keys || {}).map(([name, key]) => {
      const versions = (key.versions || []).map((version) => {
        const states = ['revoked', 'destroyed', 'retired'].filter((state) => version[state]);
        const state = version.version === key.activeVersion ? 'active' : states[0] || 'kept';
        return el('div', { class: 'version' }, `v${version.version} `, el('span', { class: 'muted' }, state), ' ', el('code', { title: 'key ID' }, version.kid || ''));
      });
      return [name, key.algorithm || '—', el('code', {}, key.id || '—'), exact(key.activeVersion), versions];
    });
    const bindings = Object.entries(catalog.bindings || {}).map(([name, binding]) => [
      name, typeof binding === 'object' && binding ? JSON.stringify(binding) : String(binding),
    ]);
    return [
      table(['Name', 'Algorithm', 'Key ID', 'Active', 'Versions'], rows, { caption: `Managed keys · ${db} · revision ${exact(catalog.revision ?? entry.revision)}`, empty: 'No managed keys in this catalog', numeric: [3] }),
      bindings.length ? table(['Binding', 'Key and usages'], bindings, { caption: `Guest bindings · ${db}` }) : null,
    ];
  });
  panel.set(
    ...(sections.length ? sections : [el('p', { class: 'muted' }, 'No database of this group has managed keys.')]),
    el('p', { class: 'muted small' }, 'Every database keeps a catalog of its own. The listing carries names, algorithms and versions only; key material never leaves the server.'));
}

// ------------------------------------------------------------- backup

/** Continuous backup on this member, the last API backup and the hold. */
export function renderBackup(panel, backup, act) {
  const continuous = backup.continuous;
  const last = backup.last_backup;
  const hold = (backup.log && backup.log.hold) || {};
  const state = !backup.enabled
    ? chip('neutral', 'not configured')
    : continuous && continuous.failing ? chip('critical', 'failing')
      : chip('good', continuous ? continuous.role : 'configured');
  panel.set(
    el('div', { class: 'panel-meta' }, state,
      action('Back up now…', () => act.backup(), { disabled: !backup.enabled, kind: 'primary', title: backup.enabled ? 'Take a full backup now' : 'Backup is not configured on this member' })),
    hold.enabled ? meter(hold.held_bytes, hold.max_bytes, bytes, { label: 'Log held for the archive' }) : null,
    facts([
      continuous ? ['Ticks', `${count(continuous.ticks)} · last ${ago(continuous.last_tick_ms)}`] : null,
      continuous ? ['Archived through', exact(continuous.archived_through)] : null,
      continuous ? ['Archive', `${count(continuous.segments_archived)} segments · ${count(continuous.entries_archived)} entries`] : null,
      continuous ? ['Bases taken', `${count(continuous.bases_taken)}${continuous.newest_base ? ` · newest ${shortHex(continuous.newest_base.backup_id || '')}` : ''}`] : null,
      continuous ? ['Errors', `${count(continuous.errors)}${continuous.last_error ? ` · last ${ago(continuous.last_error[1])}: ${continuous.last_error[0]}` : ''}`] : null,
      ['Last API backup', last ? `${shortHex(last.backup_id)} · ${count(last.objects)} objects · index ${exact(last.covered_index)}${isNumber(last.ciphertext_bytes) ? ` · ${bytes(last.ciphertext_bytes)}` : ''}` : 'none this process'],
      ['Log hold', hold.enabled ? `released through ${exact(hold.released)} · first retained ${exact(backup.log.first_retained)}` : 'off'],
    ]));
}

// ---------------------------------------------------------- retention

/** One database's retry retention, with rotate, advance and collect. */
export function renderRetention(panel, databases, act) {
  const cards = databases.map(([db, retention]) => {
    if (retention instanceof Error) {
      return el('div', { class: 'subcard' }, el('h4', {}, db), el('p', { class: 'panel-error' }, describe(retention)));
    }
    const stats = retention.stats || {};
    const receipts = retention.receipts || {};
    const field = (label, value, name) => el('label', { class: 'field' }, label,
      el('input', { type: 'number', min: '0', step: '1', name, value: value ?? '', inputmode: 'numeric' }));
    const rotate = el('form', { class: 'inline-form', 'aria-label': `Rotate retention of ${db}` },
      field('Epoch ms', retention.epoch_ms, 'epoch_ms'),
      field('Keep epochs', retention.keep_epochs ?? 2, 'keep_epochs'),
      el('button', { type: 'submit', class: 'action' }, 'Rotate…'));
    rotate.addEventListener('submit', (event) => {
      event.preventDefault();
      act.retention(db, 'rotate', numbers(rotate, ['epoch_ms', 'keep_epochs']), retention);
    });
    const advance = el('form', { class: 'inline-form', 'aria-label': `Advance retention of ${db}` },
      field('From epoch', retention.epoch, 'from_epoch'),
      field('Current epoch', (retention.epoch || 0) + 1, 'current_epoch'),
      field('Floor', retention.min_epoch, 'min_epoch'),
      el('button', { type: 'submit', class: 'action' }, 'Advance…'));
    advance.addEventListener('submit', (event) => {
      event.preventDefault();
      act.retention(db, 'advance', numbers(advance, ['from_epoch', 'current_epoch', 'min_epoch']), retention);
    });
    const collect = el('form', { class: 'inline-form', 'aria-label': `Collect receipts of ${db}` },
      field('Receipts to inspect', 1024, 'limit'),
      el('button', { type: 'submit', class: 'action' }, 'Collect…'));
    collect.addEventListener('submit', (event) => {
      event.preventDefault();
      act.retention(db, 'collect', numbers(collect, ['limit']), retention);
    });
    return el('div', { class: 'subcard' },
      el('h4', {}, db, ' ', retention.initialized ? chip('good', 'initialized') : chip('neutral', 'not initialized')),
      facts([
        ['Incarnation', el('code', {}, retention.incarnation || '—')],
        ['Epoch', `${exact(retention.epoch)} · floor ${exact(retention.min_epoch)}`],
        ['Rotation', retention.epoch_ms ? `every ${duration(retention.epoch_ms)}, keeping ${exact(retention.keep_epochs)}` : 'not rotating'],
        ['Receipts', `expiry clock ${datetime(receipts.expiry_clock_ms)} · unscoped lifetime ${duration(receipts.receipt_ttl_ms)}`],
        ['Leader maintenance', `${count(stats.advances)} advances · ${count(stats.collections)} collections · ${count(stats.deleted)} deleted · ${count(stats.walks)} walks`],
      ]),
      el('details', { class: 'retention-actions' }, el('summary', {}, 'Rotate, advance or collect'), rotate, advance, collect));
  });
  panel.set(cards.length ? el('div', { class: 'subgrid' }, cards) : el('p', { class: 'muted' }, 'No databases yet.'));
}

/** Named numeric fields of a form; blank ones are null. */
function numbers(form, names) {
  const values = {};
  for (const name of names) {
    const input = form.elements.namedItem(name);
    const text = input ? input.value.trim() : '';
    values[name] = text === '' ? null : Number(text);
  }
  return values;
}

// ------------------------------------------------- queues and timers

/** Timer counts per database and the queues this viewer tracks. */
export function renderWork(panel, { timers, queues, dbs }, act) {
  const timerRows = timers.map(([db, stats]) => stats instanceof Error
    ? [db, el('span', { class: 'panel-error' }, describe(stats)), '']
    : [db, count(stats.pending), count(stats.dead)]);
  const queueRows = queues.map(([db, queue, stats]) => [
    db, queue,
    ...(stats instanceof Error
      ? [el('span', { class: 'panel-error' }, describe(stats)), '', '']
      : stats && stats.ok === false
        ? [el('span', { class: 'panel-error' }, `${stats.code}: ${stats.message}`), '', '']
        : [count(stats && stats.pending), count(stats && stats.claimed), count(stats && stats.dead)]),
    action('Stop tracking', () => act.untrack(db, queue), { title: `Stop showing ${db}/${queue}` }),
  ]);
  const dbSelect = el('select', { name: 'db', 'aria-label': 'Database' }, dbs.map((db) => el('option', { value: db }, db)));
  const name = el('input', { name: 'queue', type: 'text', placeholder: 'queue name', 'aria-label': 'Queue name', required: true, maxlength: '192' });
  const add = el('form', { class: 'inline-form', 'aria-label': 'Track a queue' }, dbSelect, name, el('button', { type: 'submit', class: 'action' }, 'Track queue'));
  add.addEventListener('submit', (event) => {
    event.preventDefault();
    if (name.value.trim()) act.track(dbSelect.value, name.value.trim());
  });
  panel.set(el('div', { class: 'split even' },
    el('div', {},
      el('h4', {}, 'Timers'),
      table(['Database', 'Pending', 'Dead'], timerRows, { caption: 'Timers per database', empty: 'No databases yet', numeric: [1, 2] })),
    el('div', {},
      el('h4', {}, 'Queues'),
      table(['Database', 'Queue', 'Pending', 'Claimed', 'Dead', ''], queueRows, { caption: 'Tracked queues', empty: 'No queues tracked', numeric: [2, 3, 4] }),
      add,
      el('p', { class: 'muted small' }, 'Queues are named by the modules that use them, so the server cannot list them: track them by name here (kept in this browser).'))));
}
