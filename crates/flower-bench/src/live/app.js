// The live view: reads the bench's state from /events (server-sent
// JSON, four times a second) and draws the shop floor, the cluster with
// its crown, the drone yard, the charts and the audit checklist. Nodes
// are built once and updated in place, so CSS transitions can run;
// every string is set as text.

import { el, svg, fill, byId, text } from './dom.js';
import { LineChart } from './charts.js';

const PHASES = {
  setup: 'Setting up',
  warmup: 'Warming up',
  load: 'Rush hour',
  drain: 'Closing time',
  audit: 'Audit',
};
const OVEN_SLOTS = 6;

const count = (value) => (Number.isFinite(value) ? Math.round(value).toLocaleString('en-US') : '—');
const perSecond = (value) => (Number.isFinite(value) ? `${value >= 100 ? Math.round(value).toLocaleString('en-US') : value.toFixed(1)}/s` : '—');
const ms = (value) => {
  if (!Number.isFinite(value)) return '—';
  if (value >= 1000) return `${(value / 1000).toFixed(2)} s`;
  if (value >= 100) return `${Math.round(value)} ms`;
  if (value >= 10) return `${value.toFixed(1)} ms`;
  return `${value.toFixed(2)} ms`;
};

// Icons: small inline SVG, colored by CSS variables.
function pie() {
  return svg('svg', { class: 'pie', viewBox: '0 0 20 20', 'aria-hidden': 'true' },
    svg('circle', { cx: 10, cy: 10, r: 9, fill: 'var(--crust)' }),
    svg('circle', { cx: 10, cy: 10, r: 6.5, fill: 'var(--cheese)' }),
    svg('circle', { cx: 8, cy: 8, r: 1.6, fill: 'var(--sauce)' }),
    svg('circle', { cx: 12.5, cy: 10.5, r: 1.5, fill: 'var(--sauce)' }),
    svg('circle', { cx: 9, cy: 13, r: 1.3, fill: 'var(--sauce)' }));
}

function box() {
  return svg('svg', { class: 'box', viewBox: '0 0 16 16', 'aria-hidden': 'true' },
    svg('rect', { x: 1, y: 3, width: 14, height: 11, rx: 2, fill: 'var(--crust)' }),
    svg('path', { d: 'M1 6h14', stroke: 'var(--oven)', 'stroke-width': 1, opacity: 0.4 }),
    svg('circle', { cx: 8, cy: 10, r: 2, fill: 'var(--sauce)' }));
}

function tower() {
  return svg('svg', { class: 'tower', viewBox: '0 0 64 64', 'aria-hidden': 'true' },
    svg('path', { d: 'M10 26 L32 8 L54 26 Z', fill: 'var(--crust)' }),
    svg('rect', { x: 14, y: 26, width: 36, height: 32, rx: 3, fill: 'var(--raised)', stroke: 'var(--axis)', 'stroke-width': 2 }),
    svg('rect', { x: 20, y: 32, width: 24, height: 5, rx: 2, fill: 'var(--s1)' }),
    svg('rect', { x: 20, y: 40, width: 24, height: 5, rx: 2, fill: 'var(--s1)', opacity: 0.6 }),
    svg('rect', { x: 20, y: 48, width: 24, height: 5, rx: 2, fill: 'var(--s1)', opacity: 0.3 }));
}

function crown() {
  return svg('svg', { class: 'crown hidden', viewBox: '0 0 44 32', role: 'img', 'aria-label': 'leader crown' },
    svg('path', { d: 'M4 28 L2 8 L13 17 L22 3 L31 17 L42 8 L40 28 Z', fill: 'var(--gold)', stroke: '#7a5800', 'stroke-width': 1.5, 'stroke-linejoin': 'round' }),
    svg('circle', { cx: 22, cy: 20, r: 3, fill: 'var(--sauce)' }));
}

function drone(carrying) {
  return svg('svg', { viewBox: '0 0 40 40', 'aria-hidden': 'true' },
    svg('ellipse', { class: 'rotor', cx: 9, cy: 8, rx: 7, ry: 2, fill: 'var(--muted)' }),
    svg('ellipse', { class: 'rotor', cx: 31, cy: 8, rx: 7, ry: 2, fill: 'var(--muted)' }),
    svg('path', { d: 'M9 9 L15 16 M31 9 L25 16', stroke: 'var(--ink2)', 'stroke-width': 2 }),
    svg('rect', { x: 12, y: 14, width: 16, height: 9, rx: 4, fill: 'var(--s1)' }),
    svg('circle', { cx: 24, cy: 18, r: 2, fill: 'var(--surface)' }),
    carrying ? svg('rect', { x: 13, y: 27, width: 14, height: 9, rx: 2, fill: 'var(--crust)' }) : null,
    carrying ? svg('path', { d: 'M20 23 V27', stroke: 'var(--ink2)', 'stroke-width': 1.5 }) : null);
}

function crate() {
  return svg('svg', { viewBox: '0 0 22 22', 'aria-hidden': 'true' },
    svg('rect', { x: 2, y: 6, width: 18, height: 13, rx: 2, fill: 'var(--crust)', transform: 'rotate(-8 11 12)' }),
    svg('path', { d: 'M4 4 L8 1 M18 4 L14 1', stroke: 'var(--critical)', 'stroke-width': 1.5 }));
}

function flame() {
  return svg('svg', { class: 'flame', viewBox: '0 0 26 26', 'aria-hidden': 'true' },
    svg('path', { d: 'M13 2 C17 8 21 10 21 16 A8 8 0 0 1 5 16 C5 11 9 10 9 5 C11 8 12 9 13 2 Z', fill: 'var(--critical)' }),
    svg('path', { d: 'M13 12 C15 15 17 16 17 19 A4 4 0 0 1 9 19 C9 16 12 15 13 12 Z', fill: 'var(--warning)' }));
}

// The page's fixed parts.
const nodes = {
  phase: byId('phase'), phaseText: byId('phase-text'), clock: byId('clock'),
  connection: byId('connection'), connectionText: byId('connection-text'),
  banner: byId('banner'), bannerText: byId('banner-text'),
  goodput: byId('t-goodput'), goodputSub: byId('t-goodput-sub'),
  write: byId('t-write'), read: byId('t-read'), readSub: byId('t-read-sub'),
  orders: byId('t-orders'), ordersSub: byId('t-orders-sub'),
  delivered: byId('t-delivered'), coins: byId('t-coins'), coinsSub: byId('t-coins-sub'),
  replicas: byId('replicas'), clusterSub: byId('cluster-sub'),
  tenants: byId('tenants'), drones: byId('drones'), crates: byId('crates'), yardSub: byId('yard-sub'),
  charts: byId('charts'), checks: byId('checks'), verdict: byId('verdict'), adminLinks: byId('admin-links'),
};
byId('brand').prepend(svg('svg', { viewBox: '0 0 32 32', 'aria-hidden': 'true' },
  svg('path', { d: 'M4 9 Q16 1 28 9 L16 30 Z', fill: 'var(--cheese)', stroke: 'var(--oven)', 'stroke-width': 2, 'stroke-linejoin': 'round' }),
  svg('path', { d: 'M4 9 Q16 1 28 9', fill: 'none', stroke: 'var(--crust)', 'stroke-width': 4, 'stroke-linecap': 'round' }),
  svg('circle', { cx: 13, cy: 14, r: 2.5, fill: 'var(--sauce)' }),
  svg('circle', { cx: 19, cy: 16, r: 2.2, fill: 'var(--sauce)' })));
const theCrown = crown();
nodes.replicas.append(theCrown);
nodes.banner.prepend(flame());

const charts = {
  goodput: new LineChart({
    title: 'Goodput', subtitle: 'Successful customer calls per second',
    series: [{ key: 'reads', label: 'Reads', slot: 1 }, { key: 'writes', label: 'Orders and tips', slot: 2 }],
    format: perSecond,
  }),
  latency: new LineChart({
    title: 'Latency p99', subtitle: 'Each second’s own p99, retries included',
    series: [{ key: 'read', label: 'Reads', slot: 1 }, { key: 'write', label: 'Orders and tips', slot: 2 }],
    format: ms,
    axis: (value) => (value >= 1000 ? `${value / 1000}s` : `${value}ms`),
  }),
  errors: new LineChart({
    title: 'Errors and retries', subtitle: 'Failed attempts and retries per second, every caller',
    series: [{ key: 'failed', label: 'Failed attempts', slot: 3 }, { key: 'retries', label: 'Retries', slot: 4 }],
    format: perSecond,
  }),
};
nodes.charts.append(charts.goodput.root, charts.latency.root, charts.errors.root);

const replicaCards = new Map();
const tenantBlocks = new Map();
const storeCards = new Map();
const droneSlots = [];
let leaderShown = null;
let checksBuilt = false;
let linksBuilt = false;

function renderRun(state) {
  const { run, finale } = state;
  const phase = finale ? 'done' : run.phase || 'setup';
  nodes.phase.className = `pill phase-${phase}`;
  text(nodes.phaseText, finale ? (finale.passed ? 'Done: passed' : 'Done: failed') : PHASES[phase] || phase);
  const facts = run.facts;
  if (Number.isFinite(run.loadT) && !finale) {
    text(nodes.clock, `${Math.min(run.loadT, 9999).toFixed(1)} s of ${facts.durationS} s`);
  } else if (!finale) {
    text(nodes.clock, `${run.elapsedS.toFixed(0)} s`);
  } else {
    text(nodes.clock, 'finished');
  }
  const chaos = state.chaos[0];
  const alarm = Boolean(chaos && !Number.isFinite(chaos.recoveredT));
  nodes.banner.classList.toggle('alarm', alarm);
  text(nodes.bannerText, run.note || facts.describe);
}

function renderTiles(state) {
  const loadPoints = state.timeline.filter((point) => point.phase === 'load');
  const last = loadPoints[loadPoints.length - 1];
  if (state.finale) {
    text(nodes.goodput, perSecond(state.finale.goodput));
    text(nodes.goodputSub, 'over the measured window');
    text(nodes.write, ms(state.finale.writeP99));
    text(nodes.read, ms(state.finale.readP99));
    text(nodes.readSub, 'over the measured window');
  } else if (last) {
    text(nodes.goodput, perSecond(last.readsPerSecond + last.writesPerSecond));
    text(nodes.write, ms(last.writeP99));
    text(nodes.read, ms(last.readP99));
    text(nodes.readSub, state.run.facts.readMethod);
  }
  const kitchen = state.kitchen;
  if (kitchen) {
    text(nodes.orders, count(kitchen.acknowledged));
    text(nodes.ordersSub, `of ${count(kitchen.maxOrders)} max`);
    text(nodes.delivered, count(kitchen.delivered));
    text(nodes.coins, count(kitchen.coins + kitchen.tips));
    text(nodes.coinsSub, `${count(kitchen.coins)} earned + ${count(kitchen.tips)} tipped`);
  }
}

function renderCluster(state) {
  nodes.replicas.style.setProperty('--members', String(Math.max(state.members.length, 1)));
  for (const member of state.members) {
    let card = replicaCards.get(member.id);
    if (!card) {
      card = {
        root: el('article', { class: 'replica' }),
        name: el('div', { class: 'name' }, `n${member.id}`),
        role: el('div', { class: 'role' }),
        facts: el('div', { class: 'facts' }),
      };
      card.root.append(tower(), card.name, card.role, card.facts,
        el('a', { class: 'admin', href: member.adminUrl, target: '_blank', rel: 'noopener noreferrer' }, 'its /admin'));
      nodes.replicas.insertBefore(card.root, theCrown);
      replicaCards.set(member.id, card);
    }
    card.root.className = `replica ${member.role}`;
    text(card.role, member.role === 'down' ? 'killed' : member.role);
    const leads = Array.isArray(member.leads) && member.leads.length
      ? ` · leads ${member.leads.join(', ')}` : '';
    text(card.facts, member.term === null || member.term === undefined
      ? 'no answer' : `term ${member.term} · applied ${count(member.applied)}${leads}`);
  }
  const leader = state.members.find((member) => member.role === 'leader');
  if (leader && replicaCards.has(leader.id)) {
    const card = replicaCards.get(leader.id).root;
    const x = card.offsetLeft + card.offsetWidth / 2 - 22;
    const at = `translate(${x}px, 0px)`;
    theCrown.style.setProperty('--crown-at', at);
    theCrown.style.transform = at;
    theCrown.classList.remove('hidden', 'falling');
    if (leaderShown !== leader.id) {
      theCrown.classList.remove('landed');
      void theCrown.getBoundingClientRect();
      theCrown.classList.add('landed');
      leaderShown = leader.id;
    }
    text(nodes.clusterSub, `n${leader.id} leads, term ${leader.term}. The crown follows the leader.`);
  } else if (state.members.length) {
    if (leaderShown !== null) theCrown.classList.add('falling');
    leaderShown = null;
    text(nodes.clusterSub, 'No leader: the replicas are electing one...');
  }
  if (!linksBuilt && state.members.length) {
    fill(nodes.adminLinks, state.members.map((member) => el('li', {},
      el('a', { href: member.adminUrl, target: '_blank', rel: 'noopener noreferrer' }, `n${member.id} ${member.adminUrl}`))));
    linksBuilt = true;
  }
}

function station(label, kind) {
  const holder = el('div', { class: kind });
  return { root: el('div', { class: 'station' }, el('span', { class: 'label' }, label), holder), holder, shown: -1 };
}

function stack(slot, amount, make) {
  if (slot.shown === amount) return;
  slot.shown = amount;
  const items = [];
  for (let index = 0; index < Math.min(amount, OVEN_SLOTS); index += 1) items.push(make());
  if (amount > OVEN_SLOTS) items.push(el('span', { class: 'more' }, `+${amount - OVEN_SLOTS}`));
  fill(slot.holder, items);
  slot.holder.setAttribute('aria-label', String(amount));
}

function storeCard(store) {
  const card = {
    root: el('article', { class: 'store' }),
    rank: el('span', { class: 'rank' }),
    name: el('h3', {}, store.name || store.key),
    oven: station('oven', 'oven'),
    shelf: station('ready', 'shelf'),
    delivered: el('strong', {}),
    coins: el('strong', {}),
    fillBar: el('div', { class: 'fill' }),
    stock: el('span', {}),
    lastDelivered: 0,
  };
  card.root.append(
    el('div', { class: 'store-head' }, card.rank, card.name),
    card.oven.root, card.shelf.root,
    el('div', { class: 'ledger' }, el('span', {}, 'delivered ', card.delivered), el('span', {}, 'coins ', card.coins)),
    el('div', { class: 'meter', role: 'img', 'aria-label': 'stock' }, card.fillBar),
    el('div', { class: 'ledger' }, el('span', {}, 'dough left'), card.stock));
  return card;
}

function renderFloor(state) {
  const stock = state.run.facts.stockPerStore;
  for (const floor of state.floor) {
    let block = tenantBlocks.get(floor.tenant);
    if (!block) {
      block = {
        watch: el('span', { class: 'watch' }),
        stores: el('div', { class: 'stores' }),
      };
      block.root = el('div', { class: 'card tenant' },
        el('div', { class: 'tenant-head' }, el('h3', {}, floor.tenant), block.watch), block.stores);
      nodes.tenants.append(block.root);
      tenantBlocks.set(floor.tenant, block);
    }
    block.watch.className = `watch${floor.connected ? '' : ' off'}`;
    fill(block.watch, el('span', { class: 'dot', 'aria-hidden': 'true' }),
      floor.connected
        ? `watching on n${floor.member} · revision ${count(floor.revision)} · ${count(floor.updates)} updates, ${count(floor.resets)} snapshots`
        : floor.member ? `reconnecting (was on n${floor.member})...` : 'connecting...');
    const ordered = [...floor.stores].sort((a, b) => String(a.key).localeCompare(String(b.key)));
    for (const store of ordered) {
      let card = storeCards.get(store.key);
      if (!card) {
        card = storeCard(store);
        block.stores.append(card.root);
        storeCards.set(store.key, card);
      }
      const rank = floor.stores.findIndex((row) => row.key === store.key) + 1;
      text(card.rank, `#${rank}`);
      card.rank.className = `rank${rank === 1 ? ' first' : ''}`;
      stack(card.oven, store.baking || 0, pie);
      card.oven.holder.classList.toggle('hot', (store.baking || 0) > 0);
      stack(card.shelf, store.ready || 0, box);
      text(card.delivered, count(store.delivered));
      text(card.coins, count((store.revenue || 0) + (store.tips || 0)));
      text(card.stock, count(store.stock));
      card.fillBar.style.width = `${Math.max(0, Math.min(100, (100 * (store.stock || 0)) / Math.max(stock, 1)))}%`;
      if ((store.delivered || 0) > card.lastDelivered) {
        card.root.classList.remove('pop');
        void card.root.offsetWidth;
        card.root.classList.add('pop');
      }
      card.lastDelivered = store.delivered || 0;
    }
  }
}

function renderDrones(state) {
  const yard = state.drones;
  if (!yard) return;
  const counters = yard.counters;
  text(nodes.yardSub, `${counters.fleet} drones · ${count(counters.claims)} claims · ${count(counters.reclaimed)} reclaimed after a lapsed lease · ${count(counters.abandoned)} dropped`);
  yard.slots.forEach((flight, index) => {
    let slot = droneSlots[index];
    if (!slot) {
      slot = { root: el('li', { class: 'drone idle' }), flying: null };
      droneSlots[index] = slot;
      nodes.drones.append(slot.root);
    }
    const flying = Boolean(flight);
    const key = flying ? `${flight.order}:${flight.attempt}` : '';
    if (slot.flying === key) return;
    slot.flying = key;
    slot.root.className = `drone ${flying ? 'flying' : 'idle'}`;
    fill(slot.root, drone(flying), el('div', { class: 'what' },
      el('strong', {}, flying ? flight.order : `drone ${index + 1}`),
      flying ? `to ${flight.shop.join(' / ')}` : 'parked',
      flying && flight.attempt > 1 ? el('div', { class: 'retry' }, `claim #${flight.attempt}: picking up a dropped one`) : null));
  });
  fill(nodes.crates, yard.dropped.length
    ? [...yard.dropped].reverse().map((dropped) => {
      const reclaimed = dropped.reclaimedBy !== null && dropped.reclaimedBy !== undefined;
      return el('li', { class: `crate${reclaimed ? ' reclaimed' : ''}` }, crate(),
        el('div', {}, el('strong', {}, dropped.order), ' ',
          reclaimed
            ? el('span', {}, el('span', { class: 'icon', 'aria-hidden': 'true' }, '✓ '), `reclaimed by drone ${dropped.reclaimedBy + 1}`)
            : dropped.leaseLeftMs > 0
              ? `lease lapses in ${(dropped.leaseLeftMs / 1000).toFixed(1)} s`
              : 'lease lapsed; a drone will take it'));
    })
    : el('li', { class: 'muted' }, 'Nothing dropped yet.'));
}

function renderCharts(state) {
  const points = state.timeline.filter((point) => point.phase === 'warmup' || point.phase === 'load');
  const marks = state.chaos.map((mark) => ({
    from: mark.killT,
    to: Number.isFinite(mark.recoveredT) ? mark.recoveredT : (state.run.loadT ?? mark.killT),
    label: Number.isFinite(mark.failoverMs) ? `n${mark.killed} killed · back in ${ms(mark.failoverMs)}` : `n${mark.killed} killed`,
  }));
  charts.goodput.update(points.map((point) => ({ t: point.t, v: { reads: point.readsPerSecond, writes: point.writesPerSecond } })), marks);
  charts.latency.update(points.map((point) => ({ t: point.t, v: { read: point.readP99, write: point.writeP99 } })), marks);
  charts.errors.update(points.map((point) => ({ t: point.t, v: { failed: point.failedAttemptsPerSecond, retries: point.retriesPerSecond } })), marks);
}

function renderAudit(state) {
  const audit = state.audit;
  if (!checksBuilt) {
    fill(nodes.checks, audit.checks.map((check) => el('li', { class: 'check pending', id: `check-${check.id}` },
      el('span', { class: 'icon', 'aria-hidden': 'true' }),
      el('span', { class: 'title' }, check.title),
      el('span', { class: 'detail' }, 'waiting for the drain'))));
    checksBuilt = true;
  }
  audit.checks.forEach((check, index) => {
    const row = document.getElementById(`check-${check.id}`);
    if (!row) return;
    const state = check.state === 'pending' && audit.state === 'running' ? 'running' : check.state;
    if (row.dataset.state === state) return;
    row.dataset.state = state;
    row.className = `check ${state}`;
    row.style.setProperty('--delay', `${index * 140}ms`);
    text(row.querySelector('.icon'), state === 'pass' ? '✓' : state === 'fail' ? '✕' : '');
    row.querySelector('.icon').setAttribute('aria-label', state);
    text(row.querySelector('.detail'), check.detail || (state === 'running' ? 'checking...' : 'waiting for the drain'));
  });
  if (audit.state === 'done') {
    const passed = audit.passed;
    nodes.verdict.className = `verdict shown ${passed ? 'pass' : 'fail'}`;
    const finale = state.finale;
    fill(nodes.verdict,
      passed ? 'Every check passed: the books balance after the chaos.' : 'The audit failed: see the checks marked ✕.',
      finale ? el('p', {}, `Reports: ${finale.reportHtml} and ${finale.reportJson}`) : null);
  }
}

function render(state) {
  renderRun(state);
  renderTiles(state);
  renderCluster(state);
  renderFloor(state);
  renderDrones(state);
  renderCharts(state);
  renderAudit(state);
}

function connect() {
  const source = new EventSource('/events');
  source.addEventListener('open', () => {
    nodes.connection.className = 'pill live';
    text(nodes.connectionText, 'live');
    document.body.classList.remove('stale');
  });
  source.addEventListener('message', (event) => {
    try {
      render(JSON.parse(event.data));
    } catch (error) {
      text(nodes.bannerText, `Could not draw the state: ${error.message}`);
    }
  });
  source.addEventListener('error', () => {
    nodes.connection.className = 'pill stale';
    text(nodes.connectionText, 'bench gone: showing the last state');
  });
}

connect();
