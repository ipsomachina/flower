// Number formatting: every figure carries its unit.

const integer = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const oneDecimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });

/** Whether a value is a finite number. */
export function isNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

/** A count: 1,284, then 12.9K, 4.2M. */
export function count(value) {
  if (!isNumber(value)) return '—';
  const magnitude = Math.abs(value);
  if (magnitude < 10_000) return Number.isInteger(value) ? integer.format(value) : oneDecimal.format(value);
  for (const [scale, suffix] of [[1e12, 'T'], [1e9, 'B'], [1e6, 'M'], [1e3, 'K']]) {
    if (magnitude >= scale) return `${oneDecimal.format(value / scale)}${suffix}`;
  }
  return integer.format(value);
}

/** An exact integer with grouping, for positions and revisions. */
export function exact(value) {
  return isNumber(value) ? integer.format(value) : '—';
}

/** Bytes in binary units: 512 B, 16 MiB, 1.5 GiB. */
export function bytes(value) {
  if (!isNumber(value)) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let scaled = value;
  let unit = 0;
  while (Math.abs(scaled) >= 1024 && unit < units.length - 1) {
    scaled /= 1024;
    unit += 1;
  }
  const text = unit === 0 || Math.abs(scaled) >= 100 ? integer.format(scaled) : oneDecimal.format(scaled);
  return `${text} ${units[unit]}`;
}

/** A per-second rate. */
export function rate(value) {
  if (!isNumber(value)) return '—';
  if (value !== 0 && Math.abs(value) < 10) return `${oneDecimal.format(value)}/s`;
  return `${count(Math.round(value))}/s`;
}

/** A percentage of 0 to 100. */
export function percent(value) {
  return isNumber(value) ? `${oneDecimal.format(value)}%` : '—';
}

/** A duration in milliseconds: 850 ms, 4.2 s, 3 m 12 s, 2 h 5 m, 3 d 4 h. */
export function duration(ms) {
  if (!isNumber(ms)) return '—';
  if (ms < 1000) return `${integer.format(ms)} ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${oneDecimal.format(seconds)} s`;
  const whole = Math.floor(seconds);
  const [d, h, m, s] = [Math.floor(whole / 86400), Math.floor(whole / 3600) % 24, Math.floor(whole / 60) % 60, whole % 60];
  if (d > 0) return `${d} d ${h} h`;
  if (h > 0) return `${h} h ${m} m`;
  return `${m} m ${s} s`;
}

/** Microseconds as a duration. */
export function micros(value) {
  if (!isNumber(value)) return '—';
  return value < 1000 ? `${integer.format(value)} µs` : duration(value / 1000);
}

/** How long ago an epoch-millisecond instant was. */
export function ago(atMs, now = Date.now()) {
  if (!isNumber(atMs)) return '—';
  const elapsed = Math.max(0, now - atMs);
  return elapsed < 1000 ? 'just now' : `${duration(elapsed)} ago`;
}

/** A local wall-clock time. */
export function clock(atMs) {
  if (!isNumber(atMs)) return '—';
  return new Date(atMs).toLocaleTimeString(undefined, { hour12: false });
}

/** A local date and time. */
export function datetime(atMs) {
  if (!isNumber(atMs)) return '—';
  return new Date(atMs).toLocaleString(undefined, { hour12: false });
}

/** A long hex identity, cut for display (the full one goes in a title). */
export function shortHex(text, keep = 12) {
  if (typeof text !== 'string') return '—';
  return text.length > keep ? `${text.slice(0, keep)}…` : text;
}

/** A yes/no flag. */
export function yesNo(value) {
  if (value === true) return 'yes';
  if (value === false) return 'no';
  return '—';
}

/** A log ID as `term-node-index`, or its index. */
export function logId(id) {
  if (!id || typeof id !== 'object') return '—';
  const leader = id.leader_id || {};
  return `${exact(id.index)} (term ${exact(leader.term)})`;
}
