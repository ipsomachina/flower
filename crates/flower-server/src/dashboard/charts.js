// Hand-rolled SVG charts: a rolling time series with crosshair,
// tooltip, keyboard readout and table view; sparklines; meters; and
// nested bars. Colors come from CSS classes (s1..s3 categorical slots,
// o1..o3 ordinal steps), so light and dark themes swap in one place.

import { el, svg, fill } from './dom.js';
import { clock, ago, isNumber } from './format.js';

const PLOT_HEIGHT = 132;
const AXIS_BAND = 20;
const MARGIN = { top: 10, right: 12, bottom: AXIS_BAND, left: 52 };
const TABLE_ROWS = 120;

let chartIds = 0;

/** Round a maximum up to a clean axis value, with its tick step. With
 *  `binary`, steps are clean in KiB, MiB and GiB instead. */
export function niceScale(max, ticks = 3, binary = false) {
  // Bytes never get fractional ticks: an idle byte chart spans 1 KiB.
  if (binary && (!isNumber(max) || max < 1024)) return { max: 1024, step: 512 };
  if (!isNumber(max) || max <= 0) return { max: 1, step: 0.5 };
  const rough = max / ticks;
  const unit = binary && rough >= 1024 ? 1024 ** Math.floor(Math.log(rough) / Math.log(1024)) : 1;
  const scaled = rough / unit;
  const power = 10 ** Math.floor(Math.log10(scaled));
  const step = unit * [1, 2, 2.5, 5, 10].map((factor) => factor * power).find((candidate) => candidate >= scaled);
  return { max: step * Math.ceil(max / step - 1e-9), step };
}

/** The x-axis tick spacing for a window, in milliseconds. */
function tickSpacing(windowMs) {
  if (windowMs <= 60_000) return 15_000;
  if (windowMs <= 300_000) return 60_000;
  return 300_000;
}

function relative(ms) {
  if (ms === 0) return 'now';
  return ms % 60_000 === 0 ? `−${ms / 60_000}m` : `−${ms / 1000}s`;
}

/**
 * A time series over a rolling window. `series` are `{key, label}` in
 * slot order (at most three: every pair stays distinguishable); `format`
 * renders values with their unit. One series gets an area wash and its
 * latest value beside the title; two or more get a legend with theirs.
 */
export class TimeChart {
  constructor({ title, subtitle, series, format, max, threshold, integer, binary }) {
    this.id = `chart-${(chartIds += 1)}`;
    this.title = title;
    this.series = series;
    this.format = format;
    this.fixedMax = max;
    this.integer = Boolean(integer);
    this.binary = Boolean(binary);
    this.threshold = threshold || null;
    this.points = [];
    this.from = 0;
    this.to = 0;
    this.width = 320;
    this.focusIndex = null;

    this.headline = el('span', { class: 'chart-value' });
    this.legend = el('ul', { class: 'legend', 'aria-label': `${title} series` });
    this.surface = svg('svg', { class: 'chart-svg', 'aria-hidden': 'true', focusable: 'false' });
    this.tooltip = el('div', { class: 'tooltip', role: 'status', 'aria-live': 'polite', hidden: true });
    this.plot = el('div', {
      class: 'plot',
      tabindex: '0',
      role: 'group',
      'aria-roledescription': 'chart',
      'aria-label': title,
    }, this.surface, this.tooltip);
    this.error = el('span', { class: 'chart-error' });
    this.tableToggle = el('button', {
      type: 'button',
      class: 'link',
      'aria-pressed': 'false',
      'aria-controls': `${this.id}-table`,
    }, 'Table');
    this.tableBox = el('div', { class: 'chart-table', id: `${this.id}-table`, hidden: true });
    this.root = el('figure', { class: 'card chart', 'aria-labelledby': `${this.id}-title` },
      el('figcaption', { class: 'chart-head' },
        el('div', {},
          el('h3', { class: 'chart-title', id: `${this.id}-title` }, title),
          subtitle ? el('p', { class: 'chart-sub' }, subtitle) : null),
        series.length === 1 ? this.headline : null),
      series.length > 1 ? this.legend : null,
      this.plot,
      el('div', { class: 'chart-foot' }, this.tableToggle, this.error),
      this.tableBox);

    this.tableToggle.addEventListener('click', () => {
      const open = this.tableBox.hidden;
      this.tableBox.hidden = !open;
      this.tableToggle.setAttribute('aria-pressed', String(open));
      this.renderTable();
    });
    this.plot.addEventListener('pointermove', (event) => this.pointer(event));
    this.plot.addEventListener('pointerleave', () => this.hideTooltip());
    this.plot.addEventListener('focus', () => this.focusAt(this.points.length - 1));
    this.plot.addEventListener('blur', () => this.hideTooltip());
    this.plot.addEventListener('keydown', (event) => this.key(event));
    new ResizeObserver((entries) => {
      const width = Math.floor(entries[0].contentRect.width);
      if (width > 0 && width !== this.width) {
        this.width = width;
        this.render();
      }
    }).observe(this.plot);
  }

  /** New data: `points` are `{t, v: {key: number|null}}`, oldest first. */
  update(points, from, to, markers = []) {
    this.points = points.filter((point) => point.t >= from - 1);
    this.from = from;
    this.to = to;
    this.markers = markers.filter((marker) => marker.t >= from && marker.t <= to);
    this.render();
    if (!this.tableBox.hidden) this.renderTable();
  }

  /** Hold the last render, dimmed, with the error beside it. */
  setError(message) {
    this.root.classList.toggle('stale', Boolean(message));
    this.error.textContent = message || '';
  }

  latest(key) {
    for (let index = this.points.length - 1; index >= 0; index -= 1) {
      const value = this.points[index].v[key];
      if (isNumber(value)) return value;
    }
    return null;
  }

  render() {
    const width = Math.max(this.width, 160);
    const height = PLOT_HEIGHT + MARGIN.top + MARGIN.bottom;
    const inner = { left: MARGIN.left, right: width - MARGIN.right, top: MARGIN.top, bottom: MARGIN.top + PLOT_HEIGHT };
    let peak = 0;
    for (const point of this.points) {
      for (const { key } of this.series) {
        const value = point.v[key];
        if (isNumber(value) && value > peak) peak = value;
      }
    }
    const threshold = this.threshold ? this.threshold() : null;
    if (threshold && isNumber(threshold.value)) peak = Math.max(peak, threshold.value);
    let scale = this.fixedMax ? { max: this.fixedMax, step: this.fixedMax / 2 } : niceScale(peak, 3, this.binary);
    if (this.integer && scale.step < 1) {
      // Gauges count whole requests: no fractional ticks.
      scale = { max: Math.max(2, Math.ceil(scale.max)), step: 1 };
      if (scale.max > 2) scale = niceScale(scale.max);
    }
    const span = Math.max(1, this.to - this.from);
    const x = (t) => inner.left + ((t - this.from) / span) * (inner.right - inner.left);
    const y = (value) => inner.bottom - (Math.min(value, scale.max) / scale.max) * (inner.bottom - inner.top);
    this.scale = { x, y, inner };

    const parts = [];
    // Gridlines and y ticks: hairlines, the baseline one step stronger.
    for (let tick = 0; tick <= scale.max + 1e-9; tick += scale.step) {
      const at = y(tick);
      parts.push(svg('line', { class: tick === 0 ? 'baseline' : 'gridline', x1: inner.left, x2: inner.right, y1: at, y2: at }));
      parts.push(svg('text', { class: 'tick', x: inner.left - 6, y: at, 'text-anchor': 'end', 'dominant-baseline': 'middle' }, this.format(tick)));
    }
    const spacing = tickSpacing(span);
    for (let back = 0; back <= span; back += spacing) {
      const at = x(this.to - back);
      if (at < inner.left + 12 && back !== 0) continue;
      parts.push(svg('text', {
        class: 'tick',
        x: at,
        y: inner.bottom + 14,
        'text-anchor': back === 0 ? 'end' : 'middle',
      }, relative(back)));
    }
    if (threshold && isNumber(threshold.value) && threshold.value <= scale.max) {
      const at = y(threshold.value);
      parts.push(svg('line', { class: 'threshold', x1: inner.left, x2: inner.right, y1: at, y2: at }));
      parts.push(svg('text', { class: 'threshold-label', x: inner.left + 4, y: at - 4 }, threshold.label));
    }
    // Leader and term changes: a hairline each, labelled where there
    // is room; the title says when and what.
    let labelled = -Infinity;
    for (const marker of this.markers || []) {
      const at = x(marker.t);
      const room = at - labelled >= 44;
      parts.push(svg('g', { class: 'marker' },
        svg('title', {}, marker.title || marker.label),
        svg('line', { x1: at, x2: at, y1: inner.top, y2: inner.bottom }),
        room ? svg('text', { x: at + 3, y: inner.top + 8 }, marker.label) : null));
      if (room) labelled = at;
    }

    const clipId = `${this.id}-clip`;
    parts.push(svg('defs', {}, svg('clipPath', { id: clipId },
      svg('rect', { x: inner.left, y: inner.top - 6, width: inner.right - inner.left + 6, height: inner.bottom - inner.top + 12 }))));
    const marks = svg('g', { 'clip-path': `url(#${clipId})` });
    this.series.forEach(({ key }, index) => {
      const slot = `s${index + 1}`;
      const runs = [];
      let run = [];
      for (const point of this.points) {
        const value = point.v[key];
        if (isNumber(value)) {
          run.push([x(point.t), y(value)]);
        } else if (run.length) {
          runs.push(run);
          run = [];
        }
      }
      if (run.length) runs.push(run);
      for (const segment of runs) {
        const line = segment.map(([px, py], at) => `${at ? 'L' : 'M'}${px.toFixed(1)},${py.toFixed(1)}`).join('');
        if (this.series.length === 1 && segment.length > 1) {
          const first = segment[0][0].toFixed(1);
          const last = segment[segment.length - 1][0].toFixed(1);
          marks.append(svg('path', { class: `area ${slot}`, d: `${line}L${last},${inner.bottom}L${first},${inner.bottom}Z` }));
        }
        marks.append(svg('path', { class: `line ${slot}`, d: segment.length > 1 ? line : `${line}l0.1,0` }));
      }
      const latest = runs.length ? runs[runs.length - 1] : null;
      if (latest) {
        const [px, py] = latest[latest.length - 1];
        marks.append(svg('circle', { class: `dot ${slot}`, cx: px, cy: py, r: 4 }));
      }
    });
    parts.push(marks);
    this.cursor = svg('g', { class: 'cursor' });
    parts.push(this.cursor);
    this.surface.setAttribute('viewBox', `0 0 ${width} ${height}`);
    this.surface.setAttribute('width', width);
    this.surface.setAttribute('height', height);
    fill(this.surface, parts);

    this.renderHeader();
    if (this.focusIndex !== null) this.focusAt(Math.min(this.focusIndex, this.points.length - 1), false);
  }

  renderHeader() {
    const summary = [];
    if (this.series.length === 1) {
      const value = this.latest(this.series[0].key);
      this.headline.textContent = this.format(value);
      summary.push(`latest ${this.format(value)}`);
    } else {
      fill(this.legend, this.series.map(({ key, label }, index) => {
        const value = this.latest(key);
        summary.push(`${label} ${this.format(value)}`);
        return el('li', {},
          el('span', { class: `key s${index + 1}`, 'aria-hidden': 'true' }),
          el('span', { class: 'legend-label' }, label),
          el('span', { class: 'legend-value' }, this.format(value)));
      }));
    }
    this.plot.setAttribute('aria-label',
      `${this.title}: ${summary.join(', ')}. Arrow keys read earlier samples; the Table button lists them.`);
  }

  pointer(event) {
    if (!this.points.length || !this.scale) return;
    const box = this.surface.getBoundingClientRect();
    const px = event.clientX - box.left;
    const t = this.from + ((px - this.scale.inner.left) / (this.scale.inner.right - this.scale.inner.left)) * (this.to - this.from);
    let best = 0;
    for (let index = 1; index < this.points.length; index += 1) {
      if (Math.abs(this.points[index].t - t) < Math.abs(this.points[best].t - t)) best = index;
    }
    this.focusAt(best);
  }

  key(event) {
    if (!this.points.length) return;
    const last = this.points.length - 1;
    const current = this.focusIndex === null ? last : this.focusIndex;
    const moves = { ArrowLeft: current - 1, ArrowRight: current + 1, Home: 0, End: last };
    if (event.key === 'Escape') {
      this.hideTooltip();
      return;
    }
    if (!(event.key in moves)) return;
    event.preventDefault();
    this.focusAt(Math.max(0, Math.min(last, moves[event.key])));
  }

  /** Put the crosshair and the tooltip on sample `index`. */
  focusAt(index, announce = true) {
    if (index < 0 || !this.points.length || !this.scale) {
      this.hideTooltip();
      return;
    }
    this.focusIndex = index;
    const point = this.points[index];
    const { x, y, inner } = this.scale;
    const px = x(point.t);
    const marks = [svg('line', { class: 'crosshair', x1: px, x2: px, y1: inner.top, y2: inner.bottom })];
    const rows = this.series.map(({ key, label }, slot) => {
      const value = point.v[key];
      if (isNumber(value)) marks.push(svg('circle', { class: `dot s${slot + 1}`, cx: px, cy: y(value), r: 4 }));
      return el('div', { class: 'tooltip-row' },
        el('span', { class: `key s${slot + 1}`, 'aria-hidden': 'true' }),
        el('strong', {}, this.format(value)),
        el('span', { class: 'tooltip-label' }, label));
    });
    fill(this.cursor, marks);
    fill(this.tooltip, el('div', { class: 'tooltip-time' }, `${clock(point.t)} · ${ago(point.t, this.to)}`), rows);
    this.tooltip.hidden = false;
    if (!announce) this.tooltip.setAttribute('aria-live', 'off');
    else this.tooltip.setAttribute('aria-live', 'polite');
    const tipWidth = this.tooltip.offsetWidth || 160;
    const left = px + 12 + tipWidth > this.width ? px - 12 - tipWidth : px + 12;
    this.tooltip.style.left = `${Math.max(0, left)}px`;
    this.tooltip.style.top = `${inner.top}px`;
  }

  hideTooltip() {
    this.focusIndex = null;
    this.tooltip.hidden = true;
    if (this.cursor) fill(this.cursor);
  }

  renderTable() {
    if (this.tableBox.hidden) return;
    const rows = this.points.slice(-TABLE_ROWS).reverse();
    const head = el('tr', {}, el('th', { scope: 'col' }, 'Time'),
      this.series.map(({ label }) => el('th', { scope: 'col', class: 'num' }, label)));
    const body = rows.map((point) => el('tr', {},
      el('td', {}, clock(point.t)),
      this.series.map(({ key }) => el('td', { class: 'num' }, this.format(point.v[key])))));
    fill(this.tableBox, el('div', { class: 'table-wrap' }, el('table', {},
      el('caption', {}, `${this.title}, newest first (last ${rows.length} samples)`),
      el('thead', {}, head),
      el('tbody', {}, body.length ? body : el('tr', {}, el('td', { colspan: this.series.length + 1, class: 'empty' }, 'No samples yet'))))));
  }
}

/** A small trend line: the series in de-emphasis ink, the latest value
 *  in the accent. Decorative: the numbers sit beside it. */
export function sparkline(values, { width = 96, height = 24 } = {}) {
  const numbers = values.map((value) => (isNumber(value) ? value : null));
  const peak = Math.max(1e-9, ...numbers.filter(isNumber));
  const step = numbers.length > 1 ? (width - 6) / (numbers.length - 1) : 0;
  let path = '';
  let pen = false;
  let last = null;
  numbers.forEach((value, index) => {
    if (value === null) {
      pen = false;
      return;
    }
    const px = 3 + index * step;
    const py = height - 3 - (value / peak) * (height - 6);
    path += `${pen ? 'L' : 'M'}${px.toFixed(1)},${py.toFixed(1)}`;
    pen = true;
    last = [px, py];
  });
  return svg('svg', { class: 'sparkline', viewBox: `0 0 ${width} ${height}`, width, height, 'aria-hidden': 'true', focusable: 'false' },
    path ? svg('path', { class: 'spark-line', d: path }) : null,
    last ? svg('circle', { class: 'spark-dot', cx: last[0], cy: last[1], r: 3 }) : null);
}

/** A meter of `value` against `max`: the fill carries severity, the
 *  text says the numbers. */
export function meter(value, max, format, { label, progress } = {}) {
  const ratio = isNumber(value) && isNumber(max) && max > 0 ? Math.min(1, value / max) : null;
  // Progress toward a goal carries no severity; usage of a budget does.
  const severity = ratio === null || progress ? '' : ratio >= 0.95 ? 'critical' : ratio >= 0.8 ? 'warning' : '';
  const bar = el('div', { class: `meter-fill ${severity}` });
  bar.style.width = `${ratio === null ? 0 : Math.max(ratio * 100, ratio > 0 ? 1 : 0)}%`;
  const text = ratio === null
    ? `${format(value)}${isNumber(max) && max > 0 ? ` of ${format(max)}` : ''}`
    : `${format(value)} of ${format(max)} (${Math.round(ratio * 100)}%)`;
  return el('div', { class: 'meter-row' },
    label ? el('div', { class: 'meter-label' }, label) : null,
    el('div', {
      class: 'meter',
      role: 'meter',
      'aria-label': label || 'usage',
      'aria-valuemin': '0',
      'aria-valuemax': isNumber(max) ? String(max) : undefined,
      'aria-valuenow': isNumber(value) ? String(value) : undefined,
      'aria-valuetext': text,
    }, bar),
    el('div', { class: 'meter-text' }, severity ? el('span', { class: 'meter-flag', 'aria-hidden': 'true' }, '! ') : null, text));
}

/**
 * Nested horizontal bars on one baseline and one scale: each row's
 * `parts` (largest first) draw over each other in ordinal steps o1..o3,
 * so the smaller measures read as the darker share of the larger.
 */
export function nestedBars(rows, legend, format) {
  const peak = Math.max(1, ...rows.flatMap((row) => row.parts.map((part) => (isNumber(part.value) ? part.value : 0))));
  return el('div', { class: 'nested' },
    el('ul', { class: 'legend' }, legend.map((name, index) =>
      el('li', {}, el('span', { class: `swatch o${index + 1}`, 'aria-hidden': 'true' }), el('span', { class: 'legend-label' }, name)))),
    rows.map((row) => {
      const track = el('div', { class: 'nested-track', role: 'img', 'aria-label': `${row.label}: ${row.parts.map((part) => `${part.name} ${format(part.value)}`).join(', ')}` });
      row.parts.forEach((part, index) => {
        if (!isNumber(part.value)) return;
        const bar = el('div', { class: `nested-bar o${index + 1}`, title: `${part.name}: ${format(part.value)}` });
        bar.style.width = `${Math.max((part.value / peak) * 100, part.value > 0 ? 0.5 : 0)}%`;
        track.append(bar);
      });
      return el('div', { class: 'nested-row' },
        el('div', { class: 'nested-label' }, row.label),
        track,
        el('div', { class: 'nested-values' }, row.parts.map((part) => `${part.name} ${format(part.value)}`).join(' · ')));
    }));
}
