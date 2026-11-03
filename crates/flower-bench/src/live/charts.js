// Hand-rolled SVG line charts for the live view, after the admin
// dashboard's: one y axis, 2px lines, end dots ringed in the surface
// color, hairline grid, a crosshair that snaps to the nearest second
// with a tooltip listing every series (pointer and keyboard), a legend
// with current values, and a table view. Colors come from CSS classes
// (s1..s3), so themes swap in one place. Every string is text, never
// markup.

import { el, svg, fill } from './dom.js';

const HEIGHT = 170;
const MARGIN = { top: 10, right: 12, bottom: 20, left: 44 };

/** Round a maximum up to a clean axis value, with its tick step. */
export function niceScale(max, ticks = 3) {
  if (!Number.isFinite(max) || max <= 0) return { max: 1, step: 0.5 };
  const rough = max / ticks;
  const power = 10 ** Math.floor(Math.log10(rough));
  const step = [1, 2, 2.5, 5, 10].map((factor) => factor * power).find((candidate) => candidate >= rough);
  return { max: step * Math.ceil(max / step - 1e-9), step };
}

function compact(value) {
  if (value >= 1000) return `${(value / 1000).toFixed(value >= 10000 ? 0 : 1)}K`;
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
}

let ids = 0;

/** A rolling line chart. `series` are `{key, label, slot}`; `format`

 *  renders a value with its unit; `axis` renders a tick (compact
 *  numbers by default). */
export class LineChart {
  constructor({ title, subtitle, series, format, axis = compact }) {
    this.id = `chart-${(ids += 1)}`;
    this.series = series;
    this.format = format;
    this.axis = axis;
    this.points = [];
    this.marks = [];
    this.width = 360;
    this.focus = null;
    this.surface = svg('svg', { 'aria-hidden': 'true', focusable: 'false' });
    this.tip = el('div', { class: 'tooltip', hidden: true });
    this.values = new Map();
    this.legend = el('ul', { class: 'legend', 'aria-label': `${title} series` },
      series.map((entry) => {
        const value = el('span', { class: 'value' }, '—');
        this.values.set(entry.key, value);
        return el('li', {}, el('span', { class: `key k${entry.slot}`, 'aria-hidden': 'true' }), entry.label, value);
      }));
    this.plot = el('div', { class: 'plot', tabindex: '0', role: 'group', 'aria-roledescription': 'chart', 'aria-label': title }, this.surface, this.tip);
    this.table = el('div', { class: 'table-wrap' });
    this.details = el('details', {}, el('summary', {}, 'Show as a table'), this.table);
    this.root = el('figure', { class: 'card chart', 'aria-labelledby': `${this.id}-title` },
      el('div', { class: 'chart-head' }, el('h3', { id: `${this.id}-title` }, title)),
      el('p', { class: 'chart-sub' }, subtitle),
      this.legend, this.plot, this.details);
    this.plot.addEventListener('pointermove', (event) => this.pointer(event));
    this.plot.addEventListener('pointerleave', () => this.hide());
    this.plot.addEventListener('focus', () => this.show(this.points.length - 1));
    this.plot.addEventListener('blur', () => this.hide());
    this.plot.addEventListener('keydown', (event) => this.key(event));
    this.details.addEventListener('toggle', () => this.renderTable());
    new ResizeObserver((entries) => {
      const width = Math.floor(entries[0].contentRect.width);
      if (width > 0 && width !== this.width) {
        this.width = width;
        this.render();
      }
    }).observe(this.plot);
  }

  /** New data: `points` are `{t, v: {key: number|null}}`, oldest first;
   *  `marks` are `{from, to, label}` in the same seconds. */
  update(points, marks) {
    this.points = points;
    this.marks = marks;
    for (const entry of this.series) {
      const last = [...points].reverse().find((point) => Number.isFinite(point.v[entry.key]));
      this.values.get(entry.key).textContent = last ? this.format(last.v[entry.key]) : '—';
    }
    this.render();
    if (this.details.open) this.renderTable();
    if (this.focus !== null) this.show(Math.min(this.focus, points.length - 1));
  }

  scales() {
    const width = Math.max(this.width, 200);
    const inner = { left: MARGIN.left, right: width - MARGIN.right, top: MARGIN.top, bottom: MARGIN.top + HEIGHT - MARGIN.top - MARGIN.bottom };
    const times = this.points.map((point) => point.t);
    const from = times.length ? times[0] : 0;
    const to = times.length > 1 ? times[times.length - 1] : from + 1;
    let max = 0;
    for (const point of this.points) for (const entry of this.series) {
      const value = point.v[entry.key];
      if (Number.isFinite(value)) max = Math.max(max, value);
    }
    const scale = niceScale(max);
    return {
      width, inner, from, to, scale,
      x: (t) => inner.left + ((t - from) / (to - from || 1)) * (inner.right - inner.left),
      y: (value) => inner.bottom - (value / scale.max) * (inner.bottom - inner.top),
    };
  }

  render() {
    const { width, inner, from, to, scale, x, y } = this.scales();
    this.surface.setAttribute('viewBox', `0 0 ${width} ${HEIGHT}`);
    const nodes = [];
    for (let tick = 0; tick <= scale.max + 1e-9; tick += scale.step) {
      nodes.push(svg('line', { class: tick === 0 ? 'baseline' : 'gridline', x1: inner.left, x2: inner.right, y1: y(tick), y2: y(tick) }));
      nodes.push(svg('text', { class: 'tick', x: inner.left - 6, y: y(tick) + 3, 'text-anchor': 'end' }, this.axis(tick)));
    }
    const span = to - from;
    const every = [5, 10, 15, 30, 60, 120].find((step) => span / step <= 6) || 300;
    for (let t = Math.ceil(from / every) * every; t <= to; t += every) {
      nodes.push(svg('text', { class: 'tick', x: x(t), y: HEIGHT - 5, 'text-anchor': 'middle' }, `${Math.round(t)}s`));
    }
    for (const mark of this.marks) {
      if (mark.from > to || (mark.to ?? mark.from) < from) continue;
      const left = x(Math.max(mark.from, from));
      const right = x(Math.min(mark.to ?? to, to));
      nodes.push(svg('rect', { class: 'band', x: left, y: inner.top, width: Math.max(right - left, 1.5), height: inner.bottom - inner.top }));
      if (mark.from >= from) {
        nodes.push(svg('line', { class: 'killline', x1: left, x2: left, y1: inner.top, y2: inner.bottom }));
        nodes.push(svg('text', { class: 'marklabel', x: left + 4, y: inner.top + 10 }, mark.label));
      }
    }
    for (const entry of this.series) {
      let path = '';
      let pen = false;
      let last = null;
      for (const point of this.points) {
        const value = point.v[entry.key];
        if (!Number.isFinite(value)) { pen = false; continue; }
        path += `${pen ? 'L' : 'M'}${x(point.t).toFixed(1)},${y(value).toFixed(1)}`;
        pen = true;
        last = [x(point.t), y(value)];
      }
      if (path) nodes.push(svg('path', { class: `series s${entry.slot}`, d: path }));
      if (last) nodes.push(svg('circle', { class: `end s${entry.slot}`, cx: last[0], cy: last[1], r: 4 }));
    }
    if (this.focus !== null && this.points[this.focus]) {
      const at = x(this.points[this.focus].t);
      nodes.push(svg('line', { class: 'cross', x1: at, x2: at, y1: inner.top, y2: inner.bottom }));
    }
    fill(this.surface, nodes);
  }

  pointer(event) {
    if (!this.points.length) return;
    const box = this.surface.getBoundingClientRect();
    const { x } = this.scales();
    const at = ((event.clientX - box.left) / box.width) * Math.max(this.width, 200);
    let nearest = 0;
    this.points.forEach((point, index) => {
      if (Math.abs(x(point.t) - at) < Math.abs(x(this.points[nearest].t) - at)) nearest = index;
    });
    this.show(nearest);
  }

  key(event) {
    if (!this.points.length) return;
    const moves = { ArrowLeft: -1, ArrowRight: 1, Home: -Infinity, End: Infinity };
    if (!(event.key in moves)) return;
    event.preventDefault();
    const current = this.focus ?? this.points.length - 1;
    this.show(Math.max(0, Math.min(this.points.length - 1, current + moves[event.key])));
  }

  show(index) {
    const point = this.points[index];
    if (!point) return;
    this.focus = index;
    const { x } = this.scales();
    fill(this.tip,
      el('div', { class: 'small muted' }, `${point.t.toFixed(0)} s`),
      this.series.map((entry) => el('div', { class: 'row' },
        el('span', { class: `key k${entry.slot}`, 'aria-hidden': 'true' }),
        el('strong', {}, Number.isFinite(point.v[entry.key]) ? this.format(point.v[entry.key]) : '—'),
        el('span', { class: 'muted' }, entry.label))));
    this.tip.hidden = false;
    const left = Math.min(Math.max(x(point.t) + 10, 0), Math.max(this.width - 150, 0));
    this.tip.style.left = `${left}px`;
    this.render();
  }

  hide() {
    this.focus = null;
    this.tip.hidden = true;
    this.render();
  }

  renderTable() {
    if (!this.details.open) return;
    fill(this.table, el('table', {},
      el('thead', {}, el('tr', {}, el('th', {}, 'Second'), this.series.map((entry) => el('th', {}, entry.label)))),
      el('tbody', {}, [...this.points].reverse().map((point) => el('tr', {},
        el('td', {}, point.t.toFixed(0)),
        this.series.map((entry) => el('td', {}, Number.isFinite(point.v[entry.key]) ? this.format(point.v[entry.key]) : '—')))))));
  }
}
