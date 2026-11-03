// DOM helpers. Every string goes into the page as text or as an
// attribute value, never as markup: server-provided names and error
// messages cannot inject anything. Inline handlers and style attributes
// are refused too (the CSP would block them anyway); styling is by
// class, and the few computed sizes go through the CSSOM.

const SVG_NS = 'http://www.w3.org/2000/svg';

function build(node, attrs, children) {
  for (const [name, value] of Object.entries(attrs || {})) {
    if (value === undefined || value === null || value === false) continue;
    if (name.startsWith('on') || name === 'style') {
      throw new Error(`refusing attribute ${name}`);
    }
    node.setAttribute(name, value === true ? '' : String(value));
  }
  append(node, children);
  return node;
}

/** Append children: strings and numbers as text, nodes as they are. */
export function append(node, children) {
  for (const child of children.flat(Infinity)) {
    if (child === undefined || child === null || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

/** An HTML element with attributes and children. */
export function el(tag, attrs, ...children) {
  return build(document.createElement(tag), attrs, children);
}

/** An SVG element with attributes and children. */
export function svg(tag, attrs, ...children) {
  return build(document.createElementNS(SVG_NS, tag), attrs, children);
}

/** Replace a node's children. */
export function fill(node, ...children) {
  node.replaceChildren();
  return append(node, children);
}

/** The element with this ID; it must exist. */
export function byId(id) {
  const node = document.getElementById(id);
  if (!node) throw new Error(`missing #${id}`);
  return node;
}

/** A status chip: an icon and a label, never color alone. */
export function chip(status, label, title) {
  const icons = { good: '✓', warning: '!', serious: '!', critical: '✕', info: 'i', neutral: '•' };
  return el('span', { class: `chip ${status}`, title },
    el('span', { class: 'chip-icon', 'aria-hidden': 'true' }, icons[status] || '•'),
    el('span', {}, label));
}

/** A two-column list of label and value pairs. */
export function facts(pairs) {
  return el('dl', { class: 'facts' }, pairs
    .filter((pair) => pair)
    .map(([label, value, title]) => [el('dt', {}, label), el('dd', { title }, value)]));
}

/** A table from column headers and rows of cells (text or nodes). */
export function table(columns, rows, { caption, empty = 'None', numeric = [] } = {}) {
  const head = el('thead', {}, el('tr', {}, columns.map((column, index) =>
    el('th', { scope: 'col', class: numeric.includes(index) ? 'num' : undefined }, column))));
  const body = el('tbody', {}, rows.length
    ? rows.map((row) => el('tr', {}, row.map((cell, index) =>
      el('td', { class: numeric.includes(index) ? 'num' : undefined }, cell))))
    : el('tr', {}, el('td', { colspan: columns.length, class: 'empty' }, empty)));
  return el('div', { class: 'table-wrap' },
    el('table', {}, caption ? el('caption', { class: 'sr-only' }, caption) : null, head, body));
}
