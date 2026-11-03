// DOM helpers, as the admin dashboard's: every string goes into the page
// as text or as an attribute value, never as markup, so names and
// messages from the run cannot inject anything. Inline handlers and
// style attributes are refused (the CSP would block them anyway); the
// few computed sizes go through the CSSOM.

const SVG_NS = 'http://www.w3.org/2000/svg';

function build(node, attrs, children) {
  for (const [name, value] of Object.entries(attrs || {})) {
    if (value === undefined || value === null || value === false) continue;
    if (name.startsWith('on') || name === 'style') throw new Error(`refusing attribute ${name}`);
    node.setAttribute(name, value === true ? '' : String(value));
  }
  return append(node, children);
}

/** Append children: strings and numbers as text, nodes as they are. */
export function append(node, children) {
  for (const child of [children].flat(Infinity)) {
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

/** Set text only when it changed, so screen readers hear changes. */
export function text(node, value) {
  const next = String(value);
  if (node.textContent !== next) node.textContent = next;
}
