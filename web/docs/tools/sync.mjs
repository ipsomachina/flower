// Keeps the Flower docs' shared chrome in step with the navigation.
//
//   bun web/docs/tools/sync.mjs          rewrite every page, rebuild search
//   bun web/docs/tools/sync.mjs --check  report what would change, exit 1 if anything
//
// Each page's own text lives between `<!-- content -->` and
// `<!-- /content -->`; everything around it (head, header, sidebar,
// table of contents, previous/next links, footer) is generated here from
// NAV below. The script also gives every h2/h3 an id, rebuilds the
// search index (assets/search-index.js) and checks every internal link
// and anchor. Pages stay plain HTML: nothing needs building to read them.
//
// A code block written `<code class="language-rust"
// data-include="examples/snippets/src/x.rs#name">` is filled with the
// lines between `// ANCHOR: name` and `// ANCHOR_END: name` of that file
// (the whole file without a #name), so the pages quote code that
// examples/Cargo.toml compiles.

import { readFileSync, writeFileSync, existsSync, mkdirSync, statSync } from "node:fs";
import { dirname, join, resolve, posix } from "node:path";
import { fileURLToPath } from "node:url";

const DOCS = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const CHECK = process.argv.includes("--check");

export const NAV = [
  {
    section: "Get started",
    pages: [
      ["index.html", "Introduction", "What Flower is, how an application fits together, and where to go next."],
      ["quickstart.html", "Quickstart", "Install the tools, write a guest module, serve it and call it — in about ten minutes."],
    ],
  },
  {
    section: "Concepts",
    pages: [
      ["concepts/databases.html", "Databases, modules and groups", "Logical databases, module instances, named partitions and the Raft groups that host them."],
      ["concepts/collections.html", "Collections and indexes", "Typed values and keys, collections, indexes, references and range scans."],
      ["concepts/methods.html", "Queries and mutations", "Declaring methods and their options, the App, HTTP exposure, errors, deadlines and calls between modules."],
      ["concepts/derived.html", "Derived values and aggregates", "Reactive computations, materialization, delta-maintained aggregates, triggers and maintenance tasks."],
      ["concepts/consistency.html", "Revisions and consistency", "What a revision is, linearizable and replica-local reads, leader leases and read-your-writes."],
      ["concepts/retries.html", "Retries and receipts", "Retry identities, receipts, duplicate results and how long Flower remembers them."],
      ["concepts/watches.html", "Watches", "Live query results over server-sent events, and when they change."],
      ["concepts/time.html", "Time", "The invocation clock, now versus clock, and changes_at with earliest-wins."],
      ["concepts/queues-timers.html", "Queues and timers", "Durable jobs with fenced leases, and not-before timers that fire mutations."],
      ["concepts/modules.html", "Modules and interfaces", "Exports, imports, interface crates, slots and bindings."],
      ["concepts/transactions.html", "Transactions and partitions", "Atomic work across logical databases, named partitions and groups."],
    ],
  },
  {
    section: "Guides",
    pages: [
      ["guides/rust-client.html", "Calling Flower from Rust", "flower-client: calls, typed calls, scoping, credentials, watches and transactions."],
      ["guides/sql.html", "SQL queries", "Typed plans, scoped SQL endpoints, resource limits, explain and diagnostics."],
      ["guides/errors-retries.html", "Errors and retries in clients", "What can fail, what is safe to retry, deadlines, cancellation and overload."],
      ["guides/auth.html", "Authentication and authorization", "The authorization hook, typed authenticators such as JwtBearer, and access policies with redaction."],
      ["guides/crypto.html", "Guest cryptography", "Hashing, JWT and JWE, WebAuthn ceremonies and managed keys inside guest code."],
      ["guides/workers.html", "Worker queues", "Native workers with flower-worker: lines, chained claims, idle jobs and drain deadlines."],
      ["guides/deployments.html", "Deployments", "Building, planning, staging and activating modules, bindings and removal."],
      ["guides/backups.html", "Backups and restore", "Continuous encrypted backup to object storage and point-in-time restore."],
      ["guides/operations.html", "Operating a cluster", "Serving nodes, membership, health, metrics, overload and the admin dashboard."],
      ["guides/testing.html", "Testing applications", "Running guest code natively against the real engine, and mocking the client transport."],
    ],
  },
  {
    section: "Reference",
    pages: [
      ["reference/http.html", "HTTP API", "Every endpoint, header, status code and the error body."],
      ["reference/cli.html", "cargo flower", "Every subcommand and flag, including serve."],
      ["reference/sdk.html", "flower-sdk", "The guest SDK surface: types, macros, contexts and components."],
      ["reference/errors.html", "Error codes and limits", "Failure codes, their HTTP statuses, retryability and runtime limits."],
    ],
  },
];

const PAGES = NAV.flatMap((group) =>
  group.pages.map(([path, title, description]) => ({ path, title, description, section: group.section }))
);

// The landing page's flower.
const LOGO =
  '<svg viewBox="0 0 80 80" width="32" height="32" fill="none" stroke="currentColor" stroke-width="2.2" aria-hidden="true">' +
  [0, 60, 120, 180, 240, 300]
    .map((angle) => `<ellipse cx="40" cy="25" rx="10" ry="18" transform="rotate(${angle} 40 40)"/>`)
    .join("") +
  "</svg>";

const ICON_MENU =
  '<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="M3 5.5h14M3 10h14M3 14.5h14"/></svg>';

function escape(text) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function slug(text) {
  return text
    .toLowerCase()
    .replace(/<[^>]+>/g, "")
    .replace(/&[a-z]+;/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function stripTags(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/g, " ")
    .replace(/<svg[\s\S]*?<\/svg>/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&mdash;/g, "—")
    .replace(/&ndash;/g, "–")
    .replace(/&rarr;/g, "→")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function rootOf(path) {
  const depth = path.split("/").length - 1;
  return depth ? "../".repeat(depth) : "";
}

function hrefFrom(fromPath, toPath) {
  const rel = posix.relative(posix.dirname(fromPath), toPath);
  return rel || posix.basename(toPath);
}

// Give every h2/h3 an id; collect them for the table of contents.
function addIds(content) {
  const seen = new Set();
  const toc = [];
  const html = content.replace(/<h([23])((?:\s[^>]*)?)>([\s\S]*?)<\/h\1>/g, (whole, level, attrs, inner) => {
    let id = (/\sid="([^"]+)"/.exec(attrs) || [])[1];
    if (!id) {
      id = slug(inner) || "section";
      let n = 2;
      while (seen.has(id)) id = `${slug(inner)}-${n++}`;
      attrs = `${attrs} id="${id}"`;
    }
    seen.add(id);
    toc.push({ level: Number(level), id, text: stripTags(inner) });
    return `<h${level}${attrs}>${inner}</h${level}>`;
  });
  return { html, toc };
}

function sidebar(page) {
  return NAV.map((group) => {
    const items = group.pages
      .map(([path, title]) => {
        const current = path === page.path ? ' aria-current="page"' : "";
        return `      <li><a href="${hrefFrom(page.path, path)}"${current}>${escape(title)}</a></li>`;
      })
      .join("\n");
    return `    <h2>${escape(group.section)}</h2>\n    <ul>\n${items}\n    </ul>`;
  }).join("\n");
}

function tocHtml(toc) {
  const items = toc.filter((entry) => entry.level === 2 || entry.level === 3);
  if (items.length < 2) return "";
  const list = items
    .map((entry) => `        <li class="depth-${entry.level}"><a href="#${entry.id}">${escape(entry.text)}</a></li>`)
    .join("\n");
  return `    <nav class="toc" aria-label="On this page">\n      <h2>On this page</h2>\n      <ul>\n${list}\n      </ul>\n    </nav>`;
}

function pageNav(page) {
  const at = PAGES.findIndex((other) => other.path === page.path);
  const prev = PAGES[at - 1];
  const next = PAGES[at + 1];
  const link = (other, cls, label) =>
    other ? `<a class="${cls}" href="${hrefFrom(page.path, other.path)}" rel="${cls}">${label}</a>` : "";
  const prevLink = link(prev, "prev", prev && `← ${escape(prev.title)}`);
  const nextLink = link(next, "next", next && `${escape(next.title)} →`);
  return `      <nav class="page-nav" aria-label="Previous and next">\n        ${prevLink}\n        ${nextLink}\n      </nav>`;
}

function layout(page, content, toc) {
  const root = rootOf(page.path);
  const title = page.path === "index.html" ? "Flower documentation" : `${page.title} · Flower docs`;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light dark">
  <title>${escape(title)}</title>
  <meta name="description" content="${escape(page.description)}">
  <meta property="og:type" content="website">
  <meta property="og:url" content="https://flower.ipsomachina.com/docs/${page.path === "index.html" ? "" : page.path}">
  <meta property="og:title" content="${escape(page.path === "index.html" ? "Flower documentation" : page.title)}">
  <meta property="og:description" content="${escape(page.description)}">
  <meta property="og:image" content="https://flower.ipsomachina.com/social.png">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:image:alt" content="Flower logo on a white background">
  <meta name="twitter:card" content="summary_large_image">
  <link rel="icon" type="image/svg+xml" href="${root}../favicon.svg">
  <link rel="stylesheet" href="${root}assets/docs.css">
</head>
<body data-root="${root}">
  <a class="skip-link" href="#content">Skip to content</a>
  <header class="site-header">
    <button class="menu-button" id="menu-toggle" type="button" aria-label="Open navigation" aria-expanded="false" aria-controls="sidebar">${ICON_MENU}</button>
    <a class="brand" href="${root}index.html">${LOGO}<span class="brand-name">FLOWER</span><span class="brand-docs">Documentation</span></a>
    <div class="header-spacer"></div>
    <div class="search" role="search">
      <input id="search-input" type="search" placeholder="Search" autocomplete="off" spellcheck="false" aria-label="Search the documentation" aria-controls="search-results" aria-expanded="false" role="combobox">
      <div class="search-results" id="search-results" role="listbox" hidden></div>
    </div>
    <a class="header-link" href="${root}../index.html">Home</a>
    <a class="header-link" href="https://github.com/ipsomachina/flower">GitHub</a>
  </header>
  <div class="scrim" id="scrim" hidden></div>
  <div class="layout">
  <nav class="sidebar" id="sidebar" aria-label="Documentation">
${sidebar(page)}
  </nav>
  <div class="main">
    <main class="content" id="content">
      <p class="breadcrumb">${escape(page.section)}</p>
<!-- content -->
${content.trim()}
<!-- /content -->
${pageNav(page)}
      <footer class="site-footer">
        <p><a href="https://github.com/ipsomachina/flower">Flower on GitHub</a> · <a href="${root}../LICENSE">MIT license</a> · <a href="https://ipsomachina.com/privacy">Privacy &amp; legal notice</a></p>
        <p>© 2026 <a href="https://ipsomachina.com">Ipso Machina</a> · Avenue de Chamonix 3 · 1207 Genève · Switzerland</p>
      </footer>
    </main>
${tocHtml(toc)}
  </div>
  </div>
  <script src="${root}assets/search-index.js"></script>
  <script src="${root}assets/docs.js"></script>
</body>
</html>
`;
}

function placeholder(page) {
  return `<h1>${escape(page.title)}</h1>\n<p class="lede">${escape(page.description)}</p>\n<p>This page is being written.</p>`;
}

function readContent(page) {
  const file = join(DOCS, page.path);
  if (!existsSync(file)) return placeholder(page);
  const text = readFileSync(file, "utf8");
  const match = /<!-- content -->\n?([\s\S]*?)\n?<!-- \/content -->/.exec(text);
  if (!match) throw new Error(`${page.path}: no <!-- content --> markers`);
  return fillIncludes(page, match[1]);
}

// Fill every data-include code block from its source file.
function fillIncludes(page, content) {
  return content.replace(
    /(<code\b[^>]*\sdata-include="([^"#]+)(?:#([^"]+))?"[^>]*>)[\s\S]*?(<\/code>)/g,
    (whole, open, file, anchor, close) => {
      const path = join(DOCS, file);
      if (!existsSync(path)) throw new Error(`${page.path}: include of missing file ${file}`);
      let lines = readFileSync(path, "utf8").replace(/\r\n/g, "\n").split("\n");
      if (anchor) {
        const start = lines.findIndex((line) => line.trim() === `// ANCHOR: ${anchor}`);
        const end = lines.findIndex((line) => line.trim() === `// ANCHOR_END: ${anchor}`);
        if (start < 0 || end < start) throw new Error(`${page.path}: no anchor ${anchor} in ${file}`);
        lines = lines.slice(start + 1, end);
      }
      lines = lines.filter((line) => !/^\s*\/\/ ANCHOR(_END)?: /.test(line));
      while (lines.length && !lines[0].trim()) lines.shift();
      while (lines.length && !lines[lines.length - 1].trim()) lines.pop();
      const indent = Math.min(...lines.filter((line) => line.trim()).map((line) => /^ */.exec(line)[0].length));
      const code = lines.map((line) => line.slice(indent)).join("\n");
      return open + escapeCode(code) + close;
    }
  );
}

function escapeCode(text) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// Search entries: one per page section, with the identifiers its code mentions.
function searchEntries(page, html) {
  const entries = [];
  const parts = html.split(/(?=<h[123][\s>])/);
  for (const part of parts) {
    const head = /^<h([123])[^>]*?(?:\sid="([^"]+)")?[^>]*>([\s\S]*?)<\/h\1>/.exec(part);
    const level = head ? Number(head[1]) : 1;
    const heading = head && level > 1 ? stripTags(head[3]) : page.title;
    const anchor = head && level > 1 && head[2] ? `#${head[2]}` : "";
    const body = head ? part.slice(head[0].length) : part;
    const prose = stripTags(body.replace(/<pre[\s\S]*?<\/pre>/g, " "));
    const codes = new Set();
    for (const code of body.matchAll(/<code[^>]*>([\s\S]*?)<\/code>/g)) {
      const text = stripTags(code[1]);
      if (text.length <= 60) codes.add(text);
    }
    for (const code of body.matchAll(/<pre[^>]*>([\s\S]*?)<\/pre>/g)) {
      for (const word of stripTags(code[1]).matchAll(/[A-Za-z_][A-Za-z0-9_:.-]{3,}/g)) codes.add(word[0]);
    }
    if (!prose && !codes.size && level > 1) continue;
    entries.push({
      t: page.title,
      h: heading,
      u: page.path + anchor,
      d: level === 1 ? 1 : 0,
      x: prose.slice(0, 700),
      k: [...codes].join(" ").slice(0, 1500),
    });
  }
  return entries;
}

function checkLinks(page, html, ids) {
  const problems = [];
  for (const match of html.matchAll(/\shref="([^"]+)"/g)) {
    const href = match[1];
    if (/^(https?:|mailto:|data:)/.test(href)) continue;
    const [path, anchor] = href.split("#");
    const target = path ? posix.normalize(posix.join(posix.dirname(page.path), path)) : page.path;
    if (target.startsWith("..")) continue;
    if (!ids.has(target)) {
      // Downloadable schemas and other static artifacts have no page anchors.
      if (!anchor && !target.endsWith(".html") && existsSync(join(DOCS, target)) && statSync(join(DOCS, target)).isFile()) continue;
      problems.push(`${page.path}: link to missing page ${href}`);
      continue;
    }
    if (anchor && !ids.get(target).has(anchor)) problems.push(`${page.path}: link to missing anchor ${href}`);
  }
  return problems;
}

const rendered = [];
const ids = new Map();
for (const page of PAGES) {
  const { html, toc } = addIds(readContent(page));
  const pageIds = new Set(toc.map((entry) => entry.id));
  for (const match of html.matchAll(/\sid="([^"]+)"/g)) pageIds.add(match[1]);
  ids.set(page.path, pageIds);
  rendered.push({ page, html, toc });
}

let changed = 0;
const licenseText = readFileSync(resolve(DOCS, "..", "..", "LICENSE"), "utf8");
const licenseFile = resolve(DOCS, "..", "LICENSE");
if (!existsSync(licenseFile) || readFileSync(licenseFile, "utf8") !== licenseText) {
  changed++;
  if (CHECK) console.log("would rewrite ../LICENSE");
  else writeFileSync(licenseFile, licenseText);
}
const problems = [];
const search = [];
for (const { page, html, toc } of rendered) {
  problems.push(...checkLinks(page, html, ids));
  search.push(...searchEntries(page, html));
  const file = join(DOCS, page.path);
  const output = layout(page, html, toc);
  const before = existsSync(file) ? readFileSync(file, "utf8") : "";
  if (before !== output) {
    changed++;
    if (CHECK) console.log(`would rewrite ${page.path}`);
    else {
      mkdirSync(dirname(file), { recursive: true });
      writeFileSync(file, output);
    }
  }
}

const indexFile = join(DOCS, "assets", "search-index.js");
const indexText =
  "// Generated by tools/sync.mjs from the pages' text. Do not edit.\n" +
  `window.FLOWER_SEARCH = ${JSON.stringify(search)};\n`;
const indexBefore = existsSync(indexFile) ? readFileSync(indexFile, "utf8") : "";
if (indexBefore !== indexText) {
  changed++;
  if (CHECK) console.log("would rewrite assets/search-index.js");
  else writeFileSync(indexFile, indexText);
}

for (const problem of problems) console.error(problem);
console.log(
  `${PAGES.length} pages, ${search.length} search entries, ${changed} file(s) ${CHECK ? "out of date" : "written"}, ${problems.length} link problem(s)`
);
if (problems.length || (CHECK && changed)) process.exit(1);
