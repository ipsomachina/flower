import { NAV, PAGES, escape, rootOf, hrefFrom, addIds, fillIncludes } from "../_lib/docs.mjs";

// The landing page's flower.
const LOGO =
  '<svg viewBox="0 0 80 80" width="32" height="32" fill="none" stroke="currentColor" stroke-width="2.2" aria-hidden="true">' +
  [0, 60, 120, 180, 240, 300]
    .map((angle) => `<ellipse cx="40" cy="25" rx="10" ry="18" transform="rotate(${angle} 40 40)"/>`)
    .join("") +
  "</svg>";

const ICON_MENU =
  '<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="M3 5.5h14M3 10h14M3 14.5h14"/></svg>';

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

export default function(data) {
  const path = data.page.filePathStem.replace(/^\/docs\//, "") + ".html";
  const page = PAGES.find(page => page.path === path);
  if (!page) throw new Error(`No documentation navigation entry for ${path}`);
  const { html, toc } = addIds(fillIncludes(page, data.content));
  return layout(page, html, toc);
}
