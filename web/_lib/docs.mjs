import { readFileSync, existsSync } from "node:fs";
import { dirname, join, resolve, posix } from "node:path";
import { fileURLToPath } from "node:url";
import NAV from "../_data/navigation.js";

export const DOCS = resolve(dirname(fileURLToPath(import.meta.url)), "../docs");
export { NAV };

const PAGES = NAV.flatMap((group) =>
  group.pages.map(([path, title, description]) => ({ path, title, description, section: group.section }))
);

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

export function readContent(page) {
  return readFileSync(join(DOCS, page.path), "utf8");
}

export { PAGES, escape, rootOf, hrefFrom, addIds, fillIncludes, searchEntries };
