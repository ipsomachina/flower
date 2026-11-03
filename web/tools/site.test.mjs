import { test, expect } from "bun:test";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, readFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { PAGES, addIds, fillIncludes, readContent, searchEntries } from "../_lib/docs.mjs";
import { validateSite } from "./check-site.mjs";

test("all documentation pages contribute searchable, anchored content", () => {
  const entries = PAGES.flatMap(page => {
    const { html } = addIds(fillIncludes(page, readContent(page)));
    return searchEntries(page, html);
  });
  for (const page of PAGES) expect(entries.some(entry => entry.u === page.path)).toBe(true);
  expect(entries.some(entry => entry.u.includes("#") && entry.k.includes("Transaction"))).toBe(true);
});

test("headings preserve existing anchors and disambiguate generated ones", () => {
  const { html, toc } = addIds('<h2 id="kept">Title</h2><h2>Next</h2><h3>Next</h3>');
  expect(toc.map(entry => entry.id)).toEqual(["kept", "next", "next-2"]);
  expect(html).toContain('<h3 id="next-2">Next</h3>');
});

test("code includes read Rust sources and reject missing files or anchors", () => {
  const page = { path: "concepts/derived.html" };
  const block = '<code data-include="examples/snippets/src/derived.rs#derived"></code>';
  const result = fillIncludes(page, block);
  expect(result).toContain("fn ");
  expect(result).not.toContain("// ANCHOR:");
  expect(() => fillIncludes(page, block.replace("#derived", "#missing"))).toThrow("no anchor");
  expect(() => fillIncludes(page, block.replace("derived.rs", "missing.rs"))).toThrow("missing file");
});

test("link checks cover nested pages, assets, root links and missing fragments", () => {
  const root = mkdtempSync(join(tmpdir(), "flower-site-test-"));
  try {
    mkdirSync(join(root, "docs"));
    writeFileSync(join(root, "index.html"), '<a href="docs/page.html#example">Docs</a><script src="app.js"></script>');
    writeFileSync(join(root, "docs/page.html"), '<h1 id="example">Example</h1><a href="/">Home</a><a href="../LICENSE">License</a>');
    writeFileSync(join(root, "app.js"), "");
    writeFileSync(join(root, "LICENSE"), "MIT");
    expect(validateSite(root)).toBe(2);
    writeFileSync(join(root, "docs/page.html"), '<h1 id="renamed">Example</h1>');
    expect(() => validateSite(root)).toThrow("missing anchor docs/page.html#example");
    writeFileSync(join(root, "docs/page.html"), '<h1 id="example">Example</h1><a href="missing.html">Missing</a>');
    expect(() => validateSite(root)).toThrow("missing target missing.html");
    rmSync(join(root, "app.js"));
    expect(() => validateSite(root)).toThrow("missing target app.js");
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("the built site preserves public paths and excludes development sources", () => {
  const root = fileURLToPath(new URL("../_site/", import.meta.url));
  const files = readdirSync(root, { recursive: true }).map(path => path.replaceAll("\\", "/"));
  expect(files.filter(path => path.endsWith(".html")).sort()).toEqual(
    ["index.html", ...PAGES.map(page => `docs/${page.path}`)].sort(),
  );
  for (const path of ["LICENSE", "fonts/OFL.txt", "docs/assets/search-index.js", "docs/assets/protocol/v1.schema.json"]) {
    expect(files).toContain(path);
  }
  expect(files.some(path => /(?:node_modules|examples|tools|_includes|_lib|_data|package\.json|bun\.lock)/.test(path))).toBe(false);
  const quickstart = readFileSync(join(root, "docs/quickstart.html"), "utf8");
  expect(quickstart).toContain('<html lang="en">');
  expect(quickstart).toContain('aria-current="page"');
  expect(quickstart).not.toMatch(/data-include="[^"]+"><\/code>/);
  expect(readFileSync(join(root, "LICENSE"), "utf8")).toBe(readFileSync(new URL("../../LICENSE", import.meta.url), "utf8"));
});
