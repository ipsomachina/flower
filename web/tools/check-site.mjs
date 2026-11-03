import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { resolve, relative, sep } from "node:path";

// Validate the built site, including links between the landing page and docs,
// downloadable files, stylesheet/script references and fragment identifiers.
export function validateSite(directory) {
  const root = resolve(directory);
  const pages = new Map();
  function collect(path) {
    for (const entry of readdirSync(path, { withFileTypes: true })) {
      const file = resolve(path, entry.name);
      if (entry.isDirectory()) collect(file);
      else if (entry.name.endsWith(".html")) {
        const html = readFileSync(file, "utf8");
        pages.set(file, { html, ids: new Set([...html.matchAll(/\sid="([^"]+)"/g)].map(match => match[1])) });
      }
    }
  }
  collect(root);
  const problems = [];
  for (const [file, { html }] of pages) {
    for (const [, href] of html.matchAll(/\s(?:href|src)="([^"]+)"/g)) {
      if (/^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(href)) continue;
      const url = new URL(href, `https://flower.invalid/${relative(root, file).split(sep).join("/")}`);
      let target = resolve(root, "." + decodeURIComponent(url.pathname));
      if (target !== root && !target.startsWith(root + sep)) {
        problems.push(`${relative(root, file)}: link outside the site ${href}`);
        continue;
      }
      if (existsSync(target) && statSync(target).isDirectory()) target = resolve(target, "index.html");
      if (!existsSync(target) || !statSync(target).isFile()) {
        problems.push(`${relative(root, file)}: missing target ${href}`);
      } else if (url.hash && pages.has(target) && !pages.get(target).ids.has(decodeURIComponent(url.hash.slice(1)))) {
        problems.push(`${relative(root, file)}: missing anchor ${href}`);
      }
    }
  }
  if (problems.length) throw new Error(problems.join("\n"));
  console.log(`${pages.size} pages checked; all local links, anchors and assets resolve.`);
  return pages.size;
}
