// Check exported artifacts and the ECMAScript counter pattern independently of Rust's u64 parser.
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
const docs = resolve(dirname(fileURLToPath(import.meta.url)), "..");
for (const [source, output] of [["schema/v1.schema.json", "v1.schema.json"], ["fixtures/v1.json", "v1.fixtures.json"]]) {
  assert.equal(readFileSync(resolve(docs, "../../crates/flower-protocol", source), "utf8"), readFileSync(resolve(docs, "assets/protocol", output), "utf8"), `${output} is stale; run the protocol exporter`);
}
const schema = JSON.parse(readFileSync(resolve(docs, "assets/protocol/v1.schema.json"), "utf8"));
const counter = new RegExp(schema.$defs.Counter.pattern);
const maximum = (1n << 64n) - 1n;
const canonical = text => {
  try { const n = BigInt(text); return n >= 0n && n <= maximum && n.toString() === text; }
  catch { return false; }
};
const cases = ["", "0", "00", "01", "+1", "-0", "-1", "1.0", "1e2", " 1", "1\n", "1\r", "1\t", "１", maximum.toString(), (maximum + 1n).toString()];
for (let n = maximum - 100n; n < maximum + 100n; n++) cases.push(n.toString());
let seed = 7n;
for (let i = 0; i < 2000; i++) {
  seed = (seed * 6364136223846793005n + 1442695040888963407n) & ((1n << 128n) - 1n);
  for (const n of [seed, seed & maximum]) cases.push(n.toString(), n.toString() + "\n", "0" + n.toString());
}
for (const text of cases) assert.equal(counter.test(text), canonical(text), `counter pattern disagrees for ${JSON.stringify(text)}`);
console.log(`Protocol exports match; ${cases.length} canonical-counter checks passed.`);
