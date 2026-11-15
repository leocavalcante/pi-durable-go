#!/usr/bin/env node
// Writes model/catalog/openai-completions.json: the models of pi-ai's
// catalog that its openai-completions adapter serves, as pi-ai defines them.
//
//   node scripts/gen-openai-catalog.mjs [pi-ai package directory]
//
// The default directory is the installed Pi's (PI_AI_DIR overrides it).
import { readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const dir =
	process.argv[2] ??
	process.env.PI_AI_DIR ??
	join(homedir(), ".pi/agent/install/releases/1.5.2/node_modules/@earendil-works/pi-ai");
const { MODELS } = await import(join(dir, "dist/models.generated.js"));
const { version } = JSON.parse(readFileSync(join(dir, "package.json"), "utf8"));
const models = [];
for (const entries of Object.values(MODELS)) {
	for (const model of Object.values(entries)) if (model.api === "openai-completions") models.push(model);
}
const order = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
models.sort((a, b) => order(a.provider, b.provider) || order(a.id, b.id));
const out = new URL("../model/catalog/openai-completions.json", import.meta.url);
// One model per line, so a new catalog diffs by model.
const lines = models.map((model) => JSON.stringify(model)).join(",\n");
writeFileSync(out, `{"source":${JSON.stringify(`@earendil-works/pi-ai ${version}`)},"models":[\n${lines}\n]}\n`);
console.log(`${models.length} models from @earendil-works/pi-ai ${version}`);
