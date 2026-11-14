// Runs one code mode script with pi-codemode's own CodemodeSandbox (a worker
// thread with a QuickJS VM) for a Go host. Protocol, one JSON value per line:
// - stdin: the start ({ packageDir, codemodeDir, memoryLimitBytes, code, options }),
//   then { type: "deliver", seq, now, id, outcome } and { type: "abort", message }.
// - stdout: { type: "unavailable", message } or { type: "rejected", message } or
//   { type: "accepted" }, then { type: "started", fingerprint }, { type: "call", call },
//   { type: "idle", seq, issued }, and last { type: "result", result }.
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";

const PACKAGE = "@earendil-works/pi-codemode";

function write(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

// The last message: the process ends once it is written (pipes can be asynchronous).
function finish(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`, () => process.exit(0));
}

function messageOf(error) {
  return error instanceof Error ? error.message : String(error);
}

// pi-codemode exports only an import condition, so it is found in the coding
// agent's dependency tree rather than with require.resolve.
async function load(config) {
  let dir = config.codemodeDir ? resolve(config.codemodeDir) : undefined;
  if (dir === undefined) {
    if (!config.packageDir) throw new Error("no Pi package directory");
    const require = createRequire(join(resolve(config.packageDir), "package.json"));
    for (const candidate of require.resolve.paths(PACKAGE) ?? []) {
      const found = join(candidate, ...PACKAGE.split("/"));
      if (existsSync(join(found, "package.json"))) {
        dir = found;
        break;
      }
    }
    if (dir === undefined) throw new Error(`${PACKAGE} is not installed with Pi`);
  }
  const pkg = JSON.parse(readFileSync(join(dir, "package.json"), "utf8"));
  const entry = pkg.exports?.["."]?.import ?? "./dist/index.js";
  return import(pathToFileURL(join(dir, entry)).href);
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
const queue = [];
let waiting;
lines.on("line", (line) => {
  if (waiting) {
    const resume = waiting;
    waiting = undefined;
    resume(line);
  } else queue.push(line);
});
let closed = false;
lines.on("close", () => {
  closed = true;
  waiting?.(undefined);
});
function next() {
  if (queue.length > 0) return Promise.resolve(queue.shift());
  if (closed) return Promise.resolve(undefined);
  return new Promise((resolve) => {
    waiting = resolve;
  });
}

async function main() {
  const first = await next();
  if (first === undefined) return;
  const config = JSON.parse(first);
  let CodemodeSandbox;
  try {
    ({ CodemodeSandbox } = await load(config));
    if (typeof CodemodeSandbox !== "function") throw new Error(`${PACKAGE} has no CodemodeSandbox`);
  } catch (error) {
    finish({ type: "unavailable", message: messageOf(error) });
    return;
  }
  const sandbox = new CodemodeSandbox(
    config.memoryLimitBytes === undefined ? {} : { memoryLimitBytes: config.memoryLimitBytes },
  );
  const controller = new AbortController();
  const options = config.options;
  let run;
  try {
    run = sandbox.start(config.code, {
      startedAt: options.startedAt,
      seed: options.seed,
      tools: options.tools,
      globals: options.globals,
      store: options.store,
      signal: controller.signal,
      ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
      onStarted: (fingerprint) => write({ type: "started", fingerprint }),
      onCall: (call) => write({ type: "call", call }),
      onIdle: (seq, issued) => write({ type: "idle", seq, issued }),
    });
  } catch (error) {
    finish({ type: "rejected", message: messageOf(error) });
    return;
  }
  write({ type: "accepted" });
  run.result.then((result) => finish({ type: "result", result }));
  for (let line = await next(); line !== undefined; line = await next()) {
    const message = JSON.parse(line);
    if (message.type === "deliver") {
      try {
        run.deliver(message.seq, message.now, message.id, message.outcome);
      } catch (error) {
        // The Go side checks deliveries first; a failure here is a host bug.
        controller.abort(new Error(`Sandbox host failed: ${messageOf(error)}`));
      }
    } else if (message.type === "abort") {
      controller.abort(new Error(message.message));
    }
  }
  // The host is gone: end the run, which ends the process.
  controller.abort(new Error("Sandbox closed"));
}

main().catch((error) => finish({ type: "unavailable", message: messageOf(error) }));
