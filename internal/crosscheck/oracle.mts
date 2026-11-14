// Runs upstream Pi Durable's own code (the pinned release's TypeScript
// sources, which Node runs with type stripping) on cases the Go cross-check
// tests send: stdin is {"op", "input"}, stdout the op's JSON result. The
// upstream checkout is PI_DURABLE_CROSSCHECK_UPSTREAM, prepared by
// scripts/crosscheck-setup.sh. Upstream logging goes to stderr.
import { readFileSync } from "node:fs";
import { createServer } from "node:http";

for (const method of ["log", "info", "debug", "warn"] as const) {
	console[method] = (...args: unknown[]) => process.stderr.write(`${args.map(String).join(" ")}\n`);
}

const root = process.env.PI_DURABLE_CROSSCHECK_UPSTREAM;
if (!root) throw new Error("PI_DURABLE_CROSSCHECK_UPSTREAM is not set");
const durable = (path: string): Promise<any> => import(`${root}/packages/durable/src/${path}`);
const ai = (path: string): Promise<any> => import(`${root}/packages/ai/src/${path}`);
const chord = (path: string): Promise<any> => import(`${root}/packages/chord/src/${path}`);
const installed = (path: string): Promise<any> => import(`${root}/packages/durable/node_modules/${path}`);

const ops: Record<string, (input: any) => Promise<unknown>> = {
	/** pi-schema parse() with coerce: true, as pi-ai compiles tool schemas. */
	async coerce(cases: { schema: unknown; args: unknown }[]) {
		const { compile } = await installed("@earendil-works/pi-schema/dist/index.js");
		const { formats } = await installed("@earendil-works/pi-schema/dist/formats/index.js");
		return cases.map(({ schema, args }) => {
			try {
				const result = compile(schema, { formats, coerce: true, unresolvedRefs: "reject" }).parse(structuredClone(args));
				return result.ok ? { ok: true, value: result.value } : { ok: false };
			} catch (error: any) {
				return { ok: false, compileError: String(error?.message ?? error) };
			}
		});
	},

	/** pi-ai validateToolArguments: the coerced arguments or the message. */
	async validate(cases: { name: string; schema: unknown; args: unknown }[]) {
		const { validateToolArguments } = await ai("utils/validation.ts");
		return cases.map(({ name, schema, args }) => {
			try {
				const tool = { name, description: "", parameters: schema };
				return { ok: true, value: validateToolArguments(tool, { type: "toolCall", id: "1", name, arguments: args }) };
			} catch (error: any) {
				return { ok: false, error: String(error?.message ?? error) };
			}
		});
	},

	/** The edit tool's matching, application, and diffs. */
	async editDiff(cases: { content: string; edits: { oldText: string; newText: string }[] }[]) {
		const m = await durable("tools/edit-diff.ts");
		return cases.map(({ content, edits }) => {
			const out: Record<string, unknown> = { fuzzy: m.normalizeForFuzzyMatch(content) };
			try {
				const { baseContent, newContent } = m.applyEditsToNormalizedContent(m.normalizeToLF(content), edits, "f.txt");
				const diff = m.generateDiffString(baseContent, newContent);
				Object.assign(out, {
					base: baseContent,
					next: newContent,
					diff: diff.diff,
					first: diff.firstChangedLine ?? 0,
					patch: m.generateUnifiedPatch("f.txt", baseContent, newContent),
				});
			} catch (error: any) {
				out.error = String(error?.message ?? error);
			}
			return out;
		});
	},

	/** The read tool over a Node environment rooted at dir. */
	async read({ dir, cases }: { dir: string; cases: [string, Record<string, unknown>][] }) {
		const { createReadTool } = await durable("tools/read.ts");
		const { NodeExecutionEnv } = await durable("env/node.ts");
		const { BACKGROUND_CONTEXT } = await chord("context/index.ts");
		const env = new NodeExecutionEnv({ cwd: dir });
		const tool = createReadTool();
		const out: unknown[] = [];
		for (const [path, args] of cases) {
			const api = { env, diagnostic: () => {}, output: () => {}, retainedOutput: () => ({ text: "", truncated: false }) };
			try {
				const result = await tool.execute({ path, ...args }, api, BACKGROUND_CONTEXT);
				out.push({
					text: (result.output ?? []).map((block: any) => (block.type === "text" ? block.text : "[image]")).join(""),
					diagnostics: (result.diagnostics ?? []).map((d: any) => `${d.severity}|${d.code ?? ""}|${d.message}`),
				});
			} catch (error: any) {
				out.push({ error: String(error?.message ?? error) });
			}
		}
		return out;
	},

	/** A summarized transcript and each message's token estimate. */
	async serialize({ messages }: { messages: unknown[] }) {
		const compaction = await durable("harness/compaction.ts");
		const { estimateMessageTokens } = await ai("utils/estimate.ts");
		return {
			serialized: compaction.serializeConversation(messages),
			tokens: messages.map((message) => estimateMessageTokens(message)),
		};
	},

	/** Compaction's cut and context estimate over views of contributions. */
	async cut({ views, keeps }: { views: { head: boolean; contributions: unknown[][] }[]; keeps: number[] }) {
		const compaction = await durable("harness/compaction.ts");
		return views.map(({ head, contributions }) => {
			const entries = contributions.map((_, index) => ({ id: index + 1 }));
			const view = { head: head ? entries[0] : undefined, entries, contributions, messages: contributions.flat() };
			return {
				cuts: keeps.map((keep) => compaction.selectCut(view, keep) ?? -1),
				estimate: compaction.estimateContext(view, []),
			};
		});
	},

	/** Tool search ranking. */
	async rank({ docs, queries, limit }: { docs: unknown[]; queries: string[]; limit: number }) {
		const { rankDocuments } = await durable("search.ts");
		return queries.map((query) => rankDocuments(query, docs, limit));
	},

	/** Model context ordering of tool results. */
	async order(sequences: unknown[][]) {
		const { orderToolResults } = await durable("harness/context.ts");
		return sequences.map((messages) => orderToolResults(messages));
	},

	/** System entry planning: section patches and tool changes. */
	async plan({ sections, tools }: { sections: [string, string][][][]; tools: unknown[][][] }) {
		const prompt = await durable("harness/prompt.crosscheck.ts");
		return {
			sections: sections.map(([shown, desired]) => prompt.planSections(new Map(shown), new Map(desired))),
			tools: tools.map(([offered, desired]) => {
				const changes = prompt.planTools(offered, desired);
				return { removed: changes.toolsRemoved.map((t: any) => t.name), added: changes.toolsAdded.map((t: any) => t.name) };
			}),
		};
	},

	/** pi-ai's retry and context overflow classification of error messages. */
	async classify(errors: string[]) {
		const retry = await ai("utils/retry.ts");
		const overflow = await ai("utils/overflow.ts");
		const message = (text: string) => ({
			role: "assistant",
			content: [],
			stopReason: "error",
			errorMessage: text,
			usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
			api: "x",
			provider: "openai",
			model: "m",
			timestamp: 0,
		});
		return errors.map((text) => ({
			retry: retry.isRetryableAssistantError(message(text)),
			overflow: overflow.isContextOverflow(message(text)),
		}));
	},

	/**
	 * pi-ai's reading of partial tool arguments as its providers stream them (ToolCallArgumentsStream): one append of
	 * the text, which earns the credit for a fresh snapshot.
	 */
	async partial(partials: string[]) {
		const { ToolCallArgumentsStream } = await ai("utils/streaming-json.ts");
		return partials.map((text) => JSON.stringify(new ToolCallArgumentsStream().append(text)));
	},

	/** pi-ai's reading of a finished call's arguments text, and V8's JSON.parse messages. */
	async toolArguments(texts: string[]) {
		const { parseToolCallArguments } = await ai("utils/json-parse.ts");
		return texts.map((text) => {
			const { arguments: args, argumentsError } = parseToolCallArguments(text);
			let parse = "";
			try {
				JSON.parse(text);
			} catch (error) {
				parse = (error as Error).message;
			}
			return { arguments: args, ...(argumentsError === undefined ? {} : { argumentsError }), parse };
		});
	},

	/** Size formatting and Number.prototype.toFixed. */
	async numbers({ sizes, fixed }: { sizes: number[]; fixed: [number, number][] }) {
		const { formatSize } = await durable("truncate.ts");
		return { sizes: sizes.map((n) => formatSize(n)), fixed: fixed.map(([x, digits]) => x.toFixed(digits)) };
	},

	/** The coding tools as the model and programs see them. */
	async toolSpecs() {
		const tools = await durable("tools/index.ts");
		const out: Record<string, unknown> = {};
		for (const [name, create] of Object.entries(tools)) {
			if (typeof create !== "function" || !name.startsWith("create")) continue;
			try {
				const tool = (create as () => any)();
				if (tool?.name) {
					out[tool.name] = {
						description: tool.description,
						parameters: tool.parameters,
						outputSchema: tool.structuredOutputSchema ?? null,
						outputLimits: tool.outputLimits ?? null,
					};
				}
			} catch {}
		}
		return out;
	},

	/** openai-completions request bodies, captured through onPayload before any request. */
	async openaiPayload(cases: { model: any; messages: unknown[]; options?: Record<string, unknown> }[]) {
		const oai = await ai("api/openai-completions.ts");
		const out: unknown[] = [];
		for (const { model, messages, options } of cases) {
			let captured: unknown = null;
			const stream = oai.streamSimple(model, { messages }, {
				apiKey: "test",
				...options,
				onPayload: (payload: unknown) => {
					captured = payload;
					throw new Error("captured");
				},
			});
			for await (const _ of stream) {
			}
			// Without a payload, the error that stopped the request.
			out.push(captured ?? { error: (await stream.result()).errorMessage ?? null });
		}
		return out;
	},

	/** openai-completions answers to SSE streams served locally. */
	async openaiStream({ streams, compats }: { streams: string[][]; compats?: (Record<string, unknown> | null)[] }) {
		const oai = await ai("api/openai-completions.ts");
		let current = 0;
		const server = createServer((request, response) => {
			request.resume();
			request.on("end", () => {
				response.writeHead(200, { "content-type": "text/event-stream" });
				for (const line of streams[current]!) response.write(`${line}\n\n`);
				response.end();
			});
		});
		await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
		const port = (server.address() as any).port;
		const model = {
			id: "m",
			name: "m",
			api: "openai-completions",
			provider: "custom",
			baseUrl: `http://127.0.0.1:${port}/v1`,
			reasoning: false,
			input: ["text"],
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 128000,
			maxTokens: 1000,
		};
		const out: unknown[] = [];
		try {
			for (current = 0; current < streams.length; current++) {
				const compat = compats?.[current];
				const message = await oai
					.streamSimple(compat ? { ...model, compat } : model, { messages: [{ role: "user", content: "hi", timestamp: 1 }] }, { apiKey: "t", maxRetries: 0 })
					.result();
				out.push({
					stopReason: message.stopReason,
					errorMessage: message.errorMessage ?? null,
					text: message.content.flatMap((block: any) => (block.type === "text" ? [block.text] : [])).join(""),
					thinking: message.content.flatMap((block: any) => (block.type === "thinking" ? [block.thinking] : [])).join(""),
					toolCalls: message.content.flatMap((block: any) =>
						block.type === "toolCall"
							? [{ id: block.id, name: block.name, arguments: block.arguments, ...(block.argumentsError ? { argumentsError: block.argumentsError } : {}) }]
							: [],
					),
					usage: { input: message.usage.input, output: message.usage.output, cacheRead: message.usage.cacheRead, cacheWrite: message.usage.cacheWrite },
					// OpenRouter's reasoning details, which pi-ai keeps in the thinking signature.
					reasoningDetails: message.content.flatMap((block: any) => {
						if (block.type !== "thinking" || !block.thinkingSignature?.startsWith("[")) return [];
						return JSON.parse(block.thinkingSignature);
					}),
				});
			}
		} finally {
			server.close();
		}
		return out;
	},

	/**
	 * The headers openai-completions sends, as a local server receives them: identity attribution and session
	 * affinity, without the ones that vary (user-agent, the OpenAI SDK's x-stainless-*, transport).
	 */
	async openaiHeaders(cases: { model: any; options?: Record<string, unknown> }[]) {
		const oai = await ai("api/openai-completions.ts");
		let captured: Record<string, string> = {};
		const server = createServer((request, response) => {
			captured = {};
			for (const [name, value] of Object.entries(request.headers)) {
				if (/^(user-agent|x-stainless-|host|connection|content-length|content-type|accept|accept-encoding|authorization|sec-fetch-mode)/.test(name)) continue;
				captured[name] = String(value);
			}
			request.resume();
			request.on("end", () => {
				response.writeHead(200, { "content-type": "text/event-stream" });
				response.end('data: {"choices":[{"delta":{"content":"x"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n');
			});
		});
		await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
		const port = (server.address() as any).port;
		const out: unknown[] = [];
		try {
			for (const { model, options } of cases) {
				// The model's own URL decides detection; requests go to the local server.
				const local = { ...model, baseUrl: `http://127.0.0.1:${port}/v1?for=${encodeURIComponent(model.baseUrl)}` };
				await oai.streamSimple(local, { messages: [{ role: "user", content: "hi", timestamp: 1 }] }, { apiKey: "t", maxRetries: 0, ...options }).result();
				out.push(captured);
			}
		} finally {
			server.close();
		}
		return out;
	},

	/**
	 * Code mode's pure functions: pi-codemode's identifiers, declarations, and source parsing, and durable's
	 * description, section, and result formatting. Result files are written to a stub that records them.
	 */
	async codemode(input: {
		identifiers: string[];
		types: { schema: unknown; maxChars?: number }[];
		samples: { name: string; description?: string; inputSchema?: unknown; outputSchema?: unknown }[];
		sources: string[];
		descriptions: boolean[];
		sections: { tools: any[]; namespaces: any[]; budget: number }[];
		results: { result: any; options: any; failWrites?: boolean }[];
	}) {
		const declarations = await installed("@earendil-works/pi-codemode/dist/declarations.js");
		const source = await installed("@earendil-works/pi-codemode/dist/source.js");
		const describe = await durable("codemode/describe.ts");
		const output = await durable("codemode/output.ts");
		const attempt = (run: () => unknown) => {
			try {
				return { ok: true, value: run() };
			} catch (error: any) {
				return { ok: false, error: String(error?.message ?? error) };
			}
		};
		const results = [];
		for (const { result, options, failWrites } of input.results) {
			const writes: { name: string; size: number; text?: string }[] = [];
			const write = async (name: string, data: string | Uint8Array) => {
				writes.push({ name, size: data.length, ...(typeof data === "string" ? { text: data } : {}) });
				return failWrites ? { error: "disk full" } : { path: `/out/${name}` };
			};
			try {
				const formatted = await output.formatResult(result, { ...options, write });
				results.push({ ok: true, value: formatted, writes });
			} catch (error: any) {
				results.push({ ok: false, error: String(error?.message ?? error), writes });
			}
		}
		return {
			identifiers: input.identifiers.map((name) => declarations.toCodemodeIdentifier(name)),
			types: input.types.map(({ schema, maxChars }) =>
				declarations.schemaToType(schema, maxChars === undefined ? {} : { maxChars }),
			),
			samples: input.samples.map((tool) => declarations.renderToolSample(tool)),
			sources: input.sources.map((text) => attempt(() => source.parseCodemodeSource(text))),
			descriptions: input.descriptions.map((models) => describe.codemodeDescription(models)),
			sections: input.sections.map(({ tools, namespaces, budget }) => describe.codemodeSection(tools, namespaces, budget) ?? null),
			results,
		};
	},

	/**
	 * Code mode's models.*: the catalog functions, model resolution, and the classifier and image context checks, over
	 * a catalog of a classifier (with headers) and an image model.
	 */
	async codemodeModels(input: {
		catalog: { modelType: string; model: any }[];
		calls: { name: string; args: unknown }[];
		resolve: { name: string; type: "classifier" | "image"; model: unknown }[];
		classifierContexts: unknown[];
		imagesContexts: unknown[];
	}) {
		const models = await durable("codemode/models.crosscheck.ts");
		const stub = {
			getModelsOfType: (type: string, provider?: string) =>
				input.catalog.filter((entry) => entry.modelType === type && (provider === undefined || entry.model.provider === provider)).map((entry) => entry.model),
			getAvailableOfType: async (type: string, provider?: string) =>
				input.catalog.filter((entry) => entry.modelType === type && (provider === undefined || entry.model.provider === provider)).map((entry) => entry.model),
			getModelOfType: (type: string, provider: string, id: string) =>
				input.catalog.find((entry) => entry.modelType === type && entry.model.provider === provider && entry.model.id === id)?.model,
		};
		const attempt = async (run: () => unknown) => {
			try {
				const value = await run();
				return value === undefined ? { ok: true } : { ok: true, value };
			} catch (error: any) {
				return { ok: false, error: String(error?.message ?? error) };
			}
		};
		const out: Record<string, unknown[]> = { calls: [], resolve: [], classifierContexts: [], imagesContexts: [] };
		for (const { name, args } of input.calls) out.calls!.push(await attempt(() => models.catalogCall(name, args, stub, {})));
		for (const { name, type, model } of input.resolve) {
			out.resolve!.push(await attempt(() => {
				const resolved = models.resolveModel(name, type, model, stub);
				return `${resolved.provider}/${resolved.id}`;
			}));
		}
		for (const context of input.classifierContexts) out.classifierContexts!.push(await attempt(() => void models.checkClassifierContext(context)));
		for (const context of input.imagesContexts) out.imagesContexts!.push(await attempt(() => void models.checkImagesContext(context)));
		return out;
	},

	/** A scripted conversation run by upstream's Harness. */
	async scenario(scenario: unknown) {
		const { runScenario } = await import(new URL("./scenario.mts", import.meta.url).href);
		return runScenario(scenario, { durable, ai, chord, installed });
	},
};

const request = JSON.parse(readFileSync(0, "utf8"));
const op = ops[request.op];
if (op === undefined) throw new Error(`Unknown op ${request.op}`);
// Exit once written: upstream code can leave timers behind, and a pipe write
// is asynchronous.
process.stdout.write(JSON.stringify(await op(request.input)), () => process.exit(0));
