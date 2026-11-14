import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Runs a cross-check scenario through upstream's Harness: pi-ai's faux
// provider plays the scripted model, tools return scripted results, and the
// steps drive the root conversation. The result is what the Go runner
// reports for the same scenario (crosscheck_test.go): each request's
// messages, the transcript, and each submission's settlement, normalized.

type Loader = (path: string) => Promise<any>;

/** A message in the normalized form both runners report. */
export function normalizeMessage(message: any): Record<string, unknown> {
	const text = (content: any): string =>
		typeof content === "string"
			? content
			: (content ?? []).map((block: any) => (block.type === "text" ? block.text : block.type === "image" ? "[image]" : "")).join("");
	switch (message.role) {
		case "user":
			return { role: "user", text: text(message.content) };
		case "assistant": {
			const blocks: any[] = message.content ?? [];
			const out: Record<string, unknown> = {
				role: "assistant",
				text: blocks.filter((b) => b.type === "text").map((b) => b.text).join(""),
				thinking: blocks.filter((b) => b.type === "thinking").map((b) => b.thinking).join(""),
				calls: blocks.filter((b) => b.type === "toolCall").map((b) => ({ id: b.id, name: b.name, args: b.arguments })),
				stop: message.stopReason,
			};
			if (message.errorMessage !== undefined) out.error = message.errorMessage;
			return out;
		}
		case "toolResult": {
			// Text blocks joined by newlines, as pi-ai's adapters that send
			// one string do: this port keeps a result's text, diagnostics
			// included, as one string.
			const blocks: any[] = typeof message.content === "string" ? [{ type: "text", text: message.content }] : (message.content ?? []);
			const joined = blocks.filter((block) => block.type === "text").map((block) => block.text).join("\n");
			const images = blocks.filter((block) => block.type === "image").length;
			return { role: "tool", id: message.toolCallId, name: message.toolName, text: joined, images, isError: message.isError === true };
		}
		case "system":
			return {
				role: "system",
				sections: message.sections ?? {},
				added: (message.toolsAdded ?? []).map((tool: any) => tool.name),
				removed: (message.toolsRemoved ?? []).map((tool: any) => tool.name),
			};
	}
	return { role: String(message.role) };
}

/** pi.live without IDs and times: counts and presence instead. */
export function normalizeLive(live: any): Record<string, unknown> {
	if (live === undefined || live === null) return {};
	const out: Record<string, unknown> = {};
	const slot = (s: any) => ({
		callId: s.callId,
		name: s.name,
		status: s.status,
		...(s.output ? { output: s.output } : {}),
		...(s.droppedBytes ? { droppedBytes: s.droppedBytes, droppedLines: s.droppedLines ?? 0 } : {}),
		...(s.diagnostics?.length ? { diagnostics: s.diagnostics.map((d: any) => `[${d.severity}] ${d.message}`) } : {}),
	});
	if (live.run) out.run = { inputs: live.run.inputs.length, turns: live.run.turns, first: live.run.first !== undefined, afterRun: live.run.afterRun !== undefined };
	if (live.generation) {
		out.generation = {
			attempt: live.generation.attempt,
			...(live.generation.message ? { message: normalizeMessage(live.generation.message) } : {}),
			...(live.generation.retry ? { retry: live.generation.retry.error } : {}),
			...(live.generation.deferred ? { deferred: true } : {}),
		};
	}
	if (live.tools?.length) out.tools = live.tools.map((s: any) => ({ ...slot(s), task: s.taskId !== undefined, entry: s.entry !== undefined, ...(s.unoffered ? { unoffered: true } : {}) }));
	if (live.nestedTools?.length) {
		out.nestedTools = live.nestedTools.map((s: any) => ({
			...slot(s),
			parentCallId: s.parentCallId,
			arguments: s.arguments,
			...(s.summary ? { summary: { isError: s.summary.isError, ...(s.summary.error ? { error: s.summary.error } : {}) } } : {}),
		}));
	}
	if (live.compactions?.length) out.compactions = live.compactions.map((c: any) => ({ trigger: c.trigger, blocking: c.blocking, attempt: c.attempt, retry: c.retry !== undefined }));
	if (live.compactionFailure) out.compactionFailure = { trigger: live.compactionFailure.trigger, error: live.compactionFailure.error };
	if (live.runFailure) {
		out.runFailure = {
			inputs: live.runFailure.inputs.length,
			reason: live.runFailure.reason,
			...(live.runFailure.detail !== undefined ? { detail: live.runFailure.detail } : {}),
			first: live.runFailure.first !== undefined,
		};
	}
	return out;
}

type Gate = { reached: Promise<void>; reach: () => void; released: Promise<void>; release: () => void };

export async function runScenario(
	scenario: any,
	load: { durable: Loader; chord: Loader; installed: Loader },
): Promise<unknown> {
	const pd = await load.durable("index.ts");
	const piAi = await load.installed("@earendil-works/pi-ai/dist/index.js");
	const { BACKGROUND_CONTEXT: context } = await load.chord("context/index.ts");

	const gates = new Map<string, Gate>();
	const gate = (name: string): Gate => {
		let found = gates.get(name);
		if (found === undefined) {
			let reach!: () => void;
			let release!: () => void;
			const reached = new Promise<void>((resolve) => (reach = resolve));
			const released = new Promise<void>((resolve) => (release = resolve));
			found = { reached, reach, released, release };
			gates.set(name, found);
		}
		return found;
	};
	const hold = (name: string, signal?: AbortSignal): Promise<void> => {
		const g = gate(name);
		g.reach();
		return new Promise((resolve, reject) => {
			g.released.then(resolve);
			signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
		});
	};

	const faux = piAi.fauxProvider({
		models: [
			{ id: "faux-1", contextWindow: scenario.contextWindow ?? 128000, maxTokens: 16384, input: ["text", "image"] },
			...(scenario.models ?? []).map((m: any) => ({
				id: m.id,
				contextWindow: m.contextWindow ?? 128000,
				maxTokens: 16384,
				input: ["text", "image"],
				reasoning: m.reasoning ?? false,
			})),
		],
	});
	const models = piAi.createModels();
	models.setProvider(faux.provider);
	// Summarization requests take their answers from summaries, the others
	// from model, whichever order concurrent requests come in.
	const requests: unknown[] = [];
	const summaryRequests: unknown[] = [];
	const answers = [...(scenario.model ?? [])];
	const summaries = [...(scenario.summaries ?? [])];
	const isSummary = (messages: any[]): boolean => {
		const first = messages[0];
		const text = typeof first?.content === "string" ? first.content : "";
		return first?.role === "system" && text.startsWith("You are a context summarization assistant");
	};
	faux.setResponses(
		Array.from({ length: answers.length + summaries.length }, () => async (request: any, options: any, _state: any, model: any) => {
			const summary = isSummary(request.messages);
			(summary ? summaryRequests : requests).push({
				model: model?.id,
				thinking: options?.reasoning ?? "off",
				messages: request.messages.map(normalizeMessage),
				// What pi-ai's adapters offer: the system messages' tool changes, folded.
				tools: piAi.getCurrentTools(request.messages).map((tool: any) => tool.name),
			});
			const step = (summary ? summaries : answers).shift() ?? { error: "no scripted response" };
			if (step.hold) await hold(step.hold, options?.signal);
			const content: any[] = [];
			if (step.thinking) content.push(piAi.fauxThinking(step.thinking));
			if (step.text) content.push(piAi.fauxText(step.text));
			for (const call of step.calls ?? []) {
				const block = piAi.fauxToolCall(call.name, call.args ?? {}, { id: call.id });
				content.push(call.argumentsError === undefined ? block : { ...block, argumentsError: call.argumentsError });
			}
			const stopReason = step.error ? "error" : (step.stop ?? (step.calls?.length ? "toolUse" : "stop"));
			return piAi.fauxAssistantMessage(content, { stopReason, ...(step.error ? { errorMessage: step.error } : {}) });
		}),
	);

	const tools = (scenario.tools ?? []).map((tool: any) => {
		let calls = 0;
		return pd.defineTool({
			name: tool.name,
			description: tool.description ?? `The ${tool.name} tool`,
			parameters: tool.parameters ?? { type: "object" },
			...(tool.sequential ? { executionMode: "sequential" } : {}),
			...(tool.replaySafe ? { replay: "safe" } : {}),
			...(tool.callers ? { callers: tool.callers } : {}),
			...(tool.outputLimits ? { outputLimits: tool.outputLimits } : {}),
			async execute(args: unknown, api: any, callContext: any) {
				const results = tool.results ?? [];
				const result = results[Math.min(calls++, results.length - 1)] ?? { text: JSON.stringify(args) };
				if (result.hold) await hold(result.hold, callContext?.abortSignal);
				if (result.throw) throw new Error(result.throw);
				if (result.nested) {
					// Nested calls in order; their results, as programs read them.
					const nested: unknown[] = [];
					for (const call of result.nested) {
						const done = await api.executeTool(call.tool, call.args ?? {}, context, call.key === undefined ? undefined : { key: call.key });
						nested.push({
							structuredOutput: done.structuredOutput ?? null,
							isError: done.isError,
							diagnostics: done.diagnostics.map((d: any) => `[${d.severity}] ${d.message}`),
						});
					}
					return { output: [{ type: "text", text: JSON.stringify(nested) }] };
				}
				const control = {
					...(result.terminate !== undefined ? { terminate: result.terminate } : {}),
					...(result.handoff !== undefined ? { handoff: result.handoff } : {}),
					...(result.addTools !== undefined ? { addTools: result.addTools } : {}),
				};
				// Running output, then the result; a result without text or
				// images omits its output, so the retained output stands.
				for (const chunk of result.stream ?? []) api.output(chunk);
				if (result.streamRepeat) for (let i = 0; i < result.streamRepeat.times; i++) api.output(result.streamRepeat.text);
				const text = result.repeat ? result.repeat.text.repeat(result.repeat.times) : result.text;
				const images = (result.images ?? []).map((image: any) => ({ type: "image", data: image.data, mimeType: image.mimeType }));
				const output = [...(text === undefined ? [] : [{ type: "text", text }]), ...images];
				const usage =
					result.usage === undefined
						? undefined
						: {
								input: result.usage.input ?? 0,
								output: result.usage.output ?? 0,
								cacheRead: 0,
								cacheWrite: 0,
								totalTokens: (result.usage.input ?? 0) + (result.usage.output ?? 0),
								cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
							};
				return {
					...(output.length > 0 ? { output } : {}),
					...(usage === undefined ? {} : { usage }),
					...(result.isError ? { isError: true } : {}),
					...(result.diagnostics !== undefined ? { diagnostics: result.diagnostics } : {}),
					...(Object.keys(control).length > 0 ? { control } : {}),
				};
			},
		});
	});
	const registry = pd.createRegistry();
	if (tools.length > 0) registry.install(pd.defineExtension({ name: "scenario", tools }));
	// An afterRun hook asking for one task per run: it appends a
	// scenario.after-run entry with its label and completes.
	const asked: any[] = [];
	if (scenario.afterRun) {
		const spec = scenario.afterRun;
		const AfterTask = pd.defineTask({
			name: "scenario.after-run",
			version: 1,
			initial: () => ({ phase: "run" }),
			phases: {
				run: async (task: any, runtime: any, ctx: any) => {
					if (spec.hold) await hold(spec.hold, runtime.signal);
					await runtime.commit(async (tx: any) => {
						await tx.appendEntry(runtime.conversationId, { kind: "scenario.after-run", data: task.input.label });
						return { status: "terminal", outcome: { status: "completed", result: task.input.label } };
					}, ctx);
				},
			},
			abort: async (_task: any, runtime: any, ctx: any) => {
				await runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), ctx);
			},
		});
		registry.install(
			pd.defineExtension({
				name: "after-run",
				tasks: [AfterTask],
				hooks: [
					pd.hook(pd.GenerationTask, {
						afterRun: (run: any) => {
							asked.push(run);
							return { tasks: [pd.runEndTask(AfterTask, { label: `run ${asked.length}` })] };
						},
					}),
				],
			}),
		);
	}
	// Declarative extensions: static sections and hook rules.
	for (const ext of scenario.extensions ?? []) {
		const sections = (ext.sections ?? []).map((s: any) =>
			pd.section(s.key, () => s.text, s.tag === false ? { tag: false } : undefined),
		);
		const hooks: unknown[] = [];
		if (ext.beforeTool || ext.afterTool) {
			hooks.push(
				pd.hook(pd.ToolTask, {
					...(ext.beforeTool
						? {
								beforeTool: (call: any) => {
									for (const rule of ext.beforeTool) {
										if (rule.tool !== call.name) continue;
										if (rule.block !== undefined) return { block: rule.block };
										if (rule.args !== undefined) return { arguments: rule.args };
									}
									return undefined;
								},
							}
						: {}),
					...(ext.afterTool
						? {
								afterTool: (call: any, result: any) => {
									for (const rule of ext.afterTool) {
										if (rule.tool === call.name) return { ...result, output: [{ type: "text", text: rule.text }] };
									}
									return undefined;
								},
							}
						: {}),
				}),
			);
		}
		if (ext.beforeRequest || ext.afterResponse) {
			hooks.push(
				pd.hook(pd.GenerationTask, {
					...(ext.beforeRequest
						? {
								beforeRequest: (request: any) =>
									ext.beforeRequest.appendUser === undefined
										? undefined
										: { messages: [...request.messages, { role: "user", content: ext.beforeRequest.appendUser, timestamp: 0 }] },
							}
						: {}),
					...(ext.afterResponse
						? {
								afterResponse: (message: any) => {
									if (message.stopReason === "error" && ext.afterResponse.errorMessage !== undefined) {
										message.errorMessage = ext.afterResponse.errorMessage;
									}
								},
							}
						: {}),
				}),
			);
		}
		if (ext.beforeCompact) {
			hooks.push(
				pd.hook(pd.CompactionTask, {
					beforeCompact: () =>
						ext.beforeCompact.decline ? { decline: true } : ext.beforeCompact.summary !== undefined ? { summary: ext.beforeCompact.summary } : undefined,
				}),
			);
		}
		if (ext.onYield) {
			const queue = [...ext.onYield];
			hooks.push(
				pd.hook(pd.GenerationTask, {
					onYield: () => {
						const next = queue.shift();
						return next?.continue !== undefined ? { continue: next.continue } : undefined;
					},
				}),
			);
		}
		registry.install(pd.defineExtension({ name: ext.name, sections, hooks }));
	}
	// Real time: retries and timers wait for it; timestamps are not compared.
	// A scenario that reopens needs storage that outlives its Harness.
	const reopens = (scenario.steps ?? []).some((step: any) => step.reopen);
	const directory = reopens ? mkdtempSync(join(tmpdir(), "crosscheck-")) : undefined;
	const { openNodeJsonlStorage } = await load.durable("storage/jsonl/node.ts");
	const storage = async () => (directory === undefined ? new pd.MemoryStorage() : openNodeJsonlStorage(directory, context));
	const open = async () => {
		const opened = await pd.Harness.open(await storage(), { models, registry, settings: scenario.settings ?? {} }, context);
		// Tools are named in scenarios and passed as their registrations.
		const { tools: enabled, ...agent } = scenario.agent ?? {};
		const conversation = await opened.root(context, {
			agent: {
				model: { provider: "faux", modelId: "faux-1" },
				...agent,
				...(enabled === undefined ? {} : { tools: enabled.map((name: string) => tools.find((tool: any) => tool.name === name)) }),
			},
		});
		return { opened, conversation };
	};
	let { opened: harness, conversation: root } = await open();

	const submissions: any[] = [];
	const handle = (id: unknown) => harness.submission(id, context);
	// Conversation documents, version 1, starting empty.
	const docs = new Map<string, any>();
	for (const d of scenario.documents ?? []) {
		docs.set(
			d.kind,
			pd.defineDoc({ kind: d.kind, version: 1, scope: "conversation", history: d.history, fork: d.fork, initial: () => ({}), checkpointWhen: () => false }),
		);
	}
	const forks: any[] = [];
	let current = root;
	const lives: unknown[] = [];
	const builtins: unknown[] = [];
	for (const step of scenario.steps ?? []) {
		if (step.reopen) {
			if (forks.length > 0) throw new Error("reopening with forks is not supported");
			await harness.close(context);
			({ opened: harness, conversation: root } = await open());
			current = root;
		} else if (step.fork !== undefined) {
			// At an entry of the current conversation, by index (negative:
			// from the end); the fork becomes current.
			const items = [...(await current.entries({}, 1000, undefined, context)).items].reverse();
			const index = step.fork.at < 0 ? items.length + step.fork.at : step.fork.at;
			current = await current.fork(items[index].id, { ownership: { kind: "ownerless" } }, context);
			forks.push(current);
		} else if (step.builtins) {
			// The built-in documents of the current conversation; the provider
			// identity is random, so only its shape.
			const provider = await harness.snapshot(pd.ProviderDoc, current.id, context);
			// Usage as token totals: the two runtimes' Usage types differ.
			const ledger = await harness.snapshot(pd.UsageDoc, current.id, context);
			const totals = (bucket: Record<string, any>) =>
				Object.fromEntries(Object.entries(bucket).map(([key, u]) => [key, u.input + u.output + u.cacheRead + u.cacheWrite]));
			builtins.push({
				"pi.live": (await harness.snapshot(pd.LiveDoc, current.id, context)) ?? null,
				"pi.inbox": (await harness.snapshot(pd.InboxDoc, current.id, context)) ?? null,
				"pi.usage": ledger === undefined ? null : { models: totals(ledger.models), tools: totals(ledger.tools) },
				"pi.provider": typeof provider?.sessionId === "string" && provider.sessionId.length === 36,
			});
		} else if (step.live) {
			lives.push(normalizeLive(await harness.snapshot(pd.LiveDoc, current.id, context)));
		} else if (step.doc !== undefined) {
			await current.commit(async (tx: any) => {
				const draft = await tx.doc(docs.get(step.doc.kind), current.id);
				for (const key of Object.keys(draft)) delete draft[key];
				Object.assign(draft, step.doc.value);
			}, context);
		} else if (step.retireDoc !== undefined) {
			await current.commit((tx: any) => tx.retireDoc(docs.get(step.retireDoc), current.id), context);
		} else if (step.use !== undefined) {
			current = step.use === "root" ? root : forks[step.use];
		} else if (step.submit) {
			try {
				submissions.push({ id: (await current.submit(step.submit, context)).id });
			} catch (error: any) {
				submissions.push({ refused: error?.name ?? "Error" });
			}
		} else if (step.wait === "idle") {
			await current.waitForIdle(context);
		} else if (step.wait === "retrying") {
			// Until a generation waits out a retry backoff.
			for (let i = 0; ; i++) {
				const live = await harness.snapshot(pd.LiveDoc, current.id, context);
				if (live?.generation?.retry !== undefined) break;
				if (i > 2000) throw new Error("no retry");
				await new Promise((resolve) => setTimeout(resolve, 5));
			}
		} else if (step.wait === "compactions") {
			// Until no compaction is listed: background ones do not keep a
			// conversation busy.
			for (let i = 0; ; i++) {
				const live = await harness.snapshot(pd.LiveDoc, current.id, context);
				if (live?.compactions === undefined || live.compactions.length === 0) break;
				if (i > 2000) throw new Error("compactions never ended");
				await new Promise((resolve) => setTimeout(resolve, 5));
			}
		} else if (step.wait === "settled") {
			for (const submission of submissions) if (submission.id !== undefined) await (await handle(submission.id)).wait(context);
		} else if (step.waitHeld) {
			await gate(step.waitHeld).reached;
		} else if (step.release) {
			gate(step.release).release();
		} else if (step.abort) {
			await current.abort(context, { background: step.abort === "background" });
		} else if (step.configure !== undefined) {
			// A model is named by its faux ID.
			const { model, ...change } = step.configure;
			await current.configure({ ...change, ...(model === undefined ? {} : { model: { provider: "faux", modelId: model } }) }, context);
		} else if (step.compact !== undefined) {
			await current.compact(typeof step.compact === "string" ? step.compact : undefined, context);
		} else if (step.reset !== undefined) {
			await current.reset(typeof step.reset === "string" ? step.reset : undefined, context);
		}
	}
	const transcriptOf = async (conversation: any) => {
		await conversation.waitForIdle(context);
		const page = await conversation.entries({}, 1000, undefined, context);
		return [...page.items].reverse();
	};
	// Entries as kinds and model messages, and the data of non-pi entries.
	const normalizeEntry = (entry: any) => ({
		kind: entry.kind,
		messages: (entry.model ?? []).map(normalizeMessage),
		...(entry.kind.startsWith("pi.") || entry.data === undefined ? {} : { data: entry.data }),
	});
	const rootEntries = await transcriptOf(root);
	const transcript = rootEntries.map(normalizeEntry);
	// afterRun summaries, their entries as root transcript positions.
	const position = (id: unknown) => rootEntries.findIndex((entry: any) => entry.id === id);
	const afterRun = asked.map((run) => ({ reason: run.reason, first: position(run.first), answer: position(run.answer), last: position(run.last) }));
	// The root's usage ledger: tokens per model and per tool, input, output,
	// and cache counts together (the faux provider counts the prompt as
	// cache reads and writes when a session ID is set).
	const ledger = (await harness.snapshot(pd.UsageDoc, root.id, context)) ?? { models: {}, tools: {} };
	const tokens = (bucket: Record<string, any>) =>
		Object.fromEntries(Object.entries(bucket).map(([key, u]) => [key, u.input + u.output + u.cacheRead + u.cacheWrite]));
	const usage = { models: tokens(ledger.models), tools: tokens(ledger.tools) };
	// Each conversation's documents, the root first; null when absent.
	const documents = [];
	for (const conversation of [root, ...forks]) {
		const values: Record<string, unknown> = {};
		for (const [kind, token] of docs) values[kind] = (await harness.snapshot(token, conversation.id, context)) ?? null;
		documents.push(values);
	}
	const forked = [];
	for (const fork of forks) forked.push((await transcriptOf(fork)).map(normalizeEntry));
	const settled = [];
	for (const submission of submissions) {
		if (submission.refused) {
			settled.push({ refused: submission.refused });
			continue;
		}
		const record = await (await handle(submission.id)).status(context);
		settled.push({ type: record.type, status: record.status, ...(record.reason ? { reason: record.reason } : {}) });
	}
	await harness.close(context);
	if (directory !== undefined) rmSync(directory, { recursive: true, force: true });
	return {
		requests,
		summaryRequests,
		transcript,
		usage,
		...(forks.length > 0 ? { forks: forked } : {}),
		...(docs.size > 0 ? { documents } : {}),
		...(lives.length > 0 ? { live: lives } : {}),
		...(builtins.length > 0 ? { builtins } : {}),
		...(scenario.afterRun ? { afterRun } : {}),
		submissions: settled,
	};
}
