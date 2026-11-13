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
			for (const call of step.calls ?? []) content.push(piAi.fauxToolCall(call.name, call.args ?? {}, { id: call.id }));
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
				const images = (result.images ?? []).map((image: any) => ({ type: "image", data: image.data, mimeType: image.mimeType }));
				return {
					output: [...(result.text === undefined ? [] : [{ type: "text", text: result.text }]), ...images],
					...(result.isError ? { isError: true } : {}),
					...(result.diagnostics !== undefined ? { diagnostics: result.diagnostics } : {}),
					...(Object.keys(control).length > 0 ? { control } : {}),
				};
			},
		});
	});
	const registry = pd.createRegistry();
	if (tools.length > 0) registry.install(pd.defineExtension({ name: "scenario", tools }));
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
	for (const step of scenario.steps ?? []) {
		if (step.reopen) {
			await harness.close(context);
			({ opened: harness, conversation: root } = await open());
		} else if (step.submit) {
			try {
				submissions.push({ id: (await root.submit(step.submit, context)).id });
			} catch (error: any) {
				submissions.push({ refused: error?.name ?? "Error" });
			}
		} else if (step.wait === "idle") {
			await root.waitForIdle(context);
		} else if (step.wait === "retrying") {
			// Until a generation waits out a retry backoff.
			for (let i = 0; ; i++) {
				const live = await harness.snapshot(pd.LiveDoc, root.id, context);
				if (live?.generation?.retry !== undefined) break;
				if (i > 2000) throw new Error("no retry");
				await new Promise((resolve) => setTimeout(resolve, 5));
			}
		} else if (step.wait === "compactions") {
			// Until no compaction is listed: background ones do not keep a
			// conversation busy.
			for (let i = 0; ; i++) {
				const live = await harness.snapshot(pd.LiveDoc, root.id, context);
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
			await root.abort(context, { background: step.abort === "background" });
		} else if (step.configure !== undefined) {
			// A model is named by its faux ID.
			const { model, ...change } = step.configure;
			await root.configure({ ...change, ...(model === undefined ? {} : { model: { provider: "faux", modelId: model } }) }, context);
		} else if (step.compact !== undefined) {
			await root.compact(typeof step.compact === "string" ? step.compact : undefined, context);
		} else if (step.reset !== undefined) {
			await root.reset(typeof step.reset === "string" ? step.reset : undefined, context);
		}
	}
	await root.waitForIdle(context);
	const page = await root.entries({}, 1000, undefined, context);
	const transcript = [...page.items].reverse().map((entry: any) => ({
		kind: entry.kind,
		messages: (entry.model ?? []).map(normalizeMessage),
	}));
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
	return { requests, summaryRequests, transcript, submissions: settled };
}
