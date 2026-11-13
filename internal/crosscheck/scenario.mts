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
		case "toolResult":
			return { role: "tool", id: message.toolCallId, name: message.toolName, text: text(message.content), isError: message.isError === true };
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
		models: [{ id: "faux-1", contextWindow: scenario.contextWindow ?? 128000, maxTokens: 16384, input: ["text", "image"] }],
	});
	const models = piAi.createModels();
	models.setProvider(faux.provider);
	const requests: unknown[] = [];
	faux.setResponses(
		(scenario.model ?? []).map((step: any) => async (request: any, options: any) => {
			requests.push(request.messages.map(normalizeMessage));
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
			async execute(args: unknown, _api: unknown, callContext: any) {
				const results = tool.results ?? [];
				const result = results[Math.min(calls++, results.length - 1)] ?? { text: JSON.stringify(args) };
				if (result.hold) await hold(result.hold, callContext?.abortSignal);
				if (result.throw) throw new Error(result.throw);
				return {
					output: result.text === undefined ? [] : [{ type: "text", text: result.text }],
					...(result.isError ? { isError: true } : {}),
				};
			},
		});
	});
	const registry = pd.createRegistry();
	if (tools.length > 0) registry.install(pd.defineExtension({ name: "scenario", tools }));
	// Real time: retries and timers wait for it; timestamps are not compared.
	const harness = await pd.Harness.open(new pd.MemoryStorage(), { models, registry, settings: scenario.settings ?? {} }, context);
	const root = await harness.root(context, {
		agent: { model: { provider: "faux", modelId: "faux-1" }, ...(scenario.agent ?? {}) },
	});

	const submissions: any[] = [];
	for (const step of scenario.steps ?? []) {
		if (step.submit) {
			try {
				submissions.push(await root.submit(step.submit, context));
			} catch (error: any) {
				submissions.push({ refused: error?.name ?? "Error" });
			}
		} else if (step.wait === "idle") {
			await root.waitForIdle(context);
		} else if (step.wait === "settled") {
			for (const submission of submissions) if (submission.wait) await submission.wait(context);
		} else if (step.waitHeld) {
			await gate(step.waitHeld).reached;
		} else if (step.release) {
			gate(step.release).release();
		} else if (step.abort) {
			await root.abort(context, { background: step.abort === "background" });
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
		const record = await submission.status(context);
		settled.push({ type: record.type, status: record.status, ...(record.reason ? { reason: record.reason } : {}) });
	}
	await harness.close(context);
	return { requests, transcript, submissions: settled };
}
