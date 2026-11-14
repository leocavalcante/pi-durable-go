package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"

	durable "github.com/leocavalcante/pi-durable-go"
)

// OpenAICompat is pi-ai's compat for an openai-completions model, as its
// catalog's model.compat sets it. Each field that is set replaces what pi-ai
// detects from the provider (the request's ModelRef.Provider) and the base
// URL; nil and empty fields keep the detected value.
type OpenAICompat struct {
	// SupportsStore sends store: false. Detected: not on the endpoints pi-ai
	// treats as non-standard.
	SupportsStore *bool
	// SupportsDeveloperRole sends a reasoning model's instructions as a
	// developer message.
	SupportsDeveloperRole *bool
	// SupportsReasoningEffort sends reasoning_effort where the thinking
	// format may.
	SupportsReasoningEffort *bool
	// SupportsUsageInStreaming false sends no stream_options.
	SupportsUsageInStreaming *bool
	// SupportsFinishReason false reads a stream without a finish reason as
	// a stop, or a tool use when it has tool calls, instead of an error.
	SupportsFinishReason *bool
	// MaxTokensField is max_completion_tokens or max_tokens.
	MaxTokensField string
	// SupportsMidConvoSystemMessages keeps later system messages in place,
	// as updates of the prompt, instead of folding them into the first.
	// SupportsMidConvoToolAdditions also declares tools added later in
	// place, offering only the tools up to the last removal or redefinition.
	SupportsMidConvoSystemMessages *bool
	SupportsMidConvoToolAdditions  *bool
	// SupportsOpenAIGrammarTools sends a tool whose ConstrainedSampling is a
	// grammar ({"type": "grammar", "variants": {"openai_lark" | "openai_regex":
	// definition}}) as an OpenAI custom tool: the model writes its single
	// required string property as grammar-constrained input.
	SupportsOpenAIGrammarTools *bool
	// RequiresToolResultName sends a tool result's tool name.
	RequiresToolResultName *bool
	// RequiresAssistantAfterToolResult puts an assistant message between
	// tool results and a user message, and sends "" for no assistant text.
	RequiresAssistantAfterToolResult *bool
	// RequiresNonNullAssistantContent sends "" for no assistant text.
	RequiresNonNullAssistantContent *bool
	// RequiresToolsWithToolHistory sends "tools": [] when a request offers no
	// tools and its history has tool calls or results.
	RequiresToolsWithToolHistory *bool
	// RequiresThinkingAsText replays an answer's thinking as text.
	RequiresThinkingAsText *bool
	// RequiresReasoningContentOnAssistantMessages sends an empty
	// reasoning_content on a reasoning model's assistant messages.
	RequiresReasoningContentOnAssistantMessages *bool
	// ThinkingFormat is how a reasoning model's thinking level is sent:
	// openai, openrouter, deepseek, together, baseten, zai, qwen,
	// chat-template, qwen-chat-template, string-thinking, or ant-ling.
	ThinkingFormat string
	// ChatTemplateKwargs and ChatTemplateArgs are the values the
	// chat-template and baseten formats send; an object {"$var":
	// "thinking.enabled" | "thinking.effort" | "thinking.budget",
	// "omitWhenOff": bool} takes the request's value.
	ChatTemplateKwargs map[string]json.RawMessage
	ChatTemplateArgs   map[string]json.RawMessage
	// OpenRouterRouting is sent as the provider routing object.
	OpenRouterRouting json.RawMessage
	// VercelGatewayRouting is sent as providerOptions.gateway.
	VercelGatewayRouting *GatewayRouting
	// ZaiToolStream sends tool_stream: true with tools.
	ZaiToolStream *bool
	// ThinkingTokenBudgetField caps reasoning with the thinking level's
	// budget (StreamOptions.ThinkingBudgets over pi-ai's defaults):
	// thinking_token_budget, thinking_budget, or thinking_budget_tokens.
	ThinkingTokenBudgetField string
	// SupportsThinkingTokenBudget is ThinkingTokenBudgetField
	// thinking_token_budget.
	SupportsThinkingTokenBudget *bool
	// SupportsLongCacheRetention allows long prompt cache retention.
	SupportsLongCacheRetention *bool
	// CacheControlFormat anthropic marks the system prompt, the last tool,
	// and the last conversation text with cache_control.
	CacheControlFormat string
	// PromptCacheKeyMode is auto, enabled, or disabled.
	PromptCacheKeyMode string
	// SendSessionAffinityHeaders sends the request's session ID in headers
	// (detected: OpenRouter). SessionAffinityFormat names them: openrouter
	// sends x-session-id; openai sends session_id, x-client-request-id, and
	// x-session-affinity; openai-nosession the last two.
	SendSessionAffinityHeaders *bool
	SessionAffinityFormat      string
	// VLLMPriority is sent as vLLM's priority.
	VLLMPriority *float64
}

// GatewayRouting is pi-ai's compat.vercelGatewayRouting.
type GatewayRouting struct {
	Only  []string `json:"only,omitempty"`
	Order []string `json:"order,omitempty"`
}

// completionsCompat is the compat a request uses (pi-ai getCompat).
type completionsCompat struct {
	supportsStore, supportsDeveloperRole, supportsReasoningEffort, supportsUsageInStreaming   bool
	supportsFinishReason                                                                      bool
	supportsOpenAIGrammarTools                                                                bool
	supportsMidConvoSystemMessages, supportsMidConvoToolAdditions                             bool
	maxTokensField                                                                            string
	requiresToolResultName, requiresAssistantAfterToolResult, requiresNonNullAssistantContent bool
	requiresToolsWithToolHistory, requiresThinkingAsText, requiresReasoningContent            bool
	thinkingFormat                                                                            string
	chatTemplateKwargs, chatTemplateArgs                                                      map[string]json.RawMessage
	openRouterRouting                                                                         json.RawMessage
	gatewayRouting                                                                            *GatewayRouting
	zaiToolStream                                                                             bool
	thinkingTokenBudgetField                                                                  string
	supportsLongCacheRetention                                                                bool
	cacheControlFormat, promptCacheKeyMode                                                    string
	sendSessionAffinityHeaders                                                                bool
	sessionAffinityFormat                                                                     string
	vllmPriority                                                                              *float64
}

// compat is pi-ai's detectCompat for the provider and base URL, with
// Compat's fields over it.
func (o OpenAI) compat(ref durable.ModelRef) completionsCompat {
	provider, base := ref.Provider, o.baseURL()
	has := func(parts ...string) bool {
		for _, part := range parts {
			if strings.Contains(base, part) {
				return true
			}
		}
		return false
	}
	isZai := provider == "zai" || provider == "zai-coding-cn" || has("api.z.ai", "open.bigmodel.cn")
	isTogether := provider == "together" || has("api.together.ai", "api.together.xyz")
	isMoonshot := provider == "moonshotai" || provider == "moonshotai-cn" || has("api.moonshot.")
	isOpenRouter := provider == "openrouter" || has("openrouter.ai")
	isWorkersAI := provider == "cloudflare-workers-ai" || has("api.cloudflare.com")
	isAIGateway := provider == "cloudflare-ai-gateway" || has("gateway.ai.cloudflare.com")
	isNvidia := provider == "nvidia" || has("integrate.api.nvidia.com")
	isAntLing := provider == "ant-ling" || has("api.ant-ling.com")
	isCerebras := provider == "cerebras" || has("cerebras.ai")
	isDeepSeek := provider == "deepseek" || strings.Contains(strings.ToLower(base), "deepseek.com")
	isGrok := provider == "xai" || has("api.x.ai")
	nonStandard := isNvidia || isCerebras || isGrok || isTogether || has("chutes.ai") || isDeepSeek || isZai || isMoonshot ||
		provider == "opencode" || has("opencode.ai") || isWorkersAI || isAIGateway || isAntLing
	c := completionsCompat{
		supportsStore: !nonStandard,
		supportsDeveloperRole: isOpenRouter && (strings.HasPrefix(ref.ID, "anthropic/") || strings.HasPrefix(ref.ID, "openai/")) ||
			!nonStandard && !isOpenRouter,
		supportsReasoningEffort:    !isGrok && !isZai && !isMoonshot && !isTogether && !isAIGateway && !isNvidia && !isAntLing,
		supportsUsageInStreaming:   true,
		supportsFinishReason:       true,
		maxTokensField:             "max_completion_tokens",
		requiresReasoningContent:   isDeepSeek,
		thinkingFormat:             "openai",
		supportsLongCacheRetention: !(isTogether || isWorkersAI || isAIGateway || isNvidia || isAntLing),
		promptCacheKeyMode:         "auto",
	}
	if has("chutes.ai") || isDeepSeek || isMoonshot || isAIGateway || isTogether || isNvidia || isAntLing || isZai {
		c.maxTokensField = "max_tokens"
	}
	switch {
	case isDeepSeek:
		c.thinkingFormat = "deepseek"
	case isZai:
		c.thinkingFormat = "zai"
	case isTogether:
		c.thinkingFormat = "together"
	case isAntLing:
		c.thinkingFormat = "ant-ling"
	case isOpenRouter:
		c.thinkingFormat = "openrouter"
	}
	if provider == "openrouter" && strings.HasPrefix(ref.ID, "anthropic/") {
		c.cacheControlFormat = "anthropic"
	}
	c.sendSessionAffinityHeaders, c.sessionAffinityFormat = isOpenRouter, "openai"
	if isOpenRouter {
		c.sessionAffinityFormat = "openrouter"
	}
	x := o.Compat
	for _, field := range []struct {
		to   *bool
		from *bool
	}{
		{&c.supportsStore, x.SupportsStore}, {&c.supportsDeveloperRole, x.SupportsDeveloperRole},
		{&c.supportsReasoningEffort, x.SupportsReasoningEffort}, {&c.supportsUsageInStreaming, x.SupportsUsageInStreaming},
		{&c.supportsFinishReason, x.SupportsFinishReason}, {&c.supportsOpenAIGrammarTools, x.SupportsOpenAIGrammarTools},
		{&c.supportsMidConvoSystemMessages, x.SupportsMidConvoSystemMessages}, {&c.supportsMidConvoToolAdditions, x.SupportsMidConvoToolAdditions},
		{&c.requiresToolResultName, x.RequiresToolResultName}, {&c.requiresAssistantAfterToolResult, x.RequiresAssistantAfterToolResult},
		{&c.requiresNonNullAssistantContent, x.RequiresNonNullAssistantContent}, {&c.requiresToolsWithToolHistory, x.RequiresToolsWithToolHistory},
		{&c.requiresThinkingAsText, x.RequiresThinkingAsText}, {&c.requiresReasoningContent, x.RequiresReasoningContentOnAssistantMessages},
		{&c.zaiToolStream, x.ZaiToolStream}, {&c.supportsLongCacheRetention, x.SupportsLongCacheRetention},
		{&c.sendSessionAffinityHeaders, x.SendSessionAffinityHeaders},
	} {
		if field.from != nil {
			*field.to = *field.from
		}
	}
	if x.MaxTokensField != "" {
		c.maxTokensField = x.MaxTokensField
	}
	if x.ThinkingFormat != "" {
		c.thinkingFormat = x.ThinkingFormat
	}
	if x.CacheControlFormat != "" {
		c.cacheControlFormat = x.CacheControlFormat
	}
	if x.PromptCacheKeyMode != "" {
		c.promptCacheKeyMode = x.PromptCacheKeyMode
	}
	if x.SessionAffinityFormat != "" {
		c.sessionAffinityFormat = x.SessionAffinityFormat
	}
	c.chatTemplateKwargs, c.chatTemplateArgs = x.ChatTemplateKwargs, x.ChatTemplateArgs
	c.openRouterRouting, c.gatewayRouting, c.vllmPriority = x.OpenRouterRouting, x.VercelGatewayRouting, x.VLLMPriority
	c.thinkingTokenBudgetField = x.ThinkingTokenBudgetField
	if c.thinkingTokenBudgetField == "" && x.SupportsThinkingTokenBudget != nil && *x.SupportsThinkingTokenBudget {
		c.thinkingTokenBudgetField = "thinking_token_budget"
	}
	return c
}

// pi-ai's DEFAULT_THINKING_BUDGETS and MIN_ANSWER_TOKENS.
var defaultThinkingBudgets = map[string]int{"minimal": 1024, "low": 2048, "medium": 8192, "high": 16384}

const minAnswerTokens = 1024

// thinkingBudget is pi-ai's resolveClampedThinkingBudget: the level's
// budget (xhigh and max read as high), leaving room for an answer under the
// output limit; 0 for none.
func (o OpenAI) thinkingBudget(effort string, budgets map[string]int, ceiling int) int {
	if effort == "" || !o.Reasoning {
		return 0
	}
	if effort == "xhigh" || effort == "max" {
		effort = "high"
	}
	budget, ok := budgets[effort]
	if !ok {
		budget = defaultThinkingBudgets[effort]
	}
	if ceiling <= 0 {
		ceiling = o.MaxTokens
	}
	budget = min(budget, max(0, ceiling-minAnswerTokens))
	return max(budget, 0)
}

// chatTemplateValues is pi-ai's buildChatTemplateValues: each value as it
// is, or the request's value for a $var object; nil when none is left.
func (o OpenAI) chatTemplateValues(values map[string]json.RawMessage, effort string, budget int) map[string]any {
	out := map[string]any{}
	for key, raw := range values {
		var value any
		if json.Unmarshal(raw, &value) != nil {
			continue
		}
		object, isObject := value.(map[string]any)
		if !isObject {
			out[key] = value
			continue
		}
		if omit, _ := object["omitWhenOff"].(bool); omit && effort == "" {
			continue
		}
		switch object["$var"] {
		case "thinking.enabled":
			out[key] = effort != ""
		case "thinking.budget":
			if budget > 0 {
				out[key] = budget
			}
		default:
			level := effort
			if level == "" {
				level = "off"
			}
			mapped, named := o.ThinkingLevelMap[level]
			switch {
			case !named && effort != "":
				out[key] = effort
			case named && mapped != nil:
				out[key] = *mapped
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cacheControl is pi-ai's getCompatCacheControl: the marker anthropic
// caching puts on the system prompt, the last tool, and the last text.
func (c completionsCompat) cacheControl(retention string) map[string]any {
	if c.cacheControlFormat != "anthropic" || retention == "none" {
		return nil
	}
	marker := map[string]any{"type": "ephemeral"}
	if retention == "long" && c.supportsLongCacheRetention {
		marker["ttl"] = "1h"
	}
	return marker
}

// markCacheText puts marker on a message's text (pi-ai
// addCacheControlToTextContent): text becomes one marked part, and parts
// get it on their last text part; false without text.
func markCacheText(msg *openAIMessage, marker map[string]any) bool {
	switch content := msg.Content.(type) {
	case string:
		if content == "" {
			return false
		}
		msg.Content = []openAIContentPart{{Type: "text", Text: content, CacheControl: marker}}
		return true
	case []openAIContentPart:
		for i := len(content) - 1; i >= 0; i-- {
			if content[i].Type == "text" {
				content[i].CacheControl = marker
				return true
			}
		}
	}
	return false
}

// applyCacheControl is pi-ai's applyAnthropicCacheControl.
func applyCacheControl(messages []openAIMessage, tools []openAITool, marker map[string]any) {
	for i := range messages {
		if messages[i].Role == "system" || messages[i].Role == "developer" {
			markCacheText(&messages[i], marker)
			break
		}
	}
	if len(tools) > 0 {
		tools[len(tools)-1].CacheControl = marker
	}
	for i := len(messages) - 1; i >= 0; i-- {
		switch messages[i].Role {
		case "user", "assistant", "tool":
			if markCacheText(&messages[i], marker) {
				return
			}
		}
	}
}

// hasToolHistory is pi-ai's: a tool result, or an answer with tool calls.
func hasToolHistory(messages []durable.Message) bool {
	for _, m := range messages {
		if m.Role == "tool" || m.Role == "assistant" && len(m.ToolCalls) > 0 {
			return true
		}
	}
	return false
}

// requestHeaders are the headers pi-ai's createClient sends: the identity's
// User-Agent and provider attribution, then the session-affinity headers
// the compat asks for; the request's own headers go over them.
func (o OpenAI) requestHeaders(req durable.ModelRequest, c completionsCompat) map[string]string {
	headers := map[string]string{}
	if ua := userAgent(req.Identity); ua != "" {
		headers["User-Agent"] = ua
	}
	for name, value := range attributionHeaders(req.Model.Provider, o.baseURL(), req.Identity) {
		headers[name] = value
	}
	if id := req.SessionID; id != "" && c.sendSessionAffinityHeaders {
		if c.sessionAffinityFormat == "openrouter" {
			headers["x-session-id"] = id
		} else {
			if c.sessionAffinityFormat == "openai" {
				headers["session_id"] = id
			}
			headers["x-client-request-id"] = id
			headers["x-session-affinity"] = id
		}
	}
	if req.Stream != nil {
		for name, value := range req.Stream.Headers {
			headers[name] = value
		}
	}
	return headers
}

// attributionHeaders are pi-ai's getAttributionHeaders: OpenRouter's
// referer, title, and categories, NVIDIA's billing origin, or OpenCode's
// client, from the identity's attribution; none without one.
func attributionHeaders(provider, base string, identity *durable.ClientIdentity) map[string]string {
	if identity == nil || len(identity.Attribution) == 0 {
		return nil
	}
	var attribution struct {
		Name       string   `json:"name"`
		URL        string   `json:"url"`
		Categories []string `json:"categories"`
	}
	if json.Unmarshal(identity.Attribution, &attribution) != nil || string(identity.Attribution) == "null" {
		return nil
	}
	host := ""
	if parsed, err := url.Parse(base); err == nil {
		host = parsed.Hostname()
	}
	switch {
	case provider == "openrouter" || strings.Contains(base, "openrouter.ai"):
		headers := map[string]string{"HTTP-Referer": attribution.URL, "X-OpenRouter-Title": attribution.Name}
		if len(attribution.Categories) > 0 {
			headers["X-OpenRouter-Categories"] = strings.Join(attribution.Categories, ",")
		}
		return headers
	case provider == "nvidia" || host == "integrate.api.nvidia.com":
		name := attribution.Name
		if name != "" {
			r, size := utf8.DecodeRuneInString(name)
			name = strings.ToUpper(string(r)) + name[size:]
		}
		return map[string]string{"X-BILLING-INVOKE-ORIGIN": name}
	case provider == "opencode" || provider == "opencode-go" || host == "opencode.ai":
		return map[string]string{"x-opencode-client": attribution.Name}
	}
	return nil
}

// grammarSampling is a grammar tool's custom format and input property.
type grammarSampling struct {
	syntax, definition, property string
}

// resolveGrammar is pi-ai's resolveGrammarConstrainedSampling: nil unless
// the tool asks for a grammar and the endpoint takes grammar tools; the
// Lark variant before the regex one.
func resolveGrammar(tool durable.ToolSpec, supported bool) (*grammarSampling, error) {
	var config struct {
		Type     string `json:"type"`
		Variants struct {
			Lark  *string `json:"openai_lark"`
			Regex *string `json:"openai_regex"`
		} `json:"variants"`
	}
	if len(tool.ConstrainedSampling) == 0 || json.Unmarshal(tool.ConstrainedSampling, &config) != nil || config.Type != "grammar" || !supported {
		return nil, nil
	}
	lark := config.Variants.Lark != nil && strings.TrimSpace(*config.Variants.Lark) != ""
	regex := config.Variants.Regex != nil && strings.TrimSpace(*config.Variants.Regex) != ""
	if !lark && !regex {
		return nil, fmt.Errorf("Tool %q cannot use grammar constrained sampling: no supported grammar variant was provided.", tool.Name)
	}
	property, err := grammarInputProperty(tool.Parameters)
	if err != nil {
		return nil, fmt.Errorf("Tool %q cannot use grammar constrained sampling: %s.", tool.Name, err)
	}
	if lark {
		return &grammarSampling{syntax: "lark", definition: *config.Variants.Lark, property: property}, nil
	}
	return &grammarSampling{syntax: "regex", definition: *config.Variants.Regex, property: property}, nil
}

// grammarInputProperty is pi-ai's inferGrammarInputProperty: the one
// required property, a string, that the input fills.
func grammarInputProperty(parameters json.RawMessage) (string, error) {
	var schema struct {
		Type       any                        `json:"type"`
		Required   []any                      `json:"required"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if json.Unmarshal(parameters, &schema) != nil || schema.Type != "object" {
		return "", errors.New("grammar constrained sampling requires an object parameter schema")
	}
	property, ok := "", len(schema.Required) == 1
	if ok {
		property, ok = schema.Required[0].(string)
	}
	if !ok {
		return "", errors.New("grammar constrained sampling requires exactly one required string property")
	}
	raw, ok := schema.Properties[property]
	if !ok || string(raw) == "null" {
		return "", fmt.Errorf("grammar constrained sampling requires a properties entry for %s", property)
	}
	var entry struct {
		Type any `json:"type"`
	}
	if json.Unmarshal(raw, &entry) != nil || entry.Type != "string" {
		return "", fmt.Errorf("grammar constrained sampling property %s must have type string", property)
	}
	return property, nil
}

// grammarProperties is pi-ai's createGrammarToolInputProperties: the input
// property of each grammar tool by name.
func grammarProperties(tools []durable.ToolSpec, supported bool) (map[string]string, error) {
	properties := map[string]string{}
	for _, tool := range tools {
		grammar, err := resolveGrammar(tool, supported)
		if err != nil {
			return nil, sanitizedError(err.Error(), false)
		}
		if grammar != nil {
			properties[tool.Name] = grammar.property
		}
	}
	return properties, nil
}

// declaredTools is pi-ai's getDeclaredTools: every tool the system messages
// declared, its latest definition in first-declaration order; then the
// request's tools that none declared.
func declaredTools(messages []durable.Message, offered []durable.ToolSpec) []durable.ToolSpec {
	var out []durable.ToolSpec
	at := map[string]int{}
	add := func(tool durable.ToolSpec) {
		if i, ok := at[tool.Name]; ok {
			out[i] = tool
			return
		}
		at[tool.Name] = len(out)
		out = append(out, tool)
	}
	for _, m := range messages {
		if m.Role == "system" {
			for _, tool := range m.ToolsAdded {
				add(tool)
			}
		}
	}
	for _, tool := range offered {
		if _, ok := at[tool.Name]; !ok {
			add(tool)
		}
	}
	return out
}

// jsQuote is JSON.stringify of a string: the two escapes, the short
// control escapes, \u00XX for other control characters.
func jsQuote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}
