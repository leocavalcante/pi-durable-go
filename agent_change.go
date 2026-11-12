package durable

import (
	"context"
	"fmt"
	"math"
)

// AgentChange changes a conversation's agent field by field (upstream
// AgentChange): a set field replaces the stored one, a field named in Clear
// is cleared, and every other field stays. Extensions and ExtensionEdits are
// one choice, as upstream's extensions are an array or {add, remove}:
// setting either clears the other; so are Tools and RemoveTools.
type AgentChange struct {
	Model          *ModelRef
	ThinkingLevel  *string
	Extensions     *[]string
	ExtensionEdits *SelectionEdits
	Tools          *[]string
	RemoveTools    *[]string
	ModelTools     *ModelTools
	Instructions   *string
	Cwd            *string
	MaxTurns       *int
	Compaction     *CompactionSettings
	// Clear names fields to clear: model, thinkingLevel, extensions, tools,
	// modelTools, instructions, cwd, maxTurns, or compaction.
	Clear []string
}

// ChangeAgent applies a change to a conversation's agent inside the
// caller's commit (upstream configure(tx, conversationId, change)).
func (tx *Tx) ChangeAgent(cid ConversationID, change AgentChange) error {
	rec, err := tx.Conversation(cid)
	if err != nil {
		return err
	}
	agent := *cloneRecord(&rec.Agent)
	if err := change.apply(&agent); err != nil {
		return err
	}
	return tx.Configure(cid, agent)
}

// ChangeAgent applies a change to the conversation's agent in a commit of its
// own.
func (c *Conversation) ChangeAgent(ctx context.Context, change AgentChange) error {
	return c.h.Commit(ctx, func(tx *Tx) error { return tx.ChangeAgent(c.id, change) })
}

func (change AgentChange) apply(agent *Agent) error {
	if change.MaxTurns != nil && *change.MaxTurns < 1 {
		return fmt.Errorf("%w: maxTurns must be an integer 1 or more, got %d", ErrInvalid, *change.MaxTurns)
	}
	if change.Compaction != nil {
		if err := checkCompactionTokens(*change.Compaction, "compaction"); err != nil {
			return err
		}
	}
	for _, field := range change.Clear {
		switch field {
		case "model":
			agent.Model = ModelRef{}
		case "thinkingLevel":
			agent.ThinkingLevel = ""
		case "extensions":
			agent.Extensions, agent.ExtensionEdits = nil, nil
		case "tools":
			agent.Tools, agent.RemoveTools = nil, nil
		case "modelTools":
			agent.ModelTools = nil
		case "instructions":
			agent.Instructions = nil
		case "cwd":
			agent.Cwd = ""
		case "maxTurns":
			agent.MaxTurns = 0
		case "compaction":
			agent.Compaction = nil
		default:
			return fmt.Errorf("%w: unknown agent field %q", ErrInvalid, field)
		}
	}
	if change.Model != nil {
		agent.Model = *change.Model
	}
	if change.ThinkingLevel != nil {
		agent.ThinkingLevel = *change.ThinkingLevel
	}
	if change.Extensions != nil {
		agent.Extensions, agent.ExtensionEdits = append([]string{}, (*change.Extensions)...), nil
	}
	if change.ExtensionEdits != nil {
		edits := SelectionEdits{Add: append([]string(nil), change.ExtensionEdits.Add...), Remove: append([]string(nil), change.ExtensionEdits.Remove...)}
		agent.Extensions, agent.ExtensionEdits = nil, &edits
	}
	if change.Tools != nil {
		agent.Tools, agent.RemoveTools = append([]string{}, (*change.Tools)...), nil
	}
	if change.RemoveTools != nil {
		agent.Tools, agent.RemoveTools = nil, append([]string{}, (*change.RemoveTools)...)
	}
	if change.ModelTools != nil {
		agent.ModelTools = cloneRecord(change.ModelTools)
	}
	if change.Instructions != nil {
		instructions := *change.Instructions
		agent.Instructions = &instructions
	}
	if change.Cwd != nil {
		agent.Cwd = *change.Cwd
	}
	if change.MaxTurns != nil {
		agent.MaxTurns = *change.MaxTurns
	}
	if change.Compaction != nil {
		agent.Compaction = cloneRecord(change.Compaction)
	}
	return nil
}

// checkCompactionTokens refuses token counts that are not integers 0 or more
// (upstream compactionFields).
func checkCompactionTokens(settings CompactionSettings, path string) error {
	for name, value := range map[string]*float64{"reserveTokens": settings.ReserveTokens, "keepRecentTokens": settings.KeepRecentTokens, "backgroundTokens": settings.BackgroundTokens} {
		if value != nil && (*value < 0 || *value != math.Trunc(*value) || math.IsInf(*value, 0) || *value > 1<<53-1) {
			return fmt.Errorf("%w: %s.%s must be an integer 0 or more, got %v", ErrInvalid, path, name, *value)
		}
	}
	for key, override := range settings.ModelOverrides {
		if err := checkCompactionTokens(override, path+".modelOverrides."+key); err != nil {
			return err
		}
	}
	return nil
}
