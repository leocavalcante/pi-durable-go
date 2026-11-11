package durable

import (
	"encoding/json"
	"fmt"
)

type migrationFailure struct {
	identity uint64
	err      error
}

type taskFit struct {
	reason   string
	err      error
	migrates bool
}

// Built-in identities never change and do not share the Registry's counter.
func (h *Harness) resolve(kind string, snapshot *RegistrySnapshot) resolvedTask {
	var definition TaskDefinition
	var identity uint64
	switch kind {
	case "pi.generation":
		definition, identity = generationTaskDefinition(), ^uint64(0)
	case "pi.tool":
		definition, identity = toolTaskDefinition(), ^uint64(0)-1
	case "pi.compaction":
		definition, identity = compactionTaskDefinition(), ^uint64(0)-2
	default:
		resolved, _ := snapshot.resolved(kind)
		return resolved
	}
	return resolvedTask{Definition: definition, Identity: identity}
}

// Called on the Session line after storage succeeds. Clearing earlier would
// retry/report a failed migration again if an orphan write were certified
// unapplied for a cancelled caller.
func (s *Scheduler) clearSettledMigrations(state *State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.migrations {
		if task := state.Tasks[id]; task == nil || task.Status == "terminal" {
			delete(s.migrations, id)
		}
	}
}

// fit is read-only. In particular, a missing migration is not cached until an
// actual start/orphan decision attempts it. Inspection runs no task code.
func fit(record *TaskRecord, resolved resolvedTask, failure migrationFailure) taskFit {
	if resolved.Identity == 0 {
		return taskFit{reason: "missing_task"}
	}
	if record.Version > resolved.Definition.Version {
		return taskFit{reason: "task_too_old"}
	}
	if failure.identity == resolved.Identity && failure.err != nil {
		return taskFit{reason: "migration_failed", err: failure.err}
	}
	return taskFit{migrates: record.Version < resolved.Definition.Version}
}

// Called only on the Session line, with Scheduler.mu released around migration.
func (s *Scheduler) resolveForStart(record *TaskRecord, snapshot *RegistrySnapshot, reports *[]Report) (resolvedTask, *MigratedTask, string) {
	resolved := s.h.resolve(record.Kind, snapshot)
	s.mu.Lock()
	failure := s.migrations[record.ID]
	if failure.identity != resolved.Identity {
		delete(s.migrations, record.ID)
		failure = migrationFailure{}
	}
	s.mu.Unlock()
	decision := fit(record, resolved, failure)
	if decision.reason != "" || !decision.migrates {
		return resolved, nil, decision.reason
	}
	var migrated MigratedTask
	err := containPanic(func() error {
		if resolved.Definition.Migrate == nil {
			return fmt.Errorf("task %s has no migration from %d", record.Kind, record.Version)
		}
		var err error
		migrated, err = resolved.Definition.Migrate(record.Version, append(json.RawMessage(nil), record.Input...), append(json.RawMessage(nil), record.Checkpoint...))
		if err != nil {
			return err
		}
		if !json.Valid(migrated.Input) || !json.Valid(migrated.Checkpoint) {
			return fmt.Errorf("%w: migrated task input/checkpoint must be JSON", ErrInvalid)
		}
		return nil
	})
	if err != nil {
		s.mu.Lock()
		s.migrations[record.ID] = migrationFailure{identity: resolved.Identity, err: err}
		s.mu.Unlock()
		*reports = append(*reports, Report{Kind: ReportMigrationFailed, Err: err, Extension: resolved.Extension,
			Task: &ReportTask{ID: record.ID, Kind: record.Kind, Conversation: record.Conversation}})
		return resolved, nil, "migration_failed"
	}
	migrated.Input = append(json.RawMessage(nil), migrated.Input...)
	migrated.Checkpoint = append(json.RawMessage(nil), migrated.Checkpoint...)
	return resolved, &migrated, ""
}

func canReserve(record *TaskRecord, resolved resolvedTask) bool {
	return resolved.Identity != 0 && (resolved.Definition.Version == record.Version || resolved.Definition.Version > record.Version && resolved.Definition.Migrate != nil)
}
func outdatedReason(record *TaskRecord, resolved resolvedTask) string {
	if resolved.Identity == 0 {
		return "missing_task"
	}
	if resolved.Definition.Version < record.Version {
		return "task_too_old"
	}
	return "migration_failed"
}
