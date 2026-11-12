package durable

import (
	"context"
	"fmt"
	"strings"
)

// revisionKey is the State.Revisions key of a document's revision at a
// sequence: the document's key, "@", and the sequence in 20 digits, so a
// document's revisions sort by sequence.
func revisionKey(docKey string, sequence uint64) string {
	return fmt.Sprintf("%s@%020d", docKey, sequence)
}

// RevisionKey is revisionKey for storages: the key a revision record is
// stored under.
func RevisionKey(docKey string, sequence uint64) string { return revisionKey(docKey, sequence) }

// revisionAt is a document's newest revision at or before a sequence: the
// State's own (all of them in full mode, those of commits not published yet
// in a bounded Session), or storage's. Nil when it had none then.
func revisionAt(st *State, docKey string, sequence uint64) (*DocumentRevision, error) {
	best := latestRevision(st.Revisions, docKey, sequence)
	if st.cold != nil {
		stored, err := st.cold.storage.DocumentRevision(context.Background(), docKey, sequence)
		if err != nil {
			err = fmt.Errorf("durable: reading revisions of document %s: %w", docKey, err)
			st.cold.fail(err)
			return nil, err
		}
		if stored != nil && (best == nil || stored.Sequence > best.Sequence) {
			best = stored
		}
	}
	return best, nil
}

// latestRevision finds a document's newest revision at or before a
// sequence in a revision table.
func latestRevision(revisions map[string]*DocumentRevision, docKey string, sequence uint64) *DocumentRevision {
	prefix := docKey + "@"
	limit := revisionKey(docKey, sequence)
	var best *DocumentRevision
	bestKey := ""
	for key, revision := range revisions {
		if strings.HasPrefix(key, prefix) && key <= limit && key > bestKey && !strings.Contains(key[len(prefix):], "@") {
			best, bestKey = revision, key
		}
	}
	return best
}

// DocumentRevision is a document's newest revision at or before a sequence,
// detached; nil when it had none then.
func (r StateRecords) DocumentRevision(docKey string, sequence uint64) *DocumentRevision {
	if r.State == nil {
		return nil
	}
	if revision := latestRevision(r.State.Revisions, docKey, sequence); revision != nil {
		return cloneRecord(revision)
	}
	return nil
}

// latestRevisions finds, in a revision table, the newest revision at or
// before a sequence of every document whose key starts with prefix.
func latestRevisions(revisions map[string]*DocumentRevision, prefix string, sequence uint64) map[string]*DocumentRevision {
	out := map[string]*DocumentRevision{}
	for key, revision := range revisions {
		at := strings.LastIndex(key, "@")
		if at < 0 || !strings.HasPrefix(key, prefix) || revision.Sequence > sequence {
			continue
		}
		doc := key[:at]
		if best := out[doc]; best == nil || revision.Sequence > best.Sequence {
			out[doc] = revision
		}
	}
	return out
}

// DocumentRevisions are the newest revisions at or before a sequence of the
// documents under a key prefix, detached.
func (r StateRecords) DocumentRevisions(prefix string, sequence uint64) map[string]*DocumentRevision {
	if r.State == nil {
		return map[string]*DocumentRevision{}
	}
	out := latestRevisions(r.State.Revisions, prefix, sequence)
	for key, revision := range out {
		out[key] = cloneRecord(revision)
	}
	return out
}

// revisionsIn are the newest revisions at or before a sequence of the
// documents under a key prefix: the State's own over storage's.
func revisionsIn(st *State, prefix string, sequence uint64) (map[string]*DocumentRevision, error) {
	out := map[string]*DocumentRevision{}
	if st.cold != nil {
		stored, err := st.cold.storage.DocumentRevisions(context.Background(), prefix, sequence)
		if err != nil {
			err = fmt.Errorf("durable: reading revisions under %s: %w", prefix, err)
			st.cold.fail(err)
			return nil, err
		}
		out = stored
	}
	for key, revision := range latestRevisions(st.Revisions, prefix, sequence) {
		if best := out[key]; best == nil || revision.Sequence > best.Sequence {
			out[key] = revision
		}
	}
	return out, nil
}
