package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
)

type fsm RaftKVStore

var _ raft.FSM = new(fsm)
var _ raft.FSMSnapshot = new(fsmSnapshot)

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		f.mu.Lock()
		defer f.mu.Unlock()
		f.m[c.Key] = c.Key
		return nil
	case "delete":
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.m, c.Key)
		return nil
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	f.m = o
	return nil
}

//--------------------fsm snapshot------------------

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		_ = sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
