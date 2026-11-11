package durable

import (
	"context"
	"testing"
	"time"
)

func TestTaskGraphShowsLiveTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gate := newGatedTool()
	h, root := runHarness(t, toolThenEcho(), Settings{}, gate.extension())
	graphs, stop, err := h.WatchTaskGraph(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	sub := submit(t, root, SubmissionRequest{Content: "tool"})
	<-gate.started
	graph, err := h.TaskGraph(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var generation, tool *TaskGraphNode
	for _, node := range graph.Tasks {
		node := node
		switch node.Kind {
		case "pi.generation":
			generation = &node
		case "pi.tool":
			tool = &node
		}
	}
	if generation == nil || tool == nil || generation.Status != "waiting" || generation.Phase != "tools" || len(generation.On) != 1 ||
		tool.Owner != generation.ID || tool.Status != "running" {
		t.Fatalf("graph=%+v", graph.Tasks)
	}
	gate.release <- struct{}{}
	waitSettled(t, h, sub)
	if err := root.WaitForIdle(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case graph := <-graphs:
			if len(graph.Tasks) == 0 {
				return
			}
		case <-deadline:
			t.Fatal("the watch never showed an empty graph")
		}
	}
}
