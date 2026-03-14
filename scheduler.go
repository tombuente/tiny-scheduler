package main

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"gonum.org/v1/gonum/graph/encoding/dot"
	"gonum.org/v1/gonum/graph/simple"
)

type scheduler struct {
	currID int64

	idLookup   map[string]int64
	taskLookup map[int64]*task

	graph *simple.DirectedGraph
}

type task struct {
	Name         string
	Dependencies []string

	finished atomic.Bool
}

func newScheduler(tasks []*task) (*scheduler, error) {
	s := &scheduler{
		idLookup:   make(map[string]int64, len(tasks)),
		taskLookup: make(map[int64]*task, len(tasks)),
		graph:      simple.NewDirectedGraph(),
	}

	for _, t := range tasks {
		s.addTask(t, nil)
	}

	for _, t := range tasks {
		if err := s.addDependency(t.Name, t.Dependencies); err != nil {
			return nil, fmt.Errorf("failed to add dependency: %w", err)
		}
	}

	return s, nil
}

func (s *scheduler) addTask(task *task, deps []string) {
	node := simple.Node(s.currID)
	s.graph.AddNode(node)

	s.idLookup[task.Name] = s.currID
	s.taskLookup[s.currID] = task

	s.currID++
}

// addDependency creates edges from each dep to name in the graph.
// Returns an error if task or any of its deps are not registered.
func (s *scheduler) addDependency(name string, deps []string) error {
	nodeID, ok := s.idLookup[name]
	if !ok {
		return fmt.Errorf("task %v not found", name)
	}

	for _, dep := range deps {
		depID, ok := s.idLookup[dep]
		if !ok {
			return fmt.Errorf("dependency %v not found", dep)
		}

		s.graph.SetEdge(s.graph.NewEdge(simple.Node(depID), simple.Node(nodeID)))
	}

	return nil
}

func (s *scheduler) run() {
	var wg sync.WaitGroup
	queue := make(chan int64, len(s.idLookup))

	// Not sure if I like wg.Add(1) before starting an actual goroutine
	enqueue := func(id int64) {
		wg.Add(1)
		queue <- id
	}

	finish := func(id int64) {
		successors := s.graph.From(id)
		for successors.Next() {
			succID := successors.Node().ID()
			parents := s.graph.To(succID)

			allDone := true
			for parents.Next() {
				parentTask, ok := s.taskLookup[parents.Node().ID()]
				if !ok {
					slog.Warn("Parent task not found in lookup", slog.Any("id", id))
					continue
				}

				if !parentTask.finished.Load() {
					allDone = false
					break
				}
			}

			if allDone {
				enqueue(succID)
			}
		}

		wg.Done() // this task is fully done (including successor enqueuing)
	}

	for _, id := range s.startIDs() {
		enqueue(id)
	}

	go func() {
		for id := range queue {
			task, ok := s.taskLookup[id]
			if !ok {
				slog.Warn("Task not found in lookup", slog.Any("id", id))
				wg.Done()
				continue
			}

			go func() {
				task.run()
				finish(id)
			}()
		}
	}()

	wg.Wait()
	close(queue)
}

// startIDs returns all ids of tasks that have no dependencies
func (s *scheduler) startIDs() []int64 {
	var result []int64

	nodes := s.graph.Nodes()
	for nodes.Next() {
		id := nodes.Node().ID()
		if s.graph.To(id).Len() == 0 { // no incoming edges
			result = append(result, id)
		}
	}

	return result
}

func (s *scheduler) dot() (string, error) {
	data, err := dot.Marshal(s.graph, "scheduler", "", "")
	if err != nil {
		return "", fmt.Errorf("failed to marshal graph: %w", err)
	}

	return string(data), nil
}

func (t *task) run() {
	slog.Info("Running Task", slog.String("name", t.Name))
	t.finished.Store(true)
}
