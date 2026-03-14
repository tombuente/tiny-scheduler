package main

import (
	"fmt"
	"log/slog"
	"os"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Fatal error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	tasks := []*task{
		{Name: "a", Dependencies: []string{"b", "c"}},
		{Name: "b"},
		{Name: "c"},
	}

	s, err := newScheduler(tasks)
	if err != nil {
		return fmt.Errorf("error creating scheduler: %w", err)
	}

	dot, err := s.dot()
	if err != nil {
		return fmt.Errorf("cannot create dot: %w", err)
	}

	fmt.Println(dot)

	s.run()

	return nil
}
