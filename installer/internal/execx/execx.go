// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package execx streams the output of external commands to the TUI. This is
// the seam the onboarding prototype lacked: the same screens run either the
// real installer commands or, under --dry-run, a scripted replay.
package execx

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Spec describes one external command.
type Spec struct {
	// Label is a short name for progress display.
	Label string
	// Display is the human-readable command line shown to the user.
	Display string
	// Dir is the working directory.
	Dir string
	// Argv is the command and its arguments.
	Argv []string
	// Env holds extra KEY=VALUE entries layered over the process environment.
	Env []string
	// SimLines is what a --dry-run replays instead of executing Argv.
	SimLines []string
}

// Event is one unit of command progress.
type Event struct {
	// Line is one line of combined stdout/stderr output, ANSI-stripped.
	Line string
	// Done marks the final event; Err is the command error, if any.
	Done bool
	Err  error
}

// Runner starts commands and streams their output.
type Runner interface {
	Start(ctx context.Context, spec Spec) <-chan Event
}

var ansi = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

const maxLineLen = 2000

// clean strips ANSI color codes and truncates pathological lines.
func clean(line string) string {
	line = ansi.ReplaceAllString(line, "")
	line = strings.TrimRight(line, "\r\n")
	if len(line) > maxLineLen {
		line = line[:maxLineLen] + "…"
	}
	return line
}

// Real executes commands with os/exec.
type Real struct {
	Log *Logger
}

// Start runs the command and streams its combined output line by line. The
// channel is closed after the Done event.
func (r Real) Start(ctx context.Context, spec Spec) <-chan Event {
	ch := make(chan Event, 64)
	go func() {
		defer close(ch)

		start := time.Now()
		if r.Log != nil {
			r.Log.LogCommandStart(spec)
		}

		cmd := exec.CommandContext(ctx, spec.Argv[0], spec.Argv[1:]...)
		cmd.Dir = spec.Dir
		cmd.Env = append(os.Environ(), spec.Env...)

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			if r.Log != nil {
				r.Log.LogCommandEnd(spec, err, time.Since(start))
			}
			ch <- Event{Done: true, Err: err}
			return
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			if r.Log != nil {
				r.Log.LogCommandEnd(spec, err, time.Since(start))
			}
			ch <- Event{Done: true, Err: err}
			return
		}
		if err := cmd.Start(); err != nil {
			if r.Log != nil {
				r.Log.LogCommandEnd(spec, err, time.Since(start))
			}
			ch <- Event{Done: true, Err: err}
			return
		}

		var wg sync.WaitGroup
		scan := func(rd io.Reader) {
			defer wg.Done()
			sc := bufio.NewScanner(rd)
			sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
			for sc.Scan() {
				raw := sc.Text()
				if r.Log != nil {
					r.Log.LogLine(raw)
				}
				if line := clean(raw); line != "" {
					ch <- Event{Line: line}
				}
			}
		}
		wg.Add(2)
		go scan(stdout)
		go scan(stderr)
		wg.Wait()

		waitErr := cmd.Wait()
		if r.Log != nil {
			r.Log.LogCommandEnd(spec, waitErr, time.Since(start))
		}
		ch <- Event{Done: true, Err: waitErr}
	}()
	return ch
}

// DryRun replays a Spec's SimLines with a small delay instead of executing.
type DryRun struct {
	// Delay between replayed lines; defaults to 250ms.
	Delay time.Duration
	Log   *Logger
}

// Start replays spec.SimLines and finishes successfully.
func (d DryRun) Start(ctx context.Context, spec Spec) <-chan Event {
	delay := d.Delay
	if delay == 0 {
		delay = 250 * time.Millisecond
	}
	ch := make(chan Event, 64)
	go func() {
		defer close(ch)
		start := time.Now()
		if d.Log != nil {
			d.Log.LogCommandStart(spec)
		}
		ch <- Event{Line: "(dry-run) " + spec.Display}
		if d.Log != nil {
			d.Log.LogLine("(dry-run) " + spec.Display)
		}
		for _, line := range spec.SimLines {
			select {
			case <-ctx.Done():
				if d.Log != nil {
					d.Log.LogCommandEnd(spec, ctx.Err(), time.Since(start))
				}
				ch <- Event{Done: true, Err: ctx.Err()}
				return
			case <-time.After(delay):
			}
			if d.Log != nil {
				d.Log.LogLine(line)
			}
			ch <- Event{Line: line}
		}
		if d.Log != nil {
			d.Log.LogCommandEnd(spec, nil, time.Since(start))
		}
		ch <- Event{Done: true}
	}()
	return ch
}
