//go:build !windows

package main

import "os/exec"

func hideWindow(*exec.Cmd) {}

// killWithParent is a no-op elsewhere; on Linux use a process supervisor
// (systemd) or Pdeathsig if workers must not outlive laneq.
func killWithParent(*exec.Cmd) {}
