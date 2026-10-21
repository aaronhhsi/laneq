package main

import (
	"os/exec"
	"syscall"
)

// hideWindow keeps a console worker (and anything it runs, like ffmpeg) from
// opening a window when laneq itself runs without one.
func hideWindow(c *exec.Cmd) {
	const createNoWindow = 0x08000000
	c.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: createNoWindow}
}
