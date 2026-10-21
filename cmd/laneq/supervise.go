package main

import (
	"context"
	"io"
	"log"
	"net"
	"os/exec"
	"time"
)

func listen(addr string) (net.Listener, error) { return net.Listen("tcp", addr) }

// supervise runs cmd until ctx is done, restarting it when it exits. The delay
// doubles after each quick crash (up to a minute) and resets once the process
// has stayed up for a minute.
func supervise(ctx context.Context, cmd []string, out io.Writer) {
	delay := time.Second
	for ctx.Err() == nil {
		c := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
		c.Stdout, c.Stderr = out, out
		hideWindow(c)
		started := time.Now()
		log.Printf("worker: starting %q", cmd)
		err := c.Run()
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			delay = time.Second
		}
		log.Printf("worker: exited (%v); restarting in %s", err, delay)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, time.Minute)
	}
}
