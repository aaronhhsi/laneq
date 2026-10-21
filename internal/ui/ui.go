// Package ui serves the dashboard: a single page that talks to the JSON API.
package ui

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var page []byte

// Handler serves the dashboard page.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Write(page)
	})
}
