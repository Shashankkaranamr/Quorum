// Package web holds the visualizer's frontend: plain HTML, CSS and JavaScript,
// embedded into the quorum-viz binary. There is no build step and no npm; the
// page is small enough that a toolchain would cost more than it returns.
package web

import "embed"

// Assets is the frontend, served at the visualizer's root.
//
//go:embed index.html app.js style.css
var Assets embed.FS
