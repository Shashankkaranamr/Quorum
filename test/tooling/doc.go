// Package tooling holds tests about the repository's own build tooling rather
// than about Quorum itself.
//
// It exists because the project ships two task runners -- a Makefile and a
// PowerShell mirror -- so that a reviewer on any platform can build it. Two
// files describing the same commands is a drift hazard, and the drift would
// show up as "the build works on my machine", which is precisely the thing a
// portfolio project cannot afford.
package tooling
