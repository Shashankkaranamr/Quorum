//go:build !windows

package supervisor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// processRuns reports whether pid is running the executable at bin. It can
// tell on Linux, through /proc; elsewhere known is false.
func processRuns(pid int, bin string) (same, known bool) {
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return false, false
	}
	return filepath.Clean(exe) == filepath.Clean(bin), true
}

// detached starts the child in a new session, away from the terminal that ran
// quorumctl, so it neither receives that terminal's signals nor dies with it.
func detached() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
