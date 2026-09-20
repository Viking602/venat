//go:build !darwin && !linux

package kit

import "os/exec"

// A child-only kill cannot establish termination of its process tree. Preserve
// the fatal timeout boundary until an equivalent platform mechanism is supplied.
func configureProcessCancellation(_ *exec.Cmd) bool { return false }
