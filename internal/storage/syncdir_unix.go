//go:build !windows

package storage

import "os"

// syncDir fsyncs a directory, which is what makes a file created in it survive
// an OS crash. On Linux a new file's directory entry is not durable until its
// directory is fsynced, whatever was done to the file itself.
func syncDir(dir string) (bool, error) {
	d, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return false, syncErr
	}
	return true, closeErr
}
