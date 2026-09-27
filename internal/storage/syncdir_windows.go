//go:build windows

package storage

// syncDir is a no-op on Windows, and reports that it did nothing.
//
// Windows has no directory fsync: FlushFileBuffers on a directory handle fails
// with ERROR_ACCESS_DENIED. NTFS journals metadata, including the creation of a
// directory entry, and FlushFileBuffers on the new file itself commits that
// file's metadata along with its data -- which the WAL does on the first Sync
// after creating a segment. This is the platform's documented behaviour rather
// than something Quorum can verify, and DESIGN.md §2 says so.
func syncDir(string) (bool, error) { return false, nil }
