package chstore

// SetLogChunkSize overrides the minimum chunk size (default 1000) for tests.
func SetLogChunkSize(n int) { logChunkSize = n }

// CheckFreeSpace exposes checkFreeSpace to the external tests.
var CheckFreeSpace = checkFreeSpace
