package datastore

import (
	"encoding/binary"
	"encoding/hex"
	"time"
)

// sprintf("%04d", i)
func ToBigEndianString(i int) string {
	a := make([]byte, 2)
	binary.BigEndian.PutUint16(a, uint16(i))
	return hex.EncodeToString(a)
}

// nextTimeBoundary returns the time when the currently open time window closes.
func nextTimeBoundary(baseTime time.Time, windowSize time.Duration) time.Time {
	// This will round down before the halfway point.
	b := baseTime.Round(windowSize)
	if b.Before(baseTime) {
		// It was rounded down, adjust up to next boundary.
		b = b.Add(windowSize)
	}
	return b
}
