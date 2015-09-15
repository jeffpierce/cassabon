package middleware

import (
	"encoding/binary"
	"encoding/hex"
)

// sprintf("%04d", i)
func ToBigEndianString(i int) string {
	a := make([]byte, 2)
	binary.BigEndian.PutUint16(a, uint16(i))
	return hex.EncodeToString(a)
}
