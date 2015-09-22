package pearson

import (
	"testing"
)

func TestHashes(t *testing.T) {

	s1 := "sample string to be hashed"
	s1h8e := byte(0)
	s1h64e := [8]byte{0, 0, 0, 0, 0, 0, 0, 0}
	s1h8 := byte(47)
	s1h64 := [8]byte{47, 40, 41, 42, 43, 36, 37, 38}

	s2 := "another sample string to be hashed"
	s2h8 := byte(206)
	s2h64 := [8]byte{206, 205, 204, 203, 202, 201, 200, 199}

	h8 := Hash8("")
	if h8 != s1h8e {
		t.Errorf("Incorrect hash calculation results: expected %v, found %v", s1h8e, h8)
	}

	h64 := Hash64("")
	if h64 != s1h64e {
		t.Errorf("Incorrect hash calculation results: expected %v, found %v", s1h64e, h64)
	}

	h8 = Hash8(s1)
	if h8 != s1h8 {
		t.Errorf("Incorrect hash calculation results: expected %v, found %v", s1h8, h8)
	}

	h8 = Hash8(s2)
	if h8 != s2h8 {
		t.Errorf("Incorrect hash calculation results: expected %v, found %v", s2h8, h8)
	}

	h64 = Hash64(s1)
	if h64 != s1h64 {
		t.Errorf("Incorrect hash calculation results: expected %v, found %v", s1h64, h64)
	}

	h64 = Hash64(s2)
	if h64 != s2h64 {
		t.Errorf("Incorrect hash calculation results: expected %v, found %v", s2h64, h64)
	}
}
