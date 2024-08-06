package godb

import (
	"testing"
)

func TestStringHistogram(t *testing.T) {
	h, err := NewStringHistogram()
	if err != nil {
		t.Fatalf("Failed to create histogram: %v", err)
	}

	for c := 0; c < 100; c++ {
		h.AddValue("test")
		h.AddValue("sam")
		h.AddValue("joe")
		h.AddValue("bill")
	}

	s := h.EstimateSelectivity(OpEq, "test")
	if s < 0.2 || s > 0.3 {
		t.Fatalf("Total selectivity should be close to 0.25. got %v", s)
	}
}
