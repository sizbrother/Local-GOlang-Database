package godb

import (
	"runtime"
	"testing"
)

func TestIntHistogramGrowth(t *testing.T) {
	h, err := NewIntHistogram(10000, 0, 100)
	if err != nil {
		t.Fatalf("Failed to create histogram: %v", err)
	}

	var start, end runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&start)
	for c := int64(0); c < 33554432; c++ {
		h.AddValue((c * 23) % 101)
	}
	runtime.ReadMemStats(&end)

	memUsed := end.HeapAlloc - start.HeapAlloc
	t.Logf("Memory usage: %v", memUsed)
	if memUsed > 100000 {
		t.Fatalf("Memory allocations should be effectively zero. got %v. Are you storing every value?", memUsed)
	}

	selectivity := 0.0
	for c := int64(0); c < 101; c++ {
		selectivity += h.EstimateSelectivity(OpEq, c)
	}

	if selectivity < 0.99 {
		t.Fatalf("Total selectivity should be close to 1. got %v", selectivity)
	}
}

func TestIntHistogramNegativeRange(t *testing.T) {
	h, err := NewIntHistogram(10, -60, -10)
	if err != nil {
		t.Fatalf("Failed to create histogram: %v", err)
	}

	// All of the values here are negative.
	// Also, there are more of them than there are bins.
	for c := int64(-60); c <= -10; c++ {
		h.AddValue(c)
		h.EstimateSelectivity(OpEq, c)
	}

	t.Logf("Histogram: %v", h)

	sel := h.EstimateSelectivity(OpEq, -33)
	if sel >= 0.3 {
		t.Fatalf("Selectivity for this particular value should be at most 0.2. got %v", h.EstimateSelectivity(OpEq, -33))
	}
	if sel <= 0.001 {
		t.Fatalf("Selectivity for this particular value should be at least 0.02. got %v", h.EstimateSelectivity(OpEq, -33))
	}
}

func TestIntHistogramEquals(t *testing.T) {
	h, err := NewIntHistogram(10, 1, 10)
	if err != nil {
		t.Fatalf("Failed to create histogram: %v", err)
	}

	h.AddValue(3)
	h.AddValue(3)
	h.AddValue(3)

	if h.EstimateSelectivity(OpEq, 3) < 0.8 {
		t.Fatalf("Selectivity for this particular value should be near 1. got %v", h.EstimateSelectivity(OpEq, 3))
	}
	if h.EstimateSelectivity(OpEq, 8) > 0.001 {
		t.Fatalf("Selectivity for this particular value should be near 0. got %v", h.EstimateSelectivity(OpEq, 8))
	}
}

func buildTestHistogram(t *testing.T) *IntHistogram {
	h, err := NewIntHistogram(10, 1, 10)
	if err != nil {
		t.Fatalf("Failed to create histogram: %v", err)
	}

	h.AddValue(3)
	h.AddValue(3)
	h.AddValue(3)
	h.AddValue(1)
	h.AddValue(10)

	return h
}

func TestIntHistogramGreaterThan(t *testing.T) {
	h := buildTestHistogram(t)
	if h.EstimateSelectivity(OpGt, -1) <= 0.999 {
		t.Fatalf("Selectivity for this particular value should be near 1. got %v", h.EstimateSelectivity(OpGt, -1))
	}
	if h.EstimateSelectivity(OpGt, 2) <= 0.6 {
		t.Fatalf("Selectivity for this particular value should be near 0.8. got %v", h.EstimateSelectivity(OpGt, 2))
	}
	if h.EstimateSelectivity(OpGt, 4) >= 0.4 {
		t.Fatalf("Selectivity for this particular value should be near 0.2. got %v", h.EstimateSelectivity(OpGt, 4))
	}
	if h.EstimateSelectivity(OpGt, 12) >= 0.001 {
		t.Fatalf("Selectivity for this particular value should be near 0. got %v", h.EstimateSelectivity(OpGt, 12))
	}
}

func TestIntHistogramLessThan(t *testing.T) {
	h := buildTestHistogram(t)
	if h.EstimateSelectivity(OpLt, -1) >= 0.001 {
		t.Fatalf("Selectivity for this particular value should be near 0. got %v", h.EstimateSelectivity(OpLt, -1))
	}
	if h.EstimateSelectivity(OpLt, 2) >= 0.4 {
		t.Fatalf("Selectivity for this particular value should be near 0.2. got %v", h.EstimateSelectivity(OpLt, 2))
	}
	if h.EstimateSelectivity(OpLt, 4) <= 0.6 {
		t.Fatalf("Selectivity for this particular value should be near 0.8. got %v", h.EstimateSelectivity(OpLt, 4))
	}
	if h.EstimateSelectivity(OpLt, 12) <= 0.999 {
		t.Fatalf("Selectivity for this particular value should be near 1. got %v", h.EstimateSelectivity(OpLt, 12))
	}
}

func TestIntHistogramGreaterThanOrEquals(t *testing.T) {
	h := buildTestHistogram(t)
	if h.EstimateSelectivity(OpGe, -1) <= 0.999 {
		t.Fatalf("Selectivity for this particular value should be near 1. got %v", h.EstimateSelectivity(OpGe, -1))
	}
	if h.EstimateSelectivity(OpGe, 2) <= 0.6 {
		t.Fatalf("Selectivity for this particular value should be near 0.8. got %v", h.EstimateSelectivity(OpGe, 2))
	}
	if h.EstimateSelectivity(OpGe, 3) <= 0.45 {
		t.Fatalf("Selectivity for this particular value should be near 0.55. got %v", h.EstimateSelectivity(OpGe, 3))
	}
	if h.EstimateSelectivity(OpGe, 4) >= 0.5 {
		t.Fatalf("Selectivity for this particular value should be near 0.4. got %v", h.EstimateSelectivity(OpGe, 4))
	}
	if h.EstimateSelectivity(OpGe, 12) >= 0.001 {
		t.Fatalf("Selectivity for this particular value should be near 0. got %v", h.EstimateSelectivity(OpGe, 12))
	}
}

func TestIntHistogramLessThanOrEquals(t *testing.T) {
	h := buildTestHistogram(t)
	if h.EstimateSelectivity(OpLe, -1) >= 0.001 {
		t.Fatalf("Selectivity for this particular value should be near 0. got %v", h.EstimateSelectivity(OpLe, -1))
	}
	if h.EstimateSelectivity(OpLe, 2) >= 0.4 {
		t.Fatalf("Selectivity for this particular value should be near 0.2. got %v", h.EstimateSelectivity(OpLe, 2))
	}
	if h.EstimateSelectivity(OpLe, 3) <= 0.45 {
		t.Fatalf("Selectivity for this particular value should be near 0.8. got %v", h.EstimateSelectivity(OpLe, 3))
	}
	if h.EstimateSelectivity(OpLe, 4) <= 0.6 {
		t.Fatalf("Selectivity for this particular value should be near 0.8. got %v", h.EstimateSelectivity(OpLe, 4))
	}
	if h.EstimateSelectivity(OpLe, 12) <= 0.999 {
		t.Fatalf("Selectivity for this particular value should be near 1. got %v", h.EstimateSelectivity(OpLe, 12))
	}
}

func TestIntHistogramNotEquals(t *testing.T) {
	h, err := NewIntHistogram(10, 1, 10)
	if err != nil {
		t.Fatalf("Failed to create histogram: %v", err)
	}

	h.AddValue(3)
	h.AddValue(3)
	h.AddValue(3)

	if h.EstimateSelectivity(OpNeq, 3) >= 0.001 {
		t.Fatalf("Selectivity for this particular value should be near 0. got %v", h.EstimateSelectivity(OpNeq, 3))
	}
	if h.EstimateSelectivity(OpNeq, 8) <= 0.01 {
		t.Fatalf("Selectivity for this particular value should be near 1. got %v", h.EstimateSelectivity(OpNeq, 8))
	}
}
