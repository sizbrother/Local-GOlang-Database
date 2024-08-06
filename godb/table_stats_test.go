package godb

import (
	"os"
	"testing"
)

func setupTableStatsTest(t *testing.T) *TableStats {
	bp, c, err := MakeTestDatabase(100, "catalog.txt")
	if err != nil {
		t.Fatalf(err.Error())
	}

	os.Remove("test_heap_file.dat")
	hf, err := c.addTable("test_heap_file", TupleDesc{Fields: []FieldType{{Fname: "name", Ftype: StringType}, {Fname: "age", Ftype: IntType}}})
	if err != nil {
		t.Fatalf(err.Error())
	}

	file, err := os.Open("test_heap_file.csv")
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = hf.(*HeapFile).LoadFromCSV(file, true, ",", false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ts, err := ComputeTableStats(bp, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return ts
}

func TestTableStatsCard(t *testing.T) {
	ts := setupTableStatsTest(t)
	card := ts.EstimateCardinality(0.2)
	if card > 80 || card < 70 {
		t.Errorf("Expected 77, got %d", card)
	}
}

func TestTableStatsCost(t *testing.T) {
	ts := setupTableStatsTest(t)
	cost := ts.EstimateScanCost()
	if cost != 4000.0 {
		t.Errorf("Expected 4000.0, got %f", cost)
	}
}

func TestTableStatsSelectivity(t *testing.T) {
	ts := setupTableStatsTest(t)

	s, err := ts.EstimateSelectivity("name", OpEq, StringField{"joe"})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if s < 0.2 || s > 0.4 {
		t.Errorf("Expected 0.3, got %f", s)
	}

	s, err = ts.EstimateSelectivity("age", OpEq, IntField{10})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if s < 0.2 || s > 0.4 {
		t.Errorf("Expected 0.3, got %f", s)
	}

	s, err = ts.EstimateSelectivity("age", OpLe, IntField{20})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if s < 0.5 || s > 0.7 {
		t.Errorf("Expected 0.66, got %f", s)
	}

	s, err = ts.EstimateSelectivity("age", OpLt, IntField{20})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if s < 0.2 || s > 0.4 {
		t.Errorf("Expected 0.3, got %f", s)
	}

	s, err = ts.EstimateSelectivity("age", OpGe, IntField{10})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if s < 0.9 {
		t.Errorf("Expected 1.0, got %f", s)
	}

	s, err = ts.EstimateSelectivity("age", OpGt, IntField{10})
	if err != nil {
		t.Fatalf(err.Error())
	}
	if s < 0.5 || s > 0.7 {
		t.Errorf("Expected 0.66, got %f", s)
	}
}
