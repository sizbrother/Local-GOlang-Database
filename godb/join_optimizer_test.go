package godb

import (
	"testing"
)

func TestEstimateJoinCardinality(t *testing.T) {
	c := EstimateJoinCardinality(100, 200)
	if c < 0 {
		t.Errorf("expected positive cardinality, got %d", c)
	}
	if c > 100*200 {
		t.Errorf("expected cardinality less than 100*200, got %d", c)
	}

	c2 := EstimateJoinCardinality(100, 0)
	if c2 != 0 {
		t.Errorf("expected 0 cardinality, got %d", c2)
	}
	c2 = EstimateJoinCardinality(0, 100)
	if c2 != 0 {
		t.Errorf("expected 0 cardinality, got %d", c2)
	}

	c3 := EstimateJoinCardinality(100, 300)
	c4 := EstimateJoinCardinality(300, 200)
	if c3 < c || c4 < c {
		t.Errorf("larger joins should not have smaller cardinality")
	}
}

func TestEstimateJoinCost(t *testing.T) {
	c := EstimateJoinCost(100, 200, 10., 20.)
	if c < 0 {
		t.Errorf("expected positive cost, got %f", c)
	}
	c2 := EstimateJoinCost(200, 300, 20., 30.)
	if c2 < c {
		t.Errorf("larger joins should cost more")
	}
	c3 := EstimateJoinCost(50, 100, 5., 10.)
	if c3 > c {
		t.Errorf("smaller joins should cost less")
	}
}

type SimpleStats struct {
	card     int
	scanCost float64
}

func (s *SimpleStats) EstimateScanCost() float64 {
	return s.scanCost
}

func (s *SimpleStats) EstimateCardinality(sel float64) int {
	return int(float64(s.card) * sel)
}

func (s *SimpleStats) EstimateSelectivity(field string, op BoolOp, value DBValue) (float64, error) {
	return 1.0, nil
}

func TestOrderJoins(t *testing.T) {
	scale_factor := 100
	emp_stats := &SimpleStats{card: 100 * scale_factor, scanCost: float64(6 * scale_factor * 100)}
	dept_stats := &SimpleStats{card: scale_factor, scanCost: float64(3 * scale_factor)}
	hobby_stats := &SimpleStats{card: scale_factor, scanCost: float64(6 * scale_factor)}
	hobbies_stats := &SimpleStats{card: 200 * scale_factor, scanCost: float64(2 * scale_factor * 100)}

	join_nodes := []*JoinNode{
		{ // emp.c1 = dept.c0
			leftTable:  TableInfo{"emp", emp_stats, 0.1},
			leftField:  "c1",
			rightTable: TableInfo{"dept", dept_stats, 1.0},
			rightField: "c0",
		},
		{ // hobbies.c0 = emp.c2
			leftTable:  TableInfo{"hobbies", hobbies_stats, 1.0},
			leftField:  "c0",
			rightTable: TableInfo{"emp", emp_stats, 0.1},
			rightField: "c2",
		},
		{ // hobbies.c1 = hobby.c0
			leftTable:  TableInfo{"hobbies", hobbies_stats, 1.0},
			leftField:  "c1",
			rightTable: TableInfo{"hobby", hobby_stats, 1.0},
			rightField: "c0",
		},
	}

	ordered_join_nodes, err := OrderJoins(join_nodes)
	if err != nil {
		t.Fatalf(err.Error())
	}

	t.Logf("Ordered joins: %v", ordered_join_nodes)
	if len(ordered_join_nodes) != 3 {
		t.Fatalf("expected 3 joins, got %d", len(ordered_join_nodes))
	}
	// if ordered_join_nodes[0].leftTable.name == "hobbies" {
	// 	t.Errorf("largest table in the join should not be the outermost table")
	// }
	// if ordered_join_nodes[2].rightTable.name == "hobbies" && ordered_join_nodes[0].rightTable.name == "hobbies" {
	// 	t.Errorf("should not force cross join with largest table")
	// }
}
