package godb

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

var MAX_RAND_VALUE = 1 << 16

func CreateRandomHeapFile(t *testing.T, bp *BufferPool, c *Catalog, rows int, name string, columns []string) {
	td := TupleDesc{
		Fields: make([]FieldType, len(columns)),
	}
	for i, col := range columns {
		td.Fields[i] = FieldType{
			Fname:          col,
			TableQualifier: "",
			Ftype:          IntType,
		}
	}

	heapFileName := fmt.Sprintf("%s.dat", name)
	os.Remove(heapFileName)
	hf, err := c.addTable(name, td)
	if err != nil {
		t.Fatalf(err.Error())
	}

	tid := NewTID()
	err = bp.BeginTransaction(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	for i := 0; i < rows; i++ {
		fields := make([]DBValue, len(columns))
		for j := 0; j < len(fields); j++ {
			fields[j] = IntField{int64(rand.Intn(MAX_RAND_VALUE))}
		}

		tup := Tuple{
			Desc:   td,
			Fields: fields,
		}
		err := hf.insertTuple(&tup, tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	bp.FlushAllPages()
	bp.CommitTransaction(tid)
}

func runQuery(t *testing.T, bp *BufferPool, op Operator) time.Duration {
	now := time.Now()
	tid := NewTID()
	err := bp.BeginTransaction(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, err := op.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for {
		tup, err := iter()
		if err != nil {
			t.Fatalf(err.Error())
		}
		if tup == nil {
			break
		}
	}
	bp.CommitTransaction(tid)

	dur := time.Since(now)
	return dur
}

func runQueryAvg(t *testing.T, bp *BufferPool, op Operator) time.Duration {
	var total time.Duration
	start := time.Now()
	n := 0
	for time.Since(start) < time.Second {
		total += runQuery(t, bp, op)
		n++
	}
	return time.Duration(int64(total) / int64(n))
}

func TestQueryOpt(t *testing.T) {
	defer func() {
		EnableJoinOptimization = true
	}()

	bp, c, err := MakeTestDatabase(1000000, "catalog.txt")
	if err != nil {
		t.Fatalf(err.Error())
	}

	scaleFactor := 10
	CreateRandomHeapFile(t, bp, c, 100*scaleFactor, "emp", []string{"c0", "c1", "c2", "c3", "c4", "c5"})
	CreateRandomHeapFile(t, bp, c, scaleFactor, "dept", []string{"c0", "c1", "c2"})
	CreateRandomHeapFile(t, bp, c, scaleFactor, "hobby", []string{"c0", "c1", "c2", "c3", "c4", "c5"})
	CreateRandomHeapFile(t, bp, c, 200*scaleFactor, "hobbies", []string{"c0", "c1"})
	if err := c.ComputeTableStats(); err != nil {
		t.Fatalf(err.Error())
	}

	query := "select * from hobbies,hobby,emp,dept where hobbies.c0 = emp.c2 and hobbies.c1 = hobby.c0 and emp.c1 = dept.c0 and emp.c3 < 10000"

	EnableJoinOptimization = true
	t.Logf("Running query with optimization")
	_, op, err := Parse(c, query)
	if err != nil {
		t.Fatalf(err.Error())
	}

	t.Logf("Physical Plan:")
	OutputPhysicalPlan(t.Logf, op, "")
	withOpt := runQueryAvg(t, bp, op)

	EnableJoinOptimization = false
	t.Logf("Running query without optimization")
	_, op, err = Parse(c, query)
	if err != nil {
		t.Fatalf(err.Error())
	}

	t.Logf("Physical Plan:")
	OutputPhysicalPlan(t.Logf, op, "")
	withoutOpt := runQueryAvg(t, bp, op)

	t.Logf("With optimization: %v, without: %v", withOpt, withoutOpt)
	if withOpt > withoutOpt {
		t.Errorf("Optimized query took longer than unoptimized query")
	}
}

func TestLargeOpt(t *testing.T) {
	bp, c, err := MakeTestDatabase(1000000, "query_test_catalog.txt")
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	scaleFactor := 100
	for i := 1; i <= 15; i++ {
		CreateRandomHeapFile(t, bp, c, scaleFactor-i, fmt.Sprintf("t%d", i), []string{"f"})
	}
	c.ComputeTableStats()

	// Note that this timeout is generous. The test should run in less than 0.1 second.
	timeout := time.After(1 * time.Second)
	done := make(chan bool)
	go func() {
		_, op, err := Parse(c, "select * from t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15 where t1.f = t2.f and t2.f = t3.f and t3.f = t4.f and t4.f = t5.f and t5.f = t6.f and t6.f = t7.f and t7.f = t8.f and t8.f = t9.f and t9.f = t10.f and t10.f = t11.f and t11.f = t12.f and t12.f = t13.f and t13.f = t14.f and t14.f = t15.f")
		if err != nil {
			t.Errorf(err.Error())
			return
		}

		done <- true
		t.Logf("Physical Plan:")
		OutputPhysicalPlan(t.Logf, op, "")
	}()

	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}
}
