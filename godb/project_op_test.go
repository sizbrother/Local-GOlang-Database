package godb

import (
	"testing"
)

func TestProject(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	//fs := make([]FieldType, 1)
	//fs[0] = t1.Desc.Fields[0]
	var outNames []string = make([]string, 1)
	outNames[0] = "outf"
	fieldExpr := FieldExpr{t1.Desc.Fields[0]}
	proj, _ := NewProjectOp([]Expr{&fieldExpr}, outNames, false, hf)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(tup.Fields) != 1 || tup.Desc.Fields[0].Fname != "outf" {
		t.Errorf("invalid output tuple")
	}

}

func TestProjectDistinctOptional(t *testing.T) {
	_, t1, t2, hf, _, tid := makeTestVars(t)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t2, tid)

	//fs := make([]FieldType, 1)
	//fs[0] = t1.Desc.Fields[0]
	var outNames []string = make([]string, 1)
	outNames[0] = "outf"
	fieldExpr := FieldExpr{t1.Desc.Fields[0]}
	proj, _ := NewProjectOp([]Expr{&fieldExpr}, outNames, true, hf)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	cnt := 0
	for {
		tup, err := iter()
		if err != nil {
			t.Fatalf(err.Error())
		}
		if tup == nil {
			break
		}
		cnt = cnt + 1
	}
	if cnt != 2 {
		t.Errorf("expected two names, got %d", cnt)

	}
}

func TestProjectOrdering(t *testing.T) {
	hf, tup, td, bp := makeOrderByOrderingVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	hf.insertTuple(&tup, tid)

	var outNames = []string{"out1", "out2"}
	exprs := []Expr{&FieldExpr{td.Fields[2]}, &FieldExpr{td.Fields[0]}}

	proj, _ := NewProjectOp(exprs, outNames, false, hf)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}

	tupOut, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	var expectedDesc = TupleDesc{Fields: []FieldType{
		{Fname: "out1", Ftype: IntType},
		{Fname: "out2", Ftype: StringType},
	}}

	if !expectedDesc.equals(&tupOut.Desc) {
		t.Fatalf("Unexpected descriptor of projected tuple")
	}
}
