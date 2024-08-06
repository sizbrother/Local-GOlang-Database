package godb

import (
	"testing"
)

func TestHeapPageBeforeImage(t *testing.T) {
	bp, c, err := MakeTestDatabase(10, "catalog.txt")
	f, err := c.GetTable("t")
	if err != nil {
		t.Error(err)
	}
	tid := NewTID()
	bp.BeginTransaction(tid)

	if err := f.insertTuple(&Tuple{Desc: *f.Descriptor(), Fields: []DBValue{StringField{"sam"}, IntField{1}}}, tid); err != nil {
		t.Error(err)
	}

	pg, err := bp.GetPage(f, 0, tid, WritePerm)
	if err != nil {
		t.Error(err)
	}
	pg.(*heapPage).SetBeforeImage()
	_, err = pg.(*heapPage).insertTuple(&Tuple{Desc: *f.Descriptor(), Fields: []DBValue{StringField{"sam"}, IntField{2}}})
	if err != nil {
		t.Error(err)
	}

	pg2 := pg.(*heapPage).BeforeImage()
	iter := pg2.(*heapPage).tupleIter()
	tup, err := iter()
	if tup == nil {
		t.Error("expected a tuple")
	}
	if tup.Fields[1].(IntField).Value != 1 {
		t.Errorf("expected 1, got %d", tup.Fields[1].(IntField).Value)
	}
	tup, err = iter()
	if tup != nil {
		t.Error("expected only one tuple")
	}
}
