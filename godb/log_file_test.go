package godb

import (
	"io"
	"math"
	"os"
	"testing"
)

func TestLogEvict(t *testing.T) {
	bp, c, err := MakeTestDatabase(1, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err := c.GetTable("t")
	if err != nil {
		t.Error(err)
	}

	// create two pages of tuples
	tid1 := NewTID()
	if err := bp.BeginTransaction(tid1); err != nil {
		t.Error(err)
	}
	for hf.NumPages() < 2 {
		if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{IntField{1}}}, tid1); err != nil {
			t.Error(err)
		}
	}
	bp.CommitTransaction(tid1)

	// "write" to the first page (actually just mark it dirty)
	tid2 := NewTID()
	if err := bp.BeginTransaction(tid2); err != nil {
		t.Error(err)
	}
	pg, err := bp.GetPage(hf, 0, tid2, WritePerm)
	if err != nil {
		t.Error(err)
	}
	pg.setDirty(tid2, true)

	// read from the second page. this should cause the first page (now dirty) to flush
	tid3 := NewTID()
	if err := bp.BeginTransaction(tid3); err != nil {
		t.Error(err)
	}
	if _, err = bp.GetPage(hf, 1, tid3, ReadPerm); err != nil {
		t.Error(err)
	}

	// evicting the first page should have caused a log entry to be written
	logFile := bp.LogFile()

	if err := logFile.seek(0, io.SeekStart); err != nil {
		t.Error(err)
	}

	iter := logFile.ForwardIterator()
	records := make([]LogRecord, 0)
	record, err := iter()
	for record != nil && err == nil {
		records = append(records, record)
		record, err = iter()
	}
	if err != nil {
		t.Error(err)
	}

	nUpdateTid1 := 0
	nUpdateTid2 := 0
	for _, record := range records {
		if record.Type() == UpdateRecord {
			if record.Tid() == tid1 {
				nUpdateTid1++
			} else if record.Tid() == tid2 {
				nUpdateTid2++
			}
		}
	}

	if nUpdateTid1 != 2 || nUpdateTid2 != 1 {
		t.Errorf("expected 2 updates for tid1 and 1 update for tid2, got %d and %d", nUpdateTid1, nUpdateTid2)
	}
}

// Tests that recovery correctly replays the log, undoes uncommitted changes,
// and restores committed changes.
//
// Creates a table with 10 pages of tuples. Starts 10 transactions, which delete
// the tuples on the corresponding page. Even-numbered transactions commit,
// odd-numbered transactions do not.
//
// After recovery, we should have 5 pages of tuples.
func TestLogRecoverUndoRedo(t *testing.T) {
	const nXactions = 10 // number of transactions to run

	// limiting the buffer pool size forces these changes to be flushed to disk
	bp, c, err := MakeTestDatabase(1, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err := c.GetTable("t")
	if err != nil {
		t.Error(err)
	}

	// insert a page of tuples for each transaction. each transaction should
	// have tuples on separate pages.
	tuplesPerPage := PageSize / hf.Descriptor().bytesPerTuple()
	tid := NewTID()
	if err := bp.BeginTransaction(tid); err != nil {
		t.Error(err)
	}
	for x := 0; x < nXactions; x++ {
		for i := 0; i < tuplesPerPage; i++ {
			if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{StringField{"sam"}, IntField{int64(x)}}}, tid); err != nil {
				t.Error(err)
			}
		}
	}
	bp.CommitTransaction(tid)

	if hf.NumPages() != nXactions {
		t.Fatalf("expected %d pages, got %d", nXactions, hf.NumPages())
	}

	// collect the tuples so we can pass them to deleteTuple
	tid = NewTID()
	bp.BeginTransaction(tid)
	tuples := make([]Tuple, 0)
	iter, err := hf.Iterator(tid)
	if err != nil {
		t.Error(err)
	}
	tup, err := iter()
	for tup != nil && err == nil {
		tuples = append(tuples, *tup)
		tup, err = iter()
	}
	if err != nil {
		t.Error(err)
	}
	bp.CommitTransaction(tid)

	expectedTuples := tuplesPerPage * nXactions
	if len(tuples) != expectedTuples {
		t.Fatalf("expected %d tuples, got %d", expectedTuples, len(tuples))
	}

	for x := 0; x < nXactions; x++ {
		tid := NewTID()
		bp.BeginTransaction(tid)
		for _, tup := range tuples {
			if tup.Fields[1].(IntField).Value == int64(x) {
				if err := hf.deleteTuple(&tup, tid); err != nil {
					t.Error(err)
				}
			}
		}
		if x%2 == 0 {
			bp.CommitTransaction(tid)
		}
	}

	// bp.logFile.OutputPrettyLog()

	// reopen the db without first flushing the buffer pool
	bp, c, err = RecoverTestDatabase(100, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err = c.GetTable("t")
	if err != nil {
		t.Error(err)
	}

	tid = NewTID()

	bp.BeginTransaction(tid)
	iter, err = hf.Iterator(tid)
	if err != nil {
		t.Error(err)
	}

	i := 0
	tup, err = iter()
	for tup != nil && err == nil {
		if tup.Fields[0].(StringField).Value != "sam" {
			t.Fatalf("unexpected tuple: %#v", tup)
		}
		if tup.Fields[1].(IntField).Value%2 == 0 {
			t.Fatalf("saw tuple that should have been deleted by a committed transaction: %#v", tup)
		}
		i++
		tup, err = iter()
	}
	if err != nil {
		t.Error(err)
	}

	expectedRemainingTuples := tuplesPerPage * nXactions / 2
	if i != expectedRemainingTuples {
		t.Fatalf("expected %d tuples, got %d", expectedRemainingTuples, i)
	}

	bp.CommitTransaction(tid)
}

// Tests that recovery correctly replays the log and undoes uncommitted changes.
func TestLogRecoverUndo(t *testing.T) {
	// limiting the buffer pool size forces these changes to be flushed to disk
	bp, c, err := MakeTestDatabase(1, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err := c.GetTable("t")
	if err != nil {
		t.Error(err)
	}

	tid := NewTID()
	if err := bp.BeginTransaction(tid); err != nil {
		t.Error(err)
	}
	for i := 0; i < 1000; i++ {
		if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{StringField{"sam"}, IntField{int64(i)}}}, tid); err != nil {
			t.Error(err)
		}
	}
	// don't commit the transaction, so the changes should be undone during
	// recovery

	info, err := os.Stat(bp.LogFile().file.Name())
	if err != nil {
		t.Error(err)
	}
	if info.Size() < int64(PageSize*2*9) {
		t.Fatalf("Log file too small. Expected at least 9 update records. Got: %d bytes. Are you writing update records when evicting pages?", info.Size())
	}

	// reopen the db without first flushing the buffer pool
	bp, c, err = RecoverTestDatabase(100, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err = c.GetTable("t")
	if err != nil {
		t.Error(err)
	}

	tid = NewTID()
	bp.BeginTransaction(tid)
	iter, err := hf.Iterator(tid)
	if err != nil {
		t.Error(err)
	}

	tup, err := iter()
	if tup != nil && err == nil {
		t.Fatalf("unexpected tuple: %#v", tup)
	}
	if err != nil {
		t.Error(err)
	}
	bp.CommitTransaction(tid)
}

// Tests that recovery correctly replays the log and restores committed changes.
func TestLogRecoverRedo(t *testing.T) {
	bp, c, err := MakeTestDatabase(100, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err := c.GetTable("t")
	if err != nil {
		t.Error(err)
	}

	tid := NewTID()
	if err := bp.BeginTransaction(tid); err != nil {
		t.Error(err)
	}
	for i := 0; i < 1000; i++ {
		if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{StringField{"sam"}, IntField{int64(i)}}}, tid); err != nil {
			t.Error(err)
		}
	}
	bp.CommitTransaction(tid)

	info, err := os.Stat(bp.LogFile().file.Name())
	if err != nil {
		t.Error(err)
	}
	if info.Size() < int64(PageSize*2*10) {
		t.Fatalf("log file too small, expected at least 10 update records, got: %d bytes", info.Size())
	}

	// reopen the db without first flushing the buffer pool
	bp, c, err = RecoverTestDatabase(100, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err = c.GetTable("t")
	if err != nil {
		t.Error(err)
	}

	tid = NewTID()
	bp.BeginTransaction(tid)
	iter, err := hf.Iterator(tid)
	if err != nil {
		t.Error(err)
	}

	tup, err := iter()
	i := 0
	for tup != nil && err == nil {
		if tup.Fields[0].(StringField).Value != "sam" && tup.Fields[1].(IntField).Value != int64(i) {
			t.Fatalf("unexpected tuple: %#v", tup)
		}
		i++
		tup, err = iter()
	}
	if err != nil {
		t.Error(err)
	}
	if i != 1000 {
		t.Fatalf("expected 1000 tuples, got %d", i)
	}
	bp.CommitTransaction(tid)
}

// Tests that flushed changes are rolled back when a transaction is aborted.
func TestLogRollbackFlushed(t *testing.T) {
	const fileName = "log_rollback_flushed.dat"
	os.Remove(fileName)

	bp, c, err := MakeTestDatabase(1, "catalog.txt")

	hf, err := c.addTable("log_rollback_flushed", TupleDesc{Fields: []FieldType{{Fname: "t1", Ftype: IntType}}})
	if err != nil {
		t.Error(err)
	}

	var checkFirstPage = func() {
		tid := NewTID()
		if err := bp.BeginTransaction(tid); err != nil {
			t.Error(err)
		}
		t.Log("Checking that the first page is still full of 1s")
		iter, err := hf.Iterator(tid)
		if err != nil {
			t.Error(err)
		}
		count := 0
		tup, err := iter()
		for tup != nil && err == nil {
			count++
			if tup.Fields[0].(IntField).Value != 1 {
				t.Fatalf("unexpected tuple: %#v. first page should be all 1s", tup)
			}
			tup, err = iter()
		}
		if count != 1 {
			t.Fatalf("expected 1 tuple, got %d", count)
		}
		bp.CommitTransaction(tid)
	}

	tid := NewTID()
	if err := bp.BeginTransaction(tid); err != nil {
		t.Error(err)
	}
	t.Log("T1:Inserting a tuple")
	// insert a tuple into a new page
	if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{IntField{1}}}, tid); err != nil {
		t.Error(err)
	}
	bp.CommitTransaction(tid)

	checkFirstPage()

	// T2 deletes every tuple and creates two pages of 2s
	tid = NewTID()
	if err := bp.BeginTransaction(tid); err != nil {
		t.Error(err)
	}
	t.Log("T2:Deleting all tuples")
	// delete all tuples
	iter, err := hf.Iterator(tid)
	if err != nil {
		t.Error(err)
	}
	tup, err := iter()
	for tup != nil && err == nil {
		if err := hf.deleteTuple(tup, tid); err != nil {
			t.Error(err)
		}
		tup, err = iter()
	}
	// fill two pages with twos. this should flush the first page to disk
	t.Log("T2:Filling two pages with 2s")
	for hf.NumPages() < 2 {
		if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{IntField{2}}}, tid); err != nil {
			t.Error(err)
		}
	}
	t.Log("T2:Should have flushed a page. Aborting transaction")

	bp.AbortTransaction(tid)

	checkFirstPage()
}

// TestLogStealForce tests that the buffer pool can evict dirty pages and that
// it writes a log entry when it does so. It also tests that dirty pages are not
// flushed when a transaction commits, but that a log entry is written.
func TestLogStealNoForce(t *testing.T) {
	const fileName = "log_steal_no_force.dat"
	const nPages = 2

	os.Remove(fileName)

	bp, c, err := MakeTestDatabase(1, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err := c.addTable("log_steal_no_force", TupleDesc{Fields: []FieldType{{Fname: "t1", Ftype: IntType}}})
	if err != nil {
		t.Error(err)
	}

	tid := NewTID()

	if err := bp.BeginTransaction(tid); err != nil {
		t.Error(err)
	}

	t.Log("Filling a page with 1s")
	// fill a page with ones
	for hf.NumPages() < 1 {
		if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{IntField{1}}}, tid); err != nil {
			t.Error(err)
		}
	}

	// fill a page with twos. the bufferpool has only one page, so the first
	// page should be evicted and written to disk (this is the STEAL policy)
	t.Log("Filling a page with 2s. First page should be evicted")
	for hf.NumPages() < 2 {
		if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{IntField{2}}}, tid); err != nil {
			t.Error(err)
		}
	}

	bp.CommitTransaction(tid)

	// at this point, the first page should be on disk and in the log and the
	// second page should be in the log but not on disk (this is the NO FORCE policy)

	page0, err := hf.readPage(0)
	if err != nil {
		t.Error(err)
	}
	pg0 := page0.(*heapPage)
	iter0 := pg0.tupleIter()

	tup, err := iter0()
	for tup != nil && err != nil {
		if tup.Fields[0].(IntField).Value != 1 {
			t.Fatalf("unexpected tuple: %#v. first page should be all 1s", tup)
		}
		tup, err = iter0()
	}

	page1, err := hf.readPage(1)
	if err != nil {
		t.Logf("Error reading page 1. This is ok, since the page should not be on disk: %v", err)
	} else {
		pg1 := page1.(*heapPage)
		iter1 := pg1.tupleIter()

		tup, err := iter1()
		if tup != nil {
			t.Fatalf("unexpected tuple: %#v. only the first page should be on disk", tup)
		}
		if err != nil {
			t.Error(err)
		}
	}

	nUpdate := 0
	var beginOffset, commitOffset int64 = -1, -1
	var minUpdateOffset int64 = math.MaxInt64
	var maxUpdateOffset int64 = math.MinInt64

	logFile := bp.LogFile()
	if err := logFile.seek(0, io.SeekStart); err != nil {
		t.Error(err)
	}

	iter := logFile.ForwardIterator()

	record, err := iter()
	for record != nil && err == nil {
		if record.Type() == BeginRecord && record.Tid() == tid {
			beginOffset = record.Offset()
		} else if record.Type() == CommitRecord && record.Tid() == tid {
			commitOffset = record.Offset()
		} else if record.Type() == UpdateRecord && record.Tid() == tid {
			nUpdate++
			minUpdateOffset = min(minUpdateOffset, record.Offset())
			maxUpdateOffset = max(maxUpdateOffset, record.Offset())
		} else {
			t.Errorf("unexpected record: %#v", record)
		}
		record, err = iter()
	}
	if err != nil {
		t.Error(err)
	}

	if beginOffset == -1 {
		t.Errorf("missing begin record")
	}
	if commitOffset == -1 {
		t.Errorf("missing commit record")
	}
	if nUpdate != nPages {
		t.Errorf("unexpected number of updates: expected %d, got %d", nPages, nUpdate)
	}
	if beginOffset > minUpdateOffset {
		t.Errorf("begin after update: %d %d", beginOffset, minUpdateOffset)
	}
	if commitOffset < maxUpdateOffset {
		t.Errorf("commit before update: %d %d", commitOffset, maxUpdateOffset)
	}
}

// Tests that update records are written to the log when a page is updated.
func TestLogUpdate(t *testing.T) {
	os.Remove("log_update.dat")

	bp, c, err := MakeTestDatabase(10, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err := c.addTable("log_update", TupleDesc{Fields: []FieldType{{Fname: "t1", Ftype: IntType}}})
	if err != nil {
		t.Error(err)
	}

	tid := NewTID()

	if err := bp.BeginTransaction(tid); err != nil {
		t.Error(err)
	}

	const nPages = 2

	inserted := 0
	for hf.NumPages() < nPages {
		if err := hf.insertTuple(&Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{IntField{1}}}, tid); err != nil {
			t.Fatal(err)
		}
		inserted++
	}
	t.Logf("Inserted %d tuples, %d pages", inserted, hf.NumPages())

	bp.CommitTransaction(tid)

	logFile := bp.LogFile()
	if err := logFile.seek(0, io.SeekStart); err != nil {
		t.Error(err)
	}

	iter := logFile.ForwardIterator()
	nUpdate := 0
	var beginOffset, commitOffset int64 = -1, -1
	var minUpdateOffset int64 = math.MaxInt64
	var maxUpdateOffset int64 = math.MinInt64

	record, err := iter()
	for record != nil && err == nil {
		if record.Type() == BeginRecord && record.Tid() == tid {
			beginOffset = record.Offset()
		} else if record.Type() == CommitRecord && record.Tid() == tid {
			commitOffset = record.Offset()
		} else if record.Type() == UpdateRecord && record.Tid() == tid {
			nUpdate++
			minUpdateOffset = min(minUpdateOffset, record.Offset())
			maxUpdateOffset = max(maxUpdateOffset, record.Offset())
		} else {
			t.Errorf("unexpected record: %#v", record)
		}
		record, err = iter()
	}
	if err != nil {
		t.Error(err)
	}

	if beginOffset == -1 {
		t.Errorf("missing begin record")
	}
	if commitOffset == -1 {
		t.Errorf("missing commit record")
	}
	if nUpdate != nPages {
		t.Errorf("unexpected number of updates: expected %d, got %d", nPages, nUpdate)
	}
	if beginOffset > minUpdateOffset {
		t.Errorf("begin after update: %d %d", beginOffset, minUpdateOffset)
	}
	if commitOffset < maxUpdateOffset {
		t.Errorf("commit before update: %d %d", commitOffset, maxUpdateOffset)
	}
}

type ActionKind int8

const (
	AbortAction  ActionKind = iota
	CommitAction ActionKind = iota
	UpdateAction ActionKind = iota
	BeginAction  ActionKind = iota
)

type Action struct {
	kind  ActionKind
	tid   TransactionID
	page  int
	value int
}

func runLogTest(t *testing.T, bufferPoolSize int, actions []Action) *LogFile {
	bp, c, err := MakeTestDatabase(bufferPoolSize, "catalog.txt")
	if err != nil {
		t.Error(err)
	}

	hf, err := c.addTable("log_test", TupleDesc{Fields: []FieldType{{Fname: "f", Ftype: IntType}}})
	if err != nil {
		t.Error(err)
	}

	for _, action := range actions {
		switch action.kind {
		case BeginAction:
			if err := bp.BeginTransaction(action.tid); err != nil {
				t.Error(err)
			}
		case CommitAction:
			bp.CommitTransaction(action.tid)
		case AbortAction:
			bp.AbortTransaction(action.tid)
		case UpdateAction:
			tup := Tuple{Desc: *hf.Descriptor(), Fields: []DBValue{IntField{int64(action.value)}}}
			pg, err := bp.GetPage(hf, action.page, action.tid, WritePerm)
			if err != nil {
				t.Error(err)
			}
			if _, err := pg.(*heapPage).insertTuple(&tup); err != nil {
				t.Error(err)
			}
			pg.setDirty(action.tid, true)
		}
	}
	return bp.LogFile()
}

func TestLogCommit(t *testing.T) {
	tid := NewTID()
	actions := []Action{
		{BeginAction, tid, 0, 0},
		{CommitAction, tid, 0, 0},
	}
	logFile := runLogTest(t, 10, actions)

	logFile.seek(0, io.SeekStart)
	iter := logFile.ForwardIterator()
	var beginOffset, commitOffset int64 = -1, -1
	record, err := iter()
	for record != nil && err == nil {
		if record.Type() == BeginRecord && record.Tid() == tid {
			beginOffset = record.Offset()
		} else if record.Type() == CommitRecord && record.Tid() == tid {
			commitOffset = record.Offset()
		} else {
			t.Errorf("unexpected record: %#v", record)
		}
		record, err = iter()
	}
	if err != nil {
		t.Error(err)
	}

	if beginOffset == -1 {
		t.Errorf("missing begin record")
	}
	if commitOffset == -1 {
		t.Errorf("missing commit record for tid1")
	}
	if beginOffset > commitOffset {
		t.Errorf("begin after commit: %d %d", beginOffset, commitOffset)
	}
}

// Tests that begin and commit records are written to the log when a transaction
// commits or aborts.
func singleTestLogCommitAbort(t *testing.T, tid1 TransactionID, tid2 TransactionID, actions []Action) {
	logFile := runLogTest(t, 10, actions)

	logFile.seek(0, io.SeekStart)
	iter := logFile.ForwardIterator()
	var beginOffset1, commitOffset, beginOffset2, abortOffset int64 = -1, -1, -1, -1

	record, err := iter()
	for record != nil && err == nil {
		if record.Type() == BeginRecord && record.Tid() == tid1 {
			beginOffset1 = record.Offset()
		} else if record.Type() == CommitRecord && record.Tid() == tid1 {
			commitOffset = record.Offset()
		} else if record.Type() == BeginRecord && record.Tid() == tid2 {
			beginOffset2 = record.Offset()
		} else if record.Type() == AbortRecord && record.Tid() == tid2 {
			abortOffset = record.Offset()
		} else {
			t.Errorf("unexpected record: %#v, tid1: %v, tid2: %v", record, tid1, tid2)
		}
		record, err = iter()
	}
	if err != nil {
		t.Error(err)
	}

	if beginOffset1 == -1 {
		t.Errorf("missing begin record for tid1")
	}
	if commitOffset == -1 {
		t.Errorf("missing commit record for tid1")
	}
	if beginOffset2 == -1 {
		t.Errorf("missing begin record for tid2")
	}
	if abortOffset == -1 {
		t.Errorf("missing abort record for tid2")
	}
	if beginOffset1 > commitOffset {
		t.Errorf("begin for tid1 after commit: %d %d", beginOffset1, commitOffset)
	}
	if beginOffset2 > abortOffset {
		t.Errorf("begin for tid2 after abort: %d %d", beginOffset2, abortOffset)
	}
}

// Run a sequence of commits and aborts and ensure that appropriate log records are written.
func TestLogCommitAbort(t *testing.T) {
	tid1 := NewTID()
	tid2 := NewTID()
	actions := [][]Action{
		{{BeginAction, tid1, 0, 0}, {CommitAction, tid1, 0, 0}, {BeginAction, tid2, 0, 0}, {AbortAction, tid2, 0, 0}},
		{{BeginAction, tid1, 0, 0}, {BeginAction, tid2, 0, 0}, {CommitAction, tid1, 0, 0}, {AbortAction, tid2, 0, 0}},
		{{BeginAction, tid1, 0, 0}, {BeginAction, tid2, 0, 0}, {AbortAction, tid2, 0, 0}, {CommitAction, tid1, 0, 0}},
		{{BeginAction, tid2, 0, 0}, {BeginAction, tid1, 0, 0}, {AbortAction, tid2, 0, 0}, {CommitAction, tid1, 0, 0}},
		{{BeginAction, tid2, 0, 0}, {AbortAction, tid2, 0, 0}, {BeginAction, tid1, 0, 0}, {CommitAction, tid1, 0, 0}},
	}

	for _, actions := range actions {
		singleTestLogCommitAbort(t, tid1, tid2, actions)
	}
}
