package godb

// The result of a page lock request
type LockResponse int

const (
	Grant LockResponse = iota
	Wait  LockResponse = iota
	Abort LockResponse = iota
)

// PageLocks represents the locks held on a page.
//
// A page can have multiple read locks, but at most one write lock.
type PageLocks struct {
	read  []TransactionID
	write *TransactionID
}

// LockTable is a table that keeps track of the locks held on each page, the
// pages that each transaction has locks on, and the wait-for graph.
type LockTable struct {
	locks       map[any]*PageLocks      // the locks on a page
	tidPageList map[TransactionID][]any // the pages that a transaction has locks on
	waitGraph   WaitFor
}

// Create a new LockTable.
func NewLockTable() *LockTable {
	return &LockTable{
		make(map[any]*PageLocks),
		make(map[TransactionID][]any),
		WaitFor{},
	}
}

// Release all locks held by the transaction. This is called when a transaction
// is aborted or committed.
func (t *LockTable) ReleaseLocks(tid TransactionID) {
	for _, pg := range t.tidPageList[tid] {
		locks := t.locks[pg]
		if locks == nil {
			continue
		}

		for i, testTid := range locks.read {
			if testTid == tid {
				// remove tid from readLocks by moving the last tid into the
				// newly free slot and shortening the slice by 1
				locks.read[i] = locks.read[len(locks.read)-1]
				locks.read = locks.read[:len(locks.read)-1]
			}
		}

		// remove tid from the write locks for this page
		if locks.write != nil && *locks.write == tid {
			locks.write = nil
		}

		// if there are no more locks on the page, remove the page from the
		// lock table
		if len(locks.read) == 0 && locks.write == nil {
			delete(t.locks, pg)
		}
	}

	delete(t.tidPageList, tid)

	t.waitGraph.RemoveTransaction(tid)
	for _, waits := range t.waitGraph {
		for i, wait := range waits {
			if wait == tid {
				// remove tid from waits by moving the last tid into the newly
				// free slot and shortening the slice by 1
				waits[i] = waits[len(waits)-1]
				waits = waits[:len(waits)-1]
			}
		}
	}
	//</silentstrip lab4>
}

// Return the page key for each page that the transaction has taken a write lock on.
//
// These are the pages that need to be written to disk when the transaction
// commits or dropped from the buffer pool when the transaction aborts.
func (t *LockTable) WriteLockedPages(tid TransactionID) []any {
	var pages []any
	for _, pg := range t.tidPageList[tid] {
		locks, ok := t.locks[pg]
		if ok && locks.write != nil && *locks.write == tid {
			pages = append(pages, pg)
		}
	}
	return pages
}

func (bp *LockTable) addTidPage(tid TransactionID, hashCode any) {
	for _, hc := range bp.tidPageList[tid] {
		if hc == hashCode {
			return
		}
	}
	bp.tidPageList[tid] = append(bp.tidPageList[tid], hashCode)
}

// Try to lock a page with the given permissions. If the lock is granted, return
// true. If the lock is not granted, return false and potentially return a
// transaction to abort in order to break a deadlock.
//
// If the lock is granted, return Grant. If the lock is not granted, return
// either Wait or Abort. Upon receiving Wait, the caller should wait and then
// try again. Upon receiving Abort, the caller should abort the transaction by
// calling AbortTransaction.
func (t *LockTable) TryLock(file DBFile, pageNo int, tid TransactionID, perm RWPerm) LockResponse {
	hashCode := file.pageKey(pageNo)

	locks := t.locks[hashCode]
	if locks == nil {
		locks = &PageLocks{}
		t.locks[hashCode] = locks
	}

	// we will reset our waiting status depending on whether we get this lock
	delete(t.waitGraph, tid)

	switch perm {
	case ReadPerm:
		// if the page has no writers (or we are the writer), take a read lock
		if locks.write == nil || *locks.write == tid {
			//add read locks
			found := false
			for _, checkTid := range locks.read {
				if checkTid == tid {
					found = true
				}
			}
			if !found {
				locks.read = append(locks.read, tid)
				t.addTidPage(tid, hashCode)
			}
			return Grant
		}

		// if we can't take the read lock, we are waiting for the writer
		t.waitGraph.AddEdges(tid, []TransactionID{*locks.write})

	case WritePerm:
		// we can write when noone holds a read lock, or we are the exclusive reader
		readOk := len(locks.read) == 0 || (len(locks.read) == 1 && locks.read[0] == tid)

		// we can writen when there are no writers or we are the writer
		writeOk := locks.write == nil || *locks.write == tid

		if readOk && writeOk {
			// if we don't hold the write lock, take it and update our page list
			if locks.write == nil || *locks.write != tid {
				locks.write = &tid
				t.addTidPage(tid, hashCode)
			}
			return Grant
		}

		// if we can't take the write lock, we are waiting for the
		// readers and writer
		if locks.write != nil {
			t.waitGraph.AddEdges(tid, []TransactionID{*locks.write})
		}

		var readWaits []TransactionID
		for _, t := range locks.read {
			if t != tid {
				readWaits = append(readWaits, t)
			}
		}
		t.waitGraph.AddEdges(tid, readWaits)
	}

	// if locking fails, check for deadlock
	if t.waitGraph.DetectDeadlock(tid) {
		return Abort
	}

	return Wait
}
