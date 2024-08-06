package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
	"sync"
	"time"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	pages     map[any]Page
	maxPages  int
	lockTable *LockTable
	logfile   *LogFile

	// the transactions that are currently running. This is a set, so the value
	// is not important
	runningTids map[TransactionID]any

	sync.Mutex
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	if numPages <= 0 {
		return nil, fmt.Errorf("numPages must be positive")
	}
	return &BufferPool{
		make(map[any]Page),
		numPages,
		NewLockTable(),
		nil,
		make(map[TransactionID]any),
		sync.Mutex{},
	}, nil
}

// Testing method -- iterate through all pages in the buffer pool and flush them
// using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for _, page := range bp.pages {
		page.getFile().flushPage(page)
	}
}

// Testing method -- flush all dirty pages in the buffer pool and set them to
// clean. Does not need to be thread/transaction safe.
func (bp *BufferPool) flushDirtyPages(tid TransactionID) error {
	for _, pg := range bp.pages {
		if pg.isDirty() {
			pg.getFile().flushPage(pg)
			pg.setDirty(tid, false)
		}
	}
	return nil
}

// Returns true if the transaction is runing.
//
// Caller must hold the bufferpool lock.
func (bp *BufferPool) tidIsRunning(tid TransactionID) bool {
	_, is_running := bp.runningTids[tid]
	return is_running
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.Lock()
	defer bp.Unlock()

	if !bp.tidIsRunning(tid) {
		fmt.Errorf("transaction error: %v", IllegalTransactionError)
		return
	}

	bp.LogFile().LogAbort(tid)
	bp.LogFile().Force()

	bp.Rollback(tid)

	for _, pg := range bp.lockTable.WriteLockedPages(tid) {
		delete(bp.pages, pg)
	}

	bp.lockTable.ReleaseLocks(tid)

	delete(bp.runningTids, tid)

}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) error {
	bp.Lock()
	defer bp.Unlock()

	if !bp.tidIsRunning(tid) {
		return fmt.Errorf("transaction error: %v", IllegalTransactionError)
	}

	for _, pageNum := range bp.lockTable.WriteLockedPages(tid) {

		page := bp.pages[pageNum]

		if page == nil {
			continue
		}

		if page.isDirty() {

			page.setDirty(tid, false)

			bp.LogFile().LogUpdate(tid, page.(*heapPage).bImage, page)
			page.(*heapPage).SetBeforeImage()
		}
	}

	bp.LogFile().LogCommit(tid)
	bp.LogFile().Force()
	bp.lockTable.ReleaseLocks(tid)
	delete(bp.runningTids, tid)

	return nil
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	bp.Lock()
	defer bp.Unlock()

	if bp.tidIsRunning(tid) {
		return fmt.Errorf("transaction error: %v", IllegalTransactionError)
	}

	bp.LogFile().LogBegin(tid)
	bp.LogFile().Force()
	bp.runningTids[tid] = nil

	return nil
}

// If necessary, evict clean page from the buffer pool. If all pages are dirty,
// return an error.
func (bp *BufferPool) evictPage() error {

	if len(bp.pages) < bp.maxPages {
		return nil
	}

	// evict first clean page
	for key, page := range bp.pages {
		if !page.isDirty() {
			delete(bp.pages, key)
			return nil
		}
	}

	for key, page := range bp.pages {
		if page.isDirty() {

			befImg := page.(*heapPage)
			bp.LogFile().LogUpdate(befImg.lastTxn, befImg.BeforeImage(), page)

			bp.LogFile().Force()
			page.getFile().flushPage(page)

			delete(bp.pages, key)
			return nil
		}
	}
	return GoDBError{BufferPoolFullError, "all pages in buffer pool are dirty"}
}

// Returns true if the transaction is runing.
func (bp *BufferPool) IsRunning(tid TransactionID) bool {
	bp.Lock()
	defer bp.Unlock()
	return bp.tidIsRunning(tid)
}

// Loads the specified page from the specified DBFile, but does not lock it.
func (bp *BufferPool) loadPage(file DBFile, pageNo int) (Page, error) {
	bp.Lock()
	defer bp.Unlock()

	hashCode := file.pageKey(pageNo)

	var pg Page
	pg, ok := bp.pages[hashCode]
	if !ok {
		var err error
		pg, err = file.readPage(pageNo)
		if err != nil {
			return nil, err
		}
		err = bp.evictPage()
		if err != nil {
			return nil, err
		}
		bp.pages[hashCode] = pg
	}
	return pg, nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	if !bp.IsRunning(tid) {
		return nil, GoDBError{IllegalTransactionError, "Transaction is not running or has aborted."}
	}

	//loop until locks are acquired
	for {
		// ensure page is in the buffer pool
		pg, err := bp.loadPage(file, pageNo)
		if err != nil {
			return nil, err
		}

		// try to lock the page
		bp.Lock()
		switch bp.lockTable.TryLock(file, pageNo, tid, perm) {
		case Grant:
			bp.Unlock()
			return pg, nil
		case Wait:
			bp.Unlock()
			time.Sleep(2 * time.Millisecond)
		case Abort:
			bp.Unlock()
			bp.AbortTransaction(tid)
			return nil, GoDBError{IllegalTransactionError, "Transaction has aborted."}
		}
	}
}
