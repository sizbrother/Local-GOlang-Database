package godb

import (
	"fmt"
)

// Rolls back a transaction by reading the log and undoing the changes made by
// the transaction.
func (bp *BufferPool) Rollback(tid TransactionID) error {
	iter, err := bp.LogFile().ReverseIterator()
	if err != nil {
		return err
	}
	for {

		record, err := iter()
		if err != nil {
			return err
		}

		if record == nil {
			break
		}

		if tid == record.Tid() {
			if record.Type() == UpdateRecord {

				before := record.(*UpdateLogRecord).Before
				file := before.(*heapPage).getFile()
				file.flushPage(before)

				for key, page := range bp.pages {
					if page.(*heapPage).lastTxn == tid {
						delete(bp.pages, key)
					}
				}

			}
		}
	}
	return nil
}

// Returns the log file associated with the buffer pool.
func (bp *BufferPool) LogFile() *LogFile {
	return bp.logfile
}

// Recover the buffer pool from a log file. This should be called when the
// database is started, even if the log file is empty.
func (bp *BufferPool) Recover(logFile *LogFile) error {

	bp.logfile = logFile

	activeTransactions := make(map[TransactionID]bool)
	completedTransactions := make(map[TransactionID]bool)

	forwardIter := logFile.ForwardIterator()
	for {
		record, err := forwardIter()
		if err != nil {
			return fmt.Errorf("error reading log during redo phase: %w", err)
		}
		if record == nil {
			break
		}

		switch rec := record.(type) {
		case *UpdateLogRecord:
			if _, completed := completedTransactions[rec.Tid()]; !completed {
				afterImage := rec.After
				if err := afterImage.getFile().flushPage(afterImage); err != nil {
					return fmt.Errorf("failed to redo logged changes: %w", err)
				}
				activeTransactions[rec.Tid()] = true
			}
		case *GenericLogRecord:
			if rec.Type() == CommitRecord || rec.Type() == AbortRecord {
				completedTransactions[rec.Tid()] = true
				delete(activeTransactions, rec.Tid())
			}
		}
	}

	reverseIter, err := logFile.ReverseIterator()
	if err != nil {
		return fmt.Errorf("error setting up reverse iterator: %w", err)
	}
	for {
		record, err := reverseIter()
		if err != nil {
			return fmt.Errorf("error reading log during undo phase: %w", err)
		}
		if record == nil {
			break
		}

		if updateRecord, ok := record.(*UpdateLogRecord); ok {
			if _, active := activeTransactions[updateRecord.Tid()]; active {
				beforeImage := updateRecord.Before
				if err := beforeImage.getFile().flushPage(beforeImage); err != nil {
					return fmt.Errorf("failed to undo changes for transaction %d: %w", updateRecord.Tid(), err)
				}
			}
		}
	}

	return nil

}
