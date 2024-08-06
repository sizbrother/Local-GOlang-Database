package godb

// WaitFor is a wait-for graph. It maps waiting transactions to the transactions
// that they are waiting for.
type WaitFor map[TransactionID][]TransactionID

// Extend the graph so that [tid] waits for each of [tids].
func (w WaitFor) AddEdges(tid TransactionID, tids []TransactionID) {
	edges := w[tid]
	for _, t := range tids {
		found := false
		for _, e := range edges {
			if e == t {
				found = true
				break
			}
		}
		if !found {
			edges = append(edges, t)
		}
	}
	w[tid] = edges
}

// Remove the transaction [tid] from the graph. After this method runs, the
// graph will not contain any references to [tid].
func (w WaitFor) RemoveTransaction(tid TransactionID) {
	delete(w, tid)
	for t := range w {
		var newWaitTid []TransactionID
		for _, tt := range w[t] {
			if tt != tid {
				newWaitTid = append(newWaitTid, tt)
			}
		}
		w[t] = newWaitTid
	}
}

func (w WaitFor) breakCycle(start TransactionID, root TransactionID, seen map[TransactionID]bool) *TransactionID {
	for _, n := range w[start] {
		if n == root { // found cycle
			return &n
		}
		if _, exists := seen[n]; !exists {
			seen[n] = true
			deadlockTID := w.breakCycle(n, root, seen)
			if deadlockTID != nil {
				return deadlockTID
			}
		}
	}
	return nil
}

// Returns true if [start] is part of a cycle and false otherwise.
func (w WaitFor) DetectDeadlock(start TransactionID) bool {
	seenSet := make(map[TransactionID]bool)
	seenSet[start] = true
	return w.breakCycle(start, start, seenSet) != nil
}
