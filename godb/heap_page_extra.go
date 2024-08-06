package godb

// Returns the before-image of the page. This is used for logging and recovery.
func (p *heapPage) BeforeImage() Page {
	return p.bImage
}

// Sets the before-image of the page to the current state of the page. Be sure
// that changing the page does not change the before-image.
func (p *heapPage) SetBeforeImage() {
	newPage := &heapPage{
		desc:     p.desc,
		numSlots: p.numSlots,
		numUsed:  p.numUsed,
		dirty:    p.dirty,
		pageNo:   p.pageNo,
		file:     p.file,
		lastTxn:  p.lastTxn,
		tuples:   make([]*Tuple, len(p.tuples)),
	}
	for i, tup := range p.tuples {
		if tup != nil {
			newPage.tuples[i] = tup.Copy()
		}
	}
	p.bImage = newPage
}

func (t *Tuple) Copy() *Tuple {
	newDesc := t.Desc.copy()
	newFields := make([]DBValue, len(t.Fields))
	for i, field := range t.Fields {
		switch f := field.(type) {
		case IntField:
			newFields[i] = IntField{Value: f.Value}
		case StringField:
			newFields[i] = StringField{Value: f.Value}
		}
	}
	return &Tuple{Desc: *newDesc, Fields: newFields, Rid: t.Rid}
}

// Returns the page number of the page.
func (p *heapPage) PageNo() int {
	return p.pageNo
}
