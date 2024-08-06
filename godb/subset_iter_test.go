package godb

import (
	"reflect"
	"testing"
)

func TestKSubsetIter(t *testing.T) {
	set := []int{0, 1, 2}
	// Test the case where k = 0
	{
		iter, _ := KSubsetIter(set, 0)
		s := iter()
		if s == nil {
			t.Errorf("expected one subset, got none")
		}
		if !reflect.DeepEqual(s, []int{}) {
			t.Errorf("expected subset [], got %v", s)
		}
		if iter() != nil {
			t.Errorf("expected one subset, got two")
		}
	}

	// Test the case where k = 1
	{
		iter, _ := KSubsetIter(set, 1)
		s := iter()
		if s == nil {
			t.Errorf("expected three subsets, got none")
		}
		if !reflect.DeepEqual(s, []int{0}) {
			t.Errorf("expected subset [0], got %v", s)
		}
		s = iter()
		if s == nil {
			t.Errorf("expected three subsets, got one")
		}
		if !reflect.DeepEqual(s, []int{1}) {
			t.Errorf("expected subset [1], got %v", s)
		}
		s = iter()
		if s == nil {
			t.Errorf("expected three subsets, got two")
		}
		if !reflect.DeepEqual(s, []int{2}) {
			t.Errorf("expected subset [2], got %v", s)
		}
		s = iter()
		if s != nil {
			t.Errorf("expected three subsets, got four: %v", s)
		}
	}

	// Test the case where k = 2
	{
		iter, _ := KSubsetIter(set, 2)
		s := iter()
		if s == nil {
			t.Errorf("expected three subsets, got none")
		}
		if !reflect.DeepEqual(s, []int{0, 1}) {
			t.Errorf("expected subset [0, 1], got %v", s)
		}
		s = iter()
		if s == nil {
			t.Errorf("expected three subsets, got one")
		}
		if !reflect.DeepEqual(s, []int{0, 2}) {
			t.Errorf("expected subset [0, 2], got %v", s)
		}
		s = iter()
		if s == nil {
			t.Errorf("expected three subsets, got two")
		}
		if !reflect.DeepEqual(s, []int{1, 2}) {
			t.Errorf("expected subset [1, 2], got %v", s)
		}
		if iter() != nil {
			t.Errorf("expected three subsets, got four")
		}
	}

	{
		iter, _ := KSubsetIter(set, 3)
		s := iter()
		if s == nil {
			t.Errorf("expected one subsets, got none")
		}
		if !reflect.DeepEqual(s, []int{0, 1, 2}) {
			t.Errorf("expected subset [0, 1, 2], got %v", s)
		}
		s = iter()
		if s != nil {
			t.Errorf("expected one subsets, got two")
		}
	}
}
