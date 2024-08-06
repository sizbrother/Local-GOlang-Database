package godb

import (
	"fmt"
)

// Given a set s (represented as a slice of elements of type T) and an integer
// k, return an iterator over the subsets of s of size k.
//
// If k is less than 0 or greater than the length of s, return an error.
func KSubsetIter[T any](s []T, k int) (func() []T, error) {
	n := len(s)
	if k < 0 || k > n {
		return nil, fmt.Errorf("cannot return %d-subsets of a %d-element set", k, n)
	}
	if k == 0 {
		done := false
		return func() []T {
			if done {
				return nil
			}
			done = true
			return []T{}
		}, nil
	}

	c := make([]int, k+3)
	for j := 1; j <= k; j++ {
		c[j] = j - 1
	}
	c[k+1] = n
	c[k+2] = 0

	first := true
	done := false

	return func() []T {
		if done {
			return nil
		}
		if first {
			first = false
			ret := make([]T, k)
			for i := 1; i <= k; i++ {
				ret[i-1] = s[c[i]]
			}
			return ret
		}

		j := 1
		for c[j]+1 == c[j+1] {
			c[j] = j - 1
			j++
		}

		if j > k {
			done = true
			return nil
		}
		c[j]++

		ret := make([]T, k)
		for i := 1; i <= k; i++ {
			ret[i-1] = s[c[i]]
		}
		return ret
	}, nil
}
