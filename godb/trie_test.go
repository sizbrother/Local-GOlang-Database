package godb

import (
	"testing"
)

func TestTrie(t *testing.T) {
	trie := NewTrie[int, string]()
	trie.Set([]int{1, 2, 3}, "hello")
	trie.Set([]int{1, 2, 4}, "world")

	v := trie.Get([]int{1, 2, 3})
	if v != "hello" {
		t.Fatalf("Trie.Get expected \"hello\", got %s", v)
	}

	v = trie.Get([]int{1, 2, 4})
	if v != "world" {
		t.Fatalf("Trie.Get expected \"world\", got %s", v)
	}

	if trie.Get([]int{1, 2, 5}) != "" {
		t.Fatalf("Trie.Get expected empty string, got %s", trie.Get([]int{1, 2, 5}))
	}
}
