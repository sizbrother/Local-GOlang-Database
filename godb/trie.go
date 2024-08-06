package godb

/* A trie is a tree-like data structure maps sequences of elements to values. */

type Trie[K comparable, V any] struct {
	children map[K]*Trie[K, V]
	value    V
}

// Create a new Trie.
func NewTrie[K comparable, V any]() *Trie[K, V] {
	var t Trie[K, V]
	t.children = make(map[K]*Trie[K, V])
	return &t
}

// Get the value associated with a key in the trie.
//
// If the key is not in the trie, the zero value for V is returned.
func (t *Trie[K, V]) Get(key []K) V {
	var zero V

	node := t
	for k := 0; k < len(key); k++ {
		child, ok := node.children[key[k]]
		if !ok {
			return zero
		}
		node = child
	}
	return node.value
}

// Set the value associated with a key in the trie.
//
// If the key is not already in the trie, it is added.
func (t *Trie[K, V]) Set(key []K, value V) {
	node := t
	for k := 0; k < len(key); k++ {
		child, ok := node.children[key[k]]
		if !ok {
			child = NewTrie[K, V]()
			node.children[key[k]] = child
		}
		node = child
	}
	node.value = value
}
