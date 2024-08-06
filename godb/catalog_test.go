package godb

import (
	"testing"
)

func TestNewCatalogFromFile(t *testing.T) {
	writeFile(t, "catalog.txt", "t (name string, age int)\nt2 (name string, age int)\n")
	c := NewCatalog("catalog.txt", nil, "./")
	if err := c.parseCatalogFile(); err != nil {
		t.Fatalf("failed to parse catalog file, %s", err.Error())
	}
	s := c.String()
	if s != "t(name string, age int)\nt2(name string, age int)\n" {
		t.Errorf("unexpected catalog: %#v", s)
	}
}
