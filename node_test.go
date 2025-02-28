//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2025 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

var conn *Conn // global connection for use in testing

// --- Examples (testable) ---

// Example of viewing a Node instance as a string.
func ExampleNode_String() {
	n := NewConn().Node("var", "sub1", "sub2")
	fmt.Println(n)
	// Output: var("sub1")("sub2")
}

// --- Tests ---

// Test Node creation.
func TestNode(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		n := NewConn().Node("var", "sub1", "sub2")
		ans := fmt.Sprintf("%v", n)
		expect := "var(\"sub1\")(\"sub2\")"
		if ans != expect {
			t.Errorf("got %s, want %s", ans, expect)
		}
	})
}

// --- Benchmarks ---

// Benchmark Setting a node repeatedly to new values each time.
func benchmarkSet(b *testing.B) {
	n := conn.Node("var") // or: db.New("varname", "sub1", "sub2")
	for i := 0; b.Loop(); i++ {
		err := n.Set(Randstr())
		if err != nil {
			panic(err)
		}
	}
}

// Benchmark Setting a node with randomly located node, where each node has 5 random subscripts.
func benchmarkSetVariantSubscripts(b *testing.B) {
	subs := make([]string, 5)
	for i := 0; b.Loop(); i++ {
		for j := range subs {
			subs[j] = Randstr()
		}
		n := conn.Node("var", subs...)
		err := n.Set(Randstr())
		if err != nil {
			panic(err)
		}
	}
}

// Run all Node benchmarks.
func BenchmarkNode(b *testing.B) {
	conn = NewConn()

	b.Run("Set", benchmarkSet)
	b.Run("SetVariantSubscripts", benchmarkSetVariantSubscripts)
}

// --- Utility functions for tests ---

var randstr = make([]string, 0, 10000) // Array of random strings for use in testing
var randstrIndex = 0

// Prepare a list of many random strings.
func initRandstr() {
	if len(randstr) > 0 {
		return // early return if already filled randstr
	}
	rnd := rand.New(rand.NewChaCha8([32]byte{}))
	for range cap(randstr) {
		s := fmt.Sprintf("%x", rnd.Uint32())
		randstr = append(randstr, s)
	}
}

func Randstr() string {
	randstrIndex = (randstrIndex + 1) % len(randstr)
	return randstr[randstrIndex]
}

func TestMain(m *testing.M) {
	initRandstr()
	m.Run()
}
