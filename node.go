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

package main

import (
	"unsafe"
)

/* #cgo CFLAGS: -I/usr/local/lib/yottadb/r201
#include "libyottadb.h"
*/
import "C"

// Type `connection` stores the transaction token and provides a buffer for error messages from YottaDB.
// You must use a different connection for each thread.
type connection struct {
	tptoken uint64
	errstr  []byte
}

// Create a new connection for the current thread.
func New() (db *connection) {
	db = new(connection)
	db.errstr = make([]byte, 1024) // default errstr len
	return db
}

// Adjust the maximum error message length for the connection instance
func (db *connection) SetMaxErr(errlen int) {
	db.errstr = make([]byte, errlen)
}

// Type `node` stores strings that reference a YottaDB node, supporting fast calls to the YottaDB C API.
//
// Instances hold references to all its subscripts as a slice of Go strings to ensure they remain allocated for C,
// and also point to those strings using C type C.ydb_buffer_t for fast calls to the YottaDB C API. This memory retention
// works because Go has 'non-moving' garbage collector, cf. https://tip.golang.org/doc/gc-guide#Tracing_Garbage_Collection
//
// Thread Safety: Regular Nodes are immutable, they are thread-safe (one thread cannot change a Node used by
// another thread). There is a mutable version of Node emitted by Node iterators (FOR loops over Node), which
// may not be shared with other threads except by first taking an immutable Node.Copy() of it.
type node struct {
	conn    connection
	bufGo   []string
	bufC    [C.YDB_MAX_SUBS + 1]C.ydb_buffer_t
	mutable bool
}

// Create a `node` instance that represents a database node and has all of the class methods defined below.
// Store all the supplied strings (varname and subscripts) in the Node object along with array of C.ydb_buffer_t
// that points each successive string to provide fast access to YottaDB API functions.
func (db *connection) New(subscripts ...string) (n *node) {
	n = new(node)
	n.conn = *db
	//n.buffers = make(C.ydb_buffer_t, len(subscripts))
	n.mutable = false
	n.bufGo = make([]string, len(subscripts))
	for i, s := range subscripts {
		// Retain a reference to each string in a Go splice
		n.bufGo[i] = s
		// Also point to these strings with a C array of ydb_buffer_t
		buf := &n.bufC[i]
		buf.buf_addr = (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
		buf.len_alloc = C.uint(len(s))
		buf.len_used = C.uint(len(s))
	}
	return n
}

func (n *node) Dump() {
	for i, s := range n.bufGo {
		println(i, s)
	}
}

func main() {
	db := New()
	n := db.New("abc", "def", "ghi")
	n.Dump()
}
