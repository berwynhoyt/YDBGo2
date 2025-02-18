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
	"runtime"
	"strings"
	"unsafe"
)

// TODO: Like YDBGo v1, this uses runtime.KeepAlive to keep Go data in place for use by C. This is risky and may not be reliable.
// It should be changed to use runtime.Pinner. I have not done so yet as I want a benchmark first to see the difference in speed due to Pinner.
// Moving it to Pinner will also allow the allocation code to be factored into a function to avoid code duplication.

// #include "libyottadb.h"
import "C"

// Note As at Go 1.24, it is not possible to runtime.pin() a string or slice, per the docs:https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers.
// There is an outstanding proposal and commits to fix this at https://github.com/golang/go/issues/65286#issuecomment-1911318072.
// In the meantime, it is possible to pin the memory underlying str and sl: Pin(unsafe.StringData(str))) and Pin(&sl[0]) to pin the entire slice.
// This workaround requires Go >= 1.22 for a fix to pin(StringData): https://github.com/golang/go/issues/65286#issuecomment-1920087602.

// / Contains the transaction token and provides a buffer for subsequent calls to YottaDB.
// You must use a different connection for each thread.
type connection struct {
	tptoken C.uint64_t
	space   []byte // Will become Go-allocated storage space pointed to by errstr below
	errstr  C.ydb_buffer_t
	pinner  runtime.Pinner
}

// / Create a new connection for the current thread.
func New() (conn *connection) {
	conn = new(connection)
	conn.tptoken = C.YDB_NOTTP
	conn.space = make([]byte, C.YDB_MAX_ERRORMSG)
	spaceUnsafe := unsafe.SliceData(conn.space)
	conn.pinner.Pin(spaceUnsafe)
	conn.errstr.buf_addr = (*C.char)(unsafe.Pointer(spaceUnsafe))
	conn.errstr.len_alloc = C.uint(len(conn.space))
	conn.errstr.len_used = 0
	runtime.AddCleanup(conn, func(p runtime.Pinner) {
		p.Unpin()
	}, conn.pinner)
	return conn
}

// / Return previous error message as an `error` type or nil if there was no error
func (conn *connection) Error(code C.int) error {
	if code == C.YDB_OK {
		return nil
	}
	// Take a copy of errstr as a Go String
	msg := C.GoStringN(conn.errstr.buf_addr, C.int(conn.errstr.len_used))
	return Error(int(code), msg)
}

// / Type `node` stores strings that reference a YottaDB node, supporting fast calls to the YottaDB C API.
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
	pinner  runtime.Pinner
	mutable bool
}

// / Create a `node` instance that represents a database node and has all of the class methods defined below.
// Store all the supplied strings (varname and subscripts) in the Node object along with array of C.ydb_buffer_t
// that points each successive string to provide fast access to YottaDB API functions.
func (conn *connection) New(subscripts ...string) (n *node) {
	if len(subscripts) == 0 {
		panic("YDB: supply node type with at least one string (typically varname)")
	}
	n = new(node)
	n.conn = *conn
	n.mutable = false
	n.bufGo = make([]string, len(subscripts))
	for i, s := range subscripts {
		// Retain a reference to each string in a Go splice
		n.bufGo[i] = s
		// Also point to these strings with a C array of ydb_buffer_t
		buf := &n.bufC[i]
		sUnsafe := unsafe.StringData(s)
		n.pinner.Pin(sUnsafe)
		buf.buf_addr = (*C.char)(unsafe.Pointer(sUnsafe))
		buf.len_alloc = C.uint(len(s))
		buf.len_used = buf.len_alloc
	}
	runtime.AddCleanup(n, func(p runtime.Pinner) {
		p.Unpin()
	}, n.pinner)
	return n
}

// / Return string representation of this database node in typical YottaDB format: `varname("sub1")("sub2")`.
func (n *node) String() string {
	var bld strings.Builder
	for i := range len(n.bufGo) {
		buf := n.bufC[i]
		s := C.GoStringN(buf.buf_addr, C.int(buf.len_used))
		if i > 0 {
			bld.WriteString("(\"")
		}
		bld.WriteString(s)
		if i > 0 {
			bld.WriteString("\")")
		}
	}
	return bld.String()
}

// / Set
func (n *node) Set(val string) error {
	// Create a ydb_buffer_t pointing to go string
	var valC C.ydb_buffer_t
	space := unsafe.StringData(val)
	var pinner runtime.Pinner
	pinner.Pin(space)
	valC.buf_addr = (*C.char)(unsafe.Pointer(space))
	valC.len_alloc = C.uint(len(val))
	valC.len_used = valC.len_alloc

	ret := C.ydb_set_st(n.conn.tptoken, &n.conn.errstr, &n.bufC[0], C.int(len(n.bufGo)-1), &n.bufC[1], &valC)
	pinner.Unpin()
	//	runtime.KeepAlive(valC) // ensure valC data hangs around until after the function call returns

	return n.conn.Error(ret)
}

// / Get the value of a database node.
// On error return value "" and error
// If deflt is supplied return string deflt[0] instead of GVUNDEF or LVUNDEF errors.
func (n *node) Get(deflt ...string) (string, error) {
	// Create a Go buffer to store returned string
	space := make([]byte, InitialBufSize)
	var pinner runtime.Pinner
	pinner.Pin(&space[0])
	// Point to the space with a ydb_buffer_t
	var buf C.ydb_buffer_t
	buf.buf_addr = (*C.char)(unsafe.Pointer(unsafe.SliceData(space)))
	buf.len_alloc = C.uint(len(space))
	buf.len_used = 0

	value := ""
	err := C.ydb_get_st(n.conn.tptoken, &n.conn.errstr, &n.bufC[0], C.int(len(n.bufGo)-1), &n.bufC[1], &buf)
	if err == C.YDB_ERR_INVSTRLEN {
		// Allocate a larger buffer of the specified size and try again
		space := make([]byte, buf.len_used)
		pinner.Pin(&space[0])
		buf.buf_addr = (*C.char)(unsafe.Pointer(unsafe.SliceData(space)))
		buf.len_alloc = C.uint(len(space))
		buf.len_used = 0
		err = C.ydb_get_st(n.conn.tptoken, &n.conn.errstr, &n.bufC[0], C.int(len(n.bufGo)-1), &n.bufC[1], &buf)
	}
	if len(deflt) > 0 && (err == C.YDB_ERR_GVUNDEF || err == C.YDB_ERR_LVUNDEF) {
		value, err = deflt[0], C.YDB_OK
	}
	if err == C.YDB_OK {
		// take a copy of the string so that we can release `space`
		value = C.GoStringN(buf.buf_addr, C.int(buf.len_used))
	}

	pinner.Unpin()
	return value, n.conn.Error(err)
}
