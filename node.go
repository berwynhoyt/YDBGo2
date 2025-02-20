//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2018-2022 YottaDB LLC and/or its subsidiaries.
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
)

/* #include "libyottadb.h"
typedef struct conn {
	uint64_t tptoken;	// place to store tptoken for thread-safe ydb_*_st() function calls
	ydb_buffer_t errstr;	// space for YottaDB to return an error string
	ydb_buffer_t value;	// temporary space to store in or out value for get/set
} conn;

typedef struct node {
	conn *conn;
	int len;		// number of buffers[] allocated to store subscripts/strings
	int datasize;		// length of string `data` field (all strings and subscripts concatenated)
	int mutable;		// whether the node is mutable (these are only emitted by node iterators)
	ydb_buffer_t buffers[];
	// char *data;		// stored after `buffers` (however large they are), which point into this data
} node;
*/
import "C"

const initial_value_size = 1024 // Initial size of value storage in each node

// Note As at Go 1.24, it is not possible to runtime.pin() a string or slice, per the docs:https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers.
// There is an outstanding proposal and commits to fix this at https://github.com/golang/go/issues/65286#issuecomment-1911318072.
// In the meantime, it is possible to pin the memory underlying str and sl: Pin(unsafe.StringData(str))) and Pin(&sl[0]) to pin the entire slice.
// This workaround requires Go >= 1.22 for a fix to pin(StringData): https://github.com/golang/go/issues/65286#issuecomment-1920087602.

// Contains the transaction token and provides a buffer for subsequent calls to YottaDB.
// You must use a different connection for each thread.
type Conn C.conn

// Create a new connection for the current thread.
func NewConn() *Conn {
	var conn Conn
	conn.tptoken = C.YDB_NOTTP
	// Create space for err
	conn.errstr.buf_addr = (*C.char)(C.malloc(C.YDB_MAX_ERRORMSG))
	conn.errstr.len_alloc = C.YDB_MAX_ERRORMSG
	conn.errstr.len_used = 0
	// Create initial space for value used by various API call/return
	// TODO: This is set to YDB_MAX_STR (1MB) for the initial version only. Later we can reduce its initial value and create logic to reallocate it when necessary,
	//       e.g. in n.Set()
	conn.value.buf_addr = (*C.char)(C.malloc(C.YDB_MAX_STR))
	conn.value.len_alloc = C.uint(C.YDB_MAX_STR)
	conn.value.len_used = 0

	runtime.AddCleanup(conn, func(conn *Conn) {
		conn.value.len_alloc, con.value.len_used = 0, 0
		conn.errstr.len_alloc, con.errstr.len_used = 0, 0
		C.free(conn.value.buf_addr)
		C.free(conn.errstr.buf_addr)
		conn.value.buf_addr = 0
		conn.errstr.buf_addr = 0
	}, &conn)
	return &conn
}

// Return previous error message as an `error` type or nil if there was no error
func (conn *Conn) Error(code C.int) error {
	if code == C.YDB_OK {
		return nil
	}
	// Take a copy of errstr as a Go String
	msg := C.GoStringN(conn.errstr.buf_addr, C.int(conn.errstr.len_used))
	return Error(int(code), msg)
}

// Node is an object containing strings that represents a YottaDB node, supporting fast calls to the YottaDB C API.
// Thread Safety: Regular Nodes are immutable, so are thread-safe (one thread cannot change a Node used by
// another thread). There is a mutable version of Node emitted by Node iterators (FOR loops over Node), which
// may not be shared with other threads except by first taking an immutable Node.Copy() of it.

type Node C.node

// Create a `Node` instance that represents a database node and has all of the class methods defined below.
// Store all the supplied strings (varname and subscripts) in the Node object along with array of C.ydb_buffer_t
// structs that point to each successive string, to provide fast access to YottaDB API functions.
// The strings and array are stored in C-allocated space to give Node methods fast access to YottaDB API functions.
// The user must call Node.Free() when finished using the object because the data is stored in C-allocated space
// which the Go garbage collector does not manage.
func (conn *Conn) New(subscripts ...string) (n *Node) {
	if len(subscripts) == 0 {
		panic("YDB: supply node type with at least one string (typically varname)")
	}
	// Concatenate strings the fastest Go way.
	// This involves creating an extra copy of subscripts but is probably faster than one C.memcpy call per subscript
	var joiner bytes.Buffer
	for _, s := range subscripts {
		joiner.WriteString(s)
	}

	size := C.sizeof_node + C.sizeof_ydb_buffer_t*len(subscripts) + joiner.Len()
	// This initial call must be to calloc() to get initialized (cleared) storage. We cannot allocate it and then
	// do another call to initialize it as that means uninitialized memory is traversing the cgo boundary which
	// is what triggers the cgo bug mentioned in the cgo docs (https://golang.org/cmd/cgo/#hdr-Passing_pointers).
	// TODO: but if we retain calloc, we need to check for memory error because Go doesn't create a wrapper for C.calloc like it does for C.malloc (cf. https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers:~:text=C.malloc%20cannot%20fail)
	// Alternatively, we could call malloc and then memset to clear just the ydb_buffer_t parts, but test which is faster.
	n = (*Node)(C.calloc(1, size))
	// Queue the cleanup function to free it
	runtime.AddCleanup(n, func(n *Node) {
		C.free(n)
	}, n)

	n.conn = conn
	n.len = len(subscripts)
	n.mutable = 0 // i.e. false

	dataptr := (*C.char)(&n.ydb_buffer_t[len(subscripts)])
	C.memcpy(dataptr, unsafe.Pointer(&joiner.Bytes()[0]), C.size_t(joiner.Len()))

	// Now fill in ydb_buffer_t pointers
	strptr := dataptr
	for i, s := range subscripts {
		buf = &n.buffers[i]
		buf.buf_addr = strptr
		l := len(s)
		buf.len_used, buf.len_alloc = len(s), len(s)
		strptr += len(s)
	}
	return n
}

// Return string representation of this database node in typical YottaDB format: `varname("sub1")("sub2")`.
func (n *Node) String() string {
	var bld strings.Builder
	for i := range n.len {
		buf := n.buffers[i]
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

// Set
func (n *Node) Set(val string) error {
	// Create a ydb_buffer_t pointing to go string
	if len(val) > n.conn.value.buf_alloc {
		panic("YDB: tried to set database value to a string that is too large")
	}
	// TODO: should the following line should be change to have a C wrapper that accepts _GoString_ to avoid risk of StringData moving? Or is it OK within one line (see Pointer docs)?
	C.memcpy(&n.conn.value.buf_addr, unsafe.Pointer(unsafe.StringData(val)), C.size_t(len(val)))
	n.conn.value.len_used = C.uint(len(val))

	ret := C.ydb_set_st(n.conn.tptoken, &n.conn.errstr, &n.bufC[0], C.int(len(n.bufGo)-1), &n.bufC[1], &n.conn.value)

	return n.conn.Error(ret)
}

// Get the value of a database node.
// On error return value "" and error
// If deflt is supplied return string deflt[0] instead of GVUNDEF or LVUNDEF errors.
func (n *Node) Get(deflt ...string) (string, error) {
	err := C.ydb_get_st(n.conn.tptoken, &n.conn.errstr, &n.bufC[0], C.int(len(n.bufGo)-1), &n.bufC[1], &n.conn.value)
	if err == C.YDB_ERR_INVSTRLEN {
		// TODO: fix the following to realloc
		panic("YDB: have not yet implemented reallocating conn.value to fit a large returned string")
	}
	if len(deflt) > 0 && (err == C.YDB_ERR_GVUNDEF || err == C.YDB_ERR_LVUNDEF) {
		return deflt[0], n.conn.Error(C.YDB_OK)
	}
	if err != C.YDB_OK {
		return "", n.conn.Error(err)
	}
	// take a copy of the string so that we can release `space`
	value := C.GoStringN(n.conn.value.buf_addr, C.int(n.conn.value.len_used))
	return value, nil
}
