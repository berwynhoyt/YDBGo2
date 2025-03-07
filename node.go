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

// Define Node type for access YottaDB database

package yottadb

import (
	"bytes"
	"runtime"
	"strings"
	"unsafe"
)

/* #include "libyottadb.h"
// Create a thread-specific 'connection' object for calling the YottaDB API.
typedef struct conn {
	uint64_t tptoken;	// place to store tptoken for thread-safe ydb_*_st() function calls
	ydb_buffer_t errstr;	// space for YottaDB to return an error string
	ydb_buffer_t value;	// temporary space to store in or out value for get/set
} conn;

// Create a representation of a database node, including a cache of its subscript strings for fast calls to the YottaDB API.
typedef struct node {
	conn *conn;
	int len;		// number of buffers[] allocated to store subscripts/strings
	int datasize;		// length of string `data` field (all strings and subscripts concatenated)
	int mutable;		// whether the node is mutable (these are only emitted by node iterators)
	ydb_buffer_t buffers[1];	// first of an array of buffers (typically varname)
	ydb_buffer_t buffersn[];	// rest of array
	// char *data;		// stored after `buffers` (however large they are), which point into this data
} node;
*/
import "C"

const initial_value_size = 1024 // Initial size of value storage in each node

// Create a thread-specific 'connection' object for calling the YottaDB API.
// You must use a different connection for each thread.
// Wrap C.conn in a Go struct so we can add methods to it.
type Conn struct {
	// Pointer to C.conn rather than the item itself so we can malloc it and point to it from C without Go moving it.
	c *C.conn
}

// Create a new connection for the current thread.
func NewConn() *Conn {
	// TODO: This is set to YDB_MAX_STR (1MB) for the initial version only. Later we can reduce its initial value and create logic to reallocate it when necessary,
	//       e.g. in n.Set()
	const initialSpace = C.YDB_MAX_STR
	var conn Conn
	conn.c = (*C.conn)(C.malloc(C.sizeof_conn))
	conn.c.tptoken = C.YDB_NOTTP
	// Create space for err
	conn.c.errstr.buf_addr = (*C.char)(C.malloc(C.YDB_MAX_ERRORMSG))
	conn.c.errstr.len_alloc = C.YDB_MAX_ERRORMSG
	conn.c.errstr.len_used = 0
	// Create initial space for value used by various API call/return
	conn.c.value.buf_addr = (*C.char)(C.malloc(initialSpace))
	conn.c.value.len_alloc = C.uint(initialSpace)
	conn.c.value.len_used = 0

	runtime.AddCleanup(&conn, func(cn *C.conn) {
		C.free(unsafe.Pointer(cn.value.buf_addr))
		C.free(unsafe.Pointer(cn.errstr.buf_addr))
		C.free(unsafe.Pointer(cn))
	}, conn.c)
	return &conn
}

// Return previous error message as an `error` type or nil if there was no error
func (conn *Conn) Error(code C.int) error {
	if code == C.YDB_OK {
		return nil
	}
	// Take a copy of errstr as a Go String
	msg := C.GoStringN(conn.c.errstr.buf_addr, C.int(conn.c.errstr.len_used))
	return Error(int(code), msg)
}

// Node is an object containing strings that represents a YottaDB node, supporting fast calls to the YottaDB C API.
// Stores all the supplied strings (varname and subscripts) in the Node object along with array of C.ydb_buffer_t
// structs that point to each successive string, to provide fast access to YottaDB API functions.
// Thread Safety: Regular Nodes are immutable, so are thread-safe (one thread cannot change a Node used by
// another thread). There is a mutable version of Node emitted by Node iterators (FOR loops over Node), which
// may not be shared with other threads except by first taking an immutable Node.Copy() of it.
// Wraps C.node in a Go struct so we can add methods to it.
type Node struct {
	// Pointer to C.node rather than the item itself so we can point to it from C without Go moving it.
	n    *C.node
	conn *Conn // Node.conn points to the Go conn; Node.n.conn will point directly to the C.conn
}

// Create a `Node` instance that represents a database node with class methods for fast calls to YottaDB.
// The strings and array are stored in C-allocated space to give Node methods fast access to YottaDB API functions.
func (conn *Conn) Node(varname string, subscripts ...string) (n *Node) {
	// Concatenate strings the fastest Go way.
	// This involves creating an extra copy of subscripts but is probably faster than one C.memcpy call per subscript
	var joiner bytes.Buffer
	joiner.WriteString(varname)
	for _, s := range subscripts {
		joiner.WriteString(s)
	}

	size := C.sizeof_node + C.sizeof_ydb_buffer_t*len(subscripts) + joiner.Len()
	// This initial call must be to calloc() to get initialized (cleared) storage. We cannot allocate it and then
	// do another call to initialize it as that means uninitialized memory is traversing the cgo boundary which
	// is what triggers the cgo bug mentioned in the cgo docs (https://golang.org/cmd/cgo/#hdr-Passing_pointers).
	// TODO: but if we retain calloc, we need to check for memory error because Go doesn't create a wrapper for C.calloc like it does for C.malloc (cf. https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers:~:text=C.malloc%20cannot%20fail)
	// Alternatively, we could call malloc and then memset to clear just the ydb_buffer_t parts, but test which is faster.
	var goNode Node
	n = &goNode
	n.n = (*C.node)(C.calloc(1, C.size_t(size)))
	// Queue the cleanup function to free it
	runtime.AddCleanup(n, func(c_n *C.node) {
		C.free(unsafe.Pointer(c_n))
	}, n.n)

	n.conn = conn // point to the Go conn
	c_n := n.n
	c_n.conn = (*C.conn)(unsafe.Pointer(conn.c)) // point to the C version of the conn
	c_n.len = C.int(len(subscripts) + 1)
	c_n.mutable = 0 // i.e. false

	dataptr := unsafe.Add(unsafe.Pointer(&c_n.buffers[0]), C.sizeof_ydb_buffer_t*(len(subscripts)+1))
	C.memcpy(dataptr, unsafe.Pointer(&joiner.Bytes()[0]), C.size_t(joiner.Len()))

	// Now fill in ydb_buffer_t pointers
	s := varname
	buf := (*C.ydb_buffer_t)(unsafe.Pointer(&c_n.buffers[0]))
	buf.buf_addr = (*C.char)(dataptr)
	buf.len_used, buf.len_alloc = C.uint(len(s)), C.uint(len(s))
	dataptr = unsafe.Add(dataptr, len(s))
	for i, s := range subscripts {
		buf := (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(&c_n.buffers[0]), C.sizeof_ydb_buffer_t*(i+1)))
		buf.buf_addr = (*C.char)(dataptr)
		buf.len_used, buf.len_alloc = C.uint(len(s)), C.uint(len(s))
		dataptr = unsafe.Add(dataptr, len(s))
	}
	return n
}

// Return string representation of this database node in typical YottaDB format: `varname("sub1")("sub2")`.
func (n *Node) String() string {
	var bld strings.Builder
	c_n := n.n // access C.node from Go node
	for i := range c_n.len {
		buf := (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(&c_n.buffers[0]), C.sizeof_ydb_buffer_t*i))
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
	c_n := n.n // access C.node from Go node
	conn := c_n.conn
	if len(val) > int(conn.value.len_alloc) {
		panic("YDB: tried to set database value to a string that is too large")
	}
	// TODO: should the following line change to have a C wrapper that accepts _GoString_ to avoid risk of StringData moving? Or is it OK within one line (see Pointer docs)?
	C.memcpy(unsafe.Pointer(conn.value.buf_addr), unsafe.Pointer(unsafe.StringData(val)), C.size_t(len(val)))
	conn.value.len_used = C.uint(len(val))

	ret := C.ydb_set_st(conn.tptoken, &conn.errstr, &c_n.buffers[0], c_n.len-1, (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(&c_n.buffers[0]), C.sizeof_ydb_buffer_t)), &conn.value)

	return n.conn.Error(ret)
}

// Get the value of a database node.
// On error return value "" and error
// If deflt is supplied return string deflt[0] instead of GVUNDEF or LVUNDEF errors.
func (n *Node) Get(deflt ...string) (string, error) {
	c_n := n.n // access C.node from Go node
	conn := c_n.conn
	err := C.ydb_get_st(conn.tptoken, &conn.errstr, &c_n.buffers[0], c_n.len-1, (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(&c_n.buffers[0]), C.sizeof_ydb_buffer_t)), &conn.value)
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
	value := C.GoStringN(conn.value.buf_addr, C.int(conn.value.len_used))
	return value, nil
}
