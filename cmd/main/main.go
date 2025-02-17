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
	"lang.yottadb.com/go/yottadb"
)

func main() {
	db := yottadb.New()
	n := db.New("^var", "sub1")
	n.Dump()
	n.Set("3")
}
