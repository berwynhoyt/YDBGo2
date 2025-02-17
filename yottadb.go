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

// This package is a Go wrapper for a YottaDB database using the SimplaAPI interface.
// It requires Go 1.24 to achieve best speed and its use of AddCleanup() instead of SetFinalizer()
//
// This wrapper uses 'cgo' to interface between this Go wrapper and the YottaDB engine written in C.
// Its use of the `node` type to pin memory references to database subscript strings gives it optimal speed.

package yottadb

// #cgo pkg-config: yottadb
// #include "libyottadb.h"
import "C"

// WrapperRelease - (string) The Go wrapper release version for YottaDB SimpleAPI. Note the third piece of this version
// will be even for a production release and odd for a development release (branch develop). When released, depending
// on new content, either the third piece of the version will be bumped to an even value or the second piece of the
// version will be bumped by 1 and the third piece of the version set to 0. On rare occasions, we may bump the first
// piece of the version and zero the others when the changes are significant.
const WrapperRelease string = "v1.2.6"

// MinimumYDBRelease - (string) Minimum YottaDB release name required by this wrapper
const MinimumYDBRelease string = "r1.34"

// MinimumGoRelease - (string) Minimum version of Go to fully support this wrapper (including tests)
const MinimumGoRelease string = "go1.24"

//go:generate ./scripts/gen_error_codes.sh
