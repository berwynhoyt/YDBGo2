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

// YDBError is a structure that defines the error message format which includes both the formated $ZSTATUS
// type message and the numeric error value.
type YDBError struct {
	code int    // The error value (e.g. YDB_ERR_DBFILERR, etc)
	msg  string // The error string - generally from $ZSTATUS when available
}

// Error is a method to return the expected error message string.
func (err *YDBError) Error() string {
	return err.msg
}

// ErrorCode is a function used to find the error return code.
func (err *YDBError) Code() int {
	return err.code
}

func Error(code int, message string) error {
	return &YDBError{code, message}
}
