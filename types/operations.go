// Package types defines the Operation type and its possible values.
package types

type Operation int

const (
	OperationPut Operation = iota
	OperationDelete
)
