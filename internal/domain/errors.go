package domain

import "errors"

var ErrStub = errors.New("not implemented")

// Domain-level sentinel errors.
var (
	ErrNotFound         = errors.New("not found")
	ErrAlreadyExists    = errors.New("already exists")
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrConflict         = errors.New("conflicting state")
	ErrRolloutNotActive = errors.New("rollout not active")
	ErrDeviceBusy       = errors.New("device locked by another active rollout")
	ErrStaleReport      = errors.New("stale or duplicate report")
)
