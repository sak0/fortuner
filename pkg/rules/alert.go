package rules

import (
		"time"
)

type AlertState int
const (
	StateInactive AlertState = iota
	StatePending
	StateFiring
)

type Alert struct {
	State 		AlertState

	Labels      map[string]string
	Annotations map[string]string

	Value 		float64

	ActiveAt   	time.Time
	FiredAt    	time.Time
	ResolvedAt 	time.Time
	LastSentAt 	time.Time
	ValidUntil 	time.Time
}