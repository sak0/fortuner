package rules

import (
	"context"
	"log"
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

type Rule interface {
	ActiveAlerts() []*Alert
	Eval(ctx context.Context, time time.Time)error
	Name()string
	Lock()
	UnLock()
	SlowdownEvalInterval(duration time.Duration)
	RestoreEvalInterval()
	LastEval() time.Time
	Interval() time.Duration
}

func dynamicQueryInterval(rule Rule, lastTook int64) {
	rule.Lock()
	defer rule.UnLock()

	if lastTook > SLOWQUERYTOOK {
		rule.SlowdownEvalInterval(SLOWQUERYINTERVAL)
	} else {
		rule.RestoreEvalInterval()
	}
}

func needEval(rule Rule, ts time.Time) bool {
	if ts.After(rule.LastEval().Add(rule.Interval())) {
		log.Printf("%s\t interval check: %v beyond eval time line: %v + %v",
			rule.Name(),
			ts.Format("2006-01-02 03:04:05 PM"),
			rule.LastEval().Format("2006-01-02 03:04:05 PM"),
			rule.Interval())
		return true
	} else {
		log.Printf("%s\t interval check: %v behind eval time line: %v + %v. skip eval this time.",
			rule.Name(),
			ts.Format("2006-01-02 03:04:05 PM"),
			rule.LastEval().Format("2006-01-02 03:04:05 PM"),
			rule.Interval())
		return false
	}
}