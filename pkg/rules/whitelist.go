package rules

import (
	"context"
	"time"
	"sync"
	"fmt"
	"log"

	"github.com/sak0/fortuner/pkg/rulefmt"
	"github.com/sak0/fortuner/pkg/query"
)

type WhiteListRule struct {
	mtx				sync.Mutex
	rule 			rulefmt.Rule
	active 			map[uint64]*Alert
	interval 		time.Duration
	origInterval 	time.Duration
	lastEval    	time.Time

	tailTime 		time.Duration
}

var (
	defaultTailTime = 30 * time.Minute
)

func (r *WhiteListRule)Name() string {
	return r.rule.Alert
}

func (r *WhiteListRule)LastEval() time.Time {
	return r.lastEval
}

func (r *WhiteListRule)Interval() time.Duration {
	return r.interval
}

func (r *WhiteListRule)updateEvalTime(ts time.Time) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.lastEval = ts
}

func (r *WhiteListRule)Lock() {
	r.mtx.Lock()
}

func (r *WhiteListRule)UnLock() {
	r.mtx.Unlock()
}

func (r *WhiteListRule)SlowdownEvalInterval(slowInterval time.Duration) {
	log.Printf("Rule %s query too slow, slow donw query interval to %v\n", r.rule.Alert, r.interval)
	r.origInterval = r.interval
	r.interval = slowInterval
}

func (r *WhiteListRule)RestoreEvalInterval() {
	if r.origInterval != 0 {
		r.interval = r.origInterval
	}
}

func (r *WhiteListRule)queryStartTime() time.Time {
	return time.Now().Add(- r.tailTime)
}

func (r *WhiteListRule)Eval(ctx context.Context, ts time.Time) error {
	if !needEval(r, ts) {
		return nil
	}
	defer r.updateEvalTime(ts)

	var err error
	client, err := query.CreateElasticSearchClient(elasticEndpoints(r.rule.ElasticHosts))
	if err != nil {
		return err
	}
	defer client.Close()
	indices, err := client.GetIndex()
	if err != nil {
		return err
	}
	if !arrayIn(indices, r.rule.Index) {
		return fmt.Errorf("Can not find index: %s on elastciSearch %s\n", r.rule.Index, r.rule.ElasticHosts)
	}

	startTime := r.queryStartTime()

	var resultCh chan *query.QueryHitResult
	var errCh chan error

	if len(r.rule.WhiteList) > 0 {
		resultCh, errCh = client.GetHitsForNotInListWithHystrix(startTime, r.rule.Index,
			r.rule.Key, r.rule.WhiteList...)
	} else if len(r.rule.BlackList) > 0 {
		resultCh, errCh = client.GetHitsForInListWithHystrix(startTime, r.rule.Index,
			r.rule.Key, r.rule.BlackList...)
	}

	select {
	case <-errCh:
		return err
	case result, ok := <- resultCh:
		if !ok {
			break
		}
		if int(result.Hits) >= 1 {
			log.Printf("Rule %s query hit %d > threshold %d, trigger an alert.", r.Name(), result.Hits, 1)
			//TODO: support one alert rule for multi indices
			r.active[0] = &Alert{
				State:StateFiring,
				Labels:r.rule.Labels,
				Annotations:r.rule.Annotations,
				FiredAt:ts,
			}
		}
		dynamicQueryInterval(r, result.Took)
	}

	return nil
}

func (r *WhiteListRule) ActiveAlerts() []*Alert {
	var res []*Alert
	for _, a := range r.currentAlerts() {
		if a.ResolvedAt.IsZero() {
			res = append(res, a)
		}
	}
	return res
}

func (r *WhiteListRule) currentAlerts() []*Alert {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	alerts := make([]*Alert, 0, len(r.active))

	for _, a := range r.active {
		anew := *a
		alerts = append(alerts, &anew)
	}
	return alerts
}

func NewWhiteListRule(rule rulefmt.Rule, interval time.Duration, tailTime time.Duration) *WhiteListRule {
	return &WhiteListRule{
		mtx:sync.Mutex{},
		rule:rule,
		active:make(map[uint64]*Alert),
		interval:interval,
		tailTime:tailTime,
	}
}