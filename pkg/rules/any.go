package rules

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/sak0/fortuner/pkg/query"
	"github.com/sak0/fortuner/pkg/rulefmt"
)

type AnyRule struct {
	*BaseRule
	mtx          	sync.Mutex
	active       	map[uint64]*Alert
	interval     	time.Duration
	origInterval 	time.Duration
	lastEval     	time.Time
}

func (r *AnyRule) Name() string {
	return r.rule.Alert
}

func (r *AnyRule) LastEval() time.Time {
	return r.lastEval
}

func (r *AnyRule) Interval() time.Duration {
	return r.interval
}

func (r *AnyRule) updateEvalTime(ts time.Time) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.lastEval = ts
}

func (r *AnyRule) Lock() {
	r.mtx.Lock()
}

func (r *AnyRule) UnLock() {
	r.mtx.Unlock()
}

func (r *AnyRule) SlowdownEvalInterval(slowInterval time.Duration) {
	glog.V(2).Infof("Rule %s query too slow, slow donw query interval to %v\n", r.rule.Alert, r.interval)
	r.origInterval = r.interval
	r.interval = slowInterval
}

func (r *AnyRule) RestoreEvalInterval() {
	if r.origInterval != 0 {
		r.interval = r.origInterval
	}
}

func (r *AnyRule) Eval(ctx context.Context, ts time.Time) error {
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
		return fmt.Errorf("can not find index: %s on elastciSearch %s", r.rule.Index, r.rule.ElasticHosts)
	}

	filter := r.rule.Filter[0]
	startTime, _ := time.Parse("2006-01-02 15:03:04", "2019-01-01 00:00:00")

	var resultCh chan *query.QueryHitResult
	var errCh chan error

	if filter.Term.Field != "" {
		resultCh, errCh = client.GetHitsForItemWithHystrix(startTime, r.rule.Index,
			filter.Term.Field, filter.Term.Value)
	} else if filter.Query.QueryString != "" {
		resultCh, errCh = client.GetHitsForQueryStringWithHystrix(startTime, r.rule.Index,
			filter.Query.QueryString)
	}

	var validUntil time.Time
	validDuration, err := time.ParseDuration(DEFAULTVALIDDURATION)
	if err != nil {
		validUntil = ts
	} else {
		validUntil = ts.Add(validDuration)
	}

	select {
	case <-errCh:
		return err
	case result, ok := <-resultCh:
		if !ok {
			break
		}
		if int(result.Hits) >= 1 {
			glog.V(2).Infof("Rule %s query hit %d > threshold %d, trigger an alert.", r.Name(), result.Hits, 1)
			//TODO: support one alert rule for multi indices
			r.active[0] = &Alert{
				Name:        	r.rule.Alert,
				State:       	StateFiring,
				Labels:      	r.rule.Labels,
				Annotations: 	r.rule.Annotations,
				FiredAt:     	ts,
				ValidUntil:		validUntil,
			}
		}
		dynamicQueryInterval(r, result.Took)
	}

	return nil
}

func (r *AnyRule) ActiveAlerts() []*Alert {
	var res []*Alert
	for _, a := range r.currentAlerts() {
		if a.ResolvedAt.IsZero() {
			res = append(res, a)
		}
	}
	return res
}

func (r *AnyRule) currentAlerts() []*Alert {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	alerts := make([]*Alert, 0, len(r.active))

	for _, a := range r.active {
		anew := *a
		alerts = append(alerts, &anew)
	}
	return alerts
}

func NewAnyRule(rule rulefmt.Rule, interval time.Duration) *AnyRule {
	return &AnyRule{
		BaseRule: &BaseRule{rule: rule},
		mtx:      sync.Mutex{},
		active:   make(map[uint64]*Alert),
		interval: interval,
	}
}
