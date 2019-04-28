package rules

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/sak0/fortuner/pkg/query"
	"github.com/sak0/fortuner/pkg/rulefmt"
)

const (
	SLOWQUERYTOOK     		= 2000
	SLOWQUERYINTERVAL 		= 2 * time.Minute

	DEFAULTVALIDDURATION	= "15m"
)

func elasticEndpoints(addr string) []string {
	var addrs []string
	for _, addr := range strings.Split(addr, ",") {
		if !strings.HasPrefix(addr, "http://") {
			addr = "http://" + addr
		}
		addrs = append(addrs, addr)
	}
	return addrs
}

func matchKey(sample map[string]string, filters map[string]string) bool {
	for filterKey, filterValue := range filters {
		sampleValue, ok := sample[filterKey]
		if !ok {
			return false
		}
		if sampleValue != filterValue {
			return false
		}
	}

	return true
}

func arrayIn(arr []string, item string) bool {
	for _, v := range arr {
		if v == item {
			return true
		}
	}
	return false
}

type FrequencyRule struct {
	*BaseRule
	mtx          sync.Mutex
	active       map[uint64]*Alert
	interval     time.Duration
	origInterval time.Duration
	lastEval     time.Time
}

func (r *FrequencyRule) Name() string {
	return r.rule.Alert
}

func (r *FrequencyRule) LastEval() time.Time {
	return r.lastEval
}

func (r *FrequencyRule) Interval() time.Duration {
	return r.interval
}

func (r *FrequencyRule) updateEvalTime(ts time.Time) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.lastEval = ts
}

func (r *FrequencyRule) Lock() {
	r.mtx.Lock()
}

func (r *FrequencyRule) UnLock() {
	r.mtx.Unlock()
}

func (r *FrequencyRule) SlowdownEvalInterval(slowInterval time.Duration) {
	glog.V(2).Infof("Rule %s query too slow, slow donw query interval to %v\n", r.rule.Alert, r.interval)
	r.origInterval = r.interval
	r.interval = slowInterval
}

func (r *FrequencyRule) RestoreEvalInterval() {
	if r.origInterval != 0 {
		r.interval = r.origInterval
	}
}

func (r *FrequencyRule) Eval(ctx context.Context, ts time.Time) error {
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

	filter := r.rule.Filter[0]
	startTime := time.Now().Add(-r.rule.TimeFrame)

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
		if uint64(result.Hits) >= r.rule.NumEvents {
			glog.V(2).Infof("Rule %s query hit %d > threshold %d, trigger an alert.", r.Name(), result.Hits, r.rule.NumEvents)
			//TODO: support one alert rule for multi indices
			r.active[0] = &Alert{
				Name:        r.rule.Alert,
				State:       StateFiring,
				Labels:      r.rule.Labels,
				Annotations: r.rule.Annotations,
				FiredAt:     ts,
				ValidUntil:  validUntil,
			}
		}
		dynamicQueryInterval(r, result.Took)
	}

	return nil
}

func (r *FrequencyRule) ActiveAlerts() []*Alert {
	var res []*Alert
	for _, a := range r.currentAlerts() {
		if a.ResolvedAt.IsZero() {
			res = append(res, a)
		}
	}
	return res
}

func (r *FrequencyRule) currentAlerts() []*Alert {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	alerts := make([]*Alert, 0, len(r.active))

	for _, a := range r.active {
		anew := *a
		alerts = append(alerts, &anew)
	}
	return alerts
}

func NewFrequencyRule(rule rulefmt.Rule, interval time.Duration) *FrequencyRule {
	return &FrequencyRule{
		BaseRule: &BaseRule{rule: rule},
		mtx:      sync.Mutex{},
		active:   make(map[uint64]*Alert),
		interval: interval,
	}
}
