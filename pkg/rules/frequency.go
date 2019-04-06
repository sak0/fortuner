package rules

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sak0/fortuner/pkg/rulefmt"
	"github.com/sak0/fortuner/pkg/query"
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

func matchKey(sample map[string]string, filters map[string]string)bool {
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

func arrayIn(arr []string, item string)bool{
	for _, v := range arr{
		if v == item{
			return true
		}
	}
	return false
}

type FrequencyRule struct {
	mtx		sync.Mutex
	rule 	rulefmt.Rule
	active 	map[uint64]*Alert
}

func (r *FrequencyRule)Name() string {
	return r.rule.Alert
}

func (r *FrequencyRule)Eval(ctx context.Context, ts time.Time) error {
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

	select {
	case <-errCh:
		return err
	case result, ok := <- resultCh:
		if !ok {
			break
		}
		if int(result.Hits) >= r.rule.NumEvents {
			log.Printf("Rule %s query hit %d > threshold %d, trigger an alert.", r.Name(), result.Hits, r.rule.NumEvents)
			//TODO: support one alert rule for multi indecis
			r.active[0] = &Alert{
				State:StateFiring,
				Labels:r.rule.Labels,
				Annotations:r.rule.Annotations,
				FiredAt:ts,
			}
		}
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

func NewFrequencyRule(rule rulefmt.Rule) *FrequencyRule {
	return &FrequencyRule{
		mtx:sync.Mutex{},
		rule:rule,
		active:make(map[uint64]*Alert),
	}
}