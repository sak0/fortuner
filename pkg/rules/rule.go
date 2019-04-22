package rules

import (
	"errors"
	"sort"
	"strings"

	"github.com/sak0/fortuner/pkg/query"
	"github.com/sak0/fortuner/pkg/rulefmt"

	"github.com/golang/glog"
)

// BaseRule about base rule
type BaseRule struct {
	rule           rulefmt.Rule
	bootstrapIndex string
}

// DetermineIndex Determine Index
func (br *BaseRule) DetermineIndex(enableFuzzyIndex bool) error {
	if br.bootstrapIndex == "" {
		br.bootstrapIndex = br.rule.Index
	}
	if !enableFuzzyIndex {
		return nil
	}
	// index must include `*`
	if !strings.HasSuffix(br.bootstrapIndex, "*") {
		return nil
	}

	client, err := query.CreateElasticSearchClient(elasticEndpoints(br.rule.ElasticHosts))
	if err != nil {
		glog.Errorf("determine index init client error: %v", err)
		return err
	}
	names, err := client.GetIndexByPrefix(br.bootstrapIndex)
	if err != nil {
		glog.Errorf("cannot get index(s) error: %v", err)
		return err
	}
	if len(names) == 0 {
		return errors.New("index(s) length as 0")
	}

	// TODO: Best method used elastic search api
	sort.Slice(names, func(i, j int) bool {
		return names[i] > names[j]
	})
	br.rule.Index = names[0]
	return nil
}
