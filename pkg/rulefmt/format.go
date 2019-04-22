package rulefmt

import (
	"fmt"
	"io/ioutil"
	"runtime/debug"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/toolkits/net"
	"gopkg.in/yaml.v2"
)

const (
	TypeFrequency = iota
	TypeAny
	TypeWhiteList
	TypeBlackList
)

const MaxTimeFrame = 24 * time.Hour * 7

func isIpList(ips []string) error {
	if len(ips) < 0 {
		return fmt.Errorf("IpAddr is empty.\n")
	}
	for _, ip := range ips {
		if net.IsIntranet(ip) == false {
			return fmt.Errorf("IpAddr %s is invalid.\n", ip)
		}
	}
	return nil
}

var RuleTypes []string = []string{
	"frequency",
	"any",
	"whitelist",
	"blacklist",
}

type LowRuleError struct {
	*RuleError
}

func (e LowRuleError) Error() string {
	return e.Msg
}

type RuleError struct {
	Inner error
	Msg   string
	Stack string
	Misc  map[string]interface{}
}

func WrapRuleError(err error, msg string, msgArgs ...interface{}) *RuleError {
	return &RuleError{
		Inner: err,
		Msg:   fmt.Sprintf(msg, msgArgs...),
		Stack: string(debug.Stack()),
		Misc:  make(map[string]interface{}),
	}
}

func HandleError(err error, msg string) {
	glog.V(2).Infof("%v", err)
	fmt.Printf("%s", msg)
}

type RuleGroups struct {
	Groups []RuleGroup `yaml:"groups"`
}

type RuleGroup struct {
	Name     string        `yaml:"name"`
	Interval time.Duration `yaml:"interval,omitempty"`
	Rules    []Rule        `yaml:"rules"`
}
type RuleFilterQuery struct {
	QueryString string `yaml:"query_string"`
}
type RuleFilterTerm struct {
	Field string `yaml:"field"`
	Value string `yaml:"value"`
}
type RuleFilter struct {
	Query RuleFilterQuery `yaml:"query,omitempty"`
	Term  RuleFilterTerm  `yaml:"term,omitempty"`
}

type Rule struct {
	Alert        string            `yaml:"alert"`
	Index        string            `yaml:"index"`
	ElasticHosts string            `yaml:"es_hosts"`
	Type         string            `yaml:"type"`
	Key          string            `yaml:"key,omitempty"`
	WhiteList    []interface{}     `yaml:"whitelist,omitempty"`
	BlackList    []interface{}     `yaml:"blacklist,omitempty"`
	Filter       []RuleFilter      `yaml:"filter,omitempty"`
	NumEvents    uint64            `yaml:"num_events,omitempty"`
	TimeFrame    time.Duration     `yaml:"time_frame"`
	Labels       map[string]string `yaml:"labels,omitempty"`
	Annotations  map[string]string `yaml:"annotations,omitempty"`
}

func (r *Rule) validateFrequency() error {
	if len(r.Filter) == 0 || r.TimeFrame == 0 || r.NumEvents == 0 {
		return fmt.Errorf("type frequency rule %s's filter, time_frame, num_events should not be none.\n", r.Alert)
	}

	return nil
}

func (r *Rule) validateAny() error {
	if len(r.Filter) == 0 {
		return fmt.Errorf("type any rule %s's filtershould not be none.\n", r.Alert)
	}

	return nil
}

func (r *Rule) validateTypeWhiteBlackList() error {
	if r.Key == "" {
		return fmt.Errorf("type white/blackList rule %s's key should not be none.\n", r.Alert)
	}

	if r.Type == RuleTypes[TypeWhiteList] {
		if len(r.WhiteList) == 0 {
			return fmt.Errorf("type whiteList rule %s's whitelist should not be none.\n", r.Alert)
		}
	}
	if r.Type == RuleTypes[TypeBlackList] {
		if len(r.BlackList) == 0 {
			return fmt.Errorf("type blacklist rule %s's blacklist should not be none.\n", r.Alert)
		}
	}

	return nil
}

func (r *Rule) Validate() error {
	if err := isIpList(strings.Split(r.ElasticHosts, ",")); err != nil {
		return fmt.Errorf("rule %s es_hosts %s is invalid.\n", r.Alert, r.ElasticHosts)
	}

	if r.TimeFrame > MaxTimeFrame {
		glog.Warningf("rule %s's time_frame %v is greater than max %v. AUTO change to %v\n",
			r.Alert, r.TimeFrame, MaxTimeFrame, MaxTimeFrame)
		r.TimeFrame = MaxTimeFrame
	}

	switch r.Type {
	case RuleTypes[TypeFrequency]:
		if err := r.validateFrequency(); err != nil {
			return err
		}
	case RuleTypes[TypeAny]:
		if err := r.validateAny(); err != nil {
			return err
		}
	case RuleTypes[TypeWhiteList]:
		if err := r.validateTypeWhiteBlackList(); err != nil {
			return err
		}
	case RuleTypes[TypeBlackList]:
		if err := r.validateTypeWhiteBlackList(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unsupport type %s", r.Type)
	}

	return nil
}

func Parse(content []byte) (*RuleGroups, error) {
	var groups RuleGroups
	if err := yaml.UnmarshalStrict(content, &groups); err != nil {
		return nil, LowRuleError{WrapRuleError(err, "Unmarshal File failed: %v", err)}
	}

	return &groups, nil
}

func ParseFile(fileName string) (*RuleGroups, error) {
	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, LowRuleError{WrapRuleError(err, "Open File %s failed.", fileName)}
	}

	return Parse(b)
}
