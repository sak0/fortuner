package rulefmt

import (
	"log"
	"time"
	"io/ioutil"
	"fmt"
	"runtime/debug"
	"gopkg.in/yaml.v2"
)

const (
	TypeFrequency = iota
	TypeAny
	TypeWhiteList
	TypeBlackList
)

var RuleTypes []string = []string{
	"frequency",
	"any",
	"whitelist",
	"blacklist",
}

type LowRuleError struct {
	*RuleError
}
func (e LowRuleError)Error()string{
	return e.Msg
}

type RuleError struct {
	Inner	error
	Msg 	string
	Stack 	string
	Misc 	map[string]interface{}
}

func WrapRuleError(err error, msg string, msgArgs ...interface{})*RuleError {
	return &RuleError{
		Inner:err,
		Msg:fmt.Sprintf(msg, msgArgs...),
		Stack:string(debug.Stack()),
		Misc:make(map[string]interface{}),
	}
}

func HandleError(err error, msg string){
	log.Printf("%v", err)
	fmt.Printf("%s", msg)
}

type RuleGroups struct {
	Groups	[]RuleGroup 	`yaml:"groups"`
}

// RuleGroup is a list of sequentially evaluated recording and alerting rules.
type RuleGroup struct {
	Name     string         `yaml:"name"`
	Interval time.Duration	`yaml:"interval,omitempty"`
	Rules    []Rule         `yaml:"rules"`
}
type RuleFilterQuery struct {
	QueryString 	string 	`yaml:"query_string"`
}
type RuleFilterTerm struct {
	Field  	string	`yaml:"field"`
	Value 	string  `yaml:"value"`
}
type RuleFilter struct {
	Query   RuleFilterQuery	`yaml:"query,omitempty"`
	Term	RuleFilterTerm 	`yaml:"term,omitempty"`
}
type Rule struct {
	Alert 			string 				`yaml:"alert"`
	Index			string				`yaml:"index"`
	ElasticHosts	string				`yaml:"es_hosts"`
	Type			string				`yaml:"type"`
	Filter			[]RuleFilter		`yaml:"filter"`
	NumEvents		int					`yaml:"num_events,omitempty"`
	TimeFrame 		time.Duration		`yaml:"time_frame"`
	Labels			map[string]string	`yaml:"labels,omitempty"`
	Annotations		map[string]string	`yaml:"annotations,omitempty"`
}

func (r *Rule)Validate()error {
	return nil
}

func Parse(content []byte)(*RuleGroups, error) {
	var groups RuleGroups
	if err := yaml.UnmarshalStrict(content, &groups); err != nil {
		return nil, LowRuleError{WrapRuleError(err, "Unmarshal File failed: %v", err)}
	}

	return &groups, nil
}

func ParseFile(fileName string)(*RuleGroups, error) {
	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, LowRuleError{WrapRuleError(err, "Open File %s failed.", fileName)}
	}

	return Parse(b)
}