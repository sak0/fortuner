package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
		"time"

	_ "net/http/pprof"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/time/rate"

	"github.com/sak0/fortuner/pkg/notifier"
	"github.com/sak0/fortuner/pkg/rules"
	"github.com/sak0/fortuner/pkg/utils"

		"github.com/golang/glog"
)

var (
	alertExtUrl        string
	alertManagerAddr   string
	ruleFilesPath      string
	evaluationInterval time.Duration
	updateInterval     time.Duration
	alertResendDelay   time.Duration
	queryTailTime      time.Duration
	isDistribution     bool
	enableFuzzyIndex   bool
)

type MyHandle struct {
	ruleManager *rules.RuleManager
	limiter     *rate.Limiter
}

func (h MyHandle) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	err := h.limiter.Wait(req.Context())
	if err != nil {
		fmt.Fprintf(w, "Request Failed: %v\n", err)
		return
	}

	switch req.URL.Path {
	case "/reload":
		h.ruleManager.Update()
	default:
		fmt.Fprintf(w, "xiaozhupeiqi\n")
	}
}

func init() {
	flag.StringVar(&ruleFilesPath, "rule-files-path",
		"/usr/local/fortuner/rules/", "path of rule files.")
	flag.StringVar(&alertManagerAddr, "alertmanager-addr",
		"http://10.211.160.34:9093", "alertManager webhook url")
	flag.StringVar(&alertExtUrl, "alert-ext-url",
		"dev.yonghui.cn", "external url for alert information")
	flag.DurationVar(&evaluationInterval, "evaluation-interval",
		60*time.Second, "interval for alert rule evaluation.")
	flag.DurationVar(&updateInterval, "update-interval",
		10*time.Second, "interval for update rules.")
	flag.DurationVar(&alertResendDelay, "alert-resend-delay",
		1*time.Second, "min delay for one alert resend.")
	flag.DurationVar(&queryTailTime, "query-tail-time",
		30*time.Minute, "default time range for tail of log.")
	flag.BoolVar(&isDistribution, "distribution",
		false, "is fortuner distribution deployed")
	flag.BoolVar(&enableFuzzyIndex, "enable-fuzzy-index", true, "enable Fuzzy Index, index like xxx*")
	flag.Parse()
}

func main() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	if err := watcher.Add(ruleFilesPath); err != nil {
		panic(err)
	}

	done := make(chan interface{})
	defer close(done)
	notifierManager := notifier.NewManager(done, alertManagerAddr, isDistribution)
	go notifierManager.Run()

	ctx := context.Background()
	ruleManager := rules.NewRuleManager(rules.ManagerOpts{
		RulesFilePath:    ruleFilesPath,
		Interval:         evaluationInterval,
		NotifyFunc:       sendAlerts(notifierManager, alertExtUrl),
		Ctx:              ctx,
		ResendDelay:      alertResendDelay,
		TailTime:         queryTailTime,
		EnableFuzzyIndex: enableFuzzyIndex,
	})
	ruleManager.Update()

	go func() {
		for {
			select {
			case <-done:
				return
			case ev, ok := <-watcher.Events:
				if !ok {
					return
				}
				glog.V(2).Infof("Receive fsnotify events for file %s", ev.Name)
				ruleManager.SetNeedUpdate()
			}
		}
	}()

	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(updateInterval):
				utils.DoResourceMonitor()
				if ruleManager.NeedUpdate() {
					ruleManager.Update()
				}
			}
		}
	}()

	http.ListenAndServe("0.0.0.0:8080", nil)

	//limit := utils.Per(10 * time.Second, 1)
	//h := MyHandle{
	//	ruleManager: ruleManager,
	//	limiter:     rate.NewLimiter(limit, 1),
	//}
	//srv := http.Server{
	//	Addr:         "0.0.0.0:17001",
	//	ReadTimeout:  30 * time.Second,
	//	WriteTimeout: 30 * time.Second,
	//	IdleTimeout:  60 * time.Second,
	//	Handler:      h,
	//}
	//srv.ListenAndServe()
	//
	//go func() {
	//	stopCh := make(chan os.Signal)
	//	signal.Notify(stopCh, os.Kill, os.Interrupt)
	//	<-stopCh
	//	close(done)
	//	srv.Close()
	//}()
}

type sender interface {
	Send(alerts ...*notifier.Alert)
}

func sendAlerts(s sender, externalURL string) rules.NotifyFunc {
	return func(ctx context.Context, alerts ...*rules.Alert) {
		var res []*notifier.Alert

		for _, alert := range alerts {
			a := &notifier.Alert{
				Name:         alert.Name,
				StartsAt:     alert.FiredAt,
				Labels:       alert.Labels,
				Annotations:  alert.Annotations,
				GeneratorURL: externalURL,
			}
			if !alert.ResolvedAt.IsZero() {
				a.EndsAt = alert.ResolvedAt
			} else {
				a.EndsAt = alert.ValidUntil
			}
			res = append(res, a)
		}

		if len(alerts) > 0 {
			s.Send(res...)
		}
	}
}
