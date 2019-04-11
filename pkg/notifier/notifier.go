package notifier

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"github.com/golang/glog"
)

const MaxNumPerBatch = 64

type Alert struct {
	Name            string              `json:"name"`
	Labels 			map[string]string 	`json:"labels"`
	Annotations 	map[string]string 	`json:"annotations"`
	StartsAt    	time.Time 			`json:"startsAt,omitempty"`
	EndsAt       	time.Time 			`json:"endsAt,omitempty"`
	GeneratorURL 	string    			`json:"generatorURL,omitempty"`
}

type NotifyManager struct {
	queue 			[]*Alert
	more 			chan interface{}
	QueueCapacity 	int
	mu 				sync.RWMutex
	done 			chan interface{}
	am 				*AlertManager
}

func (m *NotifyManager)Send(alerts ...*Alert) {
	//Send alerts to AlertManager
	// TODO: Process length of queue
	m.mu.Lock()
	defer m.mu.Unlock()

	oldLen := len(m.queue)
	m.queue = append(m.queue, alerts...)
	alertNames := func()[]string{
		var names []string
		for _, alert := range alerts {
			names = append(names, alert.Name)
		}
		return names
	}()
	glog.V(2).Infof("Processing alerts %v, QueueLength [%d] -> [%d]\n", alertNames, oldLen, len(m.queue))
	m.more<- struct{}{}

	glog.V(2).Infof("Send over alerts %v, QueueLength [%d] -> [%d]\n", alertNames, oldLen, len(m.queue))
}

func (m *NotifyManager)Run() {
	for {
		select {
		case <-m.done:
			return
		case <-m.more:
			glog.Infof("has pending alerts, trigger handleAlerts.\n")
			go m.handleAlerts()
		}
	}
}

func (m *NotifyManager)Consume() []*Alert {
	m.mu.Lock()
	defer m.mu.Unlock()

	var alertsSend []*Alert
	queueLen := len(m.queue)

	if queueLen > MaxNumPerBatch {
		glog.V(2).Infof("notify queue length %d is large the threshold %d\n", queueLen, MaxNumPerBatch)
		alertsSend = m.queue[:MaxNumPerBatch]
		m.queue = m.queue[MaxNumPerBatch:]
	} else {
		alertsSend = m.queue
		m.queue = m.queue[:0]
	}

	alertNames := func()[]string{
		var names []string
		for _, alert := range alertsSend {
			names = append(names, alert.Name)
		}
		return names
	}()

	glog.V(2).Infof("Consume alerts: %v. QueueLength [%d] -> [%d]\n", alertNames, queueLen, len(m.queue))
	return alertsSend
}

func (m *NotifyManager)sendOne(alert *Alert) error {
	alerts := []*Alert{alert}
	b, err := json.Marshal(alerts)
	if err != nil {
		return err
	}

	ctx := context.Background()
	return m.am.Send(ctx, b, alert.Name)
}

func (m *NotifyManager)sendAll(alerts ...*Alert) {
	for _, alert := range alerts {
		if err := m.sendOne(alert); err != nil {
			glog.Errorf("Send alert failed: %v\n", err)
			continue
		}
	}
}

func (m *NotifyManager)handleAlerts() {
	alerts := m.Consume()
	m.sendAll(alerts...)

	if len(m.queue) > 0 {
		select {
		case m.more<- struct{}{}:
		default:
		}
	}
}

func NewManager(done chan interface{}, amAddr string, isDistribution bool) *NotifyManager {
	return &NotifyManager{
		more:make(chan interface{}),
		done:done,
		am:NewAlertManager(amAddr, isDistribution),
	}
}