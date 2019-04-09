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
	oldLen := len(m.queue)
	m.queue = append(m.queue, alerts...)
	m.more<- struct{}{}
	glog.V(2).Infof("Processing alerts %v, QueueLength [%d] -> [%d]\n", alerts, oldLen, len(m.queue))
}

func (m *NotifyManager)Run() {
Loop:
	for {
		select {
		case <-m.done:
			return
		case <-time.After(10 * time.Second):
			continue Loop
		case <-m.more:
			m.handleAlerts()
		}
	}
}

func (m *NotifyManager)Consume() []*Alert {
	m.mu.Lock()
	defer m.mu.Unlock()

	var alertsSend []*Alert
	queueLen := len(m.queue)

	if queueLen > MaxNumPerBatch {
		alertsSend = m.queue[:MaxNumPerBatch]
		m.queue = m.queue[MaxNumPerBatch:]
	} else {
		alertsSend = m.queue
		m.queue = m.queue[:0]
	}

	glog.V(2).Infof("Consume alerts: %v. QueueLength [%d] -> [%d]\n", alertsSend, queueLen, len(m.queue))
	return alertsSend
}

func (m *NotifyManager)sendOne(alert *Alert) error {
	alerts := []*Alert{alert}
	b, err := json.Marshal(alerts)
	if err != nil {
		return err
	}

	ctx := context.Background()
	return m.am.Send(ctx, b)
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

func NewManager(done chan interface{}, amAddr string) *NotifyManager {
	return &NotifyManager{
		more:make(chan interface{}),
		done:done,
		am:NewAlertManager(amAddr),
	}
}