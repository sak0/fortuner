package notifier

import (
	"context"
	"fmt"
	"net/http"
	"bytes"
	"time"
	"golang.org/x/time/rate"
	"github.com/golang/glog"
	"github.com/sak0/fortuner/pkg/utils"
	myrate "github.com/sak0/fortuner/pkg/rate"
)

var userAgent = fmt.Sprintf("Fortuner/%s", "0.1")
const (
	contentTypeJSON	= "application/json"
	pushURL 		= "/api/v1/alerts"
)

type Limiter interface {
	//Allow() bool
	//AllowN(now time.Time, n int) bool
	Wait(ctx context.Context) error
}

type AlertManager struct {
	Endpoint 	string
	client 		*http.Client
	limiter		Limiter
}

func (a *AlertManager)do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req.WithContext(ctx))
}

func (a *AlertManager)Send(ctx context.Context, b []byte) error {
	newCtx, _ := context.WithTimeout(ctx, 10 * time.Second)
	err := a.limiter.Wait(newCtx)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", a.Endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", contentTypeJSON)
	resp, err := a.do(ctx, a.client, req)
	if err != nil {
		glog.Errorf("request failed: %v\n", resp)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad response status %v", resp.Status)
	}
	return err
}

func NewAlertManager(addr string, distribution bool) *AlertManager {
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{
		Transport: tr,
		Timeout: 30 * time.Second,
	}

	// Rate limit for call AlertManager API

	limit := utils.Per(1 * time.Second, 5)
	var limiter Limiter
	var err error
	Loop:
		for {
			if distribution {
				limit := float64(1/5)
				limiter, err = myrate.NewLimiter(limit, 10)
				if err != nil {
					glog.Errorf("Get distribution rate limiter %v failed: %v, use local rate limit", err, limiter)
					distribution = false
				} else {
					break Loop
				}
			} else {
				limiter = rate.NewLimiter(limit, 10)
				break Loop
			}
		}

	return &AlertManager{
		Endpoint:addr + pushURL,
		client:client,
		limiter:limiter,
	}
}