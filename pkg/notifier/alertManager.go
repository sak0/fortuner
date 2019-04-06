package notifier

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"bytes"
	"time"
	"golang.org/x/time/rate"
	"github.com/sak0/fortuner/pkg/utils"
)

var userAgent = fmt.Sprintf("Fortuner/%s", "0.1")
const (
	contentTypeJSON	= "application/json"
	pushURL 		= "/api/v1/alerts"
)

type AlertManager struct {
	Endpoint 	string
	client 		*http.Client
	limiter		*rate.Limiter
}

func (a *AlertManager)do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req.WithContext(ctx))
}

func (a *AlertManager)Send(ctx context.Context, b []byte) error {
	err := a.limiter.Wait(ctx)
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
		log.Printf("request failed: %v\n", resp)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad response status %v", resp.Status)
	}
	return err
}

func NewAlertManager(addr string) *AlertManager {
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

	return &AlertManager{
		Endpoint:addr + pushURL,
		client:client,
		limiter:rate.NewLimiter(limit, 10),
	}
}