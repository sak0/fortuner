package query

import (
	"context"
	"errors"
	"log"
	"os"
	"time"
	"github.com/olivere/elastic"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/golang/glog"
)

const (
	DefaultHystrixTimeout 		= 5000
	DefaultHystrixMaxRequest	= 10
)

type QueryHitResult struct {
	Hits int64
	Took int64
}

type ElasticClient struct{
	client	*elastic.Client
}

func (es *ElasticClient)GetIndex() ([]string, error) {
	return es.client.IndexNames()
}

func (es *ElasticClient)GetHitsForNotInListWithHystrix(startTime time.Time,
	indexName string, key string, blackList ...interface{}) (chan *QueryHitResult, chan error) {
	ctx := context.Background()
	resultCh := make(chan *QueryHitResult)

	hystrix.ConfigureCommand(indexName, hystrix.CommandConfig{
		Timeout: DefaultHystrixTimeout,
		MaxConcurrentRequests: DefaultHystrixMaxRequest,
	})

	errCh := hystrix.Go(indexName, func() error {
		defer close(resultCh)
		whiteListQ := elastic.NewBoolQuery()
		whiteListQ = whiteListQ.MustNot(elastic.NewTermsQuery(key, blackList...))
		whiteListQ = whiteListQ.Filter(elastic.NewRangeQuery("@timestamp").Gt(startTime))
		whiteListResult, err := es.client.Search(indexName).
			Query(whiteListQ).
			Type("doc").
			Pretty(true).Do(ctx)
		if err != nil {
			return err
		}
		result := &QueryHitResult{
			Hits:whiteListResult.TotalHits(),
			Took:whiteListResult.TookInMillis,
		}
		resultCh<- result
		glog.V(3).Infof("BlackList count: %d take %v millisecond\n",
			whiteListResult.TotalHits(), whiteListResult.TookInMillis)
		return nil
	}, nil)


	return resultCh, errCh
}

func (es *ElasticClient)GetHitsForInListWithHystrix(startTime time.Time,
	indexName string, key string, whiteList ...interface{}) (chan *QueryHitResult, chan error) {
	ctx := context.Background()
	resultCh := make(chan *QueryHitResult)

	hystrix.ConfigureCommand(indexName, hystrix.CommandConfig{
		Timeout: DefaultHystrixTimeout,
		MaxConcurrentRequests: DefaultHystrixMaxRequest,
	})

	errCh := hystrix.Go(indexName, func() error {
		defer close(resultCh)
		whiteListQ := elastic.NewBoolQuery()
		whiteListQ = whiteListQ.Must(elastic.NewTermsQuery(key, whiteList...))
		whiteListQ = whiteListQ.Filter(elastic.NewRangeQuery("@timestamp").Gt(startTime))
		whiteListResult, err := es.client.Search(indexName).
			Query(whiteListQ).
			Type("doc").
			Pretty(true).Do(ctx)
		if err != nil {
			return err
		}
		result := &QueryHitResult{
			Hits:whiteListResult.TotalHits(),
			Took:whiteListResult.TookInMillis,
		}
		resultCh<- result
		glog.V(3).Infof("WhiteList count: %d take %v millisecond\n",
			whiteListResult.TotalHits(), whiteListResult.TookInMillis)
		return nil
	}, nil)


	return resultCh, errCh
}

func (es *ElasticClient)GetHitsForQueryStringWithHystrix(startTime time.Time,
	indexName string, msg string) (chan *QueryHitResult, chan error) {
	ctx := context.Background()
	resultCh := make(chan *QueryHitResult)

	hystrix.ConfigureCommand(indexName, hystrix.CommandConfig{
		Timeout: DefaultHystrixTimeout,
		MaxConcurrentRequests: DefaultHystrixMaxRequest,
	})

	errCh := hystrix.Go(indexName, func() error {
		defer close(resultCh)

		wildCardMsg := "*" + msg + "*"
		msgWildcardQuery := elastic.NewBoolQuery()
		msgWildcardQuery =	msgWildcardQuery.Must(elastic.NewWildcardQuery("message", wildCardMsg))
		msgWildcardQuery = msgWildcardQuery.Filter(elastic.NewRangeQuery("@timestamp").Gt(startTime))
		msgWildcardResult, err := es.client.Search(indexName).
			Query(msgWildcardQuery).Type("doc").Do(ctx)

		if err != nil {
			return err
		}
		glog.V(3).Infof("msgWildcardQuery count: %d take %v milliseconds.\n",
			msgWildcardResult.TotalHits(), msgWildcardResult.TookInMillis)
		result := &QueryHitResult{
			Hits:msgWildcardResult.TotalHits(),
			Took:msgWildcardResult.TookInMillis,
		}

		resultCh<- result
		return nil
	}, nil)

	return resultCh, errCh
}

func (es *ElasticClient)GetHitsForQueryString(startTime time.Time, indexName string, msg string) (int64, error) {
	ctx := context.Background()

	wildCardMsg := "*" + msg + "*"
	msgWildcardQuery := elastic.NewBoolQuery()
	msgWildcardQuery =	msgWildcardQuery.Must(elastic.NewWildcardQuery("message", wildCardMsg))
	msgWildcardQuery = msgWildcardQuery.Filter(elastic.NewRangeQuery("@timestamp").Gt(startTime))
	msgWildcardResult, err := es.client.Search(indexName).
		Query(msgWildcardQuery).Type("doc").Do(ctx)

	if err != nil {
		return 0, err
	}
	glog.V(3).Infof("msgWildcardQuery count: %d take %v milliseconds.\n",
		msgWildcardResult.TotalHits(), msgWildcardResult.TookInMillis)

	return msgWildcardResult.TotalHits(), nil
}

func (es *ElasticClient)GetHitsForItemWithHystrix(startTime time.Time,
	indexName string, key string, value string) (chan *QueryHitResult, chan error) {
	ctx := context.Background()
	resultCh := make(chan *QueryHitResult)

	hystrix.ConfigureCommand(indexName, hystrix.CommandConfig{
		Timeout: 5000,
		MaxConcurrentRequests: 10,
	})
	errCh := hystrix.Go(indexName, func() error {
		defer close(resultCh)

		boolQ := elastic.NewBoolQuery()
		boolQ = boolQ.Must(elastic.NewMatchQuery(key, value))
		boolQ = boolQ.Filter(elastic.NewRangeQuery("@timestamp").Gt(startTime))
		bQueryResult, err := es.client.Search(indexName).
			Query(boolQ).
			Type("doc").
			Pretty(true).Do(ctx)
		if err != nil {
			return err
		}
		glog.V(3).Infof("GetHitsForItem count: %d take %v millisecond\n",
			bQueryResult.TotalHits(), bQueryResult.TookInMillis)

		result := &QueryHitResult{
			Hits: bQueryResult.TotalHits(),
			Took: bQueryResult.TookInMillis,
		}
		resultCh<- result
		return nil
	}, nil)

	return resultCh, errCh
}

func (es *ElasticClient)GetHitsForItem(startTime time.Time, indexName string, key string, value string) (int64, error) {
	ctx := context.Background()

	boolQ := elastic.NewBoolQuery()
	boolQ = boolQ.Must(elastic.NewMatchQuery(key, value))
	boolQ = boolQ.Filter(elastic.NewRangeQuery("@timestamp").Gt(startTime))
	bQueryResult, err := es.client.Search(indexName).
		Query(boolQ).
		Type("doc").
		Pretty(true).Do(ctx)
	if err != nil {
		return 0, err
	}
	glog.V(3).Infof("GetHitsForItem count: %d take %v millisecond\n", bQueryResult.TotalHits(), bQueryResult.TookInMillis)
	return bQueryResult.TotalHits(), nil
}

func (es *ElasticClient)DeleteIndex(indexName string)error{
	ctx := context.Background()
	deleteResp, err := es.client.DeleteIndex(indexName).Do(ctx)
	if err != nil {
		return err
	}
	if !deleteResp.Acknowledged{
		return errors.New("Delete index error.\n")
	}
	return nil
}

func (es *ElasticClient)Close() {
	es.client.Stop()
}

func CreateElasticSearchClient(addrs []string)(*ElasticClient, error){
	client, err := elastic.NewClient(
		elastic.SetURL(addrs...),
		elastic.SetSniff(false),
		elastic.SetHealthcheckInterval(10 * time.Second),
		elastic.SetGzip(true),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)))
	if err != nil{
		return nil, err
	}
	return &ElasticClient{
		client:client,
	}, nil
}