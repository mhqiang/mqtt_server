package utils

import (
	"log"
	"testing"
	"time"
	"utils/semaphore"

	"github.com/olivere/elastic"
	"go.uber.org/zap/zapcore"
)

func TestMulGet(t *testing.T) {
	Log := NewLogger("v3.log", zapcore.InfoLevel, 128, 100, 7, true, "v3")
	es, err := InitES("sms", "sms", []string{}, Log)
	if err != nil {
		log.Panic("init es err", err)
	}

	multiItems := []*elastic.MultiGetItem{}
	idList := []string{}

	for _, value := range idList {
		multiItems = append(multiItems, elastic.NewMultiGetItem().Index(es.IndexName).Type(es.TypeName).Id(value))
	}

	esSem := semaphore.NewWeighted(100)
	resultChan := make(chan interface{}, 1)
	dataChan := make(chan *Multi, 1)
	dataChan <- &Multi{
		ResultChan: resultChan,
		Query:      multiItems,
	}
	es.MulGet(esSem, dataChan)
}

func TestSearch(t *testing.T) {
	Log := NewLogger("v3.log", zapcore.InfoLevel, 128, 100, 7, true, "v3")
	es, err := InitES("sms", "sms", []string{}, Log)
	if err != nil {
		log.Panic("init es err", err)
	}

	content := "t1"
	query := elastic.NewBoolQuery()
	query.Must(elastic.NewRangeQuery("deliverd_result").Gte(1))
	query.Must(elastic.NewMatchQuery("content", content))

	curTime := time.Now()
	tm1 := time.Date(curTime.Year(), curTime.Month(), curTime.Day(), 0, 0, 0, 0, curTime.Location())
	start := tm1.UnixNano() / 1000
	curTime = time.Now()
	tm1 = time.Date(curTime.Year(), curTime.Month(), curTime.Day(), 23, 59, 59, 59, curTime.Location())
	end := tm1.UnixNano() / 1000
	query.Filter(elastic.NewRangeQuery("send_date").Gte(start).Lte(end))
	query.Must(elastic.NewTermsQuery("phone", 13888888123))

	esSem := semaphore.NewWeighted(100)
	recordChan := make(chan *Search, 10)

	recordChan <- &Search{
		ResultChan: make(chan interface{}, 1),
		Query:      query,
	}

	es.Search(esSem, recordChan)
}
