package utils

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"utils/semaphore"

	"github.com/cespare/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/olivere/elastic"
	"go.uber.org/zap"
)

var Json = jsoniter.ConfigCompatibleWithStandardLibrary

const ErrInfo = "err info"

type Document struct {
	MsgID             string            `json:"message_id,omitempty"`
	RDate             int64             `json:"receive_date,omitempty"`
	SDate             int64             `json:"send_date,omitempty"`
	BDate             []int64           `json:"back_date,omitempty"`
	Phone             int64             `json:"phone,omitempty"`
	UserID            int               `json:"user_id,omitempty"`
	ExtendCode        string            `json:"extend_code,omitempty"`
	SubExtendCode     string            `json:"sub_extend_code,omitempty"`
	SendExtendCode    string            `json:"sending_extend_code,omitempty"`
	PlatCount         int               `json:"plat_count,omitempty"`
	OperationCount    int               `json:"operation_count,omitempty"`
	BackType          int               `json:"back_type,omitempty"`
	MessID            []int64           `json:"messid,omitempty"`
	CustomMessid      []string          `json:"custom_messid,omitempty"`
	Content           string            `json:"content,omitempty"`
	Seq               int64             `json:"seq,omitempty"`
	DeliverResult     []string          `json:"deliverd_result,omitempty"`
	DeliveredCount    int               `json:"delivred_count,omitempty"`
	OthersCount       int               `json:"other_count,omitempty"`
	ResentCount       int               `json:"resent_count,omitempty"`
	Channel           int64             `json:"channel,omitempty"`
	SignName          string            `json:"sign_name,omitempty"`
	Status            int               `json:"status"`
	Index             int               `json:"index,omitempty"`
	LastBackDate      int64             `json:"last_back_date"`
	DeliverResultJson map[string]string `json:"deliver_result_json,omitempty"`
	BackDateJson      map[string]int64  `json:"back_date_json,omitempty"`
	Finished          int               `json:"finished,omitempty"` // 0 未完成， 1完成
}

type ESChanValue struct {
	ResultChan   chan interface{}
	InsertRecord *elastic.BulkService
}

type SentResponse struct {
	Code    int      `json:"code"`
	Results []Result `json:"results,omitempty"`
	Msg     string   `json:"msg"`
}

type Result struct {
	Status int    `json:"status"`
	Msg    string `json:"msg"`          // 错误信息
	ID     string `json:"id,omitempty"` // es id
}

type Elastic struct {
	Client    *elastic.Client
	IndexName string
	TypeName  string
	Log       *zap.Logger
}

type Search struct {
	ResultChan chan interface{}
	Query      *elastic.BoolQuery
	// Agg        *elastic.TermsAggregation
	Count         int
	SortField     string
	Ascending     bool
	Status        int
	Scope         int
	StatusCode    string
	ContentRepeat bool
}

type Multi struct {
	ResultChan chan interface{}
	Status     int
	Query      []*elastic.MultiGetItem
}

// 普通聚合查询
type Aggregations struct {
	Info TotalMetric `json:"info_agg"`
}

type TotalMetric struct {
	Buckets []Metric `json:"buckets"`
}

type Metric struct {
	Key         string      `json:"key"`
	DocCount    int64       `json:"doc_count"`
	SearchValue MetricValue `json:"top_index"`
	MaxValue    interface{} `json:"max_index"`
}

type MetricValue struct {
	Hits HitValue `json:"hits"`
}

type HitValue struct {
	Total int64         `json:"total"`
	Score interface{}   `json:"max_score"`
	Hits  []HitDocument `json:"hits"`
}

type HitDocument struct {
	Index  string        `json:"_index"`
	Type   string        `json:"_type"`
	ID     string        `json:"_id"`
	Score  interface{}   `json:"_score"`
	Source Document      `json:"_source"`
	Sort   []interface{} `json:"sort"`
}

type SearchResponse struct {
	Code     int        `json:"code"`
	Datas    []Document `json:"datas,omitempty"`
	ESIDList []string   `json:"es_id_list"`
	Msg      string     `json:"msg"`
}

type ScrollResponse struct {
	Code   int          `json:"code"`
	Result []FeedResult `json:"datas,omitempty"`
	Msg    string       `json:"msg"`
	Total  int          `json:"total"`
}

type FeedResult struct {
	StatusCode string `json:"statusCode"`
	Count      int    `json:"count"`
	Status     int    `json:"status"`
}

// scroll 查询

type Scroll struct {
	Info []Doc `json:"docs"`
}

type Doc struct {
	Index       string   `json:"_index"`
	Type        string   `json:"_type"`
	ID          string   `json:"_id"`
	Version     int      `json:"_version,omitempty"`
	Seq         int      `json:"_seq_no,omitempty"`
	PrimaryTerm int      `json:"_primary_term,omitempty"`
	Found       bool     `json:"found"`
	Source      Document `json:"_source,omitempty"`
}

func InitES(indexName string, typeName string, esName []string, logger *zap.Logger) (*Elastic, error) {
	// Create a client
	es := &Elastic{IndexName: indexName, TypeName: typeName, Log: logger}

	client, err := elastic.NewClient(elastic.SetURL(esName...))
	if err != nil {
		logger.Error("connect es error")
		return nil, err
	}
	es.Client = client
	logger.Info("connect es success")
	return es, nil
}

//
func (es *Elastic) BulkInsert(sem *semaphore.Weighted, records interface{}) (interface{}, error) {
	defer sem.Release(1)
	recordChan := records.(chan ESChanValue)
	defer func() { // 必须要先声明defer，否则不能捕获到panic异常

		if err := recover(); err != nil {
			es.Log.Error("recover", zap.String(ErrInfo, "bulk insert panic")) // 这里的err其实就是panic传入的内容，55
		}
	}()
	for {
		esChanValue := <-recordChan
		// index1Req := elastic.NewBulkIndexRequest().Index(es.IndexName)
		// bulkRequest = bulkRequest.Add(requestList)
		bulkRequest := esChanValue.InsertRecord
		bulkResponse, err := bulkRequest.Do(context.Background())

		// bulkResponse, err := bulkRequest.Do(context.TODO())
		if err != nil {
			es.Log.Error("bulkes request err", zap.String(ErrInfo, err.Error()))
			esChanValue.ResultChan <- &SentResponse{Code: http.StatusBadRequest, Msg: err.Error()}
			continue
		}

		// 结果有错误
		if bulkResponse.Items == nil && bulkResponse.Errors {
			es.Log.Error("bulk insert all err but no details")
			esChanValue.ResultChan <- &SentResponse{Code: http.StatusBadRequest, Msg: err.Error()}
			continue
		}

		sentResponse := SentResponse{
			Code: http.StatusOK,
		}

		errItems := bulkResponse.Failed()
		for _, item := range errItems {
			es.Log.Error("insert err",
				zap.String("id", item.Id),
				zap.String(ErrInfo, item.Error.Reason),
			)
		}

		for _, result := range bulkResponse.Indexed() {

			curResult := Result{}

			if result.Status >= 200 && result.Status <= 299 {
				curResult.Status = http.StatusOK
				curResult.ID = result.Id
			} else {
				es.Log.Error("insert es err", zap.String(ErrInfo, result.Error.Reason))
				curResult.Status = http.StatusBadRequest
				curResult.Msg = result.Error.Reason
			}
			sentResponse.Results = append(sentResponse.Results, curResult)

		}
		esChanValue.ResultChan <- &sentResponse
	}
}

func (es *Elastic) Search(sem *semaphore.Weighted, dataChan chan *Search) {
	defer sem.Release(1)
	for {
		data := <-dataChan
		query := data.Query
		sortField := data.SortField
		ascending := data.Ascending
		count := data.Count
		status := data.Status
		statusCode := data.StatusCode
		repeat := data.ContentRepeat

		es.Log.Error("================================",
			zap.String("statuscode", statusCode),
			zap.Bool("repeat", repeat),
		)

		// result, err := es.Client.Scroll(es.IndexName).
		// 	// Index(es.IndexName).
		// 	Type(es.TypeName).
		// 	SearchSource(searchSource).
		// 	Query(query).
		// 	Size(1).
		// 	// Aggregation("info_agg", agg).
		// 	Do(context.Background())

		result, err := es.Client.Search().
			Index(es.IndexName).
			Type(es.TypeName).
			Query(query).
			Sort(sortField, ascending).
			Size(count).
			// Aggregation("info_agg", agg).
			Do(context.Background())

		searchResponse := SearchResponse{}
		if err != nil {
			searchResponse.Code = http.StatusBadRequest
			searchResponse.Msg = err.Error()
			es.Log.Error("search err", zap.String(ErrInfo, err.Error()))
			data.ResultChan <- &searchResponse
			continue
		}

		es.Log.Error("totalHits",
			zap.Int64("result.TotalHits", result.TotalHits()),
			zap.Int64("result.Hits.TotalHits", result.Hits.TotalHits),
			zap.Int("count", count),
			zap.Int("result.Hits.Hits length", len(result.Hits.Hits)),
		)

		var contentMap map[string]int
		var h hash.Hash
		if repeat {
			h = md5.New()
		}

		if result.Hits.TotalHits > 0 {
			es.Log.Info("totla_count", zap.Int64("detail", result.Hits.TotalHits))
			// 需要去重复内容
			if repeat {
				contentMap = make(map[string]int)
			}

			curResult := []Document{}
			esIDList := []string{}

			for _, hit := range result.Hits.Hits {
				t := Document{}
				// 默认不过滤
				var filterFlag bool

				err := Json.Unmarshal(*hit.Source, &t) //另外一种取数据的方法
				if err != nil {
					es.Log.Error("Deserialization failed", zap.String("err info", err.Error()))
					searchResponse.Code = http.StatusBadRequest
					searchResponse.Msg = err.Error()
					data.ResultChan <- &searchResponse
					continue
				}

				if status > 0 {
					t.Status = status
					// 查询未反馈
					// 如果t.Status==0,
					// 如果operation_count == 0， 则没有submit,
				} else if status == 0 {
					if (t.OperationCount > 0 && t.OperationCount == len(t.DeliverResultJson)) ||
						t.Status > 0 {
						continue
					}

					t.Status = 0
					// 查询所有
				} else {
					// 收到所有deliver包， 且收到sent包， filter包没有operation
					if t.OperationCount > 0 && t.OperationCount == len(t.DeliverResultJson) {
						if t.DeliveredCount > 0 {
							t.Status = 13
						} else if t.ResentCount > 0 {
							t.Status = 15
						} else {
							t.Status = 14
						}
					} else {
						if t.Status == 0 {
							t.Status = 0
						}
					}
				}

				// 如果结果需要判断状态， 泽设置成默认过滤掉
				if statusCode != "" {
					filterFlag = true
				}

				// 更具json 写入slice
				if len(t.DeliverResultJson) > 0 {
					deliverResultMap := make(map[int]string)
					length := 0

					for k, v := range t.DeliverResultJson {
						// 已经判断成过滤才需要比较
						if filterFlag && statusCode == v {
							filterFlag = false
						}

						indexList := strings.Split(k, "_")
						indexStr := indexList[len(indexList)-1]
						index, err := strconv.Atoi(indexStr)
						if err != nil {
							es.Log.Error(" atoi deliverjosn json err",
								zap.String(ErrInfo, err.Error()),
								zap.String("index", indexStr),
							)
							continue
						}
						if index >= length {
							length = index
						}
						deliverResultMap[index] = v
					}
					resultSlice := make([]string, length+1)

					for index, v := range deliverResultMap {
						resultSlice[index] = v
					}

					t.DeliverResult = resultSlice
				}

				if len(t.BackDateJson) > 0 {
					dateResultMap := make(map[int]int64)
					length := 0
					var lastBackDate int64

					for k, v := range t.BackDateJson {
						indexList := strings.Split(k, "_")
						indexStr := indexList[len(indexList)-1]
						index, err := strconv.Atoi(indexStr)
						if err != nil {
							es.Log.Error(" atoi backdate json err",
								zap.String(ErrInfo, err.Error()),
								zap.String("index", indexStr),
							)
							continue
						}

						if err != nil {
							es.Log.Error(" atoi deliverjosn json err",
								zap.String(ErrInfo, err.Error()),
								zap.String("index", indexStr),
							)
							continue
						}
						if v > lastBackDate {
							lastBackDate = v
						}

						if index >= length {
							length = index
						}
						dateResultMap[index] = v
					}
					resultSlice := make([]int64, length+1)

					for index, v := range dateResultMap {
						resultSlice[index] = v
					}

					t.BDate = resultSlice
					t.LastBackDate = lastBackDate

				}

				// 没过滤掉数据采取判断
				if !filterFlag && repeat {
					content := t.Content
					// 获取md5
					h.Reset()
					h.Write([]byte(content))
					md5String := hex.EncodeToString(h.Sum(nil))
					if _, ok := contentMap[md5String]; ok {
						filterFlag = true
					} else {
						contentMap[md5String] = 1
					}
				}

				// 不过滤的才加进去
				if !filterFlag {
					curResult = append(curResult, t)
					esIDList = append(esIDList, hit.Id)
				}
			} // end for

			searchResponse.Code = http.StatusOK
			searchResponse.Datas = curResult
			searchResponse.ESIDList = esIDList
			data.ResultChan <- &searchResponse
		} else {
			searchResponse.Code = http.StatusOK
			data.ResultChan <- &searchResponse

		}

	}

}

func (es *Elastic) SearchScroll(sem *semaphore.Weighted, dataChan chan *Search) {
	defer sem.Release(1)
	for {
		data := <-dataChan
		query := data.Query

		scope := data.Scope
		// 状态码
		statusCode := data.StatusCode

		es.Log.Error("================================")
		// searchSource := elastic.NewSearchSource()
		fsc := elastic.NewFetchSourceContext(true).Include("deliver_result_json",
			"operation_count", "delivred_count")

		svc := es.Client.Scroll(es.IndexName).
			Index(es.IndexName).
			Type(es.TypeName).
			// SearchSource(searchSource).
			Query(query).
			FetchSourceContext(fsc).
			Size(10000)

		searchResponse := ScrollResponse{}

		searchWeight := semaphore.NewWeighted(10)
		resultChan := make(chan FeedResult, 100)
		resultMap := make(map[string]FeedResult)
		var readMapWG sync.WaitGroup
		readMapWG.Add(1)
		// 将chan 中结果写入map, 退出才能读map
		go func() {
			for {
				v, ok := <-resultChan
				if !ok {
					break
				}
				searchResponse.Total++
				code := v.StatusCode
				if result, ok := resultMap[code]; ok {
					result.Count++
					resultMap[code] = result
				} else {
					resultMap[code] = v
				}
			}
			readMapWG.Done()
		}()

		var wg sync.WaitGroup

		for {
			res, err := svc.Do(context.TODO())
			if err == io.EOF {
				es.Log.Info("search finished", zap.Int64("results", res.TotalHits()))
				break
			}

			if res == nil {
				es.Log.Error("search err",
					zap.String("scroll search err", err.Error()),
				)
				break
			}
			if res.Hits == nil {
				es.Log.Error("totalHits",
					zap.String("scroll search err", "hits nil"),
				)
				break
			}

			wg.Add(1)
			searchWeight.Acquire(context.Background(), 1)
			go func() {
				defer wg.Done()
				defer searchWeight.Release(1)
				for _, hit := range res.Hits.Hits {
					item := make(map[string]interface{})
					err := Json.Unmarshal(*hit.Source, &item)
					if err != nil {
						es.Log.Error("Deserialization failed", zap.String("err info", err.Error()))

						continue
					}
					if value, ok := item["deliver_result_json"]; ok {
						valueMap := value.(map[string]interface{})

						for _, v := range valueMap {
							status := 0
							valueString := v.(string)
							if statusCode != "" && valueString != statusCode {
								continue
							}

							switch valueString {
							case "DELIVRD":
								status = 13
							default:
								if strings.HasPrefix(valueString, "_~RESENT_") {
									status = 15
								} else {
									status = 14
								}
							}

							switch scope {
							case 0:
								resultChan <- FeedResult{
									StatusCode: valueString,
									Count:      1,
									Status:     status,
								}

							case 1:
								if status == 14 {
									resultChan <- FeedResult{
										StatusCode: valueString,
										Count:      1,
										Status:     14,
									}
								}
							case 2:
								if status == 15 {
									resultChan <- FeedResult{
										StatusCode: valueString,
										Count:      1,
										Status:     15,
									}
								}
							} // end switch
						}
					}
				}
			}()
		}
		wg.Wait()
		close(resultChan)
		readMapWG.Wait()
		curResult := []FeedResult{}
		for _, valueItem := range resultMap {
			curResult = append(curResult, valueItem)
		}
		sort.Sort(FeedSlice(curResult))
		searchResponse.Result = curResult
		searchResponse.Code = http.StatusOK
		data.ResultChan <- &searchResponse
	} // end for

}

func (es *Elastic) MulGet(sem *semaphore.Weighted, dataChan chan *Multi) {
	defer sem.Release(1)
	for {
		data := <-dataChan
		query := data.Query
		status := data.Status

		result, err := es.Client.MultiGet().
			Add(query...).Do(context.Background())

		searchResponse := SearchResponse{}
		if err != nil {
			searchResponse.Code = http.StatusBadRequest
			searchResponse.Msg = err.Error()
			es.Log.Error("search err", zap.String(ErrInfo, err.Error()))
			data.ResultChan <- &searchResponse
			continue
		}

		curResult := []Document{}
		for _, doc := range result.Docs {
			if doc.Found {
				tmpResult := Document{}
				docByte, err := doc.Source.MarshalJSON()
				if err != nil {
					searchResponse.Code = http.StatusBadRequest
					searchResponse.Msg = err.Error()
					es.Log.Error("unsmarshal err", zap.String(ErrInfo, err.Error()))
					data.ResultChan <- &searchResponse
					continue
				}

				err = Json.Unmarshal(docByte, &tmpResult)
				if err != nil {
					searchResponse.Code = http.StatusBadRequest
					searchResponse.Msg = err.Error()
					es.Log.Error("unsmarshal err", zap.String(ErrInfo, err.Error()))
					data.ResultChan <- &searchResponse
					continue
				}

				if status > 0 {
					tmpResult.Status = status
					// 查询未反馈
					// 如果t.Status==0,
					// 如果operation_count == 0， 则没有submit,
				} else if status == 0 {

					tmpResult.Status = 0
					// 查询所有
				} else {
					// 收到所有deliver包， 且收到sent包， filter包没有operation
					if tmpResult.OperationCount > 0 && tmpResult.OperationCount == len(tmpResult.DeliverResultJson) {
						if tmpResult.DeliveredCount > 0 {
							tmpResult.Status = 13
						} else if tmpResult.ResentCount > 0 {
							tmpResult.Status = 15
						} else {
							tmpResult.Status = 14
						}
					} else {
						if tmpResult.Status == 0 {
							tmpResult.Status = 0
						}
					}
				}

				// 更具json 写入slice
				if len(tmpResult.DeliverResultJson) > 0 {
					deliverResultMap := make(map[int]string)
					length := 0

					for k, v := range tmpResult.DeliverResultJson {
						indexList := strings.Split(k, "_")
						indexStr := indexList[len(indexList)-1]
						index, err := strconv.Atoi(indexStr)
						if err != nil {
							es.Log.Error(" atoi deliverjosn json err",
								zap.String(ErrInfo, err.Error()),
								zap.String("index", indexStr),
							)
							continue
						}
						if index >= length {
							length = index
						}
						deliverResultMap[index] = v
					}
					resultSlice := make([]string, length+1)

					for index, v := range deliverResultMap {
						resultSlice[index] = v
					}

					tmpResult.DeliverResult = resultSlice
				}

				// 更具json 写入slice
				if len(tmpResult.DeliverResultJson) > 0 {
					deliverResultMap := make(map[int]string)
					length := 0

					for k, v := range tmpResult.DeliverResultJson {
						indexList := strings.Split(k, "_")
						indexStr := indexList[len(indexList)-1]
						index, err := strconv.Atoi(indexStr)
						if err != nil {
							es.Log.Error(" atoi deliverjosn json err",
								zap.String(ErrInfo, err.Error()),
								zap.String("index", indexStr),
							)
							continue
						}
						if index >= length {
							length = index
						}
						deliverResultMap[index] = v
					}
					resultSlice := make([]string, length+1)

					for index, v := range deliverResultMap {
						resultSlice[index] = v
					}

					tmpResult.DeliverResult = resultSlice
				}

				if len(tmpResult.BackDateJson) > 0 {
					dateResultMap := make(map[int]int64)
					length := 0
					var lastBackDate int64

					for k, v := range tmpResult.BackDateJson {
						indexList := strings.Split(k, "_")
						indexStr := indexList[len(indexList)-1]
						index, err := strconv.Atoi(indexStr)
						if err != nil {
							es.Log.Error(" atoi backdate json err",
								zap.String(ErrInfo, err.Error()),
								zap.String("index", indexStr),
							)
							continue
						}

						if err != nil {
							es.Log.Error(" atoi deliverjosn json err",
								zap.String(ErrInfo, err.Error()),
								zap.String("index", indexStr),
							)
							continue
						}
						if v > lastBackDate {
							lastBackDate = v
						}

						if index >= length {
							length = index
						}
						dateResultMap[index] = v
					}
					resultSlice := make([]int64, length+1)

					for index, v := range dateResultMap {
						resultSlice[index] = v
					}
					tmpResult.BDate = resultSlice
					tmpResult.LastBackDate = lastBackDate
				}

				curResult = append(curResult, tmpResult)
			}
		}
		searchResponse.Code = http.StatusOK
		searchResponse.Datas = curResult
		data.ResultChan <- &searchResponse

	}
}

func hashFunc(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type Docments []HitDocument

func (d Docments) Len() int { return len(d) }
func (d Docments) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

type SortBySdateDesc struct{ Docments }
type SortBySdateAsc struct{ Docments }
type SortByBdateDesc struct{ Docments }
type SortByBdateAsc struct{ Docments }

func (d SortBySdateDesc) Less(i, j int) bool {
	return d.Docments[i].Source.SDate > d.Docments[j].Source.SDate
}

func (b SortByBdateDesc) Less(i, j int) bool {
	return b.Docments[i].Source.LastBackDate > b.Docments[j].Source.LastBackDate
}

func (d SortBySdateAsc) Less(i, j int) bool {
	return d.Docments[i].Source.SDate < d.Docments[j].Source.SDate
}

func (b SortByBdateAsc) Less(i, j int) bool {
	return b.Docments[i].Source.LastBackDate < b.Docments[j].Source.LastBackDate
}

type FeedSlice []FeedResult

func (f FeedSlice) Len() int { // 重写 Len() 方法
	return len(f)
}
func (f FeedSlice) Swap(i, j int) { // 重写 Swap() 方法
	f[i], f[j] = f[j], f[i]
}
func (f FeedSlice) Less(i, j int) bool { // 重写 Less() 方法， 从大到小排序
	return f[j].Count < f[i].Count
}
