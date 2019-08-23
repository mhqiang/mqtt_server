package utils

import (
	"errors"
	"fmt"
	"log"
	"time"
	"utils/semaphore"

	"github.com/gomodule/redigo/redis"
	"go.uber.org/zap"
)

type RedisInfo struct {
	UpdateURL   string
	ReadUrl     string
	PassWord    string
	Log         *zap.Logger
	MaxIdle     int
	IdleTimeout int
	UpdatePool  *redis.Pool
	ReadPool    *redis.Pool
	ScrollPool  *redis.Pool
}

type ReadRecord struct {
	ResultChan chan string
	Key        string
}

type RedisValueInfo struct {
	Value string
	TTL   int64
}

type SetData struct {
	Key    []string
	Value  []string
	Result chan string
}

type SetScrollData struct {
	Key    string
	Value  string
	Result chan string
}

type InsertData struct {
	Key   string
	Value string
}

type DelData struct {
	Key    []string
	Result chan int64
}

type TimeOutData struct {
	Key    []string
	Result chan int64
}

type ResultRecord struct {
	Result  chan string
	Errinfo error
}

type RedisHandler interface {
	Set()
}

func InitRedis(rHandler *RedisInfo, Log *zap.Logger) (*RedisInfo, error) {
	pool := &redis.Pool{
		MaxIdle:     rHandler.MaxIdle,
		MaxActive:   rHandler.MaxIdle,
		IdleTimeout: 0,

		Dial: func() (redis.Conn, error) {
			// c, err := redis.DialURL(rHandler.UpdateURL)
			c, err := redis.Dial("tcp", rHandler.UpdateURL)
			if err != nil {
				return nil, fmt.Errorf("redis connection error: %s", err)
			}
			//验证redis密码
			if _, authErr := c.Do("AUTH", rHandler.PassWord); authErr != nil {
				return nil, fmt.Errorf("redis auth password error: %s", authErr)
			}
			c.Do("SELECT", 0)
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				return fmt.Errorf("ping redis error: %s", err)
			}
			return nil
		},
	}
	rHandler.UpdatePool = pool

	scrollPool := &redis.Pool{
		MaxIdle:     rHandler.MaxIdle,
		MaxActive:   rHandler.MaxIdle,
		IdleTimeout: 0,

		Dial: func() (redis.Conn, error) {
			// c, err := redis.DialURL(rHandler.UpdateURL)
			c, err := redis.Dial("tcp", rHandler.UpdateURL)
			if err != nil {
				return nil, fmt.Errorf("redis connection error: %s", err)
			}
			//验证redis密码
			if _, authErr := c.Do("AUTH", rHandler.PassWord); authErr != nil {
				return nil, fmt.Errorf("redis auth password error: %s", authErr)
			}
			c.Do("SELECT", 1)
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				return fmt.Errorf("ping redis error: %s", err)
			}
			return nil
		},
	}
	rHandler.ScrollPool = scrollPool

	readPool := &redis.Pool{
		MaxIdle:     rHandler.MaxIdle,
		MaxActive:   rHandler.MaxIdle,
		IdleTimeout: 0,
		Dial: func() (redis.Conn, error) {
			// c, err := redis.DialURL(rHandler.UpdateURL)
			c, err := redis.Dial("tcp", rHandler.ReadUrl)
			if err != nil {
				return nil, fmt.Errorf("redis connection error: %s", err)
			}
			//验证redis密码
			if _, authErr := c.Do("AUTH", rHandler.PassWord); authErr != nil {
				return nil, fmt.Errorf("redis auth password error: %s", authErr)

			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				Log.Error("ping redis error", zap.String(ErrInfo, err.Error()))
			}
			return nil
		},
	}
	rHandler.ReadPool = readPool
	rHandler.Log = Log

	return rHandler, nil
}

func (handler *RedisInfo) Set(k string, v interface{}) {
	c := handler.UpdatePool.Get()
	defer c.Close()
}

// 短信一般设置过期时间72小时
func (handler *RedisInfo) SetKeyExpire(k string, ex int) {
	c := handler.UpdatePool.Get()
	defer c.Close()
	_, err := c.Do("EXPIRE", k, ex)
	if err != nil {
		handler.Log.Error("set error", zap.String(ErrInfo, err.Error()))
	}
}

func (handler *RedisInfo) CheckKey(k string) bool {
	c := handler.UpdatePool.Get()
	defer c.Close()
	exist, err := redis.Bool(c.Do("EXISTS", k))
	if err != nil {
		handler.Log.Error("checkKey error", zap.String(ErrInfo, err.Error()))
		return false
	}
	return exist

}

func (handler *RedisInfo) DelKey(k string) error {
	c := handler.UpdatePool.Get()
	defer c.Close()
	_, err := c.Do("DEL", k)
	if err != nil {
		handler.Log.Error("delkey error", zap.String(ErrInfo, err.Error()))
		return err
	}
	return nil
}

// 传过来 发送结果的chan 和msgid, 暂时先不管这个key的过期时间
func (handler *RedisInfo) GetValue(sem *semaphore.Weighted, payLoad interface{}) interface{} {
	c := handler.ReadPool.Get()
	defer c.Close()
	defer sem.Release(1)

	recordChan := payLoad.(chan interface{})
	for {
		curRecord := <-recordChan
		readRecord := curRecord.(*ReadRecord)

		result, err := redis.String(c.Do("Get", readRecord.Key))
		if err != nil {
			// log.Println("read redis", err)
			readRecord.ResultChan <- ""
			continue
		}
		readRecord.ResultChan <- result

	}
	return nil
}

func (handler *RedisInfo) SetJson(k string, data interface{}) error {
	c := handler.UpdatePool.Get()
	defer c.Close()
	value, _ := Json.Marshal(data)
	n, _ := c.Do("SETNX", k, value)
	if n != int64(1) {
		return errors.New("set failed")
	}
	return nil
}

func (handler *RedisInfo) GetJsonByte(k string) ([]byte, error) {
	c := handler.UpdatePool.Get()
	defer c.Close()
	jsonGet, err := redis.Bytes(c.Do("GET", k))
	if err != nil {
		handler.Log.Error("getJsonByte error", zap.String(ErrInfo, err.Error()))
		return nil, err
	}
	return jsonGet, nil
}

// 返回[][]byte{}
func (handler *RedisInfo) GetBulkList(k []string) ([]interface{}, error) {
	c := handler.UpdatePool.Get()
	defer c.Close()

	args := []interface{}{}
	for _, v := range k {
		args = append(args, v)
	}
	reply, err := redis.MultiBulk(c.Do("MGET", args...))
	if err != nil {
		handler.Log.Error("MGET error", zap.String(ErrInfo, err.Error()))
		return nil, err
	}

	len := len(k)
	var list = make([]interface{}, len)
	for _, v := range reply {
		s, err := redis.Bytes(v, nil)
		if err != nil {
			handler.Log.Error("redis.Bytes error", zap.String(ErrInfo, err.Error()))
			break
		}
		list = append(list, s)
	}
	return list, err
}

// func (handler *RedisInfo) SetBulkList(k []string, value []string) (string, error) {
func (handler *RedisInfo) SetBulkList(sem *semaphore.Weighted, setData interface{}) interface{} {
	c := handler.UpdatePool.Get()
	timeoutClient := handler.ScrollPool.Get()

	var cruRedisArgs SetData
	defer func() { // 必须要先声明defer，否则不能捕获到panic异常

		if err := recover(); err != nil {
			log.Println("recover", err, cruRedisArgs) // 这里的err其实就是panic传入的内容，55
			handler.Log.Error("recover", zap.String(ErrInfo, "set bulk list err"))
		}

	}()
	defer c.Close()
	defer timeoutClient.Close()
	defer sem.Release(1)

	setDataChan := setData.(chan interface{})
	for {
		curRecord := <-setDataChan
		record := curRecord.(SetData)

		var redisArgs []interface{}
		for i, v := range record.Key {
			redisArgs = append(redisArgs, v)
			redisArgs = append(redisArgs, record.Value[i])
		}

		reply, err := redis.String(c.Do("MSET", redisArgs...))
		if err != nil {
			handler.Log.Error("redis MSET err", zap.String(ErrInfo, err.Error()))
			record.Result <- ""
			continue
		}
		record.Result <- reply

		timeoutValues, err := Json.Marshal(record.Key)
		if err != nil {
			handler.Log.Error("marshal lpush key err", zap.String(ErrInfo, err.Error()))
			continue
		}

		_, err = redis.Int64(timeoutClient.Do("LPUSH", "TIME_SET", string(timeoutValues)))
		if err != nil {
			handler.Log.Error("lush redis err", zap.String(ErrInfo, err.Error()))
			continue
		}
	}
	return nil
}

// func (handler *RedisInfo) DelBulkList(k []string) (int64, error) {
func (handler *RedisInfo) DelBulkList(sem *semaphore.Weighted, kChan interface{}) interface{} {
	c := handler.UpdatePool.Get()
	defer c.Close()
	defer sem.Release(1)

	// keyChan := kChan.(chan DelData)
	keyChan := kChan.(chan interface{})
	for {
		curKey := <-keyChan
		k := curKey.(DelData)
		args := []interface{}{}
		for _, v := range k.Key {
			args = append(args, v)
		}
		reply, err := redis.Int64(c.Do("DEL", args...))
		if err != nil {
			handler.Log.Error("redis DEL err", zap.String(ErrInfo, err.Error()))
			k.Result <- 0
			continue
		}

		k.Result <- reply
	}
	return nil
}

func (handler *RedisInfo) InsertScroll(sem *semaphore.Weighted, setData interface{}) interface{} {
	c := handler.ScrollPool.Get()

	defer func() { // 必须要先声明defer，否则不能捕获到panic异常

		if err := recover(); err != nil {
			// 这里的err其实就是panic传入的内容，55
			handler.Log.Error("recover", zap.String(ErrInfo, "insertScroll err"))
		}

	}()
	defer c.Close()
	defer sem.Release(1)

	setDataChan := setData.(chan interface{})
	for {
		curRecord := <-setDataChan
		record := curRecord.(SetScrollData)

		reply, err := redis.String(c.Do("SET", record.Key, record.Value))
		if err != nil {

			handler.Log.Error("redis SET err",
				zap.String(ErrInfo, err.Error()),
				zap.String("key", record.Key),
			)
			record.Result <- ""
			continue
		}

		_, err = c.Do("EXPIRE", record.Key, 60)
		if err != nil {

			handler.Log.Error("redis expire err",
				zap.String(ErrInfo, err.Error()),
				zap.String("key", record.Key),
			)
			record.Result <- ""
			continue
		}
		record.Result <- reply
	}
	return nil
}

func (handler *RedisInfo) GetScrollValue(sem *semaphore.Weighted, payLoad interface{}) interface{} {

	c := handler.ScrollPool.Get()
	defer c.Close()
	defer sem.Release(1)

	recordChan := payLoad.(chan interface{})
	for {
		curRecord := <-recordChan
		readRecord := curRecord.(*ReadRecord)

		result, err := redis.String(c.Do("Get", readRecord.Key))
		if err != nil {
			// log.Println("read redis", err)
			handler.Log.Error("GetScrollValue get err", zap.String(ErrInfo, err.Error()))
			readRecord.ResultChan <- ""
			continue
		}

		readRecord.ResultChan <- result
	}
	return nil
}

func (handler *RedisInfo) SetTimeOut(sem *semaphore.Weighted, payLoad interface{}) interface{} {

	c := handler.UpdatePool.Get()
	defer c.Close()
	defer sem.Release(1)

	recordChan := payLoad.(chan interface{})
	for {
		curRecord := <-recordChan
		readRecord := curRecord.(*TimeOutData)

		args, err := Json.Marshal(readRecord.Key)
		if err != nil {
			log.Println("marshal lpush key err", err)
			handler.Log.Error("marshal lpush key err", zap.String(ErrInfo, err.Error()))
			readRecord.Result <- 0
			continue
		}

		reply, err := redis.Int64(c.Do("LPUSH", "TIME_SET", string(args)))
		if err != nil {
			handler.Log.Error(";push redis err", zap.String(ErrInfo, err.Error()))
			readRecord.Result <- 0
			continue
		}

		readRecord.Result <- reply
	}
	return nil
}
