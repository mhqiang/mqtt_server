package utils

import (
	"fmt"
	"testing"
	"utils"
)

func TestSetBulkList(t *testing.T) {
	redisInfo := &utils.RedisInfo{
		// UpdateURL:   "redis://192.168.30.157:6379",
		// ReadUrl:     "redis://192.168.30.158:6379",
		UpdateURL:   "192.168.30.157:6379",
		ReadUrl:     "192.168.30.158:6379",
		PassWord:    "asdfqwer1234!@#$",
		MaxIdle:     3,
		IdleTimeout: 240,
	}
	rHandler, err := InitRedis(redisInfo)
	result, err := rHandler.SetBulkList([]string{"1", "2"}, []string{"test1", "test2"})
	fmt.Println("pool", err, "result", result)
}

func TestDelBulkList(t *testing.T) {
	redisInfo := &RedisInfo{
		UpdateURL:   "192.168.30.157:6379",
		ReadUrl:     "192.168.30.158:6379",
		PassWord:    "asdfqwer1234!@#$",
		MaxIdle:     20,
		IdleTimeout: 0,
	}
	rHandler, err := InitRedis(redisInfo)
	list, err := rHandler.DelBulkList([]string{"1", "2"})
	fmt.Println("err", err)
	fmt.Println("list", list)
}
