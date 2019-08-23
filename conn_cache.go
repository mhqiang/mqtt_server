package mqtt_server


package main

import (
	"time"
	"utils"

	"github.com/allegro/bigcache"
)


type ConnectionCache struct {
	// 可以定时检查连接
	ConnCache *bigcache.BigCache
	// 可以设置超时权限等
	UserCache *UserCache
}


type ConnNode struct {
	io.Reader //Read(p []byte) (n int, err error)
	io.Writer //Write(p []byte) (n int, err error)
	sync.Mutex
	buf_lock  chan bool //当有写入一次数据设置一次
	msgChan chan interface  // 接收topic 传过来的订阅消息，
	msgCache *freecache.Cache // 需要确认发送成功的消息存储
	buffer    bytes.Buffer
	conn      net.Conn
	UpdateAt  int64
	closeFlag bool
}


// shard节点更新回调， 回调分2种情况：
//  一种是更换用户了： 所有数据替换
//  一种是统一用户重连：只替换socket
func elementCallback(src, cur interface{}) bool {
	return true
}

// 扩展queue
func expandQueueCallback(size int) (*utils.List, error) {
	curFreeLink := utils.New()
	// log.Println("expand count", size)
	curContentPool := make([]ConnNode, size)

	for index, _ := range curContentPool {
		ele := &utils.Element{Value: &curContentPool[index]}
		curFreeLink.PushBack(ele)
	}
	return curFreeLink, nil
}

// 超时回调
func expireCallback(value interface{}) uint64 {
	ele := value.(*ConnNode)
	return uint64(ele.CreatedAt)
}

// 回收的时候不清空值，赋值的时候清空
func setData(src, cur interface{}) {

}

func InitCache() (*ConnectionCache, error) {
	config := bigcache.Config{
		// number of shards (must be a power of 2)
		Shards: 128,
		// time after which entry can be evicted
		LifeWindow: 60 *time.Second,
		// rps * lifeWindow, used only in initial memory allocation
		MaxEntriesInWindow: 1024 * 100 * 60,
		// max entry size in bytes, used only in initial memory allocation
		MaxEntrySize: 500,
		// prints information about additional memory allocation
		Verbose: true,
		// cache will not allocate more memory than this limit, value in MB
		// if value is reached then the oldest entries can be overridden for the new ones
		// 0 value means no size limit
		HardMaxCacheSize: 8192,
		// callback fired when the oldest entry is removed because of its expiration time or no space left
		// for the new entry, or because delete was called. A bitmask representing the reason will be returned.
		// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
		OnRemove: nil,
		// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
		// for the new entry, or because delete was called. A constant representing the reason will be passed through.
		// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
		// Ignored if OnRemove is specified.
		OnRemoveWithReason: nil,
		CleanWindow:        60 * time.Second,
	}

	cache, initErr := bigcache.NewBigCache(config, elementCallback, expireCallback,
		expandQueueCallback, setData)
	if initErr != nil {
		return nil, initErr
	}
	connectCache := &ConnectionCache{
		ConnCache: cache,
	}

	ConnectionCache.UserCache = InitUserCache()
	return ConnectionCache, nil
}



func (connHandler *ConnectionCache) AddConn(conn net.Conn) error{
	// 是否合法用户
	var user,password string
	suc, err := connHandler.UserCache.AuthUser(user,password)
	if err!=nil{
		return err
	}
	if !suc{
		return errors.New("auth failed")
	}
		
	return nil
}

func (connHandler *ConnectionCache) CloseConn(conn net.Conn) error{
 
	return nil
}
