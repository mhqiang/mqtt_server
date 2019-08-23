package bigcache

import (
	"sync"
	"sync/atomic"
	"utils"

	"github.com/allegro/bigcache/link_list"
)

type onRemoveCallback func(wrappedEntry []byte, reason RemoveReason)
type onGetExpireCallback func(value interface{}) uint64
type elementCallback func(src, cur interface{}) bool // 确认是否完成
type setDataFunc func(src, cur interface{})

type cacheShard struct {
	// hashmap     map[uint64]uint32
	hashmap     map[uint64]*utils.Element
	entries     *link_list.BytesQueue
	lock        sync.RWMutex
	entryBuffer []byte
	onRemove    onRemoveCallback
	elementFunc elementCallback
	onExpire    onGetExpireCallback
	setData     setDataFunc

	isVerbose  bool
	logger     Logger
	clock      clock
	lifeWindow uint64

	stats Stats
}

func (s *cacheShard) get(key string, hashedKey uint64) (interface{}, error) {
	s.lock.RLock()
	itemEle := s.hashmap[hashedKey]

	if itemEle == nil {
		s.lock.RUnlock()
		s.miss()
		return nil, ErrEntryNotFound
	}

	s.lock.RUnlock()
	s.hit()
	return itemEle.Value, nil
}

// update 传递update函数, 将新的值跟新进入到内存中
func (s *cacheShard) update(key string, hashedKey uint64,
	entry interface{}) (err error, finished bool) {
	s.lock.Lock()
	preValue := s.hashmap[hashedKey]
	if preValue == nil {
		preValue, err = s.entries.Push()
		if err != nil {
			s.lock.Unlock()
			return
		}

		// copier.Copy(preValue.Value, entry)
		s.setData(preValue.Value, entry)
		// s.setData(preValue.Value, hashedKey)
		preValue.HashKey = hashedKey
		s.hashmap[hashedKey] = preValue
		s.lock.Unlock()
		return
	}

	finished = s.elementFunc(preValue.Value, entry)
	// copier.Copy(preValue.Value, entry)
	s.lock.Unlock()
	return
}

func (s *cacheShard) set(key string, hashedKey uint64, entry interface{}) (err error) {
	s.lock.Lock()
	preValue, err := s.entries.Push()
	if err != nil {
		s.lock.Unlock()
		return err
	}
	preValue.HashKey = hashedKey

	s.setData(preValue.Value, entry)
	s.hashmap[hashedKey] = preValue
	s.lock.Unlock()
	return nil
}

func (s *cacheShard) del(key string, hashedKey uint64) error {
	// Optimistic pre-check using only readlock
	s.lock.Lock()
	itemResult := s.hashmap[hashedKey]

	if itemResult == nil {
		s.lock.Unlock()
		s.delmiss()
		return ErrEntryNotFound
	}

	err := s.entries.Pop(itemResult)
	if err != nil {
		s.lock.Unlock()
		return err
	}
	delete(s.hashmap, hashedKey)

	s.lock.Unlock()
	s.delhit()
	return nil
}

// 超过lifetime
func (s *cacheShard) onEvict(back *utils.Element, currentTimestamp uint64) bool {
	oldestTimestamp := s.onExpire(back.Value)
	if currentTimestamp-oldestTimestamp < s.lifeWindow {
		return false
	}
	return true
}

func (s *cacheShard) cleanUp(currentTimestamp uint64) {
	s.lock.Lock()
	for {
		if front, err := s.entries.Peek(); err != nil {
			break
		} else if evicted := s.onEvict(front, currentTimestamp); !evicted {
			break
		} else {
			err = s.entries.Pop(front)
			if err != nil {
				s.lock.Unlock()
				return
			}
			delete(s.hashmap, front.HashKey)
			s.delhit()
		}

	}
	s.lock.Unlock()
}

func (s *cacheShard) getOldestEntry() (*utils.Element, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.entries.Peek()
}

func (s *cacheShard) copyKeys() (elements []*utils.Element, keys []string,
	hashs []uint64, next int) {
	s.lock.RLock()
	elements = make([]*utils.Element, len(s.hashmap))
	// 暂时不传送key
	keys = make([]string, len(s.hashmap))
	hashs = make([]uint64, len(s.hashmap))

	for hash, value := range s.hashmap {
		elements[next] = value
		hashs[next] = hash
		keys[next] = ""
		next++
	}

	s.lock.RUnlock()
	return
}

//
func (s *cacheShard) reset(config Config) {
	s.lock.Lock()
	s.hashmap = make(map[uint64]*utils.Element, config.initialShardSize())

	s.entries.Reset()
	s.lock.Unlock()
}

func (s *cacheShard) len() int {
	s.lock.RLock()
	res := len(s.hashmap)
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) capacity() int {
	s.lock.RLock()
	res := s.entries.Capacity()
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) getStats() Stats {
	var stats = Stats{
		Hits:       atomic.LoadInt64(&s.stats.Hits),
		Misses:     atomic.LoadInt64(&s.stats.Misses),
		DelHits:    atomic.LoadInt64(&s.stats.DelHits),
		DelMisses:  atomic.LoadInt64(&s.stats.DelMisses),
		Collisions: atomic.LoadInt64(&s.stats.Collisions),
	}
	return stats
}

func (s *cacheShard) hit() {
	atomic.AddInt64(&s.stats.Hits, 1)
}

func (s *cacheShard) miss() {
	atomic.AddInt64(&s.stats.Misses, 1)
}

func (s *cacheShard) delhit() {
	atomic.AddInt64(&s.stats.DelHits, 1)
}

func (s *cacheShard) delmiss() {
	atomic.AddInt64(&s.stats.DelMisses, 1)
}

func (s *cacheShard) collision() {
	atomic.AddInt64(&s.stats.Collisions, 1)
}

func initNewShard(config Config, callback onRemoveCallback, clock clock, eleCallback elementCallback,
	expireCallback onGetExpireCallback, expandQueue func(size int) (*utils.List, error),
	setData setDataFunc) cacheShard {

	return cacheShard{
		hashmap: make(map[uint64]*utils.Element, config.initialShardSize()),
		entries: link_list.NewBytesQueue(config.initialShardSize(),
			config.maximumShardSize(), config.Verbose, expandQueue),

		onRemove:    callback,
		elementFunc: eleCallback,
		onExpire:    expireCallback,
		setData:     setData,

		isVerbose:  config.Verbose,
		logger:     newLogger(config.Logger),
		clock:      clock,
		lifeWindow: uint64(config.LifeWindow.Seconds()),
	}
}
