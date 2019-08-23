package link_list

import (
	"errors"
	"utils"
)

const (
	// Number of bytes used to keep information about entry size
	headerEntrySize = 4
	// Bytes before left margin are not used. Zero index means element does not exist in queue, useful while reading slice from index
	leftMarginIndex = 1
	// Minimum empty blob size in bytes. Empty blob fills space between tail and head in additional memory allocation.
	// It keeps entries indexes unchanged
	minimumEmptyBlobSize = 32 + headerEntrySize
)

var (
	errEmptyQueue       = &queueError{"Empty queue"}
	errInvalidIndex     = &queueError{"Index must be greater than zero. Invalid index."}
	errIndexOutOfBounds = &queueError{"Index out of range"}
)

type expandCallback func(size int) (*utils.List, error)

// 链表
type Entry struct {
	data interface{} // struct 的byte
	head *Entry
	tail *Entry
}

// BytesQueue is a non-thread safe queue type of fifo based on bytes array.
// For every push operation index of entry is returned. It can be used to read the entry later
type BytesQueue struct {
	array           *utils.List // 从尾部push节点，形成时间顺序的链表
	arrayFree       *utils.List // 空闲链表， 从头获取pop节点， 从尾部push节点
	capacity        int
	maxCapacity     int
	verbose         bool
	initialCapacity int
	expandQueue     expandCallback
}

type queueError struct {
	message string
}

// NewBytesQueue initialize new bytes queue.
// Initial capacity is used in bytes array allocation
// When verbose flag is set then information about memory allocation are printed
func NewBytesQueue(initialCapacity int, maxCapacity int, verbose bool,
	expandFunc expandCallback) *BytesQueue {
	initList, err := expandFunc(initialCapacity)
	if err != nil {
		return nil
	}
	return &BytesQueue{
		array:           utils.New(),
		arrayFree:       initList,
		capacity:        initialCapacity,
		maxCapacity:     maxCapacity,
		verbose:         verbose,
		initialCapacity: initialCapacity,
		expandQueue:     expandFunc,
	}
}

// Reset pop all used entries to free entries
func (q *BytesQueue) Reset() {
	// Just reset indexes
	// 将used entries的数据全部pop， 并push进入free entries
	// q.count = 0
	q.arrayFree.ExpansionList(q.array)
	q.array = utils.New()
}

// Push copies entry at the end of queue and moves tail pointer. Allocates more space if needed.
// Returns index for pushed data or error if maximum size queue limit is reached.
func (q *BytesQueue) Push() (*utils.Element, error) {
	needMalloc, count := q.NeedMalloc()
	if needMalloc {
		newList, err := q.expandQueue(count)
		if err != nil {
			return nil, err
		}
		if !q.ExpandArrayFree(newList) {
			return nil, errors.New("expand array err")
		}
	}

	item := q.arrayFree.Remove(q.arrayFree.Front())
	element := q.array.PushBack(item)

	return element, nil
}

// NeedMalloc to check the arrayFree whether to expand
func (q *BytesQueue) NeedMalloc() (bool, int) {
	if q.arrayFree.Len() <= 10 {
		return true, 2 * q.array.Len()
	}
	return false, 0
}

func (q *BytesQueue) ExpandArrayFree(expandList *utils.List) bool {
	q.arrayFree.ExpansionList(expandList)
	return true
}

func (q *BytesQueue) Pop(data *utils.Element) error {
	item := q.array.Remove(data)
	if item != nil {
		q.arrayFree.PushBack(item)
		return nil
	}

	return errors.New("the element not in array")
}

// 从前开始匹配
func (q *BytesQueue) Peek() (*utils.Element, error) {
	front := q.array.Front()
	if front != nil {
		return front, nil
	}
	return nil, errors.New("nil queue")
}

// Capacity returns number of allocated bytes for queue
func (q *BytesQueue) Capacity() int {
	return q.arrayFree.Len()
}

// Len returns number of entries kept in queue
func (q *BytesQueue) Len() int {
	return q.array.Len()
}

// Error returns error message
func (e *queueError) Error() string {
	return e.message
}
