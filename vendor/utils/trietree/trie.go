package trietree

import (
	"errors"
	"strconv"
)

// 手机号后10位, node 分5层
func GetValueList(number string) ([]int8, error) {
	if len(number) != 11 {
		return []int8{}, errors.New("err phone")
	}

	numStr := number[1:11]
	indexList := []int8{}
	for i := 0; i < 5; i++ {
		start := i * 2
		end := i*2 + 2
		curNum := numStr[start:end]
		index, err := strconv.Atoi(curNum)
		if err != nil {
			return []int8{}, err
		}
		indexList = append(indexList, int8(index))
	}
	return indexList, nil
}

type TrieNode struct {
	ValueLeaf map[int8]bool
	Children  map[int8]TrieNode
}

type LeafNode struct {
}

// 只有最后一个子节点有valueleaf
func CreateTrieNode() *TrieNode {
	Root := &TrieNode{
		Children: make(map[int8]TrieNode),
	}
	return Root
}

// 号码都是1开头
func (tree *TrieNode) Insert(number string) error {
	numList, err := GetValueList(number)
	if err != nil {
		return err
	}

	length := len(numList)
	curNode := *tree
	for index, num := range numList {
		switch index {
		case length - 1:
			if index == length-1 {
				if _, ok := curNode.ValueLeaf[num]; !ok {
					curNode.ValueLeaf[num] = true
				}
				return nil
			}
		case length - 2:
			if _, ok := curNode.Children[num]; !ok {
				curNode.Children[num] = TrieNode{
					ValueLeaf: make(map[int8]bool),
					Children:  make(map[int8]TrieNode),
				}
			}
			curNode = curNode.Children[num]
		default:
			if _, ok := curNode.Children[num]; !ok {
				curNode.Children[num] = TrieNode{
					ValueLeaf: make(map[int8]bool),
					Children:  make(map[int8]TrieNode),
				}
			}
			curNode = curNode.Children[num]
		}

	}

	return errors.New("insert err")
}

func (tree *TrieNode) Search(number string) bool {
	numList, err := GetValueList(number)
	if err != nil {
		return false
	}

	length := len(numList)
	curNode := *tree
	for index, num := range numList {
		switch index {
		case length - 1:
			if index == length-1 {
				if _, ok := curNode.ValueLeaf[num]; !ok {
					return false
				}
				break
			}
		case length - 2:
			if _, ok := curNode.Children[num]; !ok {
				return false
			}
			curNode = curNode.Children[num]
		default:
			if _, ok := curNode.Children[num]; !ok {
				return false
			}
			curNode = curNode.Children[num]

		}

	}

	return true
}
