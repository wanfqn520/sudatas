package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// IndexType 表示索引类型
type IndexType int

const (
	BTreeIndex IndexType = iota
	HashIndex
)

// Index 接口定义了索引的基本操作
type Index interface {
	Add(key interface{}, rowID uint64) error
	Find(key interface{}) ([]uint64, error)
	Remove(key interface{}, rowID uint64) error
	Save() error
	Load() error
}

// BPlusTree B+树节点结构
type BPlusTreeNode struct {
	IsLeaf   bool
	Keys     []interface{}
	Children []*BPlusTreeNode
	Values   [][]uint64     // 只在叶子节点使用
	Next     *BPlusTreeNode // 叶子节点链表
}

// BPlusTreeIndex B+树索引实现
type BPlusTreeIndex struct {
	mu      sync.RWMutex
	root    *BPlusTreeNode
	degree  int         // B+树的度
	path    string      // 索引文件路径
	compare CompareFunc // 比较函数
}

type CompareFunc func(a, b interface{}) int

// NewBPlusTreeIndex 创建新的B+树索引
func NewBPlusTreeIndex(path string, degree int, compare CompareFunc) *BPlusTreeIndex {
	return &BPlusTreeIndex{
		degree:  degree,
		path:    path,
		compare: compare,
		root:    &BPlusTreeNode{IsLeaf: true},
	}
}

// Add 添加索引项
func (idx *BPlusTreeIndex) Add(key interface{}, rowID uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 查找合适的叶子节点
	leaf := idx.findLeaf(key)
	pos := idx.findPos(leaf.Keys, key)

	// 如果键已存在，将rowID添加到值列表中
	if pos < len(leaf.Keys) && idx.compare(leaf.Keys[pos], key) == 0 {
		leaf.Values[pos] = append(leaf.Values[pos], rowID)
		return idx.Save()
	}

	// 插入新的键值对
	leaf.Keys = append(leaf.Keys, nil)
	leaf.Values = append(leaf.Values, nil)
	copy(leaf.Keys[pos+1:], leaf.Keys[pos:])
	copy(leaf.Values[pos+1:], leaf.Values[pos:])
	leaf.Keys[pos] = key
	leaf.Values[pos] = []uint64{rowID}

	// 如果节点已满，需要分裂
	if len(leaf.Keys) > 2*idx.degree {
		idx.splitLeaf(leaf)
	}

	return idx.Save()
}

// Find 查找索引项
func (idx *BPlusTreeIndex) Find(key interface{}) ([]uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	leaf := idx.findLeaf(key)
	pos := idx.findPos(leaf.Keys, key)

	if pos < len(leaf.Keys) && idx.compare(leaf.Keys[pos], key) == 0 {
		return leaf.Values[pos], nil
	}

	return nil, nil
}

// Remove 删除索引项
func (idx *BPlusTreeIndex) Remove(key interface{}, rowID uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	leaf := idx.findLeaf(key)
	pos := idx.findPos(leaf.Keys, key)

	if pos < len(leaf.Keys) && idx.compare(leaf.Keys[pos], key) == 0 {
		// 从值列表中删除rowID
		values := leaf.Values[pos]
		newValues := make([]uint64, 0, len(values)-1)
		for _, v := range values {
			if v != rowID {
				newValues = append(newValues, v)
			}
		}

		if len(newValues) == 0 {
			// 如果值列表为空，删除整个键
			leaf.Keys = append(leaf.Keys[:pos], leaf.Keys[pos+1:]...)
			leaf.Values = append(leaf.Values[:pos], leaf.Values[pos+1:]...)
		} else {
			leaf.Values[pos] = newValues
		}
	}

	return idx.Save()
}

// Save 保存索引到文件
func (idx *BPlusTreeIndex) Save() error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(idx.root); err != nil {
		return fmt.Errorf("encoding index: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(idx.path), 0755); err != nil {
		return fmt.Errorf("creating index directory: %w", err)
	}

	return os.WriteFile(idx.path, buf.Bytes(), 0644)
}

// Load 从文件加载索引
func (idx *BPlusTreeIndex) Load() error {
	data, err := os.ReadFile(idx.path)
	if err != nil {
		if os.IsNotExist(err) {
			idx.root = &BPlusTreeNode{IsLeaf: true}
			return nil
		}
		return fmt.Errorf("reading index file: %w", err)
	}

	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&idx.root); err != nil {
		return fmt.Errorf("decoding index: %w", err)
	}

	return nil
}

// 辅助方法
func (idx *BPlusTreeIndex) findLeaf(key interface{}) *BPlusTreeNode {
	node := idx.root
	for !node.IsLeaf {
		pos := idx.findPos(node.Keys, key)
		if pos == len(node.Keys) {
			node = node.Children[pos]
		} else {
			node = node.Children[pos+1]
		}
	}
	return node
}

func (idx *BPlusTreeIndex) findPos(keys []interface{}, key interface{}) int {
	for i, k := range keys {
		if idx.compare(k, key) >= 0 {
			return i
		}
	}
	return len(keys)
}

func (idx *BPlusTreeIndex) splitLeaf(leaf *BPlusTreeNode) {
	mid := len(leaf.Keys) / 2
	newLeaf := &BPlusTreeNode{
		IsLeaf: true,
		Keys:   append([]interface{}{}, leaf.Keys[mid:]...),
		Values: append([][]uint64{}, leaf.Values[mid:]...),
		Next:   leaf.Next,
	}
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]
	leaf.Next = newLeaf

	if leaf == idx.root {
		idx.root = &BPlusTreeNode{
			IsLeaf:   false,
			Keys:     []interface{}{newLeaf.Keys[0]},
			Children: []*BPlusTreeNode{leaf, newLeaf},
		}
	}
}
