package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// StorageType 存储类型
type StorageType string

const (
	JsonStorage  StorageType = "json"
	TextStorage  StorageType = "text"
	TableStorage StorageType = "table"
	GraphStorage StorageType = "graph"
	MaxDatabases             = 8 // 每个集合最大数据库数量
)

// Collection 数据库集合
type Collection struct {
	Name      string              `json:"name"`
	Owner     string              `json:"owner"`
	Databases map[string]Database `json:"databases"`
	mu        sync.RWMutex        `json:"-"`
	basePath  string              `json:"-"`
}

// Database 数据库定义
type Database struct {
	Name        string      `json:"name"`
	Type        StorageType `json:"type"`
	Description string      `json:"description"`
	Created     int64       `json:"created"`
	Updated     int64       `json:"updated"`
}

// CollectionManager 集合管理器
type CollectionManager struct {
	mu          sync.RWMutex
	collections map[string]*Collection
	dataDir     string
}

// NewCollectionManager 创建集合管理器
func NewCollectionManager(dataDir string) (*CollectionManager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败: %w", err)
	}

	cm := &CollectionManager{
		collections: make(map[string]*Collection),
		dataDir:     dataDir,
	}

	// 加载现有集合
	if err := cm.loadCollections(); err != nil {
		return nil, err
	}

	return cm, nil
}

// CreateCollection 创建新的集合
func (cm *CollectionManager) CreateCollection(name, owner string) (*Collection, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.collections[name]; exists {
		return nil, fmt.Errorf("集合已存在: %s", name)
	}

	collectionPath := filepath.Join(cm.dataDir, name)
	if err := os.MkdirAll(collectionPath, 0755); err != nil {
		return nil, fmt.Errorf("创建集合目录失败: %w", err)
	}

	collection := &Collection{
		Name:      name,
		Owner:     owner,
		Databases: make(map[string]Database),
		basePath:  collectionPath,
	}

	cm.collections[name] = collection
	if err := collection.save(); err != nil {
		os.RemoveAll(collectionPath)
		delete(cm.collections, name)
		return nil, err
	}

	return collection, nil
}

// CreateDatabase 在集合中创建数据库
func (c *Collection) CreateDatabase(name string, dbType StorageType, description string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Databases) >= MaxDatabases {
		return fmt.Errorf("已达到最大数据库数量限制(%d)", MaxDatabases)
	}

	if _, exists := c.Databases[name]; exists {
		return fmt.Errorf("数据库已存在: %s", name)
	}

	// 创建数据库目录
	dbPath := filepath.Join(c.basePath, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("创建数据库目录失败: %w", err)
	}

	// 初始化存储引擎
	if err := c.initializeStorage(dbPath, dbType); err != nil {
		os.RemoveAll(dbPath)
		return err
	}

	now := time.Now().Unix()
	c.Databases[name] = Database{
		Name:        name,
		Type:        dbType,
		Description: description,
		Created:     now,
		Updated:     now,
	}

	return c.save()
}

// initializeStorage 初始化存储引擎
func (c *Collection) initializeStorage(dbPath string, dbType StorageType) error {
	switch dbType {
	case JsonStorage:
		return initJsonStorage(dbPath)
	case TextStorage:
		return initTextStorage(dbPath)
	case TableStorage:
		return initTableStorage(dbPath)
	case GraphStorage:
		return initGraphStorage(dbPath)
	default:
		return fmt.Errorf("不支持的存储类型: %s", dbType)
	}
}

// 各种存储类型的初始化函数
func initJsonStorage(path string) error {
	metaFile := filepath.Join(path, "meta.json")
	meta := struct {
		Type    string `json:"type"`
		Version string `json:"version"`
	}{
		Type:    "json",
		Version: "1.0",
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(metaFile, data, 0644)
}

func initTextStorage(path string) error {
	// 创建文本数据目录
	return os.MkdirAll(filepath.Join(path, "texts"), 0755)
}

func initTableStorage(path string) error {
	// 创建表格相关目录
	if err := os.MkdirAll(filepath.Join(path, "tables"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "indexes"), 0755); err != nil {
		return err
	}
	return nil
}

func initGraphStorage(path string) error {
	// 创建图数据库相关目录
	if err := os.MkdirAll(filepath.Join(path, "nodes"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "edges"), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "indexes"), 0755); err != nil {
		return err
	}
	return nil
}

// save 保存集合元数据
func (c *Collection) save() error {
	metaFile := filepath.Join(c.basePath, "meta.json")
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化集合元数据失败: %w", err)
	}
	return os.WriteFile(metaFile, data, 0644)
}

// loadCollections 加载所有集合
func (cm *CollectionManager) loadCollections() error {
	entries, err := os.ReadDir(cm.dataDir)
	if err != nil {
		return fmt.Errorf("读取数据目录失败: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		collectionPath := filepath.Join(cm.dataDir, entry.Name())
		metaFile := filepath.Join(collectionPath, "meta.json")

		data, err := os.ReadFile(metaFile)
		if err != nil {
			continue // 跳过无效的集合
		}

		var collection Collection
		if err := json.Unmarshal(data, &collection); err != nil {
			continue
		}

		collection.basePath = collectionPath
		cm.collections[collection.Name] = &collection
	}

	return nil
}

// GetCollection 获取集合
func (cm *CollectionManager) GetCollection(name string) (*Collection, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if collection, exists := cm.collections[name]; exists {
		return collection, nil
	}
	return nil, fmt.Errorf("集合不存在: %s", name)
}

// ListCollections 列出所有集合
func (cm *CollectionManager) ListCollections() []*Collection {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	collections := make([]*Collection, 0, len(cm.collections))
	for _, collection := range cm.collections {
		collections = append(collections, collection)
	}
	return collections
}

// DeleteCollection 删除集合
func (cm *CollectionManager) DeleteCollection(name string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	collection, exists := cm.collections[name]
	if !exists {
		return fmt.Errorf("集合不存在: %s", name)
	}

	// 删除集合目录
	if err := os.RemoveAll(collection.basePath); err != nil {
		return fmt.Errorf("删除集合目录失败: %w", err)
	}

	delete(cm.collections, name)
	return nil
}
