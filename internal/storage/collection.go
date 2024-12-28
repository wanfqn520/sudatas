package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"sudatas/internal/security"
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

// Collection 集合结构
type Collection struct {
	Name      string                  `json:"name"`
	Owner     string                  `json:"owner"`
	Created   time.Time               `json:"created"`
	Updated   time.Time               `json:"updated"`
	Databases map[string]Database     `json:"databases"`
	basePath  string                  `json:"-"`
	crypto    *security.CryptoManager `json:"-"` // 添加加密管理器
}

// Database 数据库定义
type Database struct {
	Name        string      `json:"name"`
	Type        StorageType `json:"type"`
	Description string      `json:"description"`
	Created     time.Time   `json:"created"` // 改为 time.Time
	Updated     time.Time   `json:"updated"` // 改为 time.Time
}

// CollectionManager 集合管理器
type CollectionManager struct {
	mu          sync.RWMutex
	collections map[string]*Collection
	dataDir     string
	builtinDir  string
	crypto      *security.CryptoManager
}

// NewCollectionManager 创建集合管理器
func NewCollectionManager(dataDir, builtinDir string, crypto *security.CryptoManager) (*CollectionManager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败: %w", err)
	}

	cm := &CollectionManager{
		collections: make(map[string]*Collection),
		dataDir:     dataDir,
		builtinDir:  builtinDir,
		crypto:      crypto,
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

	now := time.Now()
	collection := &Collection{
		Name:      name,
		Owner:     owner,
		Created:   now,
		Updated:   now,
		Databases: make(map[string]Database),
		basePath:  collectionPath,
		crypto:    cm.crypto, // 传递加密管理器
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
	// 检查数据库是否已存在
	if _, exists := c.Databases[name]; exists {
		return fmt.Errorf("数据库已存在: %s", name)
	}

	// 创建数据库目录
	dbPath := filepath.Join(c.basePath, name) // 移除 .sudb 后缀
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("创建数据库目录失败: %w", err)
	}

	now := time.Now()
	c.Databases[name] = Database{
		Name:        name,
		Type:        dbType,
		Description: description,
		Created:     now,
		Updated:     now,
	}

	// 初始化存储引擎
	if err := c.initializeStorage(dbPath, dbType); err != nil {
		delete(c.Databases, name)
		os.RemoveAll(dbPath) // 清理失败的目录
		return fmt.Errorf("初始化存储引擎失败: %w", err)
	}

	// 保存集合元数据
	if err := c.save(); err != nil {
		delete(c.Databases, name)
		os.RemoveAll(dbPath) // 清理失败的目录
		return fmt.Errorf("保存集合元数据失败: %w", err)
	}

	return nil
}

// initializeStorage 初始化存储引擎
func (c *Collection) initializeStorage(dbPath string, dbType StorageType) error {
	switch dbType {
	case JsonStorage:
		// 创建数据目录
		dataDir := filepath.Join(dbPath, "data")
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("创建数据目录失败: %w", err)
		}

		// 创建并加密元数据文件
		metaFile := filepath.Join(dbPath, "meta.sudb")
		meta := struct {
			Type    string    `json:"type"`
			Version string    `json:"version"`
			Created time.Time `json:"created"`
		}{
			Type:    "json",
			Version: "1.0",
			Created: time.Now(),
		}

		data, err := json.MarshalIndent(meta, "", "  ")
		if err != nil {
			return fmt.Errorf("序列化元数据失败: %w", err)
		}

		// 加密元数据
		encrypted, err := c.crypto.EncryptSM4(data)
		if err != nil {
			return fmt.Errorf("加密元数据失败: %w", err)
		}

		if err := os.WriteFile(metaFile, encrypted, 0600); err != nil {
			return fmt.Errorf("写入元数据失败: %w", err)
		}

		return nil

	case TextStorage:
		return os.MkdirAll(filepath.Join(dbPath, "texts"), 0755)

	case TableStorage:
		if err := os.MkdirAll(filepath.Join(dbPath, "tables"), 0755); err != nil {
			return err
		}
		return os.MkdirAll(filepath.Join(dbPath, "indexes"), 0755)

	case GraphStorage:
		if err := os.MkdirAll(filepath.Join(dbPath, "nodes"), 0755); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(dbPath, "edges"), 0755); err != nil {
			return err
		}
		return os.MkdirAll(filepath.Join(dbPath, "indexes"), 0755)

	default:
		return fmt.Errorf("不支持的存储类型: %s", dbType)
	}
}

// save 保存集合元数据（加密）
func (c *Collection) save() error {
	metaFile := filepath.Join(c.basePath, "meta.sudb")
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化集合元数据失败: %w", err)
	}

	// 使用SM4加密数据
	encrypted, err := c.crypto.EncryptSM4(data)
	if err != nil {
		return fmt.Errorf("加密元数据失败: %w", err)
	}

	return os.WriteFile(metaFile, encrypted, 0600)
}

// loadCollections 加载所有集合（解密）
func (cm *CollectionManager) loadCollections() error {
	entries, err := os.ReadDir(cm.dataDir)
	if err != nil {
		return fmt.Errorf("读取数据目录失败: %w", err)
	}

	// 清空现有集合
	cm.collections = make(map[string]*Collection)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		collectionPath := filepath.Join(cm.dataDir, entry.Name())
		metaFile := filepath.Join(collectionPath, "meta.sudb")

		// 读取加密数据
		encrypted, err := os.ReadFile(metaFile)
		if err != nil {
			continue // 跳过无效的集合
		}

		// 解密数据
		data, err := cm.crypto.DecryptSM4(encrypted)
		if err != nil {
			continue // 跳过无法解密的集合
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

// GetPath 获取集合路径
func (c *Collection) GetPath() string {
	return c.basePath
}
