package storage

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sudatas/internal/security"
	"sync"
	"time"
)

// MemoryStore 内存存储管理器
type MemoryStore struct {
	mu           sync.RWMutex
	data         map[string]map[string][]Row // data[collection][database][]Row
	crypto       *security.CryptoManager
	dataDir      string    // 用于持久化
	lastSave     time.Time // 上次保存时间
	saveInterval time.Duration
	stopChan     chan struct{} // 用于停止定时保存
	dirty        bool          // 数据是否被修改
}

// NewMemoryStore 创建内存存储管理器
func NewMemoryStore(dataDir string, crypto *security.CryptoManager) *MemoryStore {
	ms := &MemoryStore{
		data:         make(map[string]map[string][]Row),
		crypto:       crypto,
		dataDir:      dataDir,
		saveInterval: time.Minute * 30, // 30分钟保存一次
		stopChan:     make(chan struct{}),
	}

	// 加载数据
	if err := ms.LoadFromDisk(); err != nil {
		log.Printf("加载数据失败: %v", err)
	}

	// 启动定时保存
	go ms.autoSave()

	return ms
}

// InsertRecord 插入记录
func (ms *MemoryStore) InsertRecord(collection, database string, record Row) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// 确保集合存在
	if _, exists := ms.data[collection]; !exists {
		ms.data[collection] = make(map[string][]Row)
	}

	// 确保数据库存在
	if _, exists := ms.data[collection][database]; !exists {
		ms.data[collection][database] = make([]Row, 0)
	}

	// 添加记录
	ms.data[collection][database] = append(ms.data[collection][database], record)
	ms.dirty = true // 标记数据已修改
	return nil
}

// autoSave 定时自动保存
func (ms *MemoryStore) autoSave() {
	ticker := time.NewTicker(ms.saveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ms.mu.RLock()
			if ms.dirty {
				if err := ms.SaveToDisk(); err != nil {
					log.Printf("自动保存失败: %v", err)
				} else {
					ms.dirty = false
				}
			}
			ms.mu.RUnlock()
		case <-ms.stopChan:
			return
		}
	}
}

// Stop 停止定时保存并执行最后一次保存
func (ms *MemoryStore) Stop() {
	ms.mu.Lock()
	if ms.dirty {
		if err := ms.SaveToDisk(); err != nil {
			log.Printf("最终保存失败: %v", err)
		}
	}
	ms.mu.Unlock()
	close(ms.stopChan)
}

// QueryRecords 查询记录
func (ms *MemoryStore) QueryRecords(collection, database string, filter map[string]interface{}) ([]Row, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// 检查集合和数据库是否存在
	if _, exists := ms.data[collection]; !exists {
		return []Row{}, nil
	}
	if _, exists := ms.data[collection][database]; !exists {
		return []Row{}, nil
	}

	records := ms.data[collection][database]
	if filter == nil {
		// 返回所有记录的副本
		result := make([]Row, len(records))
		copy(result, records)
		return result, nil
	}

	// 过滤记录
	var result []Row
	for _, record := range records {
		if MatchConditions(record, filter) {
			result = append(result, record)
		}
	}
	return result, nil
}

// SaveToDisk 保存数据到磁盘
func (ms *MemoryStore) SaveToDisk() error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	for collection, databases := range ms.data {
		// 创建集合目录
		collectionPath := filepath.Join(ms.dataDir, collection)
		if err := os.MkdirAll(collectionPath, 0755); err != nil {
			log.Printf("创建集合目录失败: %v", err)
			continue
		}

		for database, records := range databases {
			// 创建数据库目录
			dbPath := filepath.Join(collectionPath, database)
			if err := os.MkdirAll(dbPath, 0755); err != nil {
				log.Printf("创建数据库目录失败: %v", err)
				continue
			}

			// 序列化数据
			dataPath := filepath.Join(dbPath, "data.sudb")
			data, err := json.MarshalIndent(records, "", "  ")
			if err != nil {
				log.Printf("序列化数据失败: %v", err)
				continue
			}

			// 先创建备份
			if _, err := os.Stat(dataPath); err == nil {
				if err := os.Rename(dataPath, dataPath+".bak"); err != nil {
					log.Printf("创建备份失败: %v", err)
				}
			}

			// 使用临时文件保存
			tempPath := dataPath + ".tmp"
			if err := os.WriteFile(tempPath, data, 0644); err != nil {
				log.Printf("写入临时文件失败: %v", err)
				continue
			}

			// 重命名临时文件
			if err := os.Rename(tempPath, dataPath); err != nil {
				os.Remove(tempPath)
				log.Printf("重命名文件失败: %v", err)
				continue
			}

			log.Printf("保存数据成功: %s (%d 条记录)", dataPath, len(records))
		}
	}

	ms.lastSave = time.Now()
	ms.dirty = false
	return nil
}

// LoadFromDisk 从磁盘加载数据
func (ms *MemoryStore) LoadFromDisk() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// 清空现有数据
	ms.data = make(map[string]map[string][]Row)

	// 遍历所有集合
	collections, err := os.ReadDir(ms.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, col := range collections {
		if !col.IsDir() {
			continue
		}

		collectionPath := filepath.Join(ms.dataDir, col.Name())
		databases, err := os.ReadDir(collectionPath)
		if err != nil {
			continue
		}

		for _, db := range databases {
			if !db.IsDir() {
				continue
			}

			// 读取数据文件（.sudb 后缀）
			dataPath := filepath.Join(collectionPath, db.Name(), "data.sudb")
			data, err := os.ReadFile(dataPath)
			if err != nil {
				if !os.IsNotExist(err) {
					log.Printf("读取数据文件失败: %v", err)
				}
				continue
			}

			// 尝试从备份文件恢复
			if err := json.Unmarshal(data, &[]Row{}); err != nil {
				log.Printf("数据文件损坏，尝试从备份恢复: %v", err)
				backupPath := dataPath + ".bak"
				if backupData, err := os.ReadFile(backupPath); err == nil {
					data = backupData
				} else {
					log.Printf("备份文件不存在或损坏，跳过加载: %v", err)
					continue
				}
			}

			// 解析JSON数据
			var records []Row
			if err := json.Unmarshal(data, &records); err != nil {
				log.Printf("解析数据失败: %v", err)
				continue
			}

			// 保存到内存
			if _, exists := ms.data[col.Name()]; !exists {
				ms.data[col.Name()] = make(map[string][]Row)
			}
			ms.data[col.Name()][db.Name()] = records
			log.Printf("加载数据成功: %s (%d 条记录)", dataPath, len(records))

			// 创建备份
			if err := os.WriteFile(dataPath+".bak", data, 0644); err != nil {
				log.Printf("创建备份失败: %v", err)
			}
		}
	}

	ms.dirty = false
	return nil
}
