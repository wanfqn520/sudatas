package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sudatas/internal/security"
)

// Operation 操作类型
type Operation struct {
	Type  OperationType
	Table string
	Data  Row
	Where *Condition // 使用从 condition.go 导入的 Condition 类型
}

// OperationType 操作类型
type OperationType int

const (
	Insert OperationType = iota
	Update
	Delete
)

// Row 数据行
type Row map[string]interface{}

// Table 表结构
type Table struct {
	Name    string
	Columns []Column
	Rows    []Row
	Indexes map[string]Index
}

// Column 列定义
type Column struct {
	Name    string
	Type    string
	Indexed bool
	IdxType IndexType
}

// Transaction 事务结构
type Transaction struct {
	engine     *Engine
	operations []Operation
}

// Engine 存储引擎
type Engine struct {
	dataDir     string // 用户数据目录
	builtinDir  string // 系统文件目录
	collections *CollectionManager
	backup      *BackupManager
	crypto      *security.CryptoManager
	MemStore    *MemoryStore // 添加内存存储
}

func NewEngine(dataDir, builtinDir string, crypto *security.CryptoManager) (*Engine, error) {
	cm, err := NewCollectionManager(dataDir, builtinDir, crypto)
	if err != nil {
		return nil, err
	}

	engine := &Engine{
		dataDir:     dataDir,
		builtinDir:  builtinDir,
		collections: cm,
		crypto:      crypto,
	}

	// 初始化内存存储
	engine.MemStore = NewMemoryStore(dataDir, crypto)
	if err := engine.MemStore.LoadFromDisk(); err != nil {
		log.Printf("加载数据失败: %v", err)
	}

	// 初始化备份管理器
	backupDir := filepath.Join(builtinDir, "backups")
	bm, err := NewBackupManager(backupDir, engine)
	if err != nil {
		return nil, err
	}
	engine.backup = bm

	return engine, nil
}

func (e *Engine) CreateTable(name string, columns []Column) error {
	table := &Table{
		Name:    name,
		Columns: columns,
	}

	// 将表结构保存为.sudb文件
	filename := filepath.Join(e.dataDir, name+".sudb")
	data, err := json.Marshal(table)
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

// 添加数据
func (e *Engine) Insert(tableName string, row Row) error {
	table, err := e.loadTable(tableName)
	if err != nil {
		return err
	}

	// 验证数据结构
	if err := e.validateRow(table, row); err != nil {
		return err
	}

	table.Rows = append(table.Rows, row)
	return e.saveTable(table)
}

// 查询数据
func (e *Engine) Select(tableName string, columns []string, where *Condition) ([]Row, error) {
	table, err := e.loadTable(tableName)
	if err != nil {
		return nil, err
	}

	// 如果有索引且where条件匹配索引列，使用索引查询
	if where != nil {
		if index, ok := table.Indexes[where.Column]; ok {
			rowIDs, err := index.Find(where.Value)
			if err != nil {
				return nil, err
			}

			result := make([]Row, 0, len(rowIDs))
			for _, id := range rowIDs {
				if id < uint64(len(table.Rows)) {
					row := table.Rows[id]
					if e.matchCondition(row, where) {
						if len(columns) == 0 {
							result = append(result, row)
						} else {
							filteredRow := make(Row)
							for _, col := range columns {
								if val, ok := row[col]; ok {
									filteredRow[col] = val
								}
							}
							result = append(result, filteredRow)
						}
					}
				}
			}
			return result, nil
		}
	}

	// 如果没有可用的索引，使用全表扫描
	var result []Row
	for _, row := range table.Rows {
		if where == nil || e.matchCondition(row, where) {
			if len(columns) == 0 {
				result = append(result, row)
			} else {
				filteredRow := make(Row)
				for _, col := range columns {
					if val, ok := row[col]; ok {
						filteredRow[col] = val
					}
				}
				result = append(result, filteredRow)
			}
		}
	}

	return result, nil
}

// 更新数据
func (e *Engine) Update(tableName string, updates Row, where *Condition) error {
	table, err := e.loadTable(tableName)
	if err != nil {
		return err
	}

	for i, row := range table.Rows {
		if where == nil || e.matchCondition(row, where) {
			for k, v := range updates {
				table.Rows[i][k] = v
			}
		}
	}

	return e.saveTable(table)
}

// 删除数据
func (e *Engine) Delete(tableName string, where *Condition) error {
	table, err := e.loadTable(tableName)
	if err != nil {
		return err
	}

	var newRows []Row
	for _, row := range table.Rows {
		if !e.matchCondition(row, where) {
			newRows = append(newRows, row)
		}
	}

	table.Rows = newRows
	return e.saveTable(table)
}

// 开始事务
func (e *Engine) BeginTransaction() *Transaction {
	return &Transaction{
		engine:     e,
		operations: make([]Operation, 0),
	}
}

// 提交事务
func (t *Transaction) Commit() error {
	for _, op := range t.operations {
		switch op.Type {
		case Insert:
			if err := t.engine.Insert(op.Table, op.Data); err != nil {
				return err
			}
		case Update:
			if err := t.engine.Update(op.Table, op.Data, op.Where); err != nil {
				return err
			}
		case Delete:
			if err := t.engine.Delete(op.Table, op.Where); err != nil {
				return err
			}
		}
	}
	return nil
}

// 辅助函数
func (e *Engine) loadTable(name string) (*Table, error) {
	filename := filepath.Join(e.dataDir, name+".sudb")
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var table Table
	if err := json.Unmarshal(data, &table); err != nil {
		return nil, err
	}

	return &table, nil
}

func (e *Engine) saveTable(table *Table) error {
	filename := filepath.Join(e.dataDir, table.Name+".sudb")
	data, err := json.Marshal(table)
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

func (e *Engine) validateRow(table *Table, row Row) error {
	for _, col := range table.Columns {
		val, exists := row[col.Name]
		if !exists {
			return fmt.Errorf("missing column: %s", col.Name)
		}

		// 简单的类型检查
		switch col.Type {
		case "string":
			if _, ok := val.(string); !ok {
				return fmt.Errorf("invalid type for column %s: expected string", col.Name)
			}
		case "int":
			if _, ok := val.(float64); !ok {
				return fmt.Errorf("invalid type for column %s: expected int", col.Name)
			}
		}
	}
	return nil
}

func (e *Engine) matchCondition(row Row, cond *Condition) bool {
	if cond == nil {
		return true
	}

	val, exists := row[cond.Column]
	if !exists {
		return false
	}

	switch cond.Operator {
	case "=":
		return val == cond.Value
	case ">":
		return compareValues(val, cond.Value) > 0
	case "<":
		return compareValues(val, cond.Value) < 0
	case ">=":
		return compareValues(val, cond.Value) >= 0
	case "<=":
		return compareValues(val, cond.Value) <= 0
	case "!=":
		return val != cond.Value
	}

	return false
}

func compareValues(a, b interface{}) int {
	// 实现通用的值比较
	switch v1 := a.(type) {
	case string:
		if v2, ok := b.(string); ok {
			switch {
			case v1 < v2:
				return -1
			case v1 > v2:
				return 1
			default:
				return 0
			}
		}
	case float64:
		if v2, ok := b.(float64); ok {
			switch {
			case v1 < v2:
				return -1
			case v1 > v2:
				return 1
			default:
				return 0
			}
		}
	}
	return 0
}

func (e *Engine) CreateIndex(tableName, columnName string, idxType IndexType) error {
	table, err := e.loadTable(tableName)
	if err != nil {
		return err
	}

	// 检查列是否存在
	var col *Column
	for i := range table.Columns {
		if table.Columns[i].Name == columnName {
			col = &table.Columns[i]
			break
		}
	}
	if col == nil {
		return fmt.Errorf("column %s not found", columnName)
	}

	// 初始化索引映射
	if table.Indexes == nil {
		table.Indexes = make(map[string]Index)
	}

	// 创建索引
	indexPath := filepath.Join(e.dataDir, fmt.Sprintf("%s_%s.idx", tableName, columnName))
	var index Index
	switch idxType {
	case BTreeIndex:
		index = NewBPlusTreeIndex(indexPath, 4, compareValues)
	default:
		return fmt.Errorf("unsupported index type: %v", idxType)
	}

	// 为现有数据建立索引
	for i, row := range table.Rows {
		if val, ok := row[columnName]; ok {
			if err := index.Add(val, uint64(i)); err != nil {
				return err
			}
		}
	}

	table.Indexes[columnName] = index
	col.Indexed = true
	col.IdxType = idxType

	return e.saveTable(table)
}

// CreateCollection 创建新的集合
func (e *Engine) CreateCollection(name, owner string) error {
	_, err := e.collections.CreateCollection(name, owner)
	return err
}

// CreateDatabase 在集合中创建数据库
func (e *Engine) CreateDatabase(collection, dbName string, dbType StorageType, description string) error {
	col, err := e.collections.GetCollection(collection)
	if err != nil {
		return err
	}
	return col.CreateDatabase(dbName, dbType, description)
}

// GetCollection 获取集合
func (e *Engine) GetCollection(name string) (*Collection, error) {
	return e.collections.GetCollection(name)
}

// ListCollections 列出所有集合
func (e *Engine) ListCollections() []*Collection {
	return e.collections.ListCollections()
}

// DeleteCollection 删除集合
func (e *Engine) DeleteCollection(name string) error {
	return e.collections.DeleteCollection(name)
}

// 添加备份相关方法
func (e *Engine) BackupCollection(collectionName, description string) (*BackupInfo, error) {
	return e.backup.BackupCollection(collectionName, description)
}

func (e *Engine) RestoreCollection(backupID string) error {
	return e.backup.RestoreCollection(backupID)
}

func (e *Engine) ListBackups() ([]*BackupInfo, error) {
	return e.backup.ListBackups()
}

func (e *Engine) DeleteBackup(backupID string) error {
	return e.backup.DeleteBackup(backupID)
}

// 添加事务操作方法
func (t *Transaction) AddOperation(op Operation) {
	t.operations = append(t.operations, op)
}

// 示例使用方法
func (t *Transaction) InsertRow(table string, data Row) {
	t.AddOperation(Operation{
		Type:  Insert,
		Table: table,
		Data:  data,
	})
}

func (t *Transaction) UpdateRows(table string, data Row, where *Condition) {
	t.AddOperation(Operation{
		Type:  Update,
		Table: table,
		Data:  data,
		Where: where,
	})
}

func (t *Transaction) DeleteRows(table string, where *Condition) {
	t.AddOperation(Operation{
		Type:  Delete,
		Table: table,
		Where: where,
	})
}

// Shutdown 关闭引擎
func (e *Engine) Shutdown() error {
	// 停止定时保存
	e.MemStore.Stop()

	// 最后保存一次数据
	if err := e.MemStore.SaveToDisk(); err != nil {
		log.Printf("保存数据失败: %v", err)
	}

	// ... 其他关闭代码 ...
	return nil
}
