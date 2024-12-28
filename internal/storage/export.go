package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ExportOptions 导出选项
type ExportOptions struct {
	IncludeSchema bool   // 是否包含表结构
	Format        string // 导出格式（sql, json等）
	Directory     string // 导出目录
	Filename      string // 导出文件名（可选）
}

// ExportDatabase 导出数据库
func (ms *MemoryStore) ExportDatabase(collection, database string, opts ExportOptions) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// 检查集合和数据库是否存在
	if _, exists := ms.data[collection]; !exists {
		return fmt.Errorf("集合不存在: %s", collection)
	}
	if _, exists := ms.data[collection][database]; !exists {
		return fmt.Errorf("数据库不存在: %s", database)
	}

	// 生成文件名
	if opts.Filename == "" {
		opts.Filename = fmt.Sprintf("%s_%s_%s.suql",
			collection,
			database,
			time.Now().Format("20060102_150405"),
		)
	}

	// 确保目录存在
	if err := os.MkdirAll(opts.Directory, 0755); err != nil {
		return fmt.Errorf("创建导出目录失败: %w", err)
	}

	// 打开文件
	filePath := filepath.Join(opts.Directory, opts.Filename)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("创建导出文件失败: %w", err)
	}
	defer file.Close()

	// 写入文件头
	header := fmt.Sprintf("-- SuDB 导出\n"+
		"-- 集合: %s\n"+
		"-- 数据库: %s\n"+
		"-- 导出时间: %s\n"+
		"-- 版本: 1.0\n\n",
		collection,
		database,
		time.Now().Format("2006-01-02 15:04:05"),
	)
	if _, err := file.WriteString(header); err != nil {
		return err
	}

	// 写入创建集合语句
	createCollection := fmt.Sprintf("CREATE COLLECTION IF NOT EXISTS %s;\n\n", collection)
	if _, err := file.WriteString(createCollection); err != nil {
		return err
	}

	// 写入创建数据库语句
	createDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s.%s TYPE json DESCRIPTION '导出的数据库';\n\n",
		collection, database)
	if _, err := file.WriteString(createDatabase); err != nil {
		return err
	}

	// 写入数据
	records := ms.data[collection][database]
	for _, record := range records {
		// 将记录转换为SQL语句
		sql, err := recordToSQL(collection, database, record)
		if err != nil {
			return fmt.Errorf("转换记录失败: %w", err)
		}

		// 写入SQL语句
		if _, err := file.WriteString(sql + "\n"); err != nil {
			return err
		}
	}

	return nil
}

// recordToSQL 将记录转换为SQL语句
func recordToSQL(collection, database string, record Row) (string, error) {
	// 将记录转换为JSON字符串
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return "", err
	}

	// 构造INSERT语句
	sql := fmt.Sprintf("INSERT INTO %s.%s VALUES %s;",
		collection,
		database,
		string(data),
	)

	return sql, nil
}

// ImportFromFile 从文件导入数据
func (ms *MemoryStore) ImportFromFile(filePath string, targetCollection string) error {
	// 读取文件
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("读取文件失败: %w", err)
	}
	log.Printf("读取文件成功: %s", filePath)

	// 分割SQL语句
	statements := make([]string, 0)
	currentStmt := ""
	inValues := false

	// 按行读取，处理多行JSON
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)

		// 跳过注释和空行
		if strings.HasPrefix(line, "--") || line == "" {
			continue
		}

		// 处理VALUES子句
		if strings.Contains(line, "VALUES") {
			inValues = true
		}

		// 如果在VALUES子句中，保留空格和换行
		if inValues {
			currentStmt += line
		} else {
			currentStmt += " " + line
		}

		// 检查语句是否结束
		if strings.HasSuffix(line, ";") {
			statements = append(statements, strings.TrimSpace(currentStmt))
			currentStmt = ""
			inValues = false
		}
	}

	log.Printf("解析到 %d 条SQL语句", len(statements))

	// 执行每个语句
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		log.Printf("执行第 %d 条语句: %s", i+1, stmt)
		// 执行SQL语句，传入目标集合名称
		if err := ms.executeImportStatement(stmt, targetCollection); err != nil {
			return fmt.Errorf("执行语句失败 [%d]: %w", i+1, err)
		}
	}

	ms.dirty = true
	return nil
}

// executeImportStatement 执行导入语句
func (ms *MemoryStore) executeImportStatement(stmt string, targetCollection string) error {
	// 去除语句末尾的分号
	stmt = strings.TrimSuffix(stmt, ";")

	if strings.HasPrefix(stmt, "CREATE COLLECTION") {
		// 使用目标集合名称替代原始集合名称
		collection := targetCollection
		log.Printf("使用目标集合: %s", collection)

		// 创建集合
		ms.mu.Lock()
		if _, exists := ms.data[collection]; !exists {
			ms.data[collection] = make(map[string][]Row)
			log.Printf("创建新集合: %s", collection)
		}
		ms.mu.Unlock()

	} else if strings.HasPrefix(stmt, "CREATE DATABASE") {
		// 处理创建数据库语句
		parts := strings.Fields(stmt)
		log.Printf("CREATE DATABASE parts: %v", parts)

		// 找到数据库名称
		var dbNamePart string
		hasIfNotExists := false

		for i := 0; i < len(parts); i++ {
			if strings.EqualFold(parts[i], "DATABASE") {
				i++ // 跳过 DATABASE
				if i >= len(parts) {
					return fmt.Errorf("缺少数据库名称")
				}

				// 检查是否有 IF NOT EXISTS
				if i+3 < len(parts) &&
					strings.EqualFold(parts[i], "IF") &&
					strings.EqualFold(parts[i+1], "NOT") &&
					strings.EqualFold(parts[i+2], "EXISTS") {
					hasIfNotExists = true
					dbNamePart = parts[i+3]
					i += 3
				} else {
					dbNamePart = parts[i]
				}
				break
			}
		}

		if dbNamePart == "" {
			return fmt.Errorf("无效的CREATE DATABASE语句")
		}

		// 解析数据库名称
		names := strings.Split(dbNamePart, ".")
		if len(names) != 2 {
			return fmt.Errorf("无效的数据库名称格式: %s", dbNamePart)
		}

		// 使用目标集合名称
		collection := targetCollection
		log.Printf("创建数据库: %s.%s (IF NOT EXISTS: %v)", collection, names[1], hasIfNotExists)

		// 创建数据库
		ms.mu.Lock()
		if _, exists := ms.data[collection]; !exists {
			ms.data[collection] = make(map[string][]Row)
		}
		if _, exists := ms.data[collection][names[1]]; !exists {
			ms.data[collection][names[1]] = make([]Row, 0)
			log.Printf("创建新数据库: %s.%s", collection, names[1])
		}
		ms.mu.Unlock()

	} else if strings.HasPrefix(stmt, "INSERT INTO") {
		// 处理插入语句
		parts := strings.Fields(stmt)
		log.Printf("INSERT INTO parts: %v", parts)
		if len(parts) < 4 {
			return fmt.Errorf("无效的INSERT语句")
		}

		// 使用目标集合名称
		names := strings.Split(parts[2], ".")
		if len(names) != 2 {
			return fmt.Errorf("无效的数据库名称格式: %s", parts[2])
		}
		database := names[1]
		collection := targetCollection

		log.Printf("插入数据到: %s.%s", collection, database)

		// 解析JSON数据
		valuesIndex := strings.Index(strings.ToUpper(stmt), "VALUES")
		if valuesIndex == -1 {
			return fmt.Errorf("无效的INSERT语句：缺少VALUES关键字")
		}

		jsonData := strings.TrimSpace(stmt[valuesIndex+6:])
		log.Printf("JSON数据: %s", jsonData)

		var record Row
		if err := json.Unmarshal([]byte(jsonData), &record); err != nil {
			return fmt.Errorf("解析JSON数据失败: %w", err)
		}

		// 插入记录
		return ms.InsertRecord(collection, database, record)
	}

	return nil
}
