package parser

import (
	"encoding/json"
	"fmt"
	"strings"

	"sudatas/internal/storage"
)

// SQLParser SQL解析器
type SQLParser struct{}

// Statement SQL语句解析结果
type Statement struct {
	Type        string
	Table       string
	Collection  string
	Database    string
	DBType      storage.StorageType
	Owner       string
	Description string
	Columns     []string
	Data        storage.Row
	Filter      map[string]interface{}
	Where       *storage.Condition
	FilePath    string
}

// NewSQLParser 创建新的SQL解析器
func NewSQLParser() *SQLParser {
	return &SQLParser{}
}

// Parse 解析SQL语句
func (p *SQLParser) Parse(sql string) (*Statement, error) {
	// 简单的SQL解析实现
	parts := strings.Fields(sql)
	if len(parts) == 0 {
		return nil, fmt.Errorf("空SQL语句")
	}

	stmt := &Statement{}
	stmt.Type = strings.ToUpper(parts[0])

	switch stmt.Type {
	case "INSERT":
		// INSERT INTO collection.database VALUES {...}
		if len(parts) < 4 {
			return nil, fmt.Errorf("无效的INSERT语句")
		}
		if strings.ToUpper(parts[1]) != "INTO" {
			return nil, fmt.Errorf("INSERT语句缺少INTO关键字")
		}

		// 解析集合和数据库名称
		names := strings.Split(parts[2], ".")
		if len(names) != 2 {
			return nil, fmt.Errorf("无效的数据库名称格式，应为: collection.database")
		}
		stmt.Collection = names[0]
		stmt.Database = names[1]

		// 解析VALUES关键字
		if strings.ToUpper(parts[3]) != "VALUES" {
			return nil, fmt.Errorf("INSERT语句缺少VALUES关键字")
		}

		// 解析JSON数据
		jsonData := strings.Join(parts[4:], " ")
		var data storage.Row
		if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
			return nil, fmt.Errorf("解析JSON数据失败: %w", err)
		}
		stmt.Data = data

		return stmt, nil

	case "SELECT":
		// SELECT * FROM collection.database WHERE {...}
		if len(parts) < 4 {
			return nil, fmt.Errorf("无效的SELECT语句")
		}
		if strings.ToUpper(parts[2]) != "FROM" {
			return nil, fmt.Errorf("SELECT语句缺少FROM关键字")
		}

		// 解析列
		if parts[1] == "*" {
			stmt.Columns = nil // 表示所有列
		} else {
			stmt.Columns = strings.Split(parts[1], ",")
		}

		// 解析集合和数据库名称
		names := strings.Split(parts[3], ".")
		if len(names) != 2 {
			return nil, fmt.Errorf("无效的数据库名称格式，应为: collection.database")
		}
		stmt.Collection = names[0]
		stmt.Database = names[1]

		// 解析WHERE子句
		if len(parts) > 4 {
			if strings.ToUpper(parts[4]) != "WHERE" {
				return nil, fmt.Errorf("SELECT语句的WHERE子句无效")
			}
			// 解析JSON条件
			jsonFilter := strings.Join(parts[5:], " ")
			var filter map[string]interface{}
			if err := json.Unmarshal([]byte(jsonFilter), &filter); err != nil {
				return nil, fmt.Errorf("解析WHERE条件失败: %w", err)
			}
			stmt.Filter = filter
		}

		return stmt, nil

	case "CREATE":
		if len(parts) < 2 {
			return nil, fmt.Errorf("无效的CREATE语句")
		}
		objectType := strings.ToUpper(parts[1])
		switch objectType {
		case "COLLECTION":
			if len(parts) < 3 {
				return nil, fmt.Errorf("缺少集合名称")
			}
			stmt.Type = "CREATE_COLLECTION"
			stmt.Collection = parts[2]
			stmt.Owner = "root" // 暂时使用默认用户
			return stmt, nil

		case "DATABASE":
			if len(parts) < 3 {
				return nil, fmt.Errorf("缺少数据库名称")
			}
			stmt.Type = "CREATE_DATABASE"
			names := strings.Split(parts[2], ".")
			if len(names) != 2 {
				return nil, fmt.Errorf("无效的数据库名称格式，应为: collection.database")
			}
			stmt.Collection = names[0]
			stmt.Database = names[1]

			// 解析类型和描述
			for i := 3; i < len(parts); i++ {
				switch strings.ToUpper(parts[i]) {
				case "TYPE":
					if i+1 < len(parts) {
						stmt.DBType = storage.StorageType(parts[i+1])
						i++
					}
				case "DESCRIPTION":
					if i+1 < len(parts) {
						stmt.Description = strings.Trim(parts[i+1], "'")
						i++
					}
				}
			}
			return stmt, nil

		default:
			return nil, fmt.Errorf("不支持的CREATE类型: %s", objectType)
		}

	case "SHOW":
		if len(parts) < 2 {
			return nil, fmt.Errorf("无效的SHOW语句")
		}
		switch strings.ToUpper(parts[1]) {
		case "COLLECTIONS":
			stmt.Type = "SHOW_COLLECTIONS"
			return stmt, nil
		case "DATABASES":
			if len(parts) < 4 || strings.ToUpper(parts[2]) != "FROM" {
				return nil, fmt.Errorf("无效的SHOW DATABASES语句")
			}
			stmt.Type = "SHOW_DATABASES"
			stmt.Collection = parts[3]
			return stmt, nil
		default:
			return nil, fmt.Errorf("不支持的SHOW类型: %s", parts[1])
		}

	case "IMPORT":
		// IMPORT FROM filepath
		if len(parts) < 3 || strings.ToUpper(parts[1]) != "FROM" {
			return nil, fmt.Errorf("无效的IMPORT语句")
		}
		stmt.Type = "IMPORT"
		stmt.FilePath = strings.Join(parts[2:], " ")
		return stmt, nil

	case "EXPORT":
		// EXPORT collection.database TO filepath
		if len(parts) < 4 || strings.ToUpper(parts[2]) != "TO" {
			return nil, fmt.Errorf("无效的EXPORT语句")
		}

		// 解析集合和数据库名称
		names := strings.Split(parts[1], ".")
		if len(names) != 2 {
			return nil, fmt.Errorf("无效的数据库名称格式，应为: collection.database")
		}
		stmt.Collection = names[0]
		stmt.Database = names[1]
		stmt.FilePath = strings.Join(parts[3:], " ")
		return stmt, nil

	default:
		return nil, fmt.Errorf("不支持的SQL语句: %s", sql)
	}

	return nil, fmt.Errorf("SQL语句解析失败")
}
