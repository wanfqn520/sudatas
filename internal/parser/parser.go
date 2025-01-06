package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
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

	case "UPDATE":
		// UPDATE collection.database SET field = value WHERE {...}
		if len(parts) < 4 {
			return nil, fmt.Errorf("无效的UPDATE语句")
		}

		// 解析集合和数据库名称
		names := strings.Split(parts[1], ".")
		if len(names) != 2 {
			return nil, fmt.Errorf("无效的数据库名称格式，应为: collection.database")
		}
		stmt.Collection = names[0]
		stmt.Database = names[1]

		// 查找 SET 和 WHERE 关键字的位置
		setIndex := -1
		whereIndex := -1
		for i, part := range parts {
			if strings.ToUpper(part) == "SET" {
				setIndex = i
			} else if strings.ToUpper(part) == "WHERE" {
				whereIndex = i
				break
			}
		}

		if setIndex == -1 {
			return nil, fmt.Errorf("UPDATE语句缺少SET子句")
		}

		// 解析SET子句
		var updates = make(map[string]interface{})
		setStr := strings.Join(parts[setIndex+1:whereIndex], " ")

		// 使用状态机解析SET子句
		var key, value string
		var inQuote bool
		var current strings.Builder

		for i := 0; i < len(setStr); i++ {
			ch := setStr[i]

			switch ch {
			case '\'':
				inQuote = !inQuote
				current.WriteByte(ch)
			case '=':
				if !inQuote {
					key = strings.TrimSpace(current.String())
					current.Reset()
					continue
				}
				current.WriteByte(ch)
			case ',':
				if !inQuote {
					value = strings.TrimSpace(current.String())
					// 处理键值对
					if key != "" {
						// 处理字符串值（去除引号）
						if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
							value = value[1 : len(value)-1]
						}
						updates[key] = value
					}
					key = ""
					current.Reset()
					continue
				}
				current.WriteByte(ch)
			default:
				current.WriteByte(ch)
			}
		}

		// 处理最后一个键值对
		if key != "" {
			value = strings.TrimSpace(current.String())
			if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
				value = value[1 : len(value)-1]
			}
			updates[key] = value
		}

		stmt.Data = updates

		// 解析WHERE子句
		if whereIndex != -1 {
			whereStr := strings.Join(parts[whereIndex+1:], " ")
			// 构造简单的条件映射
			filter := make(map[string]interface{})
			// 解析 key = value 格式
			whereParts := strings.Split(whereStr, "=")
			if len(whereParts) != 2 {
				return nil, fmt.Errorf("无效的WHERE子句格式")
			}
			key := strings.TrimSpace(whereParts[0])
			value := strings.TrimSpace(whereParts[1])

			// 尝试将值转换为数字
			if numVal, err := strconv.ParseFloat(value, 64); err == nil {
				filter[key] = numVal
			} else {
				// 否则作为字符串处理
				filter[key] = value
			}
			stmt.Filter = filter
		}

		return stmt, nil

	default:
		return nil, fmt.Errorf("不支持的SQL语句: %s", sql)
	}

	return nil, fmt.Errorf("SQL语句解析失败")
}
