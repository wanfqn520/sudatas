package storage

import (
	"fmt"
)

// Condition 查询条件
type Condition struct {
	Column   string      `json:"column"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

// Conditions 多个条件的组合
type Conditions struct {
	And []Condition
	Or  []Condition
}

// ParseCondition 从 map 解析条件
func ParseCondition(data map[string]interface{}) (*Condition, error) {
	// 处理 nil 条件（查询所有数据）
	if data == nil {
		return nil, nil
	}

	// 处理简单条件（相等比较）
	if len(data) == 1 {
		for k, v := range data {
			// 如果值是 map，说明是复杂条件
			if condMap, ok := v.(map[string]interface{}); ok {
				operator, _ := condMap["operator"].(string)
				value := condMap["value"]
				if operator == "" {
					operator = "="
				}
				return &Condition{
					Column:   k,
					Operator: operator,
					Value:    value,
				}, nil
			}
			// 否则是简单的相等比较
			return &Condition{
				Column:   k,
				Operator: "=",
				Value:    v,
			}, nil
		}
	}

	// 处理多个条件（AND 组合）
	conditions := make([]Condition, 0)
	for k, v := range data {
		if condMap, ok := v.(map[string]interface{}); ok {
			operator, _ := condMap["operator"].(string)
			value := condMap["value"]
			if operator == "" {
				operator = "="
			}
			conditions = append(conditions, Condition{
				Column:   k,
				Operator: operator,
				Value:    value,
			})
		} else {
			conditions = append(conditions, Condition{
				Column:   k,
				Operator: "=",
				Value:    v,
			})
		}
	}

	if len(conditions) > 0 {
		return &conditions[0], nil
	}

	return nil, fmt.Errorf("无效的条件格式")
}

// MatchConditions 检查记录是否匹配所有条件
func MatchConditions(record Row, conditions map[string]interface{}) bool {
	if conditions == nil {
		return true
	}

	// 检查每个条件
	for k, v := range conditions {
		if condMap, ok := v.(map[string]interface{}); ok {
			// 复杂条件
			operator, _ := condMap["operator"].(string)
			value := condMap["value"]
			if !matchSingleCondition(record, k, operator, value) {
				return false
			}
		} else {
			// 简单相等条件
			if !matchSingleCondition(record, k, "=", v) {
				return false
			}
		}
	}

	return true
}

// matchSingleCondition 检查单个条件
func matchSingleCondition(record Row, column, operator string, value interface{}) bool {
	val, exists := record[column]
	if !exists {
		return false
	}

	switch operator {
	case "=":
		return val == value
	case ">":
		return compareValues(val, value) > 0
	case "<":
		return compareValues(val, value) < 0
	case ">=":
		return compareValues(val, value) >= 0
	case "<=":
		return compareValues(val, value) <= 0
	case "!=":
		return val != value
	}

	return false
}
