package auth

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// Permission 权限类型
type Permission string

// 系统预定义权限
const (
	// 数据库操作权限
	PermCreateDB    Permission = "CREATE_DATABASE"
	PermDropDB      Permission = "DROP_DATABASE"
	PermCreateTable Permission = "CREATE_TABLE"
	PermDropTable   Permission = "DROP_TABLE"
	PermAlterTable  Permission = "ALTER_TABLE"

	// 数据操作权限
	PermSelect Permission = "SELECT"
	PermInsert Permission = "INSERT"
	PermUpdate Permission = "UPDATE"
	PermDelete Permission = "DELETE"

	// 用户管理权限
	PermCreateUser Permission = "CREATE_USER"
	PermDropUser   Permission = "DROP_USER"
	PermGrant      Permission = "GRANT"
	PermRevoke     Permission = "REVOKE"

	// 系统管理权限
	PermBackup      Permission = "BACKUP"
	PermRestore     Permission = "RESTORE"
	PermViewAudit   Permission = "VIEW_AUDIT"
	PermManageAudit Permission = "MANAGE_AUDIT"
)

// ResourceType 资源类型
type ResourceType string

const (
	ResDatabase ResourceType = "DATABASE"
	ResTable    ResourceType = "TABLE"
	ResColumn   ResourceType = "COLUMN"
)

// Resource 资源标识
type Resource struct {
	Type ResourceType `json:"type"`
	Name string       `json:"name"`
	Sub  string       `json:"sub,omitempty"` // 子资源，如列名
}

// PermissionRule 权限规则
type PermissionRule struct {
	Permission Permission `json:"permission"`
	Resource   Resource   `json:"resource"`
	Grant      bool       `json:"grant"`     // 是否可以授权给其他用户
	Condition  string     `json:"condition"` // 条件表达式
}

// Role 角色定义
type Role struct {
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Rules       []PermissionRule `json:"rules"`
}

// PermissionManager 权限管理器
type PermissionManager struct {
	mu    sync.RWMutex
	roles map[string]*Role
	// 用户-角色映射
	userRoles map[string][]string
	// 用户-直接权限映射
	userPermissions map[string][]PermissionRule
}

// NewPermissionManager 创建权限管理器
func NewPermissionManager() *PermissionManager {
	pm := &PermissionManager{
		roles:           make(map[string]*Role),
		userRoles:       make(map[string][]string),
		userPermissions: make(map[string][]PermissionRule),
	}

	// 初始化预定义角色
	pm.initPredefinedRoles()
	return pm
}

// initPredefinedRoles 初始化预定义角色
func (pm *PermissionManager) initPredefinedRoles() {
	// 管理员角色
	adminRole := &Role{
		Name:        "admin",
		Description: "系统管理员",
		Rules: []PermissionRule{
			{Permission: PermCreateDB, Resource: Resource{Type: ResDatabase}},
			{Permission: PermDropDB, Resource: Resource{Type: ResDatabase}},
			{Permission: PermCreateUser, Resource: Resource{Type: ResDatabase}},
			{Permission: PermDropUser, Resource: Resource{Type: ResDatabase}},
			{Permission: PermGrant, Resource: Resource{Type: ResDatabase}},
			{Permission: PermRevoke, Resource: Resource{Type: ResDatabase}},
			{Permission: PermBackup, Resource: Resource{Type: ResDatabase}},
			{Permission: PermRestore, Resource: Resource{Type: ResDatabase}},
			{Permission: PermViewAudit, Resource: Resource{Type: ResDatabase}},
			{Permission: PermManageAudit, Resource: Resource{Type: ResDatabase}},
		},
	}
	pm.roles["admin"] = adminRole

	// 只读角色
	readOnlyRole := &Role{
		Name:        "readonly",
		Description: "只读用户",
		Rules: []PermissionRule{
			{Permission: PermSelect, Resource: Resource{Type: ResTable}},
		},
	}
	pm.roles["readonly"] = readOnlyRole

	// 开发者角色
	developerRole := &Role{
		Name:        "developer",
		Description: "开发人员",
		Rules: []PermissionRule{
			{Permission: PermSelect, Resource: Resource{Type: ResTable}},
			{Permission: PermInsert, Resource: Resource{Type: ResTable}},
			{Permission: PermUpdate, Resource: Resource{Type: ResTable}},
			{Permission: PermDelete, Resource: Resource{Type: ResTable}},
			{Permission: PermCreateTable, Resource: Resource{Type: ResDatabase}},
			{Permission: PermAlterTable, Resource: Resource{Type: ResTable}},
		},
	}
	pm.roles["developer"] = developerRole
}

// AssignRole 为用户分配角色
func (pm *PermissionManager) AssignRole(username, roleName string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.roles[roleName]; !exists {
		return fmt.Errorf("角色不存在: %s", roleName)
	}

	if roles, exists := pm.userRoles[username]; exists {
		for _, r := range roles {
			if r == roleName {
				return nil // 已经拥有该角色
			}
		}
		pm.userRoles[username] = append(roles, roleName)
	} else {
		pm.userRoles[username] = []string{roleName}
	}

	return nil
}

// GrantPermission 为用户直接授予权限
func (pm *PermissionManager) GrantPermission(username string, rule PermissionRule) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if perms, exists := pm.userPermissions[username]; exists {
		pm.userPermissions[username] = append(perms, rule)
	} else {
		pm.userPermissions[username] = []PermissionRule{rule}
	}

	return nil
}

// CheckPermission 检查用户是否有特定权限
func (pm *PermissionManager) CheckPermission(username string, perm Permission, res Resource) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// 检查直接权限
	if rules, exists := pm.userPermissions[username]; exists {
		for _, rule := range rules {
			if pm.matchPermissionRule(rule, perm, res) {
				return true
			}
		}
	}

	// 检查角色权限
	if roles, exists := pm.userRoles[username]; exists {
		for _, roleName := range roles {
			if role, exists := pm.roles[roleName]; exists {
				for _, rule := range role.Rules {
					if pm.matchPermissionRule(rule, perm, res) {
						return true
					}
				}
			}
		}
	}

	return false
}

// matchPermissionRule 检查权限规则是否匹配
func (pm *PermissionManager) matchPermissionRule(rule PermissionRule, perm Permission, res Resource) bool {
	if rule.Permission != perm {
		return false
	}

	// 检查资源类型
	if rule.Resource.Type != res.Type {
		return false
	}

	// 如果规则没有指定具体资源名称，则允许访问所有该类型资源
	if rule.Resource.Name == "" {
		return true
	}

	// 支持通配符匹配
	if strings.Contains(rule.Resource.Name, "*") {
		pattern := strings.ReplaceAll(rule.Resource.Name, "*", ".*")
		matched, _ := regexp.MatchString(pattern, res.Name)
		return matched
	}

	return rule.Resource.Name == res.Name
}

// ListUserPermissions 列出用户所有权限
func (pm *PermissionManager) ListUserPermissions(username string) []PermissionRule {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var allRules []PermissionRule

	// 收集直接权限
	if rules, exists := pm.userPermissions[username]; exists {
		allRules = append(allRules, rules...)
	}

	// 收集角色权限
	if roles, exists := pm.userRoles[username]; exists {
		for _, roleName := range roles {
			if role, exists := pm.roles[roleName]; exists {
				allRules = append(allRules, role.Rules...)
			}
		}
	}

	return allRules
}
