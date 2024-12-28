package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"sudatas/internal/auth"
	"sudatas/internal/security"
)

// UserManager 用户管理器
type UserManager struct {
	mu       sync.RWMutex
	users    map[string]*User
	crypto   *security.CryptoManager
	filename string
	permMgr  *auth.PermissionManager
}

// User 用户信息
type User struct {
	Username    string   `json:"username"`
	Password    string   `json:"password"` // SM4加密存储
	Permissions []string `json:"permissions"`
	Roles       []string `json:"roles"`  // 新增
	Status      string   `json:"status"` // 新增：active/locked/disabled
}

// NewUserManager 创建用户管理器
func NewUserManager(filename string, crypto *security.CryptoManager) (*UserManager, error) {
	um := &UserManager{
		users:    make(map[string]*User),
		crypto:   crypto,
		filename: filename,
		permMgr:  auth.NewPermissionManager(),
	}

	// 如果文件不存在，创建默认用户
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		// 创建默认管理员用户
		if err := um.CreateUser("root", "123456", []string{"admin"}); err != nil {
			return nil, err
		}
		return um, nil
	}

	// 读取并解密用户数据
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取用户数据失败: %w", err)
	}

	// 如果文件为空，创建默认用户
	if len(data) == 0 {
		if err := um.CreateUser("root", "123456", []string{"admin"}); err != nil {
			return nil, err
		}
		return um, nil
	}

	// 解密数据
	decrypted, err := crypto.DecryptSM4(data)
	if err != nil {
		// 如果解密失败，重新创建用户文件
		if err := um.CreateUser("root", "123456", []string{"admin"}); err != nil {
			return nil, err
		}
		return um, nil
	}

	// 解析用户数据
	if err := json.Unmarshal(decrypted, &um.users); err != nil {
		// 如果解析失败，重新创建用户文件
		if err := um.CreateUser("root", "123456", []string{"admin"}); err != nil {
			return nil, err
		}
		return um, nil
	}

	return um, nil
}

// CreateUser 创建用户
func (um *UserManager) CreateUser(username, password string, roles []string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	if _, exists := um.users[username]; exists {
		return fmt.Errorf("用户已存在")
	}

	// 直接存储密码（暂时不加密）
	user := &User{
		Username: username,
		Password: password,
		Roles:    roles,
		Status:   "active",
	}

	um.users[username] = user

	// 分配角色
	for _, role := range roles {
		if err := um.permMgr.AssignRole(username, role); err != nil {
			return err
		}
	}

	return um.Save()
}

// ValidateUser 验证用户
func (um *UserManager) ValidateUser(username, password string) bool {
	um.mu.RLock()
	defer um.mu.RUnlock()

	user, exists := um.users[username]
	if !exists || user.Status != "active" {
		return false
	}

	// 直接比较密码（暂时不加密）
	return user.Password == password
}

// Save 保存用户信息
func (um *UserManager) Save() error {
	data, err := json.MarshalIndent(um.users, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化用户数据失败: %w", err)
	}

	// 加密数据
	encrypted, err := um.crypto.EncryptSM4(data)
	if err != nil {
		return fmt.Errorf("加密用户数据失败: %w", err)
	}

	// 创建目录（如果不存在）
	dir := filepath.Dir(um.filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建用户数据目录失败: %w", err)
	}

	// 保存到文件
	if err := os.WriteFile(um.filename, encrypted, 0600); err != nil {
		return fmt.Errorf("保存用户数据失败: %w", err)
	}

	return nil
}

// Load 加载用户信息
func (um *UserManager) Load() error {
	data, err := os.ReadFile(um.filename)
	if err != nil {
		return err
	}

	// 解密数据
	decrypted, err := um.crypto.DecryptSM4(data)
	if err != nil {
		return err
	}

	return json.Unmarshal(decrypted, &um.users)
}

// CheckPermission 检查用户权限
func (um *UserManager) CheckPermission(username string, perm auth.Permission, res auth.Resource) bool {
	um.mu.RLock()
	defer um.mu.RUnlock()

	user, exists := um.users[username]
	if !exists || user.Status != "active" {
		return false
	}

	// root 用户拥有所有权限
	if username == "root" {
		return true
	}

	// 检查用户角色中是否包含 admin
	for _, role := range user.Roles {
		if role == "admin" {
			return true
		}
	}

	return um.permMgr.CheckPermission(username, perm, res)
}

// LockUser 锁定用户
func (um *UserManager) LockUser(username string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	user, exists := um.users[username]
	if !exists {
		return fmt.Errorf("用户不存在")
	}

	user.Status = "locked"
	return um.Save()
}

// UnlockUser 解锁用户
func (um *UserManager) UnlockUser(username string) error {
	um.mu.Lock()
	defer um.mu.Unlock()

	user, exists := um.users[username]
	if !exists {
		return fmt.Errorf("用户不存在")
	}

	user.Status = "active"
	return um.Save()
}
