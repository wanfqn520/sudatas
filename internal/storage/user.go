package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/yourusername/sudatas/internal/auth"
	"github.com/yourusername/sudatas/internal/security"
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

	if err := um.Load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	// 如果没有用户，创建默认管理员用户
	if len(um.users) == 0 {
		if err := um.CreateUser("root", "123456", []string{"admin"}); err != nil {
			return nil, err
		}
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

	// 加密密码
	encryptedPass, err := um.crypto.EncryptSM4([]byte(password))
	if err != nil {
		return err
	}

	user := &User{
		Username: username,
		Password: string(encryptedPass),
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
	if !exists {
		return false
	}

	// 解密存储的密码
	decryptedPass, err := um.crypto.DecryptSM4([]byte(user.Password))
	if err != nil {
		return false
	}

	return string(decryptedPass) == password
}

// Save 保存用户信息
func (um *UserManager) Save() error {
	data, err := json.Marshal(um.users)
	if err != nil {
		return err
	}

	// 加密数据
	encrypted, err := um.crypto.EncryptSM4(data)
	if err != nil {
		return err
	}

	return os.WriteFile(um.filename, encrypted, 0600)
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
