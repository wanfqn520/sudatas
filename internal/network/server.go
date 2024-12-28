package network

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/yourusername/sudatas/internal/audit"
	"github.com/yourusername/sudatas/internal/auth"
	"github.com/yourusername/sudatas/internal/protocol"
	"github.com/yourusername/sudatas/internal/security"
	"github.com/yourusername/sudatas/internal/storage"
)

// 默认认证信息
const (
	DefaultUser     = "root"
	DefaultPassword = "123456"
)

// Server TCP服务器结构
type Server struct {
	engine     *storage.Engine
	mu         sync.RWMutex
	pool       *Pool
	crypto     *security.CryptoManager
	userMgr    *storage.UserManager
	maxClients int
	auditLog   *audit.AuditLogger
	parser     *SQLParser
}

// Client 客户端连接
type Client struct {
	conn net.Conn
	auth bool
	user string
}

// Auth 认证信息
type Auth struct {
	Users map[string]string
}

// NewServer 创建新的服务器实例
func NewServer(engine *storage.Engine, maxClients int) (*Server, error) {
	// 初始化加密管理器
	crypto, err := security.NewCryptoManager()
	if err != nil {
		return nil, err
	}

	// 加载或创建密钥
	if err := crypto.LoadKeys("key.sudb"); err != nil {
		if os.IsNotExist(err) {
			if err := crypto.SaveKeys("key.sudb"); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	// 初始化用户管理器
	userMgr, err := storage.NewUserManager("user.sudb", crypto)
	if err != nil {
		return nil, err
	}

	// 创建连接池
	pool := NewPool(
		func() (net.Conn, error) {
			return nil, nil // 实际连接创建逻辑
		},
		maxClients/2,  // maxIdle
		maxClients,    // maxOpen
		time.Minute*5, // timeout
	)

	// 初始化审计日志
	auditLog, err := audit.NewAuditLogger("logs/audit", crypto, 10*1024*1024) // 10MB
	if err != nil {
		return nil, fmt.Errorf("初始化审计日志失败: %w", err)
	}

	return &Server{
		engine:     engine,
		pool:       pool,
		crypto:     crypto,
		userMgr:    userMgr,
		maxClients: maxClients,
		auditLog:   auditLog,
		parser:     NewSQLParser(),
	}, nil
}

// Serve 启动服务器
func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("接受连接错误: %v", err)
			continue
		}

		client := &Client{
			conn: conn,
			auth: false,
		}

		s.mu.Lock()
		s.clients[conn] = client
		s.mu.Unlock()

		go s.handleConnection(client)
	}
}

// handleConnection 处理客户端连接
func (s *Server) handleConnection(client *Client) {
	defer func() {
		s.mu.Lock()
		delete(s.clients, client.conn)
		s.mu.Unlock()
		client.conn.Close()
	}()

	reader := bufio.NewReader(client.conn)

	for {
		// 读取消息
		msg, err := protocol.ReadMessage(reader)
		if err != nil {
			log.Printf("读取消息错误: %v", err)
			return
		}

		// 处理消息
		response, err := s.handleMessage(client, msg)
		if err != nil {
			response = &protocol.Message{
				Type:    protocol.ErrorMessage,
				Payload: []byte(fmt.Sprintf("错误: %v", err)),
			}
		}

		// 发送响应
		if err := protocol.WriteMessage(client.conn, response); err != nil {
			log.Printf("发送响应错误: %v", err)
			return
		}
	}
}

// handleMessage 处理客户端消息
func (s *Server) handleMessage(client *Client, msg *protocol.Message) (*protocol.Message, error) {
	// 如果未认证，只处理认证消息
	if !client.auth && msg.Type != protocol.AuthMessage {
		return nil, fmt.Errorf("需要认证")
	}

	switch msg.Type {
	case protocol.AuthMessage:
		return s.handleAuth(client, msg)
	case protocol.QueryMessage:
		return s.handleQuery(client, msg)
	default:
		return nil, fmt.Errorf("未知的消息类型")
	}
}

// handleAuth 处理认证请求
func (s *Server) handleAuth(client *Client, msg *protocol.Message) (*protocol.Message, error) {
	// 解密认证数据
	decrypted, err := s.crypto.DecryptSM2(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("解密认证数据失败")
	}

	var auth struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.Unmarshal(decrypted, &auth); err != nil {
		return nil, fmt.Errorf("无效的认证数据")
	}

	if s.userMgr.ValidateUser(auth.Username, auth.Password) {
		client.auth = true
		client.user = auth.Username

		response := "认证成功"
		encrypted, err := s.crypto.EncryptSM4([]byte(response))
		if err != nil {
			return nil, err
		}

		// 记录审计日志
		s.auditLog.Log(&audit.LogEntry{
			Timestamp: time.Now(),
			Level:     audit.INFO,
			User:      auth.Username,
			Action:    "AUTH",
			Object:    "USER",
			Status:    "SUCCESS",
			Details:   "用户登录成功",
			IP:        client.conn.RemoteAddr().String(),
		})

		return &protocol.Message{
			Type:    protocol.ResultMessage,
			Payload: encrypted,
		}, nil
	}

	return nil, fmt.Errorf("认证失败")
}

// handleQuery 处理查询请求
func (s *Server) handleQuery(client *Client, msg *protocol.Message) (*protocol.Message, error) {
	// 解析SQL语句，获取操作类型和资源信息
	stmt, err := s.parser.Parse(string(msg.Payload))
	if err != nil {
		return nil, err
	}

	// 检查权限
	var perm auth.Permission
	var res auth.Resource

	switch stmt.Type {
	case "SELECT":
		perm = auth.PermSelect
		res = auth.Resource{Type: auth.ResTable, Name: stmt.Table}
	case "INSERT":
		perm = auth.PermInsert
		res = auth.Resource{Type: auth.ResTable, Name: stmt.Table}
	case "UPDATE":
		perm = auth.PermUpdate
		res = auth.Resource{Type: auth.ResTable, Name: stmt.Table}
	case "DELETE":
		perm = auth.PermDelete
		res = auth.Resource{Type: auth.ResTable, Name: stmt.Table}
	case "CREATE_TABLE":
		perm = auth.PermCreateTable
		res = auth.Resource{Type: auth.ResDatabase}
		// ... 其他操作类型
	}

	if !s.userMgr.CheckPermission(client.user, perm, res) {
		return nil, fmt.Errorf("权限不足")
	}

	// 记录审计日志
	logEntry := &audit.LogEntry{
		Timestamp: time.Now(),
		Level:     audit.INFO,
		User:      client.user,
		Action:    string(perm),
		Object:    fmt.Sprintf("%s:%s", res.Type, res.Name),
		IP:        client.conn.RemoteAddr().String(),
	}

	// 执行查询
	result, err := s.executeQuery(stmt)
	if err != nil {
		logEntry.Level = audit.ERROR
		logEntry.Status = "FAILED"
		logEntry.Details = err.Error()
		s.auditLog.Log(logEntry)
		return nil, err
	}

	logEntry.Status = "SUCCESS"
	logEntry.Details = fmt.Sprintf("操作成功: %s", string(msg.Payload))
	s.auditLog.Log(logEntry)

	return &protocol.Message{
		Type:    protocol.ResultMessage,
		Payload: result,
	}, nil
}

// Shutdown 关闭服务器
func (s *Server) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 记录服务器关闭日志
	s.auditLog.Log(&audit.LogEntry{
		Timestamp: time.Now(),
		Level:     audit.INFO,
		User:      "SYSTEM",
		Action:    "SHUTDOWN",
		Object:    "SERVER",
		Status:    "SUCCESS",
		Details:   "服务器正常关闭",
	})

	// 关闭审计日志
	if err := s.auditLog.Close(); err != nil {
		return fmt.Errorf("关闭审计日志失败: %w", err)
	}

	// 关闭其他资源
	if err := s.pool.Close(); err != nil {
		return fmt.Errorf("关闭连接池失败: %w", err)
	}

	return nil
}
