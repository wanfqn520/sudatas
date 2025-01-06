package network

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"sudatas/internal/audit"
	"sudatas/internal/auth"
	"sudatas/internal/parser"
	"sudatas/internal/protocol"
	"sudatas/internal/security"
	"sudatas/internal/storage"
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
	parser     *parser.SQLParser
	clients    map[net.Conn]*Client
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

	// 确保 builtin 目录存在
	builtinDir := "builtin"
	if err := os.MkdirAll(builtinDir, 0755); err != nil {
		return nil, fmt.Errorf("创建 builtin 目录失败: %w", err)
	}

	// 加载或创建密钥
	keyFile := filepath.Join(builtinDir, "key.sudb")
	if err := crypto.LoadKeys(keyFile); err != nil {
		return nil, fmt.Errorf("加载密钥失败: %w", err)
	}

	// 初始化用户管理器
	userFile := filepath.Join(builtinDir, "user.sudb")
	userMgr, err := storage.NewUserManager(userFile, crypto)
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
	logDir := filepath.Join(builtinDir, "logs", "audit")
	auditLog, err := audit.NewAuditLogger(logDir, crypto, 10*1024*1024) // 10MB
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
		parser:     parser.NewSQLParser(),
		clients:    make(map[net.Conn]*Client),
	}, nil
}

// Serve 启动服务器
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					log.Printf("临时错误: %v", err)
					continue
				}
				return err
			}

			client := &Client{
				conn: conn,
				auth: false,
			}

			s.mu.Lock()
			s.clients[conn] = client
			s.mu.Unlock()

			wg.Add(1)
			go func() {
				defer wg.Done()
				s.handleConnection(ctx, client)
			}()
		}
	}
}

// handleConnection 处理客户端连接
func (s *Server) handleConnection(ctx context.Context, client *Client) {
	defer func() {
		s.mu.Lock()
		delete(s.clients, client.conn)
		s.mu.Unlock()
		client.conn.Close()
		log.Printf("客户端断开连接: %s", client.conn.RemoteAddr())
	}()

	reader := bufio.NewReader(client.conn)
	log.Printf("新客户端连接: %s", client.conn.RemoteAddr())

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 设置读取超时
			client.conn.SetReadDeadline(time.Now().Add(time.Second * 30))

			// 读取消息
			msg, err := protocol.ReadMessage(reader)
			if err != nil {
				if !os.IsTimeout(err) && !strings.Contains(err.Error(), "connection reset by peer") {
					log.Printf("读取消息错误: %v", err)
				}
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
				if !strings.Contains(err.Error(), "connection reset by peer") {
					log.Printf("发送响应错误: %v", err)
				}
				return
			}
		}
	}
}

// handleMessage 处理客户端消息
func (s *Server) handleMessage(client *Client, msg *protocol.Message) (*protocol.Message, error) {
	// 如果未认证，只处理认证消息
	if !client.auth && msg.Type != protocol.AuthMessage {
		return nil, fmt.Errorf("需要认证")
	}

	// 记录请求日志
	log.Printf("收到请求 [%s]: %s", client.conn.RemoteAddr(), string(msg.Payload))

	var response *protocol.Message
	var err error

	switch msg.Type {
	case protocol.AuthMessage:
		response, err = s.handleAuth(client, msg)
	case protocol.QueryMessage:
		response, err = s.handleQuery(client, msg)
	default:
		err = fmt.Errorf("未知的消息类型")
	}

	// 记录响应日志
	if err != nil {
		log.Printf("处理失败 [%s]: %v", client.conn.RemoteAddr(), err)
	} else {
		log.Printf("处理成功 [%s]", client.conn.RemoteAddr())
	}

	return response, err
}

// handleAuth 处理认证请求
func (s *Server) handleAuth(client *Client, msg *protocol.Message) (*protocol.Message, error) {
	// 直接解析认证数据（不解密）
	var auth struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.Unmarshal(msg.Payload, &auth); err != nil {
		return nil, fmt.Errorf("无效的认证数据: %w", err)
	}

	if s.userMgr.ValidateUser(auth.Username, auth.Password) {
		client.auth = true
		client.user = auth.Username

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

		// 返回成功消息（不加密）
		return &protocol.Message{
			Type:    protocol.ResultMessage,
			Payload: []byte("认证成功"),
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
	case "INSERT":
		perm = auth.PermInsert
		res = auth.Resource{
			Type: auth.ResDatabase,
			Name: fmt.Sprintf("%s.%s", stmt.Collection, stmt.Database),
		}

	case "SELECT":
		perm = auth.PermSelect
		res = auth.Resource{
			Type: auth.ResDatabase,
			Name: fmt.Sprintf("%s.%s", stmt.Collection, stmt.Database),
		}

	case "SHOW_COLLECTIONS":
		// 允许所有已认证用户查看集合列表
		perm = auth.PermSelect
		res = auth.Resource{Type: auth.ResDatabase}

	case "SHOW_DATABASES":
		// 允许所有已认证用户查看数据库列表
		perm = auth.PermSelect
		res = auth.Resource{Type: auth.ResDatabase}

	case "CREATE_COLLECTION":
		perm = auth.PermCreateDB
		res = auth.Resource{Type: auth.ResDatabase}

	case "CREATE_DATABASE":
		perm = auth.PermCreateDB
		res = auth.Resource{Type: auth.ResDatabase}

	case "IMPORT":
		// 解析导入路径
		parts := strings.Fields(string(msg.Payload))
		if len(parts) < 4 || strings.ToUpper(parts[1]) != "FROM" || strings.ToUpper(parts[3]) != "TO" {
			return nil, fmt.Errorf("无效的IMPORT语句，格式应为: IMPORT FROM filepath TO collection")
		}
		filePath := parts[2]
		targetCollection := parts[4]

		// 导入数据
		if err := s.engine.MemStore.ImportFromFile(filePath, targetCollection); err != nil {
			return nil, fmt.Errorf("导入数据失败: %w", err)
		}

		result := map[string]interface{}{
			"message": "导入成功",
			"path":    filePath,
			"target":  targetCollection,
		}
		resultData, err := json.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("序列化结果失败: %w", err)
		}

		return &protocol.Message{
			Type:    protocol.ResultMessage,
			Payload: resultData,
		}, nil

	case "EXPORT":
		perm = auth.PermSelect // 导出需要读取权限
		res = auth.Resource{
			Type: auth.ResDatabase,
			Name: fmt.Sprintf("%s.%s", stmt.Collection, stmt.Database),
		}

	case "UPDATE":
		// 更新数据
		if err := s.engine.MemStore.UpdateRecords(stmt.Collection, stmt.Database, stmt.Data, stmt.Filter); err != nil {
			return nil, err
		}

		result := map[string]interface{}{
			"message": "更新成功",
		}
		resultData, err := json.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("序列化结果失败: %w", err)
		}

		return &protocol.Message{
			Type:    protocol.ResultMessage,
			Payload: resultData,
		}, nil

	default:
		return nil, fmt.Errorf("不支持的操作类型: %s", stmt.Type)
	}

	// root 用户跳过权限检查
	if client.user != "root" {
		if !s.userMgr.CheckPermission(client.user, perm, res) {
			return nil, fmt.Errorf("权限不足")
		}
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

	// 保存内存数据到磁盘
	if err := s.engine.MemStore.SaveToDisk(); err != nil {
		log.Printf("保存数据失败: %v", err)
	}

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

// executeQuery 执行SQL查询
func (s *Server) executeQuery(stmt *parser.Statement) ([]byte, error) {
	switch stmt.Type {
	case "INSERT":
		// 插入数据到内存
		if err := s.engine.MemStore.InsertRecord(stmt.Collection, stmt.Database, stmt.Data); err != nil {
			return nil, err
		}

		result := map[string]interface{}{
			"message": "插入成功",
		}
		return json.Marshal(result)

	case "SELECT":
		// 从内存查询数据
		records, err := s.engine.MemStore.QueryRecords(stmt.Collection, stmt.Database, stmt.Filter)
		if err != nil {
			return nil, err
		}

		// 过滤列
		if len(stmt.Columns) > 0 {
			var filtered []storage.Row
			for _, record := range records {
				row := make(storage.Row)
				for _, col := range stmt.Columns {
					if val, ok := record[col]; ok {
						row[col] = val
					}
				}
				filtered = append(filtered, row)
			}
			records = filtered
		}

		return json.Marshal(records)

	case "SHOW_COLLECTIONS":
		collections := s.engine.ListCollections()
		result := make([]map[string]interface{}, len(collections))
		for i, col := range collections {
			result[i] = map[string]interface{}{
				"name":  col.Name,
				"owner": col.Owner,
			}
		}
		return json.Marshal(result)

	case "SHOW_DATABASES":
		collection, err := s.engine.GetCollection(stmt.Collection)
		if err != nil {
			return nil, err
		}
		result := make([]map[string]interface{}, 0)
		for name, db := range collection.Databases {
			result = append(result, map[string]interface{}{
				"name":        name,
				"type":        string(db.Type),
				"description": db.Description,
				"created":     db.Created,
				"updated":     db.Updated,
			})
		}
		return json.Marshal(result)

	case "CREATE_COLLECTION":
		if err := s.engine.CreateCollection(stmt.Collection, stmt.Owner); err != nil {
			return nil, err
		}
		result := map[string]interface{}{
			"message": "集合创建成功",
			"name":    stmt.Collection,
		}
		return json.Marshal(result)

	case "CREATE_DATABASE":
		if err := s.engine.CreateDatabase(stmt.Collection, stmt.Database, stmt.DBType, stmt.Description); err != nil {
			return nil, err
		}
		result := map[string]interface{}{
			"message":    "数据库创建成功",
			"collection": stmt.Collection,
			"database":   stmt.Database,
			"type":       string(stmt.DBType),
		}
		return json.Marshal(result)

	case "EXPORT":
		// 获取数据库
		collection, err := s.engine.GetCollection(stmt.Collection)
		if err != nil {
			return nil, err
		}

		// 检查数据库是否存在
		if _, exists := collection.Databases[stmt.Database]; !exists {
			return nil, fmt.Errorf("数据库不存在: %s", stmt.Database)
		}

		// 解析文件路径
		dir := filepath.Dir(stmt.FilePath)
		filename := filepath.Base(stmt.FilePath)

		// 执行导出
		opts := storage.ExportOptions{
			IncludeSchema: true,
			Format:        "sql",
			Directory:     dir,
			Filename:      filename,
		}
		if err := s.engine.MemStore.ExportDatabase(stmt.Collection, stmt.Database, opts); err != nil {
			return nil, fmt.Errorf("导出失败: %w", err)
		}

		result := map[string]interface{}{
			"message": "导出成功",
			"path":    stmt.FilePath,
		}
		return json.Marshal(result)

	case "UPDATE":
		// 更新数据
		if err := s.engine.MemStore.UpdateRecords(stmt.Collection, stmt.Database, stmt.Data, stmt.Filter); err != nil {
			return nil, err
		}

		result := map[string]interface{}{
			"message": "更新成功",
		}
		return json.Marshal(result)

	default:
		return nil, fmt.Errorf("不支持的操作类型: %s", stmt.Type)
	}

	return nil, fmt.Errorf("SQL语句执行失败")
}

// matchCondition 检查记录是否匹配条件
func matchCondition(record storage.Row, cond *storage.Condition) bool {
	val, exists := record[cond.Column]
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

// compareValues 比较两个值
func compareValues(a, b interface{}) int {
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
