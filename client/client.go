package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yourusername/sudatas/internal/protocol"
)

// Client 数据库客户端
type Client struct {
	conn     net.Conn
	mu       sync.Mutex
	addr     string
	username string
	password string
	timeout  time.Duration
}

// ClientOption 客户端配置选项
type ClientOption func(*Client)

// WithTimeout 设置超时时间
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// NewClient 创建新的客户端
func NewClient(addr, username, password string, options ...ClientOption) *Client {
	client := &Client{
		addr:     addr,
		username: username,
		password: password,
		timeout:  time.Second * 30, // 默认超时时间
	}

	for _, opt := range options {
		opt(client)
	}

	return client
}

// Connect 连接到数据库服务器
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil // 已经连接
	}

	// 建立TCP连接
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return fmt.Errorf("连接服务器失败: %w", err)
	}

	c.conn = conn

	// 进行身份认证
	if err := c.authenticate(); err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("认证失败: %w", err)
	}

	return nil
}

// Close 关闭连接
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// authenticate 进行身份认证
func (c *Client) authenticate() error {
	auth := struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}{
		Username: c.username,
		Password: c.password,
	}

	data, err := json.Marshal(auth)
	if err != nil {
		return err
	}

	msg := &protocol.Message{
		Type:    protocol.AuthMessage,
		Payload: data,
	}

	response, err := c.sendMessage(msg)
	if err != nil {
		return err
	}

	if response.Type == protocol.ErrorMessage {
		return fmt.Errorf("认证失败: %s", string(response.Payload))
	}

	return nil
}

// Query 执行查询
func (c *Client) Query(sql string) ([]map[string]interface{}, error) {
	msg := &protocol.Message{
		Type:    protocol.QueryMessage,
		Payload: []byte(sql),
	}

	response, err := c.sendMessage(msg)
	if err != nil {
		return nil, err
	}

	if response.Type == protocol.ErrorMessage {
		return nil, fmt.Errorf("查询失败: %s", string(response.Payload))
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(response.Payload, &result); err != nil {
		return nil, fmt.Errorf("解析结果失败: %w", err)
	}

	return result, nil
}

// CreateCollection 创建集合
func (c *Client) CreateCollection(name string) error {
	sql := fmt.Sprintf("CREATE COLLECTION %s", name)
	_, err := c.Query(sql)
	return err
}

// CreateDatabase 在集合中创建数据库
func (c *Client) CreateDatabase(collection, dbName, dbType, description string) error {
	sql := fmt.Sprintf("CREATE DATABASE %s.%s TYPE %s DESCRIPTION '%s'",
		collection, dbName, dbType, description)
	_, err := c.Query(sql)
	return err
}

// ListCollections 列出所有集合
func (c *Client) ListCollections() ([]string, error) {
	result, err := c.Query("SHOW COLLECTIONS")
	if err != nil {
		return nil, err
	}

	collections := make([]string, 0, len(result))
	for _, row := range result {
		if name, ok := row["name"].(string); ok {
			collections = append(collections, name)
		}
	}

	return collections, nil
}

// ListDatabases 列出集合中的所有数据库
func (c *Client) ListDatabases(collection string) ([]map[string]interface{}, error) {
	return c.Query(fmt.Sprintf("SHOW DATABASES FROM %s", collection))
}

// Insert 插入数据
func (c *Client) Insert(collection, database string, data map[string]interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("INSERT INTO %s.%s VALUES %s", collection, database, string(jsonData))
	_, err = c.Query(sql)
	return err
}

// Find 查询数据
func (c *Client) Find(collection, database string, filter map[string]interface{}) ([]map[string]interface{}, error) {
	jsonFilter, err := json.Marshal(filter)
	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", collection, database, string(jsonFilter))
	return c.Query(sql)
}

// Update 更新数据
func (c *Client) Update(collection, database string, filter, update map[string]interface{}) error {
	jsonFilter, err := json.Marshal(filter)
	if err != nil {
		return err
	}

	jsonUpdate, err := json.Marshal(update)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		collection, database, string(jsonUpdate), string(jsonFilter))
	_, err = c.Query(sql)
	return err
}

// Delete 删除数据
func (c *Client) Delete(collection, database string, filter map[string]interface{}) error {
	jsonFilter, err := json.Marshal(filter)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", collection, database, string(jsonFilter))
	_, err = c.Query(sql)
	return err
}

// sendMessage 发送消息并接收响应
func (c *Client) sendMessage(msg *protocol.Message) (*protocol.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, fmt.Errorf("未连接到服务器")
	}

	// 设置读写超时
	deadline := time.Now().Add(c.timeout)
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	// 发送消息
	if err := protocol.WriteMessage(c.conn, msg); err != nil {
		return nil, fmt.Errorf("发送消息失败: %w", err)
	}

	// 读取响应
	response, err := protocol.ReadMessage(bufio.NewReader(c.conn))
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	return response, nil
}
