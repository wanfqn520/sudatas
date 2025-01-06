package dbclient

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

// MessageType 消息类型
type MessageType uint32

const (
	AuthMessage MessageType = iota
	QueryMessage
	ResultMessage
	ErrorMessage
)

// Message 消息结构
type Message struct {
	Type    MessageType
	Payload []byte
}

// Client 数据库客户端
type Client struct {
	conn        net.Conn
	addr        string
	username    string
	password    string
	timeout     time.Duration
	isConnected bool
}

// ClientOption 客户端配置选项
type ClientOption func(*Client)

// WithTimeout 设置超时时间
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// NewClient 创建新的客户端实例
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

// Connect 连接到服务器
func (c *Client) Connect() error {
	if c.isConnected {
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

	c.isConnected = true
	return nil
}

// Insert 插入数据（自动连接）
func (c *Client) Insert(collection, database string, data map[string]interface{}) error {
	if err := c.Connect(); err != nil {
		return err
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("INSERT INTO %s.%s VALUES %s", collection, database, string(jsonData))
	_, err = c.Query(sql)
	return err
}

// Find 查询数据（自动连接）
func (c *Client) Find(collection, database string, filter map[string]interface{}) ([]map[string]interface{}, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}

	var sql string
	if filter == nil {
		sql = fmt.Sprintf("SELECT * FROM %s.%s", collection, database)
	} else {
		jsonFilter, err := json.Marshal(filter)
		if err != nil {
			return nil, err
		}
		sql = fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", collection, database, string(jsonFilter))
	}
	return c.Query(sql)
}

// Query 执行查询（自动连接）
func (c *Client) Query(sql string) ([]map[string]interface{}, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}

	msg := &Message{
		Type:    QueryMessage,
		Payload: []byte(sql),
	}

	response, err := c.sendMessage(msg)
	if err != nil {
		return nil, err
	}

	if response.Type == ErrorMessage {
		return nil, fmt.Errorf("查询失败: %s", string(response.Payload))
	}

	// 尝试解析为数组
	var result []map[string]interface{}
	if err := json.Unmarshal(response.Payload, &result); err != nil {
		// 如果解析数组失败，尝试解析为单个对象
		var singleResult map[string]interface{}
		if err := json.Unmarshal(response.Payload, &singleResult); err != nil {
			return nil, fmt.Errorf("解析结果失败: %w", err)
		}
		result = []map[string]interface{}{singleResult}
	}

	return result, nil
}

// Update 更新数据（自动连接）
func (c *Client) Update(collection, database string, updates map[string]interface{}, where map[string]interface{}) error {
	if err := c.Connect(); err != nil {
		return err
	}

	// 构造 SET 子句
	var setParts []string
	for key, value := range updates {
		// 根据值的类型添加适当的引号
		var valueStr string
		switch v := value.(type) {
		case string:
			valueStr = fmt.Sprintf("'%s'", v)
		default:
			valueStr = fmt.Sprintf("%v", v)
		}
		setParts = append(setParts, fmt.Sprintf("%s = %s", key, valueStr))
	}

	// 构造 WHERE 子句
	whereJson, err := json.Marshal(where)
	if err != nil {
		return fmt.Errorf("序列化WHERE条件失败: %w", err)
	}

	// 构造完整的 UPDATE 语句
	sql := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		collection,
		database,
		strings.Join(setParts, ", "),
		string(whereJson),
	)

	_, err = c.Query(sql)
	return err
}

// 内部方法

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
		return fmt.Errorf("序列化认证数据失败: %w", err)
	}

	msg := &Message{
		Type:    AuthMessage,
		Payload: data,
	}

	response, err := c.sendMessage(msg)
	if err != nil {
		return err
	}

	if response.Type == ErrorMessage {
		return fmt.Errorf("认证失败: %s", string(response.Payload))
	}

	return nil
}

func (c *Client) sendMessage(msg *Message) (*Message, error) {
	// 设置读写超时
	deadline := time.Now().Add(c.timeout)
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	// 发送消息
	if err := writeMessage(c.conn, msg); err != nil {
		return nil, fmt.Errorf("发送消息失败: %w", err)
	}

	// 读取响应
	response, err := readMessage(bufio.NewReader(c.conn))
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	return response, nil
}

func writeMessage(writer net.Conn, msg *Message) error {
	// 写入消息头
	header := struct {
		Length uint32
		Type   uint32
	}{
		Length: uint32(len(msg.Payload)),
		Type:   uint32(msg.Type),
	}

	if err := binary.Write(writer, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("写入消息头错误: %w", err)
	}

	// 写入消息体
	if _, err := writer.Write(msg.Payload); err != nil {
		return fmt.Errorf("写入消息体错误: %w", err)
	}

	return nil
}

func readMessage(reader *bufio.Reader) (*Message, error) {
	// 读取消息头
	var header struct {
		Length uint32
		Type   uint32
	}
	if err := binary.Read(reader, binary.BigEndian, &header); err != nil {
		return nil, fmt.Errorf("读取消息头错误: %w", err)
	}

	// 读取消息体
	payload := make([]byte, header.Length)
	if _, err := reader.Read(payload); err != nil {
		return nil, fmt.Errorf("读取消息体错误: %w", err)
	}

	return &Message{
		Type:    MessageType(header.Type),
		Payload: payload,
	}, nil
}
