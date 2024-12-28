package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type Message struct {
	Type    MessageType
	Payload []byte
}

type MessageType int

const (
	AuthMessage MessageType = iota
	QueryMessage
	ResultMessage
	ErrorMessage
)

// 消息头部结构
type MessageHeader struct {
	Length uint32      // 消息体长度
	Type   MessageType // 消息类型
}

// ReadMessage 从连接中读取消息
func ReadMessage(reader *bufio.Reader) (*Message, error) {
	// 读取消息头
	var header MessageHeader
	if err := binary.Read(reader, binary.BigEndian, &header); err != nil {
		return nil, fmt.Errorf("读取消息头错误: %w", err)
	}

	// 读取消息体
	payload := make([]byte, header.Length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("读取消息体错误: %w", err)
	}

	return &Message{
		Type:    header.Type,
		Payload: payload,
	}, nil
}

// WriteMessage 将消息写入连接
func WriteMessage(writer io.Writer, msg *Message) error {
	// 写入消息头
	header := MessageHeader{
		Length: uint32(len(msg.Payload)),
		Type:   msg.Type,
	}

	if err := binary.Write(writer, binary.BigEndian, header); err != nil {
		return fmt.Errorf("写入消息头错误: %w", err)
	}

	// 写入消息体
	if _, err := writer.Write(msg.Payload); err != nil {
		return fmt.Errorf("写入消息体错误: %w", err)
	}

	return nil
}

func EncodeMessage(msg *Message) []byte {
	// 实现消息编码
}

func DecodeMessage(data []byte) (*Message, error) {
	// 实现消息解码
}
