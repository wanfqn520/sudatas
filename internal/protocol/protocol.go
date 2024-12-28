package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Message struct {
	Type    MessageType
	Payload []byte
}

type MessageType uint32

const (
	AuthMessage MessageType = iota
	QueryMessage
	ResultMessage
	ErrorMessage
)

// 消息头部结构
type MessageHeader struct {
	Length uint32 // 消息体长度
	Type   uint32 // 消息类型，使用固定大小的类型
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
		Type:    MessageType(header.Type),
		Payload: payload,
	}, nil
}

// WriteMessage 将消息写入连接
func WriteMessage(writer io.Writer, msg *Message) error {
	// 写入消息头
	header := MessageHeader{
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

// EncodeMessage 将消息编码为字节流
func EncodeMessage(msg *Message) []byte {
	var buf bytes.Buffer
	header := MessageHeader{
		Length: uint32(len(msg.Payload)),
		Type:   uint32(msg.Type),
	}
	binary.Write(&buf, binary.BigEndian, &header)
	buf.Write(msg.Payload)
	return buf.Bytes()
}

// DecodeMessage 从字节流解码消息
func DecodeMessage(data []byte) (*Message, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("消息太短")
	}

	var header MessageHeader
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.BigEndian, &header); err != nil {
		return nil, err
	}

	if uint32(len(data)-8) < header.Length {
		return nil, fmt.Errorf("消息负载长度不正确")
	}

	return &Message{
		Type:    MessageType(header.Type),
		Payload: data[8 : 8+header.Length],
	}, nil
}
