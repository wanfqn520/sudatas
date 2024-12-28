package audit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"sudatas/internal/security"
)

// LogLevel 日志级别
type LogLevel int

const (
	INFO LogLevel = iota
	WARN
	ERROR
)

// LogEntry 日志条目
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     LogLevel  `json:"level"`
	User      string    `json:"user"`
	Action    string    `json:"action"`
	Object    string    `json:"object"`
	Status    string    `json:"status"`
	Details   string    `json:"details"`
	IP        string    `json:"ip"`
}

// AuditLogger 审计日志管理器
type AuditLogger struct {
	mu      sync.Mutex
	file    *os.File
	crypto  *security.CryptoManager
	dir     string
	maxSize int64 // 单个日志文件最大大小（字节）
	curSize int64 // 当前日志文件大小
}

// NewAuditLogger 创建新的审计日志管理器
func NewAuditLogger(dir string, crypto *security.CryptoManager, maxSize int64) (*AuditLogger, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建日志目录失败: %w", err)
	}

	logger := &AuditLogger{
		crypto:  crypto,
		dir:     dir,
		maxSize: maxSize,
	}

	if err := logger.rotateLog(); err != nil {
		return nil, err
	}

	return logger, nil
}

// Log 记录审计日志
func (l *AuditLogger) Log(entry *LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 检查是否需要轮转日志
	if l.curSize >= l.maxSize {
		if err := l.rotateLog(); err != nil {
			return err
		}
	}

	// 序列化日志条目
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("序列化日志失败: %w", err)
	}

	// 加密日志内容
	encrypted, err := l.crypto.EncryptSM4(data)
	if err != nil {
		return fmt.Errorf("加密日志失败: %w", err)
	}

	// 写入日志文件
	n, err := l.file.Write(append(encrypted, '\n'))
	if err != nil {
		return fmt.Errorf("写入日志失败: %w", err)
	}

	l.curSize += int64(n)
	return nil
}

// rotateLog 轮转日志文件
func (l *AuditLogger) rotateLog() error {
	if l.file != nil {
		l.file.Close()
	}

	timestamp := time.Now().Format("20060102150405")
	filename := filepath.Join(l.dir, fmt.Sprintf("audit_%s.log", timestamp))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("创建日志文件失败: %w", err)
	}

	l.file = file
	l.curSize = 0
	return nil
}

// Close 关闭日志管理器
func (l *AuditLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// ReadLogs 读取指定时间范围的日志
func (l *AuditLogger) ReadLogs(start, end time.Time) ([]*LogEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var entries []*LogEntry

	// 遍历日志目录
	err := filepath.Walk(l.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}

		// 读取并解密日志文件
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("读取日志文件失败: %w", err)
		}

		// 按行处理日志
		lines := bytes.Split(data, []byte{'\n'})
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}

			// 解密日志行
			decrypted, err := l.crypto.DecryptSM4(line)
			if err != nil {
				continue // 跳过无法解密的行
			}

			var entry LogEntry
			if err := json.Unmarshal(decrypted, &entry); err != nil {
				continue // 跳过无法解析的行
			}

			// 检查时间范围
			if (entry.Timestamp.After(start) || entry.Timestamp.Equal(start)) &&
				(entry.Timestamp.Before(end) || entry.Timestamp.Equal(end)) {
				entries = append(entries, &entry)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("读取日志失败: %w", err)
	}

	return entries, nil
}
