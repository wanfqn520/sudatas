package network

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Pool 连接池配置
type Pool struct {
	mu          sync.Mutex
	connections chan net.Conn
	factory     func() (net.Conn, error)
	closed      bool
	maxIdle     int
	maxOpen     int
	timeout     time.Duration
}

// NewPool 创建新的连接池
func NewPool(factory func() (net.Conn, error), maxIdle, maxOpen int, timeout time.Duration) *Pool {
	return &Pool{
		connections: make(chan net.Conn, maxIdle),
		factory:     factory,
		maxIdle:     maxIdle,
		maxOpen:     maxOpen,
		timeout:     timeout,
	}
}

// Get 获取连接
func (p *Pool) Get() (net.Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("连接池已关闭")
	}

	select {
	case conn := <-p.connections:
		p.mu.Unlock()
		if conn == nil {
			return nil, fmt.Errorf("连接已关闭")
		}
		return &poolConn{p: p, Conn: conn}, nil
	default:
		conn, err := p.factory()
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return &poolConn{p: p, Conn: conn}, nil
	}
}

// Put 归还连接
func (p *Pool) Put(conn net.Conn) error {
	if conn == nil {
		return nil
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return conn.Close()
	}

	select {
	case p.connections <- conn:
		p.mu.Unlock()
		return nil
	default:
		p.mu.Unlock()
		return conn.Close()
	}
}

// Close 关闭连接池
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	close(p.connections)
	for conn := range p.connections {
		conn.Close()
	}
	return nil
}

// poolConn 包装的连接
type poolConn struct {
	net.Conn
	p      *Pool
	closed bool
}

func (pc *poolConn) Close() error {
	if pc.closed {
		return nil
	}
	pc.closed = true
	return pc.p.Put(pc.Conn)
}
