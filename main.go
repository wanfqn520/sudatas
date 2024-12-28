package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"sudatas/internal/network"
	"sudatas/internal/security"
	"sudatas/internal/storage"
)

var (
	addr       = flag.String("addr", ":5432", "服务器监听地址")
	dataDir    = flag.String("data", "./data", "用户数据目录")
	builtinDir = "./builtin" // 系统文件目录
	maxClient  = flag.Int("max-clients", 1000, "最大客户端连接数")
)

func main() {
	flag.Parse()

	// 创建系统目录
	if err := os.MkdirAll(builtinDir, 0755); err != nil {
		log.Fatalf("创建系统目录失败: %v", err)
	}

	// 创建数据目录
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("创建数据目录失败: %v", err)
	}

	// 初始化加密管理器
	crypto, err := security.NewCryptoManager()
	if err != nil {
		log.Fatalf("初始化加密管理器失败: %v", err)
	}

	// 加载或创建密钥
	keyFile := filepath.Join(builtinDir, "key.sudb")
	if err := crypto.LoadKeys(keyFile); err != nil {
		log.Fatalf("加载密钥失败: %v", err)
	}

	// 初始化存储引擎
	engine, err := storage.NewEngine(*dataDir, builtinDir, crypto)
	if err != nil {
		log.Fatalf("初始化存储引擎失败: %v", err)
	}

	// 创建服务器
	server, err := network.NewServer(engine, *maxClient)
	if err != nil {
		log.Fatalf("创建服务器失败: %v", err)
	}

	// 创建监听器
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("监听端口失败: %v", err)
	}

	log.Printf("服务器启动，监听地址: %s", *addr)

	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// 启动服务器
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Serve(ctx, listener); err != nil {
			log.Printf("服务器运行失败: %v", err)
		}
	}()

	// 处理优雅退出
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("正在关闭服务器...")
	cancel() // 取消上下文

	// 等待服务器关闭，最多等待5秒
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("服务器已关闭")
	case <-time.After(5 * time.Second):
		log.Println("服务器关闭超时")
	}

	// 关闭其他资源
	if err := server.Shutdown(); err != nil {
		log.Printf("关闭服务器资源失败: %v", err)
	}
}
