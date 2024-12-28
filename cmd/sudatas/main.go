package main

import (
	"log"
	"net"

	"github.com/yourusername/sudatas/internal/network"
	"github.com/yourusername/sudatas/internal/storage"
)

func main() {
	// 初始化存储引擎
	store := storage.NewEngine("./data")

	// 启动TCP服务器
	listener, err := net.Listen("tcp", ":5432")
	if err != nil {
		log.Fatalf("无法启动服务器: %v", err)
	}

	server := network.NewServer(store)
	server.Serve(listener)
}
