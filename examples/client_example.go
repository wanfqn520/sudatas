package main

import (
	"fmt"
	"log"
	"time"

	"github.com/yourusername/sudatas/client"
)

func main() {
	// 创建客户端
	c := client.NewClient(
		"localhost:5432",
		"root",
		"123456",
		client.WithTimeout(time.Second*10),
	)

	// 连接服务器
	if err := c.Connect(); err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// 创建集合
	if err := c.CreateCollection("myapp"); err != nil {
		log.Fatal(err)
	}

	// 创建数据库
	if err := c.CreateDatabase("myapp", "users", "json", "用户数据"); err != nil {
		log.Fatal(err)
	}

	// 插入数据
	user := map[string]interface{}{
		"name":  "Alice",
		"age":   25,
		"email": "alice@example.com",
	}
	if err := c.Insert("myapp", "users", user); err != nil {
		log.Fatal(err)
	}

	// 查询数据
	filter := map[string]interface{}{
		"age": map[string]interface{}{
			"$gt": 20,
		},
	}
	results, err := c.Find("myapp", "users", filter)
	if err != nil {
		log.Fatal(err)
	}

	// 打印结果
	for _, result := range results {
		fmt.Printf("用户: %+v\n", result)
	}

	// 更新数据
	update := map[string]interface{}{
		"$set": map[string]interface{}{
			"age": 26,
		},
	}
	if err := c.Update("myapp", "users", filter, update); err != nil {
		log.Fatal(err)
	}

	// 列出所有集合
	collections, err := c.ListCollections()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("集合列表: %v\n", collections)

	// 列出集合中的数据库
	databases, err := c.ListDatabases("myapp")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("数据库列表: %v\n", databases)
}
