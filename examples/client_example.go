package main

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"sudatas/client"
	"sudatas/internal/storage"
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

	// 准备测试数据
	users := []map[string]interface{}{
		{
			"name":     "Alice",
			"age":      25,
			"email":    "alice@example.com",
			"created":  time.Now(),
			"active":   true,
			"role":     "admin",
			"location": "Beijing",
		},
		{
			"name":     "Bob",
			"age":      30,
			"email":    "bob@example.com",
			"created":  time.Now(),
			"active":   true,
			"role":     "user",
			"location": "Shanghai",
		},
		{
			"name":     "Charlie",
			"age":      22,
			"email":    "charlie@example.com",
			"created":  time.Now(),
			"active":   false,
			"role":     "user",
			"location": "Beijing",
		},
		{
			"name":     "David",
			"age":      35,
			"email":    "david@example.com",
			"created":  time.Now(),
			"active":   true,
			"role":     "manager",
			"location": "Guangzhou",
		},
	}

	// 创建集合和数据库
	collectionName := "myapp"
	dbName := "users"

	log.Printf("创建集合 %s...\n", collectionName)
	if err := c.CreateCollection(collectionName); err != nil {
		if !strings.Contains(err.Error(), "已存在") {
			log.Fatal(err)
		}
	}

	log.Printf("创建数据库 %s...\n", dbName)
	if err := c.CreateDatabase(collectionName, dbName, "json", "用户数据"); err != nil {
		if !strings.Contains(err.Error(), "已存在") {
			log.Fatal(err)
		}
	}

	// 插入测试数据
	log.Println("插入测试数据...")
	for _, user := range users {
		if err := c.Insert(collectionName, dbName, user); err != nil {
			log.Fatal(err)
		}
	}

	// 执行各种查询
	log.Println("\n1. 查询所有数据:")
	results, err := c.Find(collectionName, dbName, nil)
	if err != nil {
		log.Fatal(err)
	}
	printResults(results)

	log.Println("\n2. 查询年龄大于25的用户:")
	filter := map[string]interface{}{
		"age": map[string]interface{}{
			"operator": ">",
			"value":    25,
		},
	}
	results, err = c.Find(collectionName, dbName, filter)
	if err != nil {
		log.Fatal(err)
	}
	printResults(results)

	log.Println("\n3. 查询北京的用户:")
	filter = map[string]interface{}{
		"location": "Beijing",
	}
	results, err = c.Find(collectionName, dbName, filter)
	if err != nil {
		log.Fatal(err)
	}
	printResults(results)

	log.Println("\n4. 查询活跃的管理员:")
	filter = map[string]interface{}{
		"active": true,
		"role":   "admin",
	}
	results, err = c.Find(collectionName, dbName, filter)
	if err != nil {
		log.Fatal(err)
	}
	printResults(results)

	// 导出数据
	log.Println("\n导出数据...")
	opts := storage.ExportOptions{
		IncludeSchema: true,
		Format:        "sql",
		Directory:     "./export",
		Filename:      "users_export.suql",
	}
	if err := c.ExportDatabase(collectionName, dbName, opts); err != nil {
		log.Fatal(err)
	}
	exportPath := filepath.Join(opts.Directory, opts.Filename)
	log.Printf("数据已导出到: %s\n", exportPath)

	// 导入数据到新的集合
	log.Println("\n导入数据...")
	newCollection := "imported_app"
	if err := c.CreateCollection(newCollection); err != nil {
		if !strings.Contains(err.Error(), "已存在") {
			log.Fatal(err)
		}
	}

	if err := c.ImportDatabase(exportPath, newCollection); err != nil {
		log.Fatal(err)
	}
	log.Printf("数据已从 %s 导入到 %s", exportPath, newCollection)

	// 验证导入的数据
	log.Println("\n验证导入的数据:")
	results, err = c.Find(newCollection, dbName, nil)
	if err != nil {
		log.Fatal(err)
	}
	printResults(results)
}

// printResults 格式化打印查询结果
func printResults(results []map[string]interface{}) {
	if len(results) == 0 {
		fmt.Println("没有找到数据")
		return
	}

	for i, result := range results {
		fmt.Printf("\n记录 #%d:\n", i+1)
		for k, v := range result {
			fmt.Printf("  %-10s: %v\n", k, v)
		}
	}
}
