package parser

import (
	"github.com/yourusername/sudatas/internal/storage"
)

type SQLParser struct{}

func (p *SQLParser) Parse(sql string) (Command, error) {
	// 实现基本的SQL解析
	// 支持 CREATE TABLE, INSERT, SELECT, UPDATE, DELETE 等
	// 返回解析后的命令对象
}

type Command interface {
	Execute(engine *storage.Engine) error
}
