package dbs

import (
	"context"
	"log"
	"os"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	defaultDbPath = "./data.db"
)

// NewSqliteDb 初始化数据库
func NewSqliteDb(ctx context.Context, dbPath string, customLogger logger.Interface) (*gorm.DB, error) {
	if dbPath == "" {
		dbPath = defaultDbPath
	}

	// 忽略掉 RecordNotFound 日志
	if customLogger == nil {
		// customLogger = logger.Default
		customLogger = logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             500 * time.Millisecond, // Slow SQL threshold，默认200ms
				LogLevel:                  logger.Warn,            // Log level
				IgnoreRecordNotFoundError: true,                   // Ignore ErrRecordNotFound error for logger
				ParameterizedQueries:      true,                   // Don't include params in the SQL log
				Colorful:                  true,                   // Disable color
			},
		)
	}

	gormDb, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		NowFunc: func() time.Time {
			return time.Now().Local()
		},
		SkipDefaultTransaction: true,         // 禁用它，这将获得大约 30%+ 性能提升
		PrepareStmt:            true,         // 执行任何 SQL 时都创建并缓存预编译语句，可以提高后续的调用速度
		Logger:                 customLogger, // 自定义日志
	})
	if err != nil {
		return nil, err
	}

	return gormDb.WithContext(ctx), nil
}
