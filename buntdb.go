package dbs

import (
	"github.com/tidwall/buntdb"
)

// NewBuntdb 创建buntdb数据库，容量受内存大小限制
// dbName: 数据库文件名，如果为空，则使用内存模式
// faster: true 快速模式，长时间间隔写盘+手动写盘和压缩
// faster: false 慢速模式，间隔一秒写盘+自动压缩
// 外部需要执行 db.Close()、db.Shrink()或db.Save()
func NewBuntdb(dbName string, faster bool) (*buntdb.DB, error) {
	if dbName == "" {
		dbName = ":memory:"
	}

	// 方案1：:memory: + db.Save()触发持久化
	// 方案2：buntdb.db + db.Shrink()触发持久化和压缩
	db, err := buntdb.Open(dbName)
	if err != nil {
		return nil, err
	}

	// 配置
	config := buntdb.Config{
		SyncPolicy:           buntdb.EverySecond, // 每秒同步到磁盘
		AutoShrinkPercentage: 10,                 // 10% 膨胀触发压缩
		AutoShrinkMinSize:    32 * 1024 * 1024,   // 32MB 以上文件触发压缩
	}
	if faster {
		config.SyncPolicy = buntdb.Never // os同步（比如30s）+手动同步到磁盘
		config.AutoShrinkPercentage = 0  // 关闭自动压缩
		config.AutoShrinkMinSize = 0     // 关闭自动压缩
		config.AutoShrinkDisabled = true // 关闭自动压缩
	}
	err = db.SetConfig(config)
	if err != nil {
		return nil, err
	}

	// // 只写/读写
	// if err := db.Update(func(tx *buntdb.Tx) error {
	// 	if _, _, err := tx.Set("key", "value", nil); err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }); err != nil {
	// 	return nil, err
	// }

	// // 只读
	// if err := db.View(func(tx *buntdb.Tx) error {
	// 	if _, err := tx.Get("key"); err != nil {
	// 		if err != buntdb.ErrNotFound {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// }); err != nil {
	// 	return nil, err
	// }

	return db, nil
}

func BuntdbKeyExists(db *buntdb.DB, key string) (bool, error) {
	var exists bool
	return exists, db.View(func(tx *buntdb.Tx) error {
		_, err := tx.Get(key) // 没有专用的tx.Exists(key)，因此只能用这个
		if err != nil {
			if err == buntdb.ErrNotFound {
				return nil
			}
			return err
		}
		exists = true
		return nil
	})
}
