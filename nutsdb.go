package dbs

import (
	"fmt"
	"hash/crc32"

	"github.com/cespare/xxhash/v2"
	"github.com/nutsdb/nutsdb"
)

type NutsdbInfo struct {
	Db           *nutsdb.DB
	bucketNumber int
	buckets      []string
}

// NewNutsdb 创建nutsdb数据库，容量受磁盘大小限制
// bucketNumber 分片数，一旦定下来，不能修改，否则出现数据获取不到
// 外部需要执行 db.Close()
// 若对一个key频繁操作，择机执行db.Merge()
func NewNutsdb(dbName string, bucketNumber int, faster bool) (*NutsdbInfo, error) {
	if dbName == "" {
		dbName = "nutsdb.db"
	}
	if bucketNumber <= 0 {
		bucketNumber = 1024
	}

	// 配置
	opt := nutsdb.DefaultOptions
	opt.SegmentSize = 256 * 1024 * 1024 // 256MB，避免其升级后修改了，因此这里指定
	opt.SyncEnable = false              // 若开启，性能掉的厉害
	opt.Dir = dbName
	opt.EntryIdxMode = nutsdb.HintKeyAndRAMIdxMode // key在内存，value在磁盘
	if faster {
		opt.EntryIdxMode = nutsdb.HintKeyValAndRAMIdxMode // key和value都在内存
		opt.SyncEnable = false                            // 不主动刷盘，依靠系统（比如30s）。提高性能，但断电或者系统奔溃，会有数据丢失的风险
		opt.RWMode = nutsdb.MMap                          // 使用内存映射提高性能，但断电或者系统奔溃，会有数据丢失的风险
	}

	// 方案1：磁盘模式
	// 方案2：纯内存模式
	db, err := nutsdb.Open(opt)
	if err != nil {
		return nil, err
	}
	// opts := inmemory.DefaultOptions
	// db, err := inmemory.Open(opts)
	// if err != nil {
	// 	panic(err)
	// }

	// // 检查桶数量（这块代码没用：好像tx.NewBucket()不会立即创建）
	// var existBucketNumber int
	// if err := db.View(
	// 	func(tx *nutsdb.Tx) error {
	// 		return tx.IterateBuckets(nutsdb.DataStructureBTree, "*", func(bucket string) bool {
	// 			// fmt.Println(bucket)
	// 			existBucketNumber++
	// 			// true: continue, false: break
	// 			return true
	// 		})
	// 	}); err != nil {
	// 	return nil, err
	// }
	// if existBucketNumber != 0 && existBucketNumber != bucketNumber {
	// 	return nil, fmt.Errorf("exist bucket number is not equal to bucketNumber, exist: %d, expect: %d", existBucketNumber, bucketNumber)
	// }

	// 创建 bucket 实现分片
	buckets := make([]string, bucketNumber)
	for i := 0; i < bucketNumber; i++ {
		buckets[i] = fmt.Sprintf("bucket_%d", i)
	}
	// 若桶不存在则创建
	if err := db.Update(func(tx *nutsdb.Tx) error {
		for _, bucket := range buckets {
			if exist := tx.ExistBucket(nutsdb.DataStructureBTree, bucket); exist {
				continue
			}
			if err := tx.NewBucket(nutsdb.DataStructureBTree, bucket); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// 刷盘
	err = db.ActiveFile.Sync()
	if err != nil {
		return nil, err
	}

	// // 写
	// if err := db.Update(func(tx *nutsdb.Tx) error {
	// 	if err := tx.Put("bucket_0", []byte("key"), []byte("value"), 0); err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }); err != nil {
	// 	return nil, err
	// }

	// // 读
	// if err := db.View(func(tx *nutsdb.Tx) error {
	// 	if _, err := tx.Get("bucket_0", []byte("key")); err != nil {
	// 		if err != nutsdb.ErrKeyNotFound {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// }); err != nil {
	// 	return nil, err
	// }

	return &NutsdbInfo{
		Db:           db,
		bucketNumber: bucketNumber,
		buckets:      buckets,
	}, nil
}

// 推荐分片数 = (预估最大活跃连接数 × 2) / CPU核心数
// 其他hash库："github.com/cespare/xxhash/v2"
func (n *NutsdbInfo) GetShardBucket(key []byte) string {
	hash := crc32.ChecksumIEEE(key)
	return n.buckets[int(hash)%n.bucketNumber] // 32位系统这里可能会出错
}

func (n *NutsdbInfo) GetShardBucket2(key []byte) string {
	hash := xxhash.Sum64(key)
	return n.buckets[hash%uint64(n.bucketNumber)]
}
