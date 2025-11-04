package dbs

import (
	"github.com/nutsdb/nutsdb"
	"github.com/tidwall/buntdb"
)

// 实现内存数据库的数据同步到文件数据库

// 方案1：增量同步
// 定义一个管理器协程
// 1. 提供接口：接受需要被同步的数据
// 2. 周期性将获取到的数据同步到文件数据库
// 细节优化点：
// 1. 收到数据后，循环读取是否还有数据，做批量同步

// 方案2：全量同步
// 操作缓慢且影响内存数据库的性能，需要在业务低峰期执行（比如：中午休息时间）
// 读性能无损：快照读取不影响主库写入速度
// 写性能损耗：仅来自版本树维护（约 3-5%）
// 假设数据库含 100 万条记录：
// 全量遍历耗时 ≈ 50ms~200ms
// 在此期间可能发生：
// 1. 已遍历的键被修改（数据版本不一致）
// 2. 新键插入（数据遗漏）
// 3. 未遍历的键被删除（数据丢失）
func SyncMemToFile(memDb *buntdb.DB, fileDb *NutsdbInfo) error {
	return fileDb.Db.Update(func(fileTx *nutsdb.Tx) error {
		return memDb.View(func(memTx *buntdb.Tx) error {
			// 遍历内存数据库所有键
			var syncErr error
			err := memTx.Ascend("", func(key, value string) bool {
				if syncErr = fileTx.Put( // 不能用局部变量，需要把错误传递出去
					fileDb.GetShardBucket([]byte(key)),
					[]byte(key),
					[]byte(value),
					0,
				); syncErr != nil {
					return false
				}
				return true
			})
			if err != nil {
				return err
			}
			return syncErr
		})
	})
}
