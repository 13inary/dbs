package dbs

import (
	"fmt"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/tidwall/buntdb"
)

var (
	testMemDb  *buntdb.DB
	testFileDb *NutsdbInfo
)

func init() {
	// 内存数据库
	buntDb, err := NewBuntdb("", true)
	if err != nil {
		panic(err)
	}
	testMemDb = buntDb

	// 文件数据库，这里的数据库路径别搞错，否则容易丢数据
	nutsdb, err := NewNutsdb("", 1024, false)
	if err != nil {
		panic(err)
	}
	testFileDb = nutsdb
}

func printFileDbAllData(t *testing.T, fileDb *NutsdbInfo) {
	var existBucketNumber int
	err := fileDb.Db.View(func(fileTx *nutsdb.Tx) error {
		return fileTx.IterateBuckets(nutsdb.DataStructureBTree, "*", func(bucket string) bool {
			existBucketNumber++
			keys, values, err := fileTx.GetAll(bucket)
			if err != nil {
				return false
			}
			for i := range keys {
				fmt.Println("data:", bucket, string(keys[i]), string(values[i]))
			}
			return true
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("existBucketNumber:", existBucketNumber)
}

func TestSyncMemToFile(t *testing.T) {
	// 关闭步骤不能少
	defer testMemDb.Close()
	defer testFileDb.Db.Close()

	// 内存数据库：写入数据
	for i := 0; i < 3; i++ {
		err := testMemDb.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), nil)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// 内存数据：检查写入的数据
	err := testMemDb.View(func(memTx *buntdb.Tx) error {
		return memTx.Ascend("", func(key, value string) bool {
			fmt.Println("mem:", key, value)
			return true
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	// 文件数据库：查看原本数据
	printFileDbAllData(t, testFileDb)

	// 同步数据
	if err := SyncMemToFile(testMemDb, testFileDb); err != nil {
		t.Fatal(err)
	}
	err = testFileDb.Db.ActiveFile.Sync()
	if err != nil {
		t.Fatal("merge file db", err)
	}

	// 文件数据库：检查同步后的数据
	printFileDbAllData(t, testFileDb)
}
