package dbs

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/nutsdb/nutsdb"
)

func writeNutsdb(wg *sync.WaitGroup, dbInfo *NutsdbInfo, i int) {
	defer wg.Done()

	key := fmt.Appendf(nil, "key%d", i)
	shardBucket := dbInfo.GetShardBucket2(key)

	if err := dbInfo.Db.Update(func(tx *nutsdb.Tx) error {
		if err := tx.Put(shardBucket, key, key, 0); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}

func readNutsdb(wg *sync.WaitGroup, dbInfo *NutsdbInfo, i int) {
	defer wg.Done()

	key := fmt.Appendf(nil, "key%d", i)
	shardBucket := dbInfo.GetShardBucket2(key)

	if err := dbInfo.Db.View(func(tx *nutsdb.Tx) error {
		if _, err := tx.Get(shardBucket, key); err != nil {
			if err != nutsdb.ErrKeyNotFound {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}

func BenchmarkWriteNutsdb(b *testing.B) {
	dbInfo, err := NewNutsdb("", 0, true)
	if err != nil {
		panic(err)
	}
	defer dbInfo.Db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 写
		go writeNutsdb(&wg, dbInfo, i)
	}
	wg.Wait()
}

func BenchmarkReadNutsdb(b *testing.B) {
	dbInfo, err := NewNutsdb("", 0, true)
	if err != nil {
		panic(err)
	}
	defer dbInfo.Db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 读
		go readNutsdb(&wg, dbInfo, i)
	}
	wg.Wait()
}

func BenchmarkWriteReadNutsdb(b *testing.B) {
	dbInfo, err := NewNutsdb("", 0, true)
	if err != nil {
		panic(err)
	}
	defer dbInfo.Db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(2)

		// 写
		go writeNutsdb(&wg, dbInfo, i)

		// 读
		go readNutsdb(&wg, dbInfo, i)
	}
	wg.Wait()
}

func TestWriteNutsdbCold(t *testing.T) {
	dbInfo, err := NewNutsdb("", 0, false)
	if err != nil {
		panic(err)
	}
	defer dbInfo.Db.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go writeNutsdb(&wg, dbInfo, 0)
	wg.Wait()

	err = dbInfo.Db.View(func(fileTx *nutsdb.Tx) error {
		return fileTx.IterateBuckets(nutsdb.DataStructureBTree, "*", func(bucket string) bool {
			fmt.Println("file db bucket:", bucket)
			keys, values, err := fileTx.GetAll(bucket)
			if err != nil {
				return false
			}
			for i := range keys {
				fmt.Println("file db begin:", bucket, string(keys[i]), string(values[i]))
			}
			return true
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkWriteNutsdbCold(b *testing.B) {
	dbInfo, err := NewNutsdb("", 0, false)
	if err != nil {
		panic(err)
	}
	defer dbInfo.Db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 写
		go writeNutsdb(&wg, dbInfo, i)
	}
	wg.Wait()
}

func BenchmarkReadNutsdbCold(b *testing.B) {
	dbInfo, err := NewNutsdb("", 0, false)
	if err != nil {
		panic(err)
	}
	defer dbInfo.Db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 读
		go readNutsdb(&wg, dbInfo, i)
	}
	wg.Wait()
}

func BenchmarkWriteReadNutsdbCold(b *testing.B) {
	dbInfo, err := NewNutsdb("", 0, false)
	if err != nil {
		panic(err)
	}
	defer dbInfo.Db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(2)

		// 写
		go writeNutsdb(&wg, dbInfo, i)

		// 读
		go readNutsdb(&wg, dbInfo, i)
	}
	wg.Wait()
}
