package dbs

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/tidwall/buntdb"
)

func writeBuntdb(wg *sync.WaitGroup, db *buntdb.DB, i int) {
	defer wg.Done()

	key := fmt.Sprintf("key%d", i)
	if err := db.Update(func(tx *buntdb.Tx) error {
		if _, _, err := tx.Set(key, key, nil); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}

func readBuntdb(wg *sync.WaitGroup, db *buntdb.DB, i int) {
	defer wg.Done()

	key := fmt.Sprintf("key%d", i)
	if err := db.View(func(tx *buntdb.Tx) error {
		if _, err := tx.Get(key); err != nil {
			if err != buntdb.ErrNotFound {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}

func BenchmarkWriteBuntdb(b *testing.B) {
	db, err := NewBuntdb("buntdb.db", true)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 写
		go writeBuntdb(&wg, db, i)
	}
	wg.Wait()

	err = db.Shrink()
	if err != nil {
		panic(err)
	}
}

func BenchmarkReadBuntdb(b *testing.B) {
	db, err := NewBuntdb("buntdb.db", true)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 读
		go readBuntdb(&wg, db, i)
	}
	wg.Wait()
}

func BenchmarkWriteReadBuntdb(b *testing.B) {
	db, err := NewBuntdb("buntdb.db", true)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(2)

		// 写
		go writeBuntdb(&wg, db, i)

		// 读
		go readBuntdb(&wg, db, i)
	}
	wg.Wait()

	err = db.Shrink()
	if err != nil {
		panic(err)
	}
}

func BenchmarkWriteBuntdbMem(b *testing.B) {
	db, err := NewBuntdb("", true)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 写
		go writeBuntdb(&wg, db, i)
	}
	wg.Wait()

	err = db.Shrink()
	if err != nil {
		panic(err)
	}
}

func BenchmarkReadBuntdbMem(b *testing.B) {
	db, err := NewBuntdb("", true)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 读
		go readBuntdb(&wg, db, i)
	}
	wg.Wait()
}

func BenchmarkWriteReadBuntdbMem(b *testing.B) {
	db, err := NewBuntdb("", true)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(2)

		// 写
		go writeBuntdb(&wg, db, i)

		// 读
		go readBuntdb(&wg, db, i)
	}
	wg.Wait()

	err = db.Shrink()
	if err != nil {
		panic(err)
	}
}
