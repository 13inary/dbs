package dbs

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"

	"gorm.io/gorm"
)

type User struct {
	ID   int    `gorm:"primary_key"`
	Name string `gorm:"column:name"`
}

func writeSqlite(wg *sync.WaitGroup, db *gorm.DB, i int) {
	defer wg.Done()

	err := db.Save(&User{Name: fmt.Sprintf("user%d", i)}).Error
	if err != nil {
		log.Fatal(err)
	}
}

func readSqlite(wg *sync.WaitGroup, db *gorm.DB, i int) {
	defer wg.Done()

	var user User
	err := db.Where("name = ?", fmt.Sprintf("user%d", i)).First(&user).Error
	if err != nil {
		if err != gorm.ErrRecordNotFound {
			log.Fatal(err)
		}
	}
}

func BenchmarkWriteSqlite(b *testing.B) {
	db, err := NewSqliteDb(context.Background(), "sqlite.db", nil)
	if err != nil {
		panic(err)
	}
	sqliteDb, err := db.DB()
	if err != nil {
		panic(err)
	}
	defer sqliteDb.Close()

	db.AutoMigrate(&User{})

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 写
		go writeSqlite(&wg, db, i)
	}
	wg.Wait()
}

func BenchmarkReadSqlite(b *testing.B) {
	db, err := NewSqliteDb(context.Background(), "sqlite.db", nil)
	if err != nil {
		panic(err)
	}
	sqliteDb, err := db.DB()
	if err != nil {
		panic(err)
	}
	defer sqliteDb.Close()

	db.AutoMigrate(&User{})

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		// 读
		go readSqlite(&wg, db, i)
	}
	wg.Wait()
}

func BenchmarkWriteReadSqlite(b *testing.B) {
	db, err := NewSqliteDb(context.Background(), "sqlite.db", nil)
	if err != nil {
		panic(err)
	}
	sqliteDb, err := db.DB()
	if err != nil {
		panic(err)
	}
	defer sqliteDb.Close()

	db.AutoMigrate(&User{})

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(2)

		// 写
		go writeSqlite(&wg, db, i)

		// 读
		go readSqlite(&wg, db, i)
	}
	wg.Wait()
}
