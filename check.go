package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/couchbase/moss"
	"github.com/dgraph-io/badger"
	//"github.com/boltdb/bolt"
	"log"
	"os"

	"github.com/xujiajun/nutsdb"
	bolt "go.etcd.io/bbolt"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	argType := flag.String("type", "unknown", "--type postgres/boltdb")
	arqSQL := flag.String("sql", "", "check --query --sql SELECT *")
	flag.Parse()
	if argType == nil {
		log.Fatal("choose -type")
	}

	switch *argType {
	case "unknown":
		log.Fatal("please set --type param")
	case "postgres":
		postgres()
	case "sqlite":
		benchSQLite()
	case "readsqlite":
		readSQLite()
	case "nuts":
		nuts()
	case "bader":
		benchBader()
	case "boltdb":
		writeToDB()
	case "readboltdb":
		readBoltDb()
	case "query":
		runSQL(*arqSQL)
	case "readMoss":
		readMoss()
	case "writeMoss":
		writeMoss()
	}
}

func readMoss() {
	store, collection := initMoss()
	ropts := moss.ReadOptions{}

	var bytes, records int
	for i := 0; i < 6643/100; i++ {
		for k := 0; k < 100; k++ {
			data := fmt.Sprintf("k%dsd%d", i, k)
			val, errC := collection.Get([]byte(data), ropts)
			if errC != nil {
				lg(errC)
			}
			bytes = bytes + len(val)
			records = records + 1
			if records%10000 == 0 {
				log.Printf("processed %d", records)
			}
		}
	}

	log.Printf("read rows %d, bytes %d", records, bytes)
	time.Sleep(15*time.Second)
	err := collection.Close()
	if err != nil {
		lg(err)
	}
	err = store.Close()
	checkErr(err)

}

func writeMoss() {
	store, collection := initMoss()

	batch, err := collection.NewBatch(0, 0)
	for i := 0; i < 6643976/100; i++ {
		checkErr(err)
		for k := 0; k < 100; k++ {
			data := fmt.Sprintf("k%dsd%d", i, k)
			err = batch.Set([]byte(data), []byte("1"))
			if err != nil {
				lg(err)
			}
		}

	}

	err = collection.ExecuteBatch(batch, moss.WriteOptions{})
	checkErr(err)
	err = batch.Close()
	checkErr(err)

	err = collection.Close()
	checkErr(err)
	err = store.Close()
	checkErr(err)
}

func initMoss() (*moss.Store, moss.Collection) {
	directoryPath := "./moss"

	store, collection, err := moss.OpenStoreCollection(directoryPath,
		moss.StoreOptions{}, moss.StorePersistOptions{})
	checkErr(err)

	err = collection.Start()
	if err != nil {
		lg(err)
	}

	return store, collection
}

func runSQL(query string) {
	log.Printf("run query: %s", query)
	db, err := sql.Open("sqlite3", "./sqlite.db")
	checkErr(err)
	defer db.Close()
	log.Print(db.Exec(query))
}

func postgres() {
	log.Print("benchmark postgres")
	pgString, exists := os.LookupEnv("PG_DB")
	if !exists {
		log.Print("env PG_DB is required")
	}
	pgDb, errC := sql.Open("postgres", pgString)
	if pgDb == nil || errC != nil {
		log.Fatal(errC)
	}
	defer pgDb.Close()

	rows, err := pgDb.Query("select content from vader_ip")
	if err != nil {
		log.Print(err)
	}
	defer rows.Close()

	var data string
	bytes := 0
	records := 0
	for rows.Next() {
		if err = rows.Scan(&data); err != nil {
			log.Println(err)
		}
		bytes = bytes + len(data)
		records = records + 1
		if records%100000 == 0 {
			log.Printf("%d records processed", records)
		}
	}
	log.Printf("%d bytes, %d records", bytes, records)
}

func writeToDB() {
	log.Print("benchmark boltdb")
	db, err := bolt.Open("test.db", 0600, &bolt.Options{ReadOnly: false})
	if err != nil {
		log.Println("Could not open the database", err)
	}
	db.NoSync = true
	defer db.Close()
	prev := db.Stats()
	for i := 0; i < 6643976/100; i++ {
		if err := db.Batch(func(tx *bolt.Tx) error {
			if i%1000 == 0 {
				log.Printf("processed %d records", i)
			}
			bk, err := tx.CreateBucketIfNotExists([]byte("vader_ip"))
			if err != nil {
				panic(err)
			}
			for k := 0; k < 100; k++ {
				data := fmt.Sprintf("k%dsd%d", i, k)
				if err := bk.Put([]byte(data), []byte("1")); err != nil {
					panic(err)
				}
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}
	db.NoSync = false
	collectStats(db, prev)
}

func readBoltDb() {
	log.Print("benchmark read boltdb")
	db, err := bolt.Open("test.db", 0600, &bolt.Options{ReadOnly: false})
	if err != nil {
		log.Println("Could not open the database", err)
	}
	db.NoSync = true
	defer db.Close()
	prev := db.Stats()

	for i := 0; i < 6643976/100; i++ {
		for k := 0; k < 100; k++ {
			data := fmt.Sprintf("k%dsd%d", i, k)
			if err := db.View(func(tx *bolt.Tx) error {
				if i%1000 == 0 {
					log.Printf("processed %d records", i)
				}
				bk, err := tx.CreateBucketIfNotExists([]byte("vader_ip"))
				if err != nil {
					panic(err)
				}
				if err := bk.Get([]byte(data)); err != nil {
					panic(err)
				}

				return nil
			}); err != nil {
				panic(err)
			}
		}
	}
	db.NoSync = false
	collectStats(db, prev)
}

func collectStats(db *bolt.DB, prev bolt.Stats) {
	enc := json.NewEncoder(os.Stderr)
	enc.SetIndent("", "  ")

	// Grab the current stats and diff them.
	stats := db.Stats()
	diff := stats.Sub(&prev)

	// Encode stats to JSON and print to STDERR.
	enc.Encode(diff)
}

func benchSQLite() {
	log.Print("benchmark sqlite")
	db, err := sql.Open("sqlite3", "./sqlite.db")
	checkErr(err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE vader_ip (
        content VARCHAR(64) NULL
    ); CREATE INDEX my_little_hash_index ON vader_ip(content)`)
	checkErr(err)
	for i := 0; i < 6643976/1000; i++ {
		tx, errT := db.Begin()
		checkErr(errT)
		stmt, err := tx.Prepare("INSERT INTO vader_ip(content) values(?)")
		if i%1000 == 0 {
			log.Printf("processed %d records", i)
		}
		for k := 0; k < 1000; k++ {
			data := fmt.Sprintf("k%dsd%d", i, k)
			_, err = stmt.Exec(data)
		}
		err = tx.Commit()
		checkErr(err)
	}

	checkErr(err)
}

func readSQLite() {
	log.Print("benchmark read sqlite")
	db, err := sql.Open("sqlite3", "./sqlite.db")
	checkErr(err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE vader_ip (
        content VARCHAR(64) NULL
    );`)
	lg(err)
	bytes := 0
	var rows int64 = 0
	var absTime time.Duration
	for i := 0; i < 30; i++ {
		if i%1000 == 0 {
			log.Printf("processed %d records", i)
		}
		var res string

		log.Print(res)
		for k := 0; k < 1000; k++ {
			data := fmt.Sprintf("k%dsd%d", i, k)
			curTime := time.Now()
			row := db.QueryRow("SELECT content FROM vader_ip WHERE content LIKE ?", data)
			err = row.Scan(&res)
			if res != "" {
				bytes = bytes + len(res)
				rows = rows + 1

				absTime = absTime + time.Now().Sub(curTime)
			}
			lg(err)
		}
	}

	lg(err)
	abs := float64(absTime / time.Millisecond)
	avg := abs / float64(rows)
	log.Printf("read rows %d, bytes %d, absolute time %f ms, average %f ms", rows, bytes, abs, avg)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func nuts() {
	log.Print("benchmark nuts")
	opt := nutsdb.DefaultOptions
	opt.EntryIdxMode = nutsdb.HintKeyAndRAMIdxMode
	opt.SegmentSize = 64 * 1024 * 1024
	opt.Dir = "./nutsdb"
	ndb, err := nutsdb.Open(opt)
	if err != nil {
		log.Fatal(err)
	}
	defer ndb.Close()

	for i := 0; i < 664397600; i++ {
		if i%100 == 0 {
			log.Printf("processed %d records", i)
		}
		if err := ndb.Update(func(tx *nutsdb.Tx) error {
			for k := 0; k < 10; k++ {
				data := fmt.Sprintf("k%ds%d", i, k)
				err = tx.Put("vader_ip", []byte(data), []byte("1"), 0)
				if err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			panic(err)
		}
	}
}

func benchBader() {
	log.Print("benchmark bader")
	ndb, err := badger.Open(badger.DefaultOptions("./badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer ndb.Close()

	for i := 0; i < 6643; i++ {
		if i%100 == 0 {
			log.Printf("processed %d records", i)
		}
		wb := ndb.NewWriteBatch()
		for k := 0; k < 1000000; k++ {
			data := fmt.Sprintf("vader_ip:k%ds%d", i, k)
			err = wb.Set([]byte(data), []byte("1"))
			if err != nil {
				break
			}
		}
		if err != nil {
			wb.Cancel()
			break
		} else {
			checkErr(wb.Flush())
		}
	}
}

func lg(err error) {
	if err != nil {
		log.Print(err)
	}
}
