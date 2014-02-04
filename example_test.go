package genmai_test

import (
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
	"github.com/naoina/genmai"
)

type TestModel struct {
	Id   int64
	Name string
	Addr string
}

func Example() {
	db, err := genmai.New(&genmai.SQLite3Dialect{}, ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		`CREATE TABLE test_model (
			id INTEGER NOT NULL PRIMARY KEY,
			name TEXT NOT NULL,
			addr TEXT NOT NULL
		)`,
		`INSERT INTO test_model VALUES (1, 'test1', 'addr1')`,
		`INSERT INTO test_model VALUES (2, 'test2', 'addr2')`,
		`INSERT INTO test_model VALUES (3, 'test3', 'addr3')`,
	} {
		if _, err := db.DB().Exec(query); err != nil {
			log.Fatal(err)
		}
	}
	var results []TestModel
	// SELECT * FROM "test_model";
	if err := db.Select(&results); err != nil {
		log.Fatal(err)
	}
	fmt.Println(results)
	// Output: [{1 test1 addr1} {2 test2 addr2} {3 test3 addr3}]
}
