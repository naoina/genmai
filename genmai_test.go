package genmai

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type testModel struct {
	Id   int64
	Name string
	Addr string
}

type testModelAlt struct {
	Id   int64
	Name string
	Addr string
}

type M2 struct {
	Id   int64
	Body string
}

func testDB() (*DB, error) {
	switch os.Getenv("DB") {
	case "mysql":
		return New(&MySQLDialect{}, "travis@/genmai_test")
	case "postgres":
		return New(&PostgresDialect{}, "user=postgres dbname=genmai_test sslmode=disable")
	default:
		return New(&SQLite3Dialect{}, ":memory:")
	}
}

func newTestDB(t *testing.T) *DB {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_model`,
		`CREATE TABLE IF NOT EXISTS test_model (
			id INTEGER NOT NULL PRIMARY KEY,
			name TEXT NOT NULL,
			addr TEXT NOT NULL
		);`,
		`INSERT INTO test_model (id, name, addr) VALUES (1, 'test1', 'addr1');`,
		`INSERT INTO test_model (id, name, addr) VALUES (2, 'test2', 'addr2');`,
		`INSERT INTO test_model (id, name, addr) VALUES (3, 'test3', 'addr3');`,
		`INSERT INTO test_model (id, name, addr) VALUES (4, 'other', 'addr4');`,
		`INSERT INTO test_model (id, name, addr) VALUES (5, 'other', 'addr5');`,
		`INSERT INTO test_model (id, name, addr) VALUES (6, 'dup', 'dup_addr');`,
		`INSERT INTO test_model (id, name, addr) VALUES (7, 'dup', 'dup_addr');`,
		`INSERT INTO test_model (id, name, addr) VALUES (8, 'other1', 'addr8');`,
		`INSERT INTO test_model (id, name, addr) VALUES (9, 'other2', 'addr9');`,
		`DROP TABLE IF EXISTS m2`,
		`CREATE TABLE IF NOT EXISTS m2 (
			id INTEGER NOT NULL PRIMARY KEY,
			body TEXT NOT NULL
		);`,
		`INSERT INTO m2 (id, body) VALUES (1, 'a1');`,
		`INSERT INTO m2 (id, body) VALUES (2, 'b2');`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func Test_Select(t *testing.T) {
	// SELECT * FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{1, "test1", "addr1"},
			{2, "test2", "addr2"},
			{3, "test3", "addr3"},
			{4, "other", "addr4"},
			{5, "other", "addr5"},
			{6, "dup", "dup_addr"},
			{7, "dup", "dup_addr"},
			{8, "other1", "addr8"},
			{9, "other2", "addr9"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "id" = 1;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Where("id", "=", 1)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{1, "test1", "addr1"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model ORDER BY "id" DESC LIMIT 2;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Limit(2).OrderBy("id", DESC)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{9, "other2", "addr9"}, {8, "other1", "addr8"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model LIMIT 2 OFFSET 3;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Limit(2).Offset(3)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{4, "other", "addr4"}, {5, "other", "addr5"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "id" = 1 OR ("id" = 5 AND "name" = "other");
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Where("id", "=", 1).Or(db.Where("id", "=", 5).And("name", "=", "other"))); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{1, "test1", "addr1"}, {5, "other", "addr5"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "id" = 1 AND "name" = "test1";
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Where("id", "=", 1).And("name", "=", "test1")); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{1, "test1", "addr1"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "id" IN (2, 3);
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Where("id").In(2, 3)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{2, "test2", "addr2"}, {3, "test3", "addr3"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "name" LIKE "%3";
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Where("name").Like("%3")); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{3, "test3", "addr3"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "name" = "other" ORDER BY "id" ASC LIMIT 1;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Where("name", "=", "other").Limit(1).OrderBy("id", ASC)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{4, "other", "addr4"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "name" = "other" ORDER BY "id" ASC LIMIT 1 OFFSET 1;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Where("name", "=", "other").Limit(1).OrderBy("id", ASC).Offset(1)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{5, "other", "addr5"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "id" BETWEEN 3 AND 5;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Where("id").Between(3, 5)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{3, "test3", "addr3"}, {4, "other", "addr4"}, {5, "other", "addr5"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "id" FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, "id"); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{1, "", ""},
			{2, "", ""},
			{3, "", ""},
			{4, "", ""},
			{5, "", ""},
			{6, "", ""},
			{7, "", ""},
			{8, "", ""},
			{9, "", ""},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "name", "addr" FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, []string{"name", "addr"}); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{0, "test1", "addr1"},
			{0, "test2", "addr2"},
			{0, "test3", "addr3"},
			{0, "other", "addr4"},
			{0, "other", "addr5"},
			{0, "dup", "dup_addr"},
			{0, "dup", "dup_addr"},
			{0, "other1", "addr8"},
			{0, "other2", "addr9"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT DISTINCT "name" FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Distinct("name"), db.OrderBy("name", ASC)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{0, "dup", ""},
			{0, "other", ""},
			{0, "other1", ""},
			{0, "other2", ""},
			{0, "test1", ""},
			{0, "test2", ""},
			{0, "test3", ""},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT DISTINCT "name", "addr" FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Distinct("name", "addr"), db.OrderBy("addr", ASC)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{0, "test1", "addr1"},
			{0, "test2", "addr2"},
			{0, "test3", "addr3"},
			{0, "other", "addr4"},
			{0, "other", "addr5"},
			{0, "other1", "addr8"},
			{0, "other2", "addr9"},
			{0, "dup", "dup_addr"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModelAlt
		if err := db.Select(&actual, db.From(&testModel{})); err != nil {
			t.Fatal(err)
		}
		expected := []testModelAlt{
			{1, "test1", "addr1"},
			{2, "test2", "addr2"},
			{3, "test3", "addr3"},
			{4, "other", "addr4"},
			{5, "other", "addr5"},
			{6, "dup", "dup_addr"},
			{7, "dup", "dup_addr"},
			{8, "other1", "addr8"},
			{9, "other2", "addr9"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
	}()

	// SELECT COUNT(*) FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual int64
		if err := db.Select(&actual, db.Count(), db.From(testModel{})); err != nil {
			t.Fatal(err)
		}
		expected := int64(9)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
	}()

	// SELECT COUNT("id") FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual int64
		if err := db.Select(&actual, db.Count("id"), db.From(testModel{})); err != nil {
			t.Fatal(err)
		}
		expected := int64(9)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
	}()

	// SELECT COUNT(DISTINCT "name") FROM test_model;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual int64
		if err := db.Select(&actual, db.Count(db.Distinct("name")), db.From(testModel{})); err != nil {
			t.Fatal(err)
		}
		expected := int64(7)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
	}()

	// SELECT "test_model".* FROM "test_model" JOIN "m2" ON "test_model"."id" = "m2"."id";
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.Join(&M2{}).On("id")); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{1, "test1", "addr1"},
			{2, "test2", "addr2"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "test_model".* FROM "test_model" JOIN "m2" ON "test_model"."id" = "m2"."id" WHERE "m2".id = 2;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		t2 := &M2{}
		if err := db.Select(&actual, db.Join(t2).On("id").Where(t2, "id", "=", 2)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{2, "test2", "addr2"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "test_model".* FROM "test_model" JOIN "m2" ON "test_model"."id" = "m2"."id" WHERE "m2".id = 2 AND "test_model"."name" = "test2";
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		t1 := &testModel{}
		t2 := &M2{}
		if err := db.Select(&actual, db.Join(t2).On("id").Where(t2, "id", "=", 2).And(t1, "name", "=", "test2")); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{2, "test2", "addr2"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "test_model".* FROM "test_model" LEFT JOIN "m2" ON "test_model"."name" = "m2"."body";
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		t2 := &M2{}
		if err := db.Select(&actual, db.LeftJoin(t2).On("name", "=", "body"), db.OrderBy("id", ASC)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{1, "test1", "addr1"},
			{2, "test2", "addr2"},
			{3, "test3", "addr3"},
			{4, "other", "addr4"},
			{5, "other", "addr5"},
			{6, "dup", "dup_addr"},
			{7, "dup", "dup_addr"},
			{8, "other1", "addr8"},
			{9, "other2", "addr9"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "test_model".* FROM "test_model" LEFT JOIN "m2" ON "test_model"."name" = "m2"."body" WHERE "m2"."name" IS NULL;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		t2 := &M2{}
		if err := db.Select(&actual, db.LeftJoin(t2).On("id").Where(t2, "id").IsNull()); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{3, "test3", "addr3"},
			{4, "other", "addr4"},
			{5, "other", "addr5"},
			{6, "dup", "dup_addr"},
			{7, "dup", "dup_addr"},
			{8, "other1", "addr8"},
			{9, "other2", "addr9"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "test_model".* FROM "test_model" LEFT JOIN "m2" ON "test_model"."name" = "m2"."body" WHERE "m2"."name" IS NOT NULL;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		t2 := &M2{}
		if err := db.Select(&actual, db.LeftJoin(t2).On("id").Where(t2, "id").IsNotNull()); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{1, "test1", "addr1"},
			{2, "test2", "addr2"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()
}

func TestDB_Select_differentColumnName(t *testing.T) {
	type TestTable struct {
		Id int64 `column:"tbl_id"`
	}
	db, err := New(&SQLite3Dialect{}, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`CREATE TABLE test_table (tbl_id integer)`,
		`INSERT INTO test_table VALUES (1)`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	var results []TestTable
	if err := db.Select(&results); err != nil {
		t.Fatal(err)
	}
	actual := results
	expected := []TestTable{{Id: int64(1)}}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %#v, but %#v", expected, actual)
	}
}

func TestDB_CreateTable(t *testing.T) {
	func() {
		type TestTable struct {
			Id        int64 `db:"pk"`
			Name      string
			CreatedAt time.Time
			Status    bool   `db:"notnull" column:"status" default:"true"`
			DiffCol   string `column:"col"`
			Ignore    string `db:"-"`
		}
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTable(TestTable{}); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`INSERT INTO test_table (id, name, col) VALUES (1, "test1", "col1");`,
			`INSERT INTO test_table (id, name, status, col) VALUES (2, "test2", 0, "col2");`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(err)
			}
		}
		stmt, err := db.db.Prepare(`SELECT * FROM test_table`)
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		rows, err := stmt.Query()
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		var actual interface{} = len(cols)
		var expected interface{} = 5
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
		type tempTbl struct {
			Id        int64
			Name      string
			CreatedAt *time.Time
			Status    bool
			DiffCol   string
		}
		var results []tempTbl
		for rows.Next() {
			tbl := tempTbl{}
			result := []interface{}{
				&tbl.Id,
				&tbl.Name,
				&tbl.CreatedAt,
				&tbl.Status,
				&tbl.DiffCol,
			}
			if err := rows.Scan(result...); err != nil {
				t.Fatal(err)
			}
			results = append(results, tbl)
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		actual = results
		expected = []tempTbl{
			{Id: 1, Name: "test1", CreatedAt: nil, Status: true, DiffCol: "col1"},
			{Id: 2, Name: "test2", CreatedAt: nil, Status: false, DiffCol: "col2"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for multiple calls
	func() {
		type TestTable struct {
			Id int64
		}
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTable(&TestTable{}); err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		actual := n
		expected := int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
		if err := db.CreateTable(&TestTable{}); err == nil {
			t.Errorf("Expects error, but nil")
		}
	}()
}

func TestDB_CreateTableIfNotExists(t *testing.T) {
	func() {
		type TestTable struct {
			Id        int64 `db:"pk"`
			Name      string
			CreatedAt time.Time
			Status    bool   `db:"notnull" column:"status" default:"true"`
			DiffCol   string `column:"col"`
			Ignore    string `db:"-"`
		}
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTableIfNotExists(TestTable{}); err != nil {
			t.Fatal(err)
		}
		var sql string
		if err := db.db.QueryRow(`SELECT sql FROM sqlite_master`).Scan(&sql); err != nil {
			t.Fatal(err)
		}
		actual := sql
		expected := `CREATE TABLE "test_table" (` +
			`"id" integer PRIMARY KEY AUTOINCREMENT, ` +
			`"name" text, ` +
			`"created_at" datetime, ` +
			`"status" boolean NOT NULL DEFAULT 1, ` +
			`"col" text)`
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()

	// test for multiple calls
	func() {
		type TestTable struct {
			Id int64
		}
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTableIfNotExists(TestTable{}); err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		actual := n
		expected := int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
		if err := db.CreateTableIfNotExists(TestTable{}); err != nil {
			t.Fatal(err)
		}
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		actual = n
		expected = int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T, but %[2]v(type %[2]T)", expected, actual)
		}
	}()
}

func TestDB_DropTable(t *testing.T) {
	type TestTable struct {
		Id int64
	}
	db, err := New(&SQLite3Dialect{}, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`CREATE TABLE test_table (id integer)`,
		`CREATE TABLE test_table2 (id integer)`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	query := `SELECT COUNT(*) FROM sqlite_master`
	var n int
	if err := db.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	actual := n
	expected := 2
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %v, but %v", expected, actual)
	}
	if err := db.DropTable(&TestTable{}); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	actual = n
	expected = 1
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %v, but %v", expected, actual)
	}
	query = `SELECT COUNT(*) FROM sqlite_master WHERE type = "table" AND tbl_name <> "test_table"`
	if err := db.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	actual = n
	expected = 1
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %v, but %v", expected, actual)
	}
}

func TestDB_Update(t *testing.T) {
	func() {
		type TestTable struct {
			Id     int64 `db:"pk"`
			Name   string
			Active bool
		}
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`CREATE TABLE test_table (
				id integer PRIMARY KEY,
				name text,
				active boolean
			);`,
			`INSERT INTO test_table (id, name, active) VALUES (1, "test1", 1);`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(err)
			}
		}
		obj := &TestTable{
			Id:     1,
			Name:   "updated",
			Active: false,
		}
		n, err := db.Update(obj)
		if err != nil {
			t.Fatal(err)
		}
		var actual interface{} = n
		var expected interface{} = int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
		rows := db.db.QueryRow(`SELECT * FROM test_table`)
		var (
			id     int
			name   string
			active bool
		)
		if err := rows.Scan(&id, &name, &active); err != nil {
			t.Fatal(err)
		}
		actual = []interface{}{id, name, active}
		expected = []interface{}{1, "updated", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()
}

func TestDB_Update_withTransaction(t *testing.T) {
	dbName := "go_test.db"
	dir, err := ioutil.TempDir("", "TestDB_Update_withTransaction")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, dbName)
	defer os.RemoveAll(dir)
	db1, err := New(&SQLite3Dialect{}, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db2, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	type TestTable struct {
		Id   int64 `db:"pk"`
		Name string
	}
	for _, query := range []string{
		`CREATE TABLE test_table (id integer primary key, name text)`,
		`INSERT INTO test_table VALUES (1, "test")`,
	} {
		if _, err := db1.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if err := db1.Begin(); err != nil {
		t.Fatal(err)
	}
	obj := &TestTable{Id: 1, Name: "updated"}
	affected, err := db1.Update(obj)
	if err != nil {
		t.Fatal(err)
	}
	var actual interface{} = affected
	var expected interface{} = int64(1)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %v, but %v", expected, actual)
	}
	var id int64
	var name string
	if err := db2.QueryRow(`SELECT * FROM test_table`).Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	actual = []interface{}{id, name}
	expected = []interface{}{int64(1), "test"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %#v, but %#v", expected, actual)
	}
	if err := db1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db2.QueryRow(`SELECT * FROM test_table`).Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	actual = []interface{}{id, name}
	expected = []interface{}{int64(1), "updated"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %#v, but %#v", expected, actual)
	}
}

func TestDB_Insert(t *testing.T) {
	type TestTable struct {
		Id   int64 `db:"pk"`
		Name string
	}

	// test for single.
	func() {
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`CREATE TABLE test_table (
			id integer primary key,
			name text
		)`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(err)
			}
		}
		obj := &TestTable{Id: 100, Name: "test1"}
		n, err := db.Insert(obj)
		if err != nil {
			t.Fatal(err)
		}
		var actual interface{} = n
		var expected interface{} = int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
		var id int64
		var name string
		if err := db.db.QueryRow(`SELECT * FROM test_table`).Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		actual = []interface{}{id, name}
		expected = []interface{}{int64(1), "test1"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for multiple.
	func() {
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`CREATE TABLE test_table (
			id integer primary key,
			name text
		)`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(err)
			}
		}
		objs := []TestTable{
			{Id: 200, Name: "test2"},
			{Id: 1, Name: "test3"},
		}
		n, err := db.Insert(objs)
		if err != nil {
			t.Fatal(err)
		}
		var actual interface{} = n
		var expected interface{} = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
		rows, err := db.db.Query(`SELECT * FROM test_table`)
		if err != nil {
			t.Fatal(err)
		}
		expects := [][]interface{}{
			{int64(1), "test2"},
			{int64(2), "test3"},
		}
		for i := 0; rows.Next(); i++ {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatal(err)
			}
			actual = []interface{}{id, name}
			expected = expects[i]
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %v, but %v", expected, actual)
			}
		}
	}()
}

func TestDB_Delete(t *testing.T) {
	type TestTable struct {
		Id   int64 `db:"pk"`
		Name string
	}

	// test for single.
	func() {
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`CREATE TABLE test_table (
			id integer primary key,
			name text
		)`,
			`INSERT INTO test_table (id, name) VALUES (1, "test1")`,
			`INSERT INTO test_table (id, name) VALUES (2, "test2")`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(err)
			}
		}
		obj := &TestTable{Id: 1}
		n, err := db.Delete(obj)
		if err != nil {
			t.Fatal(err)
		}
		var actual interface{} = n
		var expected interface{} = int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
		rows, err := db.db.Query(`SELECT * FROM test_table`)
		if err != nil {
			t.Fatal(err)
		}
		var id int64
		var name string
		expects := [][]interface{}{
			{int64(2), "test2"},
		}
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatal(err)
			}
			actual = []interface{}{id, name}
			expected = expects[i]
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %v, but %v", expected, actual)
			}
		}
	}()

	// test for multiple.
	func() {
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`CREATE TABLE test_table (
				id integer primary key,
				name text
			)`,
			`INSERT INTO test_table (id, name) VALUES (1, "test1")`,
			`INSERT INTO test_table (id, name) VALUES (2, "test2")`,
			`INSERT INTO test_table (id, name) VALUES (3, "test3")`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(err)
			}
		}
		n, err := db.Delete([]TestTable{{Id: 1}, {Id: 3}})
		if err != nil {
			t.Fatal(err)
		}
		var actual interface{} = n
		var expected interface{} = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
		rows, err := db.db.Query(`SELECT * FROM test_table`)
		if err != nil {
			t.Fatal(err)
		}
		expects := [][]interface{}{
			{int64(2), "test2"},
		}
		for i := 0; rows.Next(); i++ {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatal(err)
			}
			actual = []interface{}{id, name}
			expected = expects[i]
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %v, but %v", expected, actual)
			}
		}
	}()
}

func TestDB_SetLogOutput(t *testing.T) {
	type TestTable struct {
		Id   int64 `db:"pk"`
		Name string
	}

	db, err := New(&SQLite3Dialect{}, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec(`CREATE TABLE test_table (id integer PRIMARY KEY AUTOINCREMENT, name text)`); err != nil {
		t.Fatal(err)
	}
	// test for update-type query.
	var buf bytes.Buffer
	db.SetLogOutput(&buf)
	nowTime := time.Now()
	now = func() time.Time { return nowTime }
	defer func() { now = time.Now }()
	timeFormat := nowTime.Format("2006-01-02 15:04:05")
	obj := &TestTable{Name: "test"}
	if _, err := db.Insert(obj); err != nil {
		t.Error(err)
	}
	actual := buf.String()
	expected := fmt.Sprintf(`[%s] [0.00ms] INSERT INTO "test_table" ("name") VALUES (?); ["test"]`+"\n", timeFormat)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}

	// test for select-type query.
	buf.Reset()
	var out []TestTable
	if err := db.Select(&out); err != nil {
		t.Error(err)
	}
	actual = buf.String()
	expected = fmt.Sprintf(`[%s] [0.00ms] SELECT "test_table".* FROM "test_table";`+"\n", timeFormat)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}

	// test for unset.
	buf.Reset()
	db.SetLogOutput(nil)
	if err := db.Select(&out); err != nil {
		t.Error(err)
	}
	actual = buf.String()
	expected = ""
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestDB_SetLogFormat(t *testing.T) {
	type TestTable struct {
		Id   int64 `db:"pk"`
		Name string
	}

	func() {
		db, err := New(&SQLite3Dialect{}, ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.db.Exec(`CREATE TABLE test_table (id integer PRIMARY KEY AUTOINCREMENT, name text)`); err != nil {
			t.Fatal(err)
		}
		// test for update-type query.
		var buf bytes.Buffer
		db.SetLogOutput(&buf)
		if err := db.SetLogFormat(`[{{.query}}] in {{.duration}}. ({{.time.Format "2006/01/02 15:04:05"}})`); err != nil {
			t.Fatal(err)
		}
		nowTime := time.Now()
		now = func() time.Time { return nowTime }
		defer func() { now = time.Now }()
		timeFormat := nowTime.Format("2006/01/02 15:04:05")
		obj := &TestTable{Name: "test"}
		if _, err := db.Insert(obj); err != nil {
			t.Error(err)
		}
		actual := buf.String()
		expected := fmt.Sprintf(`[INSERT INTO "test_table" ("name") VALUES (?); ["test"]] in 0.00ms. (%s)`+"\n", timeFormat)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}

		// test for select-type query.
		buf.Reset()
		var out []TestTable
		if err := db.Select(&out); err != nil {
			t.Error(err)
		}
		actual = buf.String()
		expected = fmt.Sprintf(`[SELECT "test_table".* FROM "test_table";] in 0.00ms. (%s)`+"\n", timeFormat)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()
}
