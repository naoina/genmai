package genmai

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

type testModelDifferentTableName struct {
	Id   int64 `db:"pk"`
	Name string
	Addr string
}

func (t *testModelDifferentTableName) TableName() string {
	return "diff_table"
}

type M2 struct {
	Id   int64
	Body string
}

type joinTestModel struct {
	Id       int64
	PersonId int64
	AddrId   int64
}

type jTModelM2Rel struct {
	Id          int64
	TestModelId int64
	M2Id        int64
}

type joinPerson struct {
	Id   int64
	Name string
	Age  int64
}

type joinAddr struct {
	Id     int64
	Addr   string
	Nation string
}

type TestModelForHook struct {
	Id        int64 `db:"pk"`
	Name      string
	beforeErr error
	afterErr  error
	called    []string
}

func (t *TestModelForHook) BeforeUpdate() error {
	t.called = append(t.called, "BeforeUpdate")
	return t.beforeErr
}

func (t *TestModelForHook) AfterUpdate() error {
	t.called = append(t.called, "AfterUpdate")
	return t.afterErr
}

func (t *TestModelForHook) BeforeInsert() error {
	t.called = append(t.called, "BeforeInsert")
	return t.beforeErr
}

func (t *TestModelForHook) AfterInsert() error {
	t.called = append(t.called, "AfterInsert")
	return t.afterErr
}

func (t *TestModelForHook) BeforeDelete() error {
	t.called = append(t.called, "BeforeDelete")
	return t.beforeErr
}

func (t *TestModelForHook) AfterDelete() error {
	t.called = append(t.called, "AfterDelete")
	return t.afterErr
}

type testEmbeddedModelForHook struct {
	called []string

	TestModelForHook
}

func (t *testEmbeddedModelForHook) BeforeUpdate() error {
	t.called = append(append(t.called, t.TestModelForHook.called...), "embedded: BeforeUpdate")
	return nil
}

func (t *testEmbeddedModelForHook) AfterUpdate() error {
	t.called = append(append(t.called, t.TestModelForHook.called...), "embedded: AfterUpdate")
	return nil
}

func (t *testEmbeddedModelForHook) BeforeInsert() error {
	t.called = append(append(t.called, t.TestModelForHook.called...), "embedded: BeforeInsert")
	return nil
}

func (t *testEmbeddedModelForHook) AfterInsert() error {
	t.called = append(append(t.called, t.TestModelForHook.called...), "embedded: AfterInsert")
	return nil
}

func (t *testEmbeddedModelForHook) BeforeDelete() error {
	t.called = append(append(t.called, t.TestModelForHook.called...), "embedded: BeforeDelete")
	return nil
}

func (t *testEmbeddedModelForHook) AfterDelete() error {
	t.called = append(append(t.called, t.TestModelForHook.called...), "embedded: AfterDelete")
	return nil
}

func testDB(dsn ...string) (*DB, error) {
	switch os.Getenv("DB") {
	case "mysql":
		return New(&MySQLDialect{}, "travis@/genmai_test")
	case "postgres":
		return New(&PostgresDialect{}, "user=postgres dbname=genmai_test sslmode=disable")
	default:
		var DSN string
		switch len(dsn) {
		case 0:
			DSN = ":memory:"
		case 1:
			DSN = dsn[0]
		default:
			panic(fmt.Errorf("too many arguments"))
		}
		return New(&SQLite3Dialect{}, DSN)
	}
}

func newTestDB(t *testing.T) *DB {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_model`,
		createTableString("test_model", "name text not null", "addr text not null"),
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
		createTableString("m2", "body text not null"),
		`INSERT INTO m2 (id, body) VALUES (1, 'a1');`,
		`INSERT INTO m2 (id, body) VALUES (2, 'b2');`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	return db
}

func newDifferentTestDB(t *testing.T) *DB {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS diff_table`,
		createTableString("diff_table", "name text not null", "addr text not null"),
		`INSERT INTO diff_table (id, name, addr) VALUES (1, 'diff_test1', 'diff_addr1');`,
		`INSERT INTO diff_table (id, name, addr) VALUES (2, 'diff_test2', 'diff_addr2');`,
		`INSERT INTO diff_table (id, name, addr) VALUES (3, 'diff_test3', 'diff_addr3');`,
		`INSERT INTO diff_table (id, name, addr) VALUES (4, 'diff_other', 'diff_addr4');`,
		`INSERT INTO diff_table (id, name, addr) VALUES (5, 'diff_other', 'diff_addr5');`,
		`INSERT INTO diff_table (id, name, addr) VALUES (6, 'diff_dup', 'diff_dup_addr');`,
		`INSERT INTO diff_table (id, name, addr) VALUES (7, 'diff_dup', 'diff_dup_addr');`,
		`INSERT INTO diff_table (id, name, addr) VALUES (8, 'diff_other1', 'diff_addr8');`,
		`INSERT INTO diff_table (id, name, addr) VALUES (9, 'diff_other2', 'diff_addr9');`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	return db
}

func multiJoinTestDB(t *testing.T) *DB {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS join_person`,
		createTableString("join_person", "name text not null", "age integer not null"),
		`INSERT INTO join_person (id, name, age) VALUES (1, 'Taro', 25);`,
		`INSERT INTO join_person (id, name, age) VALUES (2, 'Tama', 2);`,
		`INSERT INTO join_person (id, name, age) VALUES (3, 'Mike', 34);`,
		`INSERT INTO join_person (id, name, age) VALUES (4, 'Hanako', 24);`,
		`DROP TABLE IF EXISTS join_addr`,
		createTableString("join_addr", "addr text not null", "nation varchar(255) not null"),
		`INSERT INTO join_addr (id, addr, nation) VALUES (1, 'Tokyo', 'Japan');`,
		`INSERT INTO join_addr (id, addr, nation) VALUES (2, 'Frisco', 'US');`,
		`DROP TABLE IF EXISTS join_test_model`,
		createTableString("join_test_model", "person_id integer not null", "addr_id integer not null"),
		`INSERT INTO join_test_model (id, person_id, addr_id) VALUES (1, 1, 1);`,
		`INSERT INTO join_test_model (id, person_id, addr_id) VALUES (2, 2, 1);`,
		`INSERT INTO join_test_model (id, person_id, addr_id) VALUES (3, 3, 2);`,
		`INSERT INTO join_test_model (id, person_id, addr_id) VALUES (4, 4, 1);`,
		`DROP TABLE IF EXISTS m2`,
		createTableString("m2", "body text not null"),
		`INSERT INTO m2 (id, body) VALUES (1, 'a1');`,
		`INSERT INTO m2 (id, body) VALUES (2, 'b2');`,
		`DROP TABLE IF EXISTS j_t_model_m2_rel`,
		createTableString("j_t_model_m2_rel", "j_t_model_id integer not null", "m2_id integer not null"),
		`INSERT INTO j_t_model_m2_rel (id, j_t_model_id, m2_id) VALUES (1, 1, 1);`,
		`INSERT INTO j_t_model_m2_rel (id, j_t_model_id, m2_id) VALUES (2, 2, 1);`,
		`INSERT INTO j_t_model_m2_rel (id, j_t_model_id, m2_id) VALUES (3, 3, 1);`,
		`INSERT INTO j_t_model_m2_rel (id, j_t_model_id, m2_id) VALUES (4, 1, 2);`,
		`INSERT INTO j_t_model_m2_rel (id, j_t_model_id, m2_id) VALUES (5, 2, 2);`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	return db
}

func createTableString(name string, fields ...string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s, %s)`, name, idFieldStr(), strings.Join(fields, ","))
}

func createTableStringForStringPk(name string, fields ...string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s, %s)`, name, idFieldStrForStringPk(), strings.Join(fields, ","))
}

func boolStr(b bool) string {
	switch os.Getenv("DB") {
	case "mysql":
		if b {
			return "true"
		} else {
			return "false"
		}
	case "postgres":
		if b {
			return "true"
		} else {
			return "false"
		}
	default:
		if b {
			return "1"
		} else {
			return "0"
		}
	}
}

func idFieldStr() string {
	switch os.Getenv("DB") {
	case "mysql":
		return "id INTEGER PRIMARY KEY AUTO_INCREMENT"
	case "postgres":
		return "id serial PRIMARY KEY"
	default:
		return "id integer PRIMARY KEY AUTOINCREMENT"
	}
}

func idFieldStrForStringPk() string {
	switch os.Getenv("DB") {
	case "mysql":
		return "id VARCHAR(255) PRIMARY KEY"
	case "postgres":
		return "id VARCHAR(255) PRIMARY KEY"
	default:
		return "id text PRIMARY KEY"
	}
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
	// with transaction.
	func() {
		db := newTestDB(t)
		defer db.Close()
		if err := db.Begin(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Rollback(); err != nil {
				t.Error(err)
			}
		}()
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
	// with pointer of struct.
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []*testModel
		if err := db.Select(&actual); err != nil {
			t.Fatal(err)
		}
		expected := []*testModel{
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
	// with nil pointer.
	func() {
		db := newTestDB(t)
		defer db.Close()
		var input *[]testModel
		actual := db.Select(input)
		expect := fmt.Errorf("Select: nil pointer dereference")
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.Select(%#v) => %#v; want %#v`, input, actual, expect)
		}
	}()
	// with different table name.
	func() {
		db := newDifferentTestDB(t)
		defer db.Close()
		var actual []testModelDifferentTableName
		if err := db.Select(&actual); err != nil {
			t.Fatal(err)
		}
		expected := []testModelDifferentTableName{
			{1, "diff_test1", "diff_addr1"},
			{2, "diff_test2", "diff_addr2"},
			{3, "diff_test3", "diff_addr3"},
			{4, "diff_other", "diff_addr4"},
			{5, "diff_other", "diff_addr5"},
			{6, "diff_dup", "diff_dup_addr"},
			{7, "diff_dup", "diff_dup_addr"},
			{8, "diff_other1", "diff_addr8"},
			{9, "diff_other2", "diff_addr9"},
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

	// SELECT * FROM test_model ORDER BY "name" DESC, "addr" ASC;
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		if err := db.Select(&actual, db.OrderBy("name", ASC, "addr", DESC)); err != nil {
			t.Fatal(err)
		}
		expect := []testModel{
			{6, "dup", "dup_addr"},
			{7, "dup", "dup_addr"},
			{5, "other", "addr5"},
			{4, "other", "addr4"},
			{8, "other1", "addr8"},
			{9, "other2", "addr9"},
			{1, "test1", "addr1"},
			{2, "test2", "addr2"},
			{3, "test3", "addr3"},
		}
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.Select(&actual, DB.OrderBy("name", ASC, "addr", DESC)) => %#v; want %#v`, actual, expect)
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

	// SELECT * FROM test_model WHERE "id" IN (2, 3) (by slice);
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		cond := []int32{2, 3}
		if err := db.Select(&actual, db.Where("id").In(cond)); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{2, "test2", "addr2"}, {3, "test3", "addr3"},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT * FROM test_model WHERE "id" IN (2, 3) (by array);
	func() {
		db := newTestDB(t)
		defer db.Close()
		var actual []testModel
		cond := [2]int32{2, 3}
		if err := db.Select(&actual, db.Where("id").In(cond)); err != nil {
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
	// with pointer of pointer.
	func() {
		db := newTestDB(t)
		defer db.Close()
		var cnt int64
		actual := &cnt
		if err := db.Select(&actual, db.Count(), db.From(testModel{})); err != nil {
			t.Fatal(err)
		}
		expected := int64(9)
		if !reflect.DeepEqual(*actual, expected) {
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

	// SELECT "join_test_model".* FROM "join_test_model" JOIN "join_addr" ON "join_test_model"."addr_id" = "join_addr"."id" JOIN "join_person" ON "join_test_model"."person_id" = "join_person"."id" WHERE "join_addr"."nation" = "Japan" AND "join_person"."age" > 20?;
	func() {
		db := multiJoinTestDB(t)
		defer db.Close()
		var actual []joinTestModel
		t2 := &joinAddr{}
		t3 := &joinPerson{}
		if err := db.Select(&actual, db.Join(t2).On("addr_id", "=", "id"), db.Join(t3).On("person_id", "=", "id"), db.Where(t2, "nation", "=", "Japan").And(t3, "age", ">", 20)); err != nil {
			t.Fatal(err)
		}
		expected := []joinTestModel{
			{1, 1, 1},
			{4, 4, 1},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "join_test_model".* FROM "join_test_model" JOIN "join_addr" ON "join_test_model"."addr_id" = "join_addr"."id" JOIN "join_person" ON "join_test_model"."person_id" = "join_person"."id" WHERE "join_addr"."nation" = "Japan" ORDER BY "join_person"."id" DESC;
	func() {
		db := multiJoinTestDB(t)
		defer db.Close()
		var actual []joinTestModel
		t2 := &joinAddr{}
		t3 := &joinPerson{}
		if err := db.Select(&actual, db.Join(t2).On("addr_id", "=", "id"), db.Join(t3).On("person_id", "=", "id"), db.Where(t2, "nation", "=", "Japan").OrderBy(&t3, "id", DESC)); err != nil {
			t.Fatal(err)
		}
		expected := []joinTestModel{
			{4, 4, 1},
			{2, 2, 1},
			{1, 1, 1},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// SELECT "join_test_model".* FROM "join_test_model" JOIN "j_t_model_m2_rel" ON "join_test_model"."id" = "j_t_model_m2_rel"."j_t_model_id" JOIN "m2" ON "j_t_model_m2_rel"."m2_id" = "m2"."id" WHERE "m2"."id" = 2;
	func() {
		db := multiJoinTestDB(t)
		defer db.Close()
		var actual []joinTestModel
		t2 := &M2{}
		tr := &jTModelM2Rel{}
		if err := db.Select(&actual, db.Join(tr).On("id", "=", "j_t_model_id"), db.Join(t2).On(tr, "m2_id", "=", "id"), db.Where(t2, "id", "=", 2)); err != nil {
			t.Fatal(err)
		}
		expected := []joinTestModel{
			{1, 1, 1},
			{2, 2, 1},
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
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		`CREATE TABLE test_table (tbl_id integer)`,
		`INSERT INTO test_table VALUES (1)`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
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

func TestDB_Select_embeddedStruct(t *testing.T) {
	type A struct {
		Name   string
		ignore bool
	}
	type B struct {
		Id int64
		A
	}
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS b`,
		createTableString("b", "name varchar(255)"),
		`INSERT INTO b (id, name) VALUES (1, 'test1')`,
		`INSERT INTO b (id, name) VALUES (2, 'test2')`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}

	var results []B
	if err := db.Select(&results); err != nil {
		t.Fatal(err)
	}
	actual := results
	expected := []B{{Id: 1, A: A{Name: "test1"}}, {Id: 2, A: A{Name: "test2"}}}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestDB_CreateTable(t *testing.T) {
	func() {
		type TestTable struct {
			Id         int64 `db:"pk"`
			Name       string
			CreatedAt  *time.Time
			Status     bool   `column:"status" default:"true"`
			DiffCol    string `column:"col"`
			Ignore     string `db:"-"`
			unexported string
		}
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.db.Exec(`DROP TABLE IF EXISTS test_table`); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTable(TestTable{}); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`INSERT INTO test_table (id, name, col) VALUES (1, 'test1', 'col1');`,
			fmt.Sprintf(`INSERT INTO test_table (id, name, status, col) VALUES (2, 'test2', %s, 'col2');`, boolStr(false)),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
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

	// test for different table name.
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.db.Exec(`DROP TABLE IF EXISTS diff_table`); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTable(testModelDifferentTableName{}); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`INSERT INTO diff_table (id, name, addr) VALUES (1, 'diff_test1', 'diff_addr1');`,
			`INSERT INTO diff_table (id, name, addr) VALUES (2, 'diff_test2', 'diff_addr2');`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		stmt, err := db.db.Prepare(`SELECT * FROM diff_table`)
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
		var expected interface{} = 3
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
		type tempTbl struct {
			Id   int64
			Name string
			Addr string
		}
		var results []tempTbl
		for rows.Next() {
			tbl := tempTbl{}
			if err := rows.Scan(&tbl.Id, &tbl.Name, &tbl.Addr); err != nil {
				t.Fatal(err)
			}
			results = append(results, tbl)
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		actual = results
		expected = []tempTbl{
			{Id: 1, Name: "diff_test1", Addr: "diff_addr1"},
			{Id: 2, Name: "diff_test2", Addr: "diff_addr2"},
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
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.db.Exec(`DROP TABLE IF EXISTS test_table`); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTable(&TestTable{}); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTable(&TestTable{}); err == nil {
			t.Errorf("Expects error, but nil")
		}
	}()

	// test for embedded struct.
	func() {
		type A struct {
			Name   string
			ignore bool
		}

		type B struct {
			Id int64
			A
		}
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS a`,
			`DROP TABLE IF EXISTS b`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		if err := db.CreateTable(&B{}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.db.Exec(`INSERT INTO b (id, name) VALUES (1, 'test1')`); err != nil {
			t.Fatal(err)
		}
	}()
}

func TestDB_CreateTableIfNotExists(t *testing.T) {
	func() {
		type TestTable struct {
			Id         int64 `db:"pk"`
			Name       string
			CreatedAt  *time.Time
			Status     bool   `column:"status" default:"true"`
			DiffCol    string `column:"col"`
			Ignore     string `db:"-"`
			unexported bool
		}
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.db.Exec(`DROP TABLE IF EXISTS test_table`); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTableIfNotExists(TestTable{}); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`INSERT INTO test_table (id, name, col) VALUES (1, 'test1', 'col1');`,
			fmt.Sprintf(`INSERT INTO test_table (id, name, status, col) VALUES (2, 'test2', %s, 'col2');`, boolStr(false)),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
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
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTableIfNotExists(TestTable{}); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTableIfNotExists(TestTable{}); err != nil {
			t.Fatal(err)
		}
	}()

	// test for embedded struct.
	func() {
		type A struct {
			Name   string
			ignore bool
		}

		type B struct {
			Id int64
			A
		}
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS a`,
			`DROP TABLE IF EXISTS b`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		if err := db.CreateTableIfNotExists(&B{}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.db.Exec(`INSERT INTO b (id, name) VALUES (1, 'test1')`); err != nil {
			t.Fatal(err)
		}
	}()
}

func TestDB_DropTable(t *testing.T) {
	type TestTable struct {
		Id int64
	}
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		`DROP TABLE IF EXISTS test_table2`,
		`CREATE TABLE test_table (id integer)`,
		`CREATE TABLE test_table2 (id integer)`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	query := `SELECT COUNT(*) FROM test_table`
	var n int
	if err := db.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err := db.DropTable(&TestTable{}); err != nil {
		t.Error(err)
	}
	if err := db.db.QueryRow(query).Scan(&n); err == nil {
		t.Errorf("no error occurred")
	}
	query = `SELECT COUNT(*) FROM test_table2`
	if err := db.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
}

func TestDB_DropTable_withDifferentTableName(t *testing.T) {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS diff_table`,
		`DROP TABLE IF EXISTS test_model_different_table`,
		`CREATE TABLE diff_table (id integer)`,
		`CREATE TABLE test_model_different_table (id integer)`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	query := `SELECT COUNT(*) FROM diff_table`
	var n int
	if err := db.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err := db.DropTable(testModelDifferentTableName{}); err != nil {
		t.Error(err)
	}
	if err := db.db.QueryRow(query).Scan(&n); err == nil {
		t.Errorf("no error occurred")
	}
	query = `SELECT COUNT(*) FROM test_model_different_table`
	if err := db.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
}

func TestDB_CreateIndex(t *testing.T) {
	type TestTable struct {
		Id   int64
		Name string
	}
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		createTableString("test_table", "name varchar(255)"),
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}

	// test for single.
	func() {
		if err := db.CreateIndex(&TestTable{}, "id"); err != nil {
			t.Fatal(err)
		}
		var query string
		if os.Getenv("DB") == "mysql" {
			query = "DROP INDEX index_test_table_id ON test_table"
		} else {
			query = "DROP INDEX index_test_table_id"
		}
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}()

	// test for multiple.
	func() {
		if err := db.CreateIndex(&TestTable{}, "id", "name"); err != nil {
			t.Fatal(err)
		}
		var query string
		if os.Getenv("DB") == "mysql" {
			query = "DROP INDEX index_test_table_id_name ON test_table"
		} else {
			query = "DROP INDEX index_test_table_id_name"
		}
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}()
}

func TestDB_CreateUniqueIndex(t *testing.T) {
	type TestTable struct {
		Id   int64
		Name string
	}
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		createTableString("test_table", "name varchar(255)"),
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}

	// test for single.
	func() {
		if err := db.CreateUniqueIndex(&TestTable{}, "id"); err != nil {
			t.Fatal(err)
		}
		var query string
		if os.Getenv("DB") == "mysql" {
			query = "DROP INDEX index_test_table_id ON test_table"
		} else {
			query = "DROP INDEX index_test_table_id"
		}
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}()

	// test for multiple.
	func() {
		if err := db.CreateUniqueIndex(&TestTable{}, "id", "name"); err != nil {
			t.Fatal(err)
		}
		var query string
		if os.Getenv("DB") == "mysql" {
			query = "DROP INDEX index_test_table_id_name ON test_table"
		} else {
			query = "DROP INDEX index_test_table_id_name"
		}
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}()

	// test for uniqueness.
	func() {
		defer func() {
			var query string
			if os.Getenv("DB") == "mysql" {
				query = "DROP INDEX index_test_table_name ON test_table"
			} else {
				query = "DROP INDEX index_test_table_name"
			}
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}()
		if err := db.CreateUniqueIndex(&TestTable{}, "name"); err != nil {
			t.Fatal(err)
		}
		query := `INSERT INTO test_table (name) VALUES ('test1')`
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
		if _, err := db.db.Exec(query); err == nil {
			t.Errorf("no error occurred")
		}
	}()
}

func TestDB_Update(t *testing.T) {
	func() {
		type TestTable struct {
			Id         int64 `db:"pk"`
			Name       string
			Active     bool
			unexported bool
		}
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table`,
			createTableString("test_table", "name text", "active boolean"),
			fmt.Sprintf(`INSERT INTO test_table (id, name, active) VALUES (1, 'test1', %s);`, boolStr(true)),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
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

func TestDB_Update_withDifferentTableName(t *testing.T) {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS diff_table`,
		createTableString("diff_table", "name text", "addr text"),
		`INSERT INTO diff_table (id, name, addr) VALUES (1, 'diff_test1', 'diff_addr1');`,
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	obj := &testModelDifferentTableName{
		Id:   1,
		Name: "diff_test2",
		Addr: "diff_addr2",
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
	rows := db.db.QueryRow(`SELECT * FROM diff_table`)
	var (
		id   int
		name string
		addr string
	)
	if err := rows.Scan(&id, &name, &addr); err != nil {
		t.Fatal(err)
	}
	actual = []interface{}{id, name, addr}
	expected = []interface{}{1, "diff_test2", "diff_addr2"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestDB_Update_withColumnTag(t *testing.T) {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		createTableString("test_table", "mailaddress text"),
		fmt.Sprintf(`INSERT INTO test_table (id, mailaddress) VALUES (1, 'naoina@example.com');`),
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	type TestTable struct {
		Id       int64  `db:"pk"`
		MailAddr string `column:"mailaddress"`
	}
	obj := &TestTable{
		Id:       1,
		MailAddr: "naoina@kuune.org",
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
		id       int
		mailaddr string
	)
	if err := rows.Scan(&id, &mailaddr); err != nil {
		t.Fatal(err)
	}
	actual = []interface{}{id, mailaddr}
	expected = []interface{}{1, "naoina@kuune.org"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestDB_Update_withTransaction(t *testing.T) {
	dbName := "go_test.db"
	dir, err := ioutil.TempDir("", "TestDB_Update_withTransaction")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, dbName)
	defer os.RemoveAll(dir)
	db1, err := testDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	dtmp, err := testDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db2 := dtmp.db
	type TestTable struct {
		Id   int64 `db:"pk"`
		Name string
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		createTableString("test_table", "name text"),
		`INSERT INTO test_table VALUES (1, 'test')`,
	} {
		if _, err := db1.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
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

func TestDB_Update_hook(t *testing.T) {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	initDB := func() {
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_model_for_hook;`,
			createTableString("test_model_for_hook", "name text"),
			`INSERT INTO test_model_for_hook (id, name) VALUES (1, 'alice');`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
	}

	// test for no error.
	func() {
		initDB()
		obj := &TestModelForHook{Id: 1, Name: "bob", beforeErr: nil, afterErr: nil}
		if _, err := db.Update(obj); err != nil {
			t.Error(err)
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeUpdate", "AfterUpdate"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var name string
		if err := db.db.QueryRow(`SELECT name FROM test_model_for_hook`).Scan(&name); err != nil {
			t.Fatal(err)
		}
		actual = name
		expected = "bob"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()

	// test for error in Before.
	func() {
		initDB()
		obj := &TestModelForHook{Id: 1, Name: "bob", beforeErr: fmt.Errorf("expected before error"), afterErr: nil}
		if _, err := db.Update(obj); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeUpdate"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var name string
		if err := db.db.QueryRow(`SELECT name FROM test_model_for_hook`).Scan(&name); err != nil {
			t.Error(err)
		}
		actual = name
		expected = "alice"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()

	// test for error in After.
	func() {
		initDB()
		obj := &TestModelForHook{Id: 1, Name: "bob", beforeErr: nil, afterErr: fmt.Errorf("expected after error")}
		if _, err := db.Update(obj); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeUpdate", "AfterUpdate"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var name string
		if err := db.db.QueryRow(`SELECT name FROM test_model_for_hook`).Scan(&name); err != nil {
			t.Error(err)
		}
		actual = name
		expected = "bob"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()
}

func TestDB_Insert(t *testing.T) {
	type TestTable struct {
		Id         int64 `db:"pk"`
		Name       string
		unexported bool
	}
	type TestTableStringPk struct {
		Id   string `db:"pk"`
		Name string
	}

	// test for single.
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table`,
			createTableString("test_table", "name text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
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
		expected = []interface{}{obj.Id, obj.Name}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for different table name.
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS diff_table`,
			createTableString("diff_table", "name text", "addr text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		obj := &testModelDifferentTableName{
			Id:   100,
			Name: "diff_test1",
			Addr: "diff_addr1",
		}
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
		var addr string
		if err := db.db.QueryRow(`SELECT * FROM diff_table`).Scan(&id, &name, &addr); err != nil {
			t.Fatal(err)
		}
		actual = []interface{}{id, name, addr}
		expected = []interface{}{obj.Id, obj.Name, obj.Addr}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for indirect.
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table`,
			createTableString("test_table", "name text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		obj := TestTable{Id: 100, Name: "test1"}
		_, err = db.Insert(obj)
		if err == nil {
			t.Errorf("DB.Insert(%#v) => _, nil, want error", obj)
		}
	}()

	// test for multiple.
	testCaseMultiple := func(objs interface{}) {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table`,
			createTableString("test_table", "name text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
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
		objValues := reflect.ValueOf(objs)
		for i := 0; i < objValues.Len(); i++ {
			var id int64
			if table, ok := reflect.Indirect(objValues.Index(i)).Interface().(TestTable); ok {
				id = table.Id
			} else {
				table := reflect.Indirect(objValues.Index(i)).Interface().(*TestTable)
				id = table.Id
			}
			actual := id
			expect := int64(200)
			if !reflect.DeepEqual(actual, expect) {
				t.Errorf(`DB.Insert(%#v); obj.Id => (%[2]T=%#[2]v); want (%[3]T=%#[3]v)`, objs, actual, expect)
			}
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
	}
	testCaseMultiple([]TestTable{
		{Id: 200, Name: "test2"},
		{Id: 200, Name: "test3"},
	})
	testCaseMultiple([]*TestTable{
		{Id: 200, Name: "test2"},
		{Id: 200, Name: "test3"},
	})
	testCaseMultiple([]interface{}{
		TestTable{Id: 200, Name: "test2"},
		TestTable{Id: 200, Name: "test3"},
	})
	testCaseMultiple([]interface{}{
		&TestTable{Id: 200, Name: "test2"},
		&TestTable{Id: 200, Name: "test3"},
	})

	// test for case that primary key is string.
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table_string_pk`,
			createTableStringForStringPk("test_table_string_pk", "name text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		obj := &TestTableStringPk{Id: "stringkey", Name: "test1"}
		_, err = db.Insert(obj)
		if err != nil {
			t.Fatal(err)
		}
		var id string
		var name string
		if err := db.db.QueryRow(`SELECT * FROM test_table_string_pk`).Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		actual := []interface{}{id, name}
		expected := []interface{}{"stringkey", "test1"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()
}

func TestDB_Insert_withColumnTab(t *testing.T) {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		createTableString("test_table", "mailaddress text"),
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	type TestTable struct {
		Id       int64  `db:"pk"`
		MailAddr string `column:"mailaddress"`
	}
	obj := &TestTable{Id: 100, MailAddr: "naoina@kuune.org"}
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
	var mailaddr string
	if err := db.db.QueryRow(`SELECT * FROM test_table`).Scan(&id, &mailaddr); err != nil {
		t.Fatal(err)
	}
	actual = []interface{}{id, mailaddr}
	expected = []interface{}{obj.Id, obj.MailAddr}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %v, but %v", expected, actual)
	}
}

func TestDB_LastInsertId(t *testing.T) {
	type TestTable struct {
		ID   int64
		Name string
	}
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		createTableString("test_table", "name text"),
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	for i := 1; i <= 3; i++ {
		if _, err := db.db.Exec(`INSERT INTO test_table (name) VALUES ('naoina')`); err != nil {
			t.Fatal(err)
		}
		id, err := db.LastInsertId()
		if err != nil {
			t.Error(err)
			continue
		}
		actual := id
		expect := int64(i)
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.LastInsertId() => (%[1]T=%#[1]v), nil; want (%[2]T=%#[2]v), nil`, actual, expect)
		}
	}
}

func TestDB_LastInsertId_withTransaction(t *testing.T) {
	type TestTable struct {
		ID   int64
		Name string
	}
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		createTableString("test_table", "name text"),
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
	}
	for i := 1; i <= 3; i++ {
		if _, err := db.db.Exec(`INSERT INTO test_table (name) VALUES ('naoina')`); err != nil {
			t.Fatal(err)
		}
		if err := db.Begin(); err != nil {
			t.Fatal(err)
		}
		id, err := db.LastInsertId()
		if err != nil {
			t.Error(err)
			continue
		}
		if err := db.Rollback(); err != nil {
			t.Fatal(err)
		}
		actual := id
		expect := int64(i)
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.LastInsertId() => (%[1]T=%#[1]v), nil; want (%[2]T=%#[2]v), nil`, actual, expect)
		}
	}
}

func TestDB_Insert_hook(t *testing.T) {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	initDB := func() {
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_model_for_hook`,
			createTableString("test_model_for_hook", "name text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
	}

	// test for no error.
	func() {
		initDB()
		obj := &TestModelForHook{Name: "alice", beforeErr: nil, afterErr: nil}
		if _, err := db.Insert(obj); err != nil {
			t.Error(err)
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeInsert", "AfterInsert"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var name string
		if err := db.db.QueryRow(`SELECT name FROM test_model_for_hook`).Scan(&name); err != nil {
			t.Error(err)
		}
		actual = name
		expected = "alice"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()

	// test for error in Before.
	func() {
		initDB()
		obj := &TestModelForHook{Name: "alice", beforeErr: fmt.Errorf("expected before error"), afterErr: nil}
		if _, err := db.Insert(obj); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeInsert"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(0)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for error in After.
	func() {
		initDB()
		obj := &TestModelForHook{Name: "alice", beforeErr: nil, afterErr: fmt.Errorf("expected after error")}
		if _, err := db.Insert(obj); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeInsert", "AfterInsert"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-insert with no error (direct).
	func() {
		initDB()
		objs := []TestModelForHook{
			{Name: "alice", beforeErr: nil, afterErr: nil},
			{Name: "bob", beforeErr: nil, afterErr: nil},
		}
		if _, err := db.Insert(objs); err != nil {
			t.Fatal(err)
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeInsert", "AfterInsert"}, {"BeforeInsert", "AfterInsert"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-insert with no error (indirect).
	func() {
		initDB()
		objs := []*TestModelForHook{
			{Name: "alice", beforeErr: nil, afterErr: nil},
			{Name: "bob", beforeErr: nil, afterErr: nil},
		}
		if _, err := db.Insert(objs); err != nil {
			t.Fatal(err)
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeInsert", "AfterInsert"}, {"BeforeInsert", "AfterInsert"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-insert with before error (direct).
	func() {
		initDB()
		objs := []TestModelForHook{
			{Name: "alice", beforeErr: nil, afterErr: nil},
			{Name: "bob", beforeErr: fmt.Errorf("expected before error"), afterErr: nil},
		}
		if _, err := db.Insert(objs); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeInsert"}, {"BeforeInsert"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(0)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-insert with before error (indirect).
	func() {
		initDB()
		objs := []*TestModelForHook{
			{Name: "alice", beforeErr: nil, afterErr: nil},
			{Name: "bob", beforeErr: fmt.Errorf("expected before error"), afterErr: nil},
		}
		if _, err := db.Insert(objs); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeInsert"}, {"BeforeInsert"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(0)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-insert with after error (direct).
	func() {
		initDB()
		objs := []TestModelForHook{
			{Name: "alice", beforeErr: nil, afterErr: nil},
			{Name: "bob", beforeErr: nil, afterErr: fmt.Errorf("expected before error")},
		}
		if _, err := db.Insert(objs); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeInsert", "AfterInsert"}, {"BeforeInsert", "AfterInsert"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-insert with after error (indirect).
	func() {
		initDB()
		objs := []*TestModelForHook{
			{Name: "alice", beforeErr: nil, afterErr: nil},
			{Name: "bob", beforeErr: nil, afterErr: fmt.Errorf("expected before error")},
		}
		if _, err := db.Insert(objs); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeInsert", "AfterInsert"}, {"BeforeInsert", "AfterInsert"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()
}

func TestDB_Delete(t *testing.T) {
	type TestTable struct {
		Id         int64 `db:"pk"`
		Name       string
		unexported bool
	}

	// test for single.
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table`,
			createTableString("test_table", "name text"),
			`INSERT INTO test_table (id, name) VALUES (1, 'test1')`,
			`INSERT INTO test_table (id, name) VALUES (2, 'test2')`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
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

	// test for indirect.
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table`,
			createTableString("test_table", "name text"),
			`INSERT INTO test_table (id, name) VALUES (1, 'test1')`,
			`INSERT INTO test_table (id, name) VALUES (2, 'test2')`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		obj := TestTable{Id: 1}
		_, err = db.Delete(obj)
		if err == nil {
			t.Errorf("DB.Delete(%#v) => _, nil, want error", obj)
		}
	}()

	// test for different table name.
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS diff_table`,
			createTableString("diff_table", "name text", "addr text"),
			`INSERT INTO diff_table (id, name, addr) VALUES (1, 'diff_test1', 'diff_addr1')`,
			`INSERT INTO diff_table (id, name, addr) VALUES (2, 'diff_test2', 'diff_addr2')`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		obj := &testModelDifferentTableName{Id: 1}
		n, err := db.Delete(obj)
		if err != nil {
			t.Fatal(err)
		}
		var actual interface{} = n
		var expected interface{} = int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %[1]v(type %[1]T), but %[2]v(type %[2]T)", expected, actual)
		}
		rows, err := db.db.Query(`SELECT * FROM diff_table`)
		if err != nil {
			t.Fatal(err)
		}
		var id int64
		var name string
		var addr string
		expects := [][]interface{}{
			{int64(2), "diff_test2", "diff_addr2"},
		}
		for i := 0; rows.Next(); i++ {
			if err := rows.Scan(&id, &name, &addr); err != nil {
				t.Fatal(err)
			}
			actual = []interface{}{id, name, addr}
			expected = expects[i]
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %v, but %v", expected, actual)
			}
		}
	}()

	// test for multiple.
	testCaseMultiple := func(objs interface{}) {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table`,
			createTableString("test_table", "name text"),
			`INSERT INTO test_table (id, name) VALUES (1, 'test1')`,
			`INSERT INTO test_table (id, name) VALUES (2, 'test2')`,
			`INSERT INTO test_table (id, name) VALUES (3, 'test3')`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
		n, err := db.Delete(objs)
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
	}
	testCaseMultiple([]TestTable{{Id: 1}, {Id: 3}})
	testCaseMultiple([]*TestTable{{Id: 1}, {Id: 3}})
}

func TestDB_Delete_hook(t *testing.T) {
	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	initDB := func() {
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_model_for_hook`,
			createTableString("test_model_for_hook", "name text"),
			`INSERT INTO test_model_for_hook (id, name) VALUES (1, 'alice')`,
			`INSERT INTO test_model_for_hook (id, name) VALUES (2, 'bob')`,
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}
	}

	// test for no error.
	func() {
		initDB()
		obj := &TestModelForHook{Id: 1, beforeErr: nil, afterErr: nil}
		if _, err := db.Delete(obj); err != nil {
			t.Error(err)
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeDelete", "AfterDelete"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for error in Before.
	func() {
		initDB()
		obj := &TestModelForHook{Id: 1, beforeErr: fmt.Errorf("expected before error"), afterErr: nil}
		if _, err := db.Delete(obj); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeDelete"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for error in After.
	func() {
		initDB()
		obj := &TestModelForHook{Id: 1, beforeErr: nil, afterErr: fmt.Errorf("expected after error")}
		if _, err := db.Delete(obj); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = obj.called
		var expected interface{} = []string{"BeforeDelete", "AfterDelete"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(1)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-delete with no error (direct).
	func() {
		initDB()
		objs := []TestModelForHook{
			{Id: 1, beforeErr: nil, afterErr: nil},
			{Id: 2, beforeErr: nil, afterErr: nil},
		}
		if _, err := db.Delete(objs); err != nil {
			t.Error(err)
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeDelete", "AfterDelete"}, {"BeforeDelete", "AfterDelete"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(0)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-delete with no error (indirect).
	func() {
		initDB()
		objs := []*TestModelForHook{
			{Id: 1, beforeErr: nil, afterErr: nil},
			{Id: 2, beforeErr: nil, afterErr: nil},
		}
		if _, err := db.Delete(objs); err != nil {
			t.Error(err)
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeDelete", "AfterDelete"}, {"BeforeDelete", "AfterDelete"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(0)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-delete with before error (direct).
	func() {
		initDB()
		objs := []TestModelForHook{
			{Name: "alice", beforeErr: nil, afterErr: nil},
			{Name: "bob", beforeErr: fmt.Errorf("expected before error"), afterErr: nil},
		}
		if _, err := db.Delete(objs); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeDelete"}, {"BeforeDelete"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-delete with before error (indirect).
	func() {
		initDB()
		objs := []*TestModelForHook{
			{Name: "alice", beforeErr: nil, afterErr: nil},
			{Name: "bob", beforeErr: fmt.Errorf("expected before error"), afterErr: nil},
		}
		if _, err := db.Delete(objs); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeDelete"}, {"BeforeDelete"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(2)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-delete with after error (direct).
	func() {
		initDB()
		objs := []TestModelForHook{
			{Id: 1, beforeErr: nil, afterErr: nil},
			{Id: 2, beforeErr: nil, afterErr: fmt.Errorf("expected before error")},
		}
		if _, err := db.Delete(objs); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeDelete", "AfterDelete"}, {"BeforeDelete", "AfterDelete"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(0)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()

	// test for bulk-delete with after error (indirect).
	func() {
		initDB()
		objs := []*TestModelForHook{
			{Id: 1, beforeErr: nil, afterErr: nil},
			{Id: 2, beforeErr: nil, afterErr: fmt.Errorf("expected before error")},
		}
		if _, err := db.Delete(objs); err == nil {
			t.Errorf("no error occurred")
		}
		var actual interface{} = [][]string{objs[0].called, objs[1].called}
		var expected interface{} = [][]string{{"BeforeDelete", "AfterDelete"}, {"BeforeDelete", "AfterDelete"}}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %#v, but %#v", expected, actual)
		}
		var n int64
		if err := db.db.QueryRow(`SELECT COUNT(*) FROM test_model_for_hook`).Scan(&n); err != nil {
			t.Error(err)
		}
		actual = n
		expected = int64(0)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %v, but %v", expected, actual)
		}
	}()
}

func TestDB_SetLogOutput(t *testing.T) {
	type TestTable struct {
		Id   int64 `db:"pk"`
		Name string
	}

	db, err := testDB()
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS test_table`,
		createTableString("test_table", "name text"),
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(fmt.Errorf("%v: %s", err, query))
		}
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
	var expected interface{}
	switch os.Getenv("DB") {
	case "mysql":
		expected = fmt.Sprintf(
			"[%[1]s] [0.00ms] INSERT INTO `test_table` (`name`) VALUES (?); [\"test\"]\n"+
				"[%[1]s] [0.00ms] SELECT LAST_INSERT_ID();\n",
			timeFormat)
	case "postgres":
		expected = fmt.Sprintf(
			"[%[1]s] [0.00ms] INSERT INTO \"test_table\" (\"name\") VALUES ($1); [\"test\"]\n"+
				"[%[1]s] [0.00ms] SELECT lastval();\n",
			timeFormat)
	default:
		expected = fmt.Sprintf(
			"[%[1]s] [0.00ms] INSERT INTO \"test_table\" (\"name\") VALUES (?); [\"test\"]\n"+
				"[%[1]s] [0.00ms] SELECT last_insert_rowid();\n",
			timeFormat)
	}
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
	switch os.Getenv("DB") {
	case "mysql":
		expected = fmt.Sprintf("[%s] [0.00ms] SELECT `test_table`.* FROM `test_table`;\n", timeFormat)
	default:
		expected = fmt.Sprintf("[%s] [0.00ms] SELECT \"test_table\".* FROM \"test_table\";\n", timeFormat)
	}
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
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_table`,
			createTableString("test_table", "name text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
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
		var expected interface{}
		switch os.Getenv("DB") {
		case "mysql":
			expected = fmt.Sprintf(
				"[INSERT INTO `test_table` (`name`) VALUES (?); [\"test\"]] in 0.00ms. (%[1]s)\n"+
					"[SELECT LAST_INSERT_ID();] in 0.00ms. (%[1]s)\n",
				timeFormat)
		case "postgres":
			expected = fmt.Sprintf(
				"[INSERT INTO \"test_table\" (\"name\") VALUES ($1); [\"test\"]] in 0.00ms. (%[1]s)\n"+
					"[SELECT lastval();] in 0.00ms. (%[1]s)\n",
				timeFormat)
		default:
			expected = fmt.Sprintf(
				"[INSERT INTO \"test_table\" (\"name\") VALUES (?); [\"test\"]] in 0.00ms. (%[1]s)\n"+
					"[SELECT last_insert_rowid();] in 0.00ms. (%[1]s)\n",
				timeFormat)
		}
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
		switch os.Getenv("DB") {
		case "mysql":
			expected = fmt.Sprintf("[SELECT `test_table`.* FROM `test_table`;] in 0.00ms. (%s)\n", timeFormat)
		default:
			expected = fmt.Sprintf("[SELECT \"test_table\".* FROM \"test_table\";] in 0.00ms. (%s)\n", timeFormat)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()
}

func TestEmbeddedStructHooks(t *testing.T) {
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_embedded_model_for_hook;`,
			createTableString("test_embedded_model_for_hook", "name text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}

		// test for Insert hooks.
		obj := &testEmbeddedModelForHook{}
		if _, err := db.Insert(obj); err != nil {
			t.Fatal(err)
		}
		actual := obj.called
		expected := []string{"embedded: BeforeInsert", "embedded: AfterInsert"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}

		// test for Update hooks.
		obj.called = nil
		obj.TestModelForHook.called = nil
		obj.Id = 1
		obj.Name = "foo"
		if _, err := db.Update(obj); err != nil {
			t.Fatal(err)
		}
		actual = obj.called
		expected = []string{"embedded: BeforeUpdate", "embedded: AfterUpdate"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}

		// test for Delete hooks.
		obj.called = nil
		obj.TestModelForHook.called = nil
		if _, err := db.Delete(obj); err != nil {
			t.Fatal(err)
		}
		actual = obj.called
		expected = []string{"embedded: BeforeDelete", "embedded: AfterDelete"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}()

	// test for unexported embedded field.
	func() {
		type testUnexportedEmbeddedModelForHook struct {
			Id   int64 `db:"pk"`
			Name string

			testEmbeddedModelForHook
		}
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			`DROP TABLE IF EXISTS test_unexported_embedded_model_for_hook;`,
			createTableString("test_unexported_embedded_model_for_hook", "name text"),
		} {
			if _, err := db.db.Exec(query); err != nil {
				t.Fatal(fmt.Errorf("%v: %s", err, query))
			}
		}

		// test for Insert hooks.
		obj := &testUnexportedEmbeddedModelForHook{}
		if _, err := db.Insert(obj); err != nil {
			t.Fatal(err)
		}
		actual := obj.testEmbeddedModelForHook.called
		expected := []string{"embedded: BeforeInsert", "embedded: AfterInsert"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("db.Insert(%q); obj.testEmbeddedModelForHook.called => %q, want %q", obj, actual, expected)
		}

		// test for Update hooks.
		obj.testEmbeddedModelForHook.called = nil
		obj.Id = 1
		if _, err := db.Update(obj); err != nil {
			t.Fatal(err)
		}
		actual = obj.testEmbeddedModelForHook.called
		expected = []string{"embedded: BeforeUpdate", "embedded: AfterUpdate"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("db.Update(%q); obj.testEmbeddedModelForHook.called => %q, want %q", obj, actual, expected)
		}

		// test for Delete hooks.
		obj.testEmbeddedModelForHook.called = nil
		if _, err := db.Delete(obj); err != nil {
			t.Fatal(err)
		}
		actual = obj.testEmbeddedModelForHook.called
		expected = []string{"embedded: BeforeDelete", "embedded: AfterDelete"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("db.Delete(%q); obj.TestModelForHook.called => %q, want %q", obj, actual, expected)
		}
	}()
}

func TestDB_Commit(t *testing.T) {
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		actual := db.Commit()
		expect := fmt.Errorf("genmai: transaction hasn't been started or already committed or rolled back")
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.Commit() => %#v; want %#v`, actual, expect)
		}
	}()

	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Begin(); err != nil {
			t.Fatal(err)
		}
		if err := db.Commit(); err != nil {
			t.Fatal(err)
		}
		actual := db.Commit()
		expect := fmt.Errorf("genmai: transaction hasn't been started or already committed or rolled back")
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.Commit() => %#v; want %#v`, actual, expect)
		}
	}()

	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Begin(); err != nil {
			t.Fatal(err)
		}
		if err := db.Rollback(); err != nil {
			t.Fatal(err)
		}
		actual := db.Commit()
		expect := fmt.Errorf("genmai: transaction hasn't been started or already committed or rolled back")
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.Commit() => %#v; want %#v`, actual, expect)
		}
	}()
}

func TestDB_Rollback(t *testing.T) {
	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		actual := db.Rollback()
		expect := fmt.Errorf("genmai: transaction hasn't been started or already committed or rolled back")
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.Commit() => %#v; want %#v`, actual, expect)
		}
	}()

	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Begin(); err != nil {
			t.Fatal(err)
		}
		if err := db.Rollback(); err != nil {
			t.Fatal(err)
		}
		actual := db.Rollback()
		expect := fmt.Errorf("genmai: transaction hasn't been started or already committed or rolled back")
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.Commit() => %#v; want %#v`, actual, expect)
		}
	}()

	func() {
		db, err := testDB()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Begin(); err != nil {
			t.Fatal(err)
		}
		if err := db.Commit(); err != nil {
			t.Fatal(err)
		}
		actual := db.Rollback()
		expect := fmt.Errorf("genmai: transaction hasn't been started or already committed or rolled back")
		if !reflect.DeepEqual(actual, expect) {
			t.Errorf(`DB.Commit() => %#v; want %#v`, actual, expect)
		}
	}()
}
