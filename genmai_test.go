package genmai

import (
	"reflect"
	"testing"

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

func newTestDB(t *testing.T) *DB {
	db, err := New(&SQLite3Dialect{}, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`CREATE TABLE test_model (
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
		`CREATE TABLE m2 (
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
		if err := db.Select(&actual, db.Distinct("name")); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{0, "test1", ""},
			{0, "test2", ""},
			{0, "test3", ""},
			{0, "other", ""},
			{0, "dup", ""},
			{0, "other1", ""},
			{0, "other2", ""},
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
		if err := db.Select(&actual, db.Distinct("name", "addr")); err != nil {
			t.Fatal(err)
		}
		expected := []testModel{
			{0, "test1", "addr1"},
			{0, "test2", "addr2"},
			{0, "test3", "addr3"},
			{0, "other", "addr4"},
			{0, "other", "addr5"},
			{0, "dup", "dup_addr"},
			{0, "other1", "addr8"},
			{0, "other2", "addr9"},
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
		if err := db.Select(&actual, db.From(testModel{})); err != nil {
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
}
