package genmai

import (
	"database/sql"
	"reflect"
	"testing"
	"time"
)

func Test_SQLite3Dialect_Name(t *testing.T) {
	d := &SQLite3Dialect{}
	actual := d.Name()
	expected := "sqlite3"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func Test_SQLite3Dialect_Quote(t *testing.T) {
	d := &SQLite3Dialect{}
	for _, v := range []struct {
		s, expected string
	}{
		{``, `""`},
		{`test`, `"test"`},
		{`"test"`, `"""test"""`},
		{`test"bar"baz`, `"test""bar""baz"`},
	} {
		actual := d.Quote(v.s)
		expected := v.expected
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Input %q expects %q, but %q", v.s, expected, actual)
		}
	}
}

func Test_SQLite3Dialect_PlaceHolder(t *testing.T) {
	d := &SQLite3Dialect{}
	actual := d.PlaceHolder(0)
	expected := "?"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestSQLite3Dialect_SQLType_bool(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{true, new(bool), sql.NullBool{}}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "boolean"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "boolean"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_primitiveFloat(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{float32(.1), new(float32), float64(.1), new(float64), sql.NullFloat64{}}

	// autoIncrement is false.
	for _, v := range sets {
		func(v interface{}) {
			defer func() {
				if err := recover(); err == nil {
					t.Errorf("panic hasn't been occurred by %T", v)
				}
			}()
			d.SQLType(v, false, 0)
		}(v)
	}

	// autoIncrement is true.
	for _, v := range sets {
		func(v interface{}) {
			defer func() {
				if err := recover(); err == nil {
					t.Errorf("panic hasn't been occurred by %T", v)
				}
			}()
			d.SQLType(v, false, 0)
		}(v)
	}
}

func TestSQLite3Dialect_SQLType_int(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
		new(int), new(int8), new(int16), new(int32), new(int64), new(uint), new(uint8), new(uint16), new(uint32), new(uint64),
		sql.NullInt64{}}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "integer"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "integer"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_string(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{"", new(string), sql.NullString{}}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "text"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "text"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_byteSlice(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{[]byte("")}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "blob"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "blob"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_time(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{time.Time{}, &time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "datetime"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "datetime"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_float(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{Float32(.1), new(Float32), Float64(.1), new(Float64)}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "real"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "real"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_rat(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{Rat{}, new(Rat)}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "numeric"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "numeric"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_AutoIncrement(t *testing.T) {
	d := &SQLite3Dialect{}
	actual := d.AutoIncrement()
	expected := "AUTOINCREMENT"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestSQLite3Dialect_FormatBool(t *testing.T) {
	d := &SQLite3Dialect{}
	actual := d.FormatBool(true)
	expected := "1"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}

	actual = d.FormatBool(false)
	expected = "0"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func Test_MySQLDialect_Name(t *testing.T) {
	d := &MySQLDialect{}
	actual := d.Name()
	expected := "mysql"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func Test_MySQLDialect_Quote(t *testing.T) {
	d := &MySQLDialect{}
	for _, v := range []struct {
		s, expected string
	}{
		{"", "``"},
		{"test", "`test`"},
		{"`test`", "```test```"},
		{"test`bar`baz", "`test``bar``baz`"},
	} {
		actual := d.Quote(v.s)
		expected := v.expected
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Input %q expects %q, but %q", v.s, expected, actual)
		}
	}
}

func Test_MySQLDialect_PlaceHolder(t *testing.T) {
	d := &MySQLDialect{}
	actual := d.PlaceHolder(0)
	expected := "?"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestMySQLDialect_SQLType_bool(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{true, new(bool), sql.NullBool{}}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "BOOLEAN"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "BOOLEAN"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_primitiveFloat(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{float32(.1), new(float32), float64(.1), new(float64), sql.NullFloat64{}}

	// autoIncrement is false.
	for _, v := range sets {
		func(v interface{}) {
			defer func() {
				if err := recover(); err == nil {
					t.Errorf("panic hasn't been occurred by %T", v)
				}
			}()
			d.SQLType(v, false, 0)
		}(v)
	}

	// autoIncrement is true.
	for _, v := range sets {
		func(v interface{}) {
			defer func() {
				if err := recover(); err == nil {
					t.Errorf("panic hasn't been occurred by %T", v)
				}
			}()
			d.SQLType(v, false, 0)
		}(v)
	}
}

func TestMySQLDialect_SQLType_underInt16(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{
		int8(1), new(int8),
		int16(1), new(int16),
		uint8(1), new(uint8),
		uint16(1), new(uint16),
	}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "SMALLINT"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "SMALLINT"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_int(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{
		int(1), new(int),
		int32(1), new(int32),
		uint(1), new(uint),
		uint32(1), new(uint32),
	}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "INT"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "INT"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_int64(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{
		int64(1), new(int64),
		uint64(1), new(uint64),
		sql.NullInt64{},
	}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "BIGINT"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "BIGINT"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_string(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{"", new(string), sql.NullString{}}

	func() {
		// autoIncrement is false.
		for _, v := range sets {
			actual := d.SQLType(v, false, 0)
			expected := "VARCHAR(255)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}

		// autoIncrement is true.
		for _, v := range sets {
			actual := d.SQLType(v, true, 0)
			expected := "VARCHAR(255)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 1)
			expected := "VARCHAR(1)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 2)
			expected := "VARCHAR(2)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 65532)
			expected := "VARCHAR(65532)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 65533)
			expected := "MEDIUMTEXT"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 16777215)
			expected := "MEDIUMTEXT"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 16777216)
			expected := "LONGTEXT"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()
}

func TestMySQLDialect_SQLType_byteSlice(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{[]byte("")}

	func() {
		// autoIncrement is false.
		for _, v := range sets {
			actual := d.SQLType(v, false, 0)
			expected := "VARBINARY(255)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}

		// autoIncrement is true.
		for _, v := range sets {
			actual := d.SQLType(v, true, 0)
			expected := "VARBINARY(255)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 1)
			expected := "VARBINARY(1)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 2)
			expected := "VARBINARY(2)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 65532)
			expected := "VARBINARY(65532)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 65533)
			expected := "MEDIUMBLOB"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 16777215)
			expected := "MEDIUMBLOB"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 16777216)
			expected := "LONGBLOB"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()
}

func TestMySQLDialect_SQLType_time(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{time.Time{}, &time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "DATETIME"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "DATETIME"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_float(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{Float32(.1), new(Float32), Float64(.1), new(Float64)}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "DOUBLE"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "DOUBLE"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_rat(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{Rat{}, new(Rat)}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "DECIMAL(65, 30)"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "DECIMAL(65, 30)"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_AutoIncrement(t *testing.T) {
	d := &MySQLDialect{}
	actual := d.AutoIncrement()
	expected := "AUTO_INCREMENT"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestMySQLDialect_FormatBool(t *testing.T) {
	d := &MySQLDialect{}
	actual := d.FormatBool(true)
	expected := "TRUE"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}

	actual = d.FormatBool(false)
	expected = "FALSE"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func Test_PostgresDialect_Name(t *testing.T) {
	d := &PostgresDialect{}
	actual := d.Name()
	expected := "postgres"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func Test_PostgresDialect_Quote(t *testing.T) {
	d := &PostgresDialect{}
	for _, v := range []struct {
		s, expected string
	}{
		{``, `""`},
		{`test`, `"test"`},
		{`"test"`, `"""test"""`},
		{`test"bar"baz`, `"test""bar""baz"`},
	} {
		actual := d.Quote(v.s)
		expected := v.expected
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Input %q expects %q, but %q", v.s, expected, actual)
		}
	}
}

func Test_PostgresDialect_PlaceHolder(t *testing.T) {
	d := &PostgresDialect{}
	actual := d.PlaceHolder(0)
	expected := "$0"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}

	actual = d.PlaceHolder(1)
	expected = "$1"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestPostgresDialect_SQLType_bool(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{true, new(bool), sql.NullBool{}}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "boolean"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "boolean"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_primitiveFloat(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{float32(.1), new(float32), float64(.1), new(float64), sql.NullFloat64{}}

	// autoIncrement is false.
	for _, v := range sets {
		func(v interface{}) {
			defer func() {
				if err := recover(); err == nil {
					t.Errorf("panic hasn't been occurred by %T", v)
				}
			}()
			d.SQLType(v, false, 0)
		}(v)
	}

	// autoIncrement is true.
	for _, v := range sets {
		func(v interface{}) {
			defer func() {
				if err := recover(); err == nil {
					t.Errorf("panic hasn't been occurred by %T", v)
				}
			}()
			d.SQLType(v, false, 0)
		}(v)
	}
}

func TestPostgresDialect_SQLType_underInt16(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{
		int8(1), new(int8),
		int16(1), new(int16),
		uint8(1), new(uint8),
		uint16(1), new(uint16),
	}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "smallint"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "smallserial"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_int(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{
		int(1), new(int),
		int32(1), new(int32),
		uint(1), new(uint),
		uint32(1), new(uint32),
	}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "integer"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "serial"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_int64(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{
		int64(1), new(int64),
		uint64(1), new(uint64),
		sql.NullInt64{},
	}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "bigint"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "bigserial"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_string(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{"", new(string), sql.NullString{}}

	func() {
		// autoIncrement is false.
		for _, v := range sets {
			actual := d.SQLType(v, false, 0)
			expected := "varchar(255)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}

		// autoIncrement is true.
		for _, v := range sets {
			actual := d.SQLType(v, true, 0)
			expected := "varchar(255)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 1)
			expected := "varchar(1)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 2)
			expected := "varchar(2)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 65532)
			expected := "varchar(65532)"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 65533)
			expected := "text"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 16777215)
			expected := "text"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			actual := d.SQLType(v, false, 16777216)
			expected := "text"
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()
}

func TestPostgresDialect_SQLType_byteSlice(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{[]byte("")}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "bytea"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "bytea"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_time(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{time.Time{}, &time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "timestamp with time zone"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "timestamp with time zone"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_float(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{Float32(.1), new(Float32), Float64(.1), new(Float64)}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "double precision"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "double precision"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_rat(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{Rat{}, new(Rat)}

	// autoIncrement is false.
	for _, v := range sets {
		actual := d.SQLType(v, false, 0)
		expected := "numeric(65, 30)"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		actual := d.SQLType(v, true, 0)
		expected := "numeric(65, 30)"
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_AutoIncrement(t *testing.T) {
	d := &PostgresDialect{}
	actual := d.AutoIncrement()
	expected := ""
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestPostgresDialect_FormatBool(t *testing.T) {
	d := &PostgresDialect{}
	actual := d.FormatBool(true)
	expected := "TRUE"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}

	actual = d.FormatBool(false)
	expected = "FALSE"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}
