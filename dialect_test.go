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

func TestSQLite3Dialect_SQLType_boolDirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{true, false}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"boolean", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"boolean", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_boolIndirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{new(bool), sql.NullBool{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"boolean", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"boolean", true}
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

func TestSQLite3Dialect_SQLType_intDirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{
		int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1),
		uint16(1), uint32(1), uint64(1),
	}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"integer", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"integer", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_intIndirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{
		new(int), new(int8), new(int16), new(int32), new(int64), new(uint),
		new(uint8), new(uint16), new(uint32), new(uint64), sql.NullInt64{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"integer", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"integer", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_stringDirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{""}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"text", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"text", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_stringIndirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{new(string), sql.NullString{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"text", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"text", true}
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
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"blob", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"blob", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_timeDirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"datetime", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"datetime", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_timeIndirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{&time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"datetime", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"datetime", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_floatDirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{Float32(.1), Float64(.1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"real", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"real", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_floatIndirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{new(Float32), new(Float64)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"real", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"real", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_ratDirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{Rat{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"numeric", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"numeric", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestSQLite3Dialect_SQLType_ratIndirect(t *testing.T) {
	d := &SQLite3Dialect{}
	sets := []interface{}{new(Rat)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"numeric", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"numeric", true}
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

func TestSQLite3Dialect_LastInsertID(t *testing.T) {
	d := &SQLite3Dialect{}
	actual := d.LastInsertId()
	expect := "SELECT last_insert_rowid()"
	if !reflect.DeepEqual(actual, expect) {
		t.Errorf(`SQLite3Dialect.LastInsertId() => %#v; want %#v`, actual, expect)
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

func TestMySQLDialect_SQLType_boolDirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{true, false}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"BOOLEAN", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"BOOLEAN", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_boolIndirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{new(bool), sql.NullBool{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"BOOLEAN", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"BOOLEAN", true}
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

func TestMySQLDialect_SQLType_underInt16Direct(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{int8(1), int16(1), uint8(1), uint16(1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"SMALLINT", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"SMALLINT", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_underInt16Indirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{new(int8), new(int16), new(uint8), new(uint16)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"SMALLINT", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"SMALLINT", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_intDirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{int(1), int32(1), uint(1), uint32(1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"INT", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"INT", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_intIndirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{new(int), new(int32), new(uint), new(uint32)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"INT", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"INT", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_int64Direct(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{int64(1), uint64(1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"BIGINT", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"BIGINT", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_int64Indirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{new(int64), new(uint64), sql.NullInt64{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"BIGINT", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"BIGINT", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_stringDirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{""}

	func() {
		// autoIncrement is false.
		for _, v := range sets {
			name, null := d.SQLType(v, false, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(255)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}

		// autoIncrement is true.
		for _, v := range sets {
			name, null := d.SQLType(v, true, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(255)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 1)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(1)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 2)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(2)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65532)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(65532)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65533)
			actual := []interface{}{name, null}
			expected := []interface{}{"MEDIUMTEXT", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777215)
			actual := []interface{}{name, null}
			expected := []interface{}{"MEDIUMTEXT", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777216)
			actual := []interface{}{name, null}
			expected := []interface{}{"LONGTEXT", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()
}

func TestMySQLDialect_SQLType_stringIndirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{new(string), sql.NullString{}}

	func() {
		// autoIncrement is false.
		for _, v := range sets {
			name, null := d.SQLType(v, false, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(255)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}

		// autoIncrement is true.
		for _, v := range sets {
			name, null := d.SQLType(v, true, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(255)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 1)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(1)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 2)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(2)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65532)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARCHAR(65532)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65533)
			actual := []interface{}{name, null}
			expected := []interface{}{"MEDIUMTEXT", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777215)
			actual := []interface{}{name, null}
			expected := []interface{}{"MEDIUMTEXT", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777216)
			actual := []interface{}{name, null}
			expected := []interface{}{"LONGTEXT", true}
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
			name, null := d.SQLType(v, false, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARBINARY(255)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}

		// autoIncrement is true.
		for _, v := range sets {
			name, null := d.SQLType(v, true, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARBINARY(255)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 1)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARBINARY(1)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 2)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARBINARY(2)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65532)
			actual := []interface{}{name, null}
			expected := []interface{}{"VARBINARY(65532)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65533)
			actual := []interface{}{name, null}
			expected := []interface{}{"MEDIUMBLOB", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777215)
			actual := []interface{}{name, null}
			expected := []interface{}{"MEDIUMBLOB", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777216)
			actual := []interface{}{name, null}
			expected := []interface{}{"LONGBLOB", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()
}

func TestMySQLDialect_SQLType_timeDirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DATETIME", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DATETIME", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_timeIndirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{&time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DATETIME", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DATETIME", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_floatDirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{Float32(.1), Float64(.1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DOUBLE", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DOUBLE", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_floatIndirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{new(Float32), new(Float64)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DOUBLE", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DOUBLE", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_ratDirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{Rat{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DECIMAL(65, 30)", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DECIMAL(65, 30)", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestMySQLDialect_SQLType_ratIndirect(t *testing.T) {
	d := &MySQLDialect{}
	sets := []interface{}{new(Rat)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DECIMAL(65, 30)", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"DECIMAL(65, 30)", true}
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

func TestMySQLDialect_LastInsertID(t *testing.T) {
	d := &MySQLDialect{}
	actual := d.LastInsertId()
	expect := "SELECT LAST_INSERT_ID()"
	if !reflect.DeepEqual(actual, expect) {
		t.Errorf(`MySQLDialect.LastInsertId() => %#v; want %#v`, actual, expect)
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
	expected := "$1"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}

	actual = d.PlaceHolder(1)
	expected = "$2"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestPostgresDialect_SQLType_boolDirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{true, false}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"boolean", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"boolean", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_boolIndirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{new(bool), sql.NullBool{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"boolean", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"boolean", true}
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

func TestPostgresDialect_SQLType_underInt16Direct(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{int8(1), int16(1), uint8(1), uint16(1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"smallint", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"smallserial", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_underInt16Indirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{new(int8), new(int16), new(uint8), new(uint16)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"smallint", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"smallserial", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_intDirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{int(1), int32(1), uint(1), uint32(1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"integer", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"serial", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_intIndirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{new(int), new(int32), new(uint), new(uint32)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"integer", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"serial", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_int64Direct(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{int64(1), uint64(1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"bigint", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"bigserial", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_int64Indirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{new(int64), new(uint64), sql.NullInt64{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"bigint", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"bigserial", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_stringDirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{""}

	func() {
		// autoIncrement is false.
		for _, v := range sets {
			name, null := d.SQLType(v, false, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(255)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}

		// autoIncrement is true.
		for _, v := range sets {
			name, null := d.SQLType(v, true, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(255)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 1)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(1)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 2)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(2)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65532)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(65532)", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65533)
			actual := []interface{}{name, null}
			expected := []interface{}{"text", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777215)
			actual := []interface{}{name, null}
			expected := []interface{}{"text", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777216)
			actual := []interface{}{name, null}
			expected := []interface{}{"text", false}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()
}

func TestPostgresDialect_SQLType_stringIndirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{new(string), sql.NullString{}}

	func() {
		// autoIncrement is false.
		for _, v := range sets {
			name, null := d.SQLType(v, false, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(255)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}

		// autoIncrement is true.
		for _, v := range sets {
			name, null := d.SQLType(v, true, 0)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(255)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 1)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(1)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("Expect %q, but %q", expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 2)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(2)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65532)
			actual := []interface{}{name, null}
			expected := []interface{}{"varchar(65532)", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 65533)
			actual := []interface{}{name, null}
			expected := []interface{}{"text", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777215)
			actual := []interface{}{name, null}
			expected := []interface{}{"text", true}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("%T expects %q, but %q", v, expected, actual)
			}
		}
	}()

	func() {
		for _, v := range sets {
			name, null := d.SQLType(v, false, 16777216)
			actual := []interface{}{name, null}
			expected := []interface{}{"text", true}
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
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"bytea", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"bytea", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_timeDirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"timestamp with time zone", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"timestamp with time zone", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_timeIndirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{&time.Time{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"timestamp with time zone", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"timestamp with time zone", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_floatDirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{Float32(.1), Float64(.1)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"double precision", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"double precision", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_floatIndirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{new(Float32), new(Float64)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"double precision", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"double precision", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_ratDirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{Rat{}}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"numeric(65, 30)", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"numeric(65, 30)", false}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}
}

func TestPostgresDialect_SQLType_ratIndirect(t *testing.T) {
	d := &PostgresDialect{}
	sets := []interface{}{new(Rat)}

	// autoIncrement is false.
	for _, v := range sets {
		name, null := d.SQLType(v, false, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"numeric(65, 30)", true}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%T expects %q, but %q", v, expected, actual)
		}
	}

	// autoIncrement is true.
	for _, v := range sets {
		name, null := d.SQLType(v, true, 0)
		actual := []interface{}{name, null}
		expected := []interface{}{"numeric(65, 30)", true}
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

func TestPostgresDialect_LastInsertID(t *testing.T) {
	d := &PostgresDialect{}
	actual := d.LastInsertId()
	expect := "SELECT lastval()"
	if !reflect.DeepEqual(actual, expect) {
		t.Errorf(`PostgresDialect.LastInsertId() => %#v; want %#v`, actual, expect)
	}
}
