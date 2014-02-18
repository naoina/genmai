package genmai

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Dialect is an interface that the dialect of the database.
type Dialect interface {
	// Name returns a name of the dialect.
	// Return value must be same as the driver name.
	Name() string

	// Quote returns a quoted s.
	// It is for a column name, not a value.
	Quote(s string) string

	// PlaceHolder returns the placeholder character of the database.
	// A current number of placeholder will passed to i.
	PlaceHolder(i int) string

	// SQLType returns the SQL type of the v.
	// autoIncrement is whether the field is auto increment.
	// If "size" tag specified to struct field, it will passed to size
	// argument. If it doesn't specify, size is 0.
	SQLType(v interface{}, autoIncrement bool, size uint64) string

	// AutoIncrement returns the keyword of auto increment.
	AutoIncrement() string

	// FormatBool returns boolean value as string according to the value of b.
	FormatBool(b bool) string
}

var (
	ErrUsingFloatType = errors.New("float types have a rounding error problem.\n" +
		"Please use `genmai.Rat` if you want an exact value.\n" +
		"However, if you still want a float types, please use `genmai.Float32` and `Float64`.")
)

const (
	// Precision of the fixed-point number.
	// Digits of precision before the decimal point.
	decimalPrecision = 65

	// Scale of the fixed-point number.
	// Digits of precision after the decimal point.
	decimalScale = 30
)

// SQLite3Dialect represents a dialect of the SQLite3.
// It implements the Dialect interface.
type SQLite3Dialect struct{}

// Name returns name of the dialect.
func (d *SQLite3Dialect) Name() string {
	return "sqlite3"
}

// Quote returns a quoted s for a column name.
func (d *SQLite3Dialect) Quote(s string) string {
	return fmt.Sprintf(`"%s"`, strings.Replace(s, `"`, `""`, -1))
}

// PlaceHolder returns the placeholder character of the SQLite3.
func (d *SQLite3Dialect) PlaceHolder(i int) string {
	return "?"
}

// SQLType returns the SQL type of the v for SQLite3.
func (d *SQLite3Dialect) SQLType(v interface{}, autoIncrement bool, size uint64) string {
	switch v.(type) {
	case bool, *bool, sql.NullBool:
		return "boolean"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		*int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64,
		sql.NullInt64:
		return "integer"
	case string, *string, sql.NullString:
		return "text"
	case []byte:
		return "blob"
	case time.Time, *time.Time:
		return "datetime"
	case Float32, *Float32, Float64, *Float64:
		return "real"
	case Rat, *Rat:
		return "numeric"
	case float32, *float32, float64, *float64, sql.NullFloat64:
		panic(ErrUsingFloatType)
	}
	panic(fmt.Errorf("SQLite3Dialect: unsupported SQL type: %T", v))
}

func (d *SQLite3Dialect) AutoIncrement() string {
	return "AUTOINCREMENT"
}

// FormatBool returns "1" or "0" according to the value of b as boolean for SQLite3.
func (d *SQLite3Dialect) FormatBool(b bool) string {
	if b {
		return "1"
	} else {
		return "0"
	}
}

// MySQLDialect represents a dialect of the MySQL.
// It implements the Dialect interface.
type MySQLDialect struct{}

// Name returns name of the MySQLDialect.
func (d *MySQLDialect) Name() string {
	return "mysql"
}

// Quote returns a quoted s for a column name.
func (d *MySQLDialect) Quote(s string) string {
	return fmt.Sprintf("`%s`", strings.Replace(s, "`", "``", -1))
}

// PlaceHolder returns the placeholder character of the MySQL.
func (d *MySQLDialect) PlaceHolder(i int) string {
	return "?"
}

// SQLType returns the SQL type of the v for MySQL.
func (d *MySQLDialect) SQLType(v interface{}, autoIncrement bool, size uint64) string {
	switch v.(type) {
	case bool, *bool, sql.NullBool:
		return "BOOLEAN"
	case int8, *int8, int16, *int16, uint8, *uint8, uint16, *uint16:
		return "SMALLINT"
	case int, *int, int32, *int32, uint, *uint, uint32, *uint32:
		return "INT"
	case int64, *int64, uint64, *uint64, sql.NullInt64:
		return "BIGINT"
	case string, *string, sql.NullString:
		switch {
		case size == 0:
			return "VARCHAR(255)" // default.
		case size < (1<<16)-1-2: // approximate 64KB.
			// 65533 ((2^16) - 1) - (length of prefix)
			// See http://dev.mysql.com/doc/refman/5.5/en/string-type-overview.html#idm47703458792704
			return fmt.Sprintf("VARCHAR(%d)", size)
		case size < 1<<24: // 16MB.
			return "MEDIUMTEXT"
		}
		return "LONGTEXT"
	case []byte:
		switch {
		case size == 0:
			return "VARBINARY(255)" // default.
		case size < (1<<16)-1-2: // approximate 64KB.
			// 65533 ((2^16) - 1) - (length of prefix)
			// See http://dev.mysql.com/doc/refman/5.5/en/string-type-overview.html#idm47703458759504
			return fmt.Sprintf("VARBINARY(%d)", size)
		case size < 1<<24: // 16MB.
			return "MEDIUMBLOB"
		}
		return "LONGBLOB"
	case time.Time, *time.Time:
		return "DATETIME"
	case Rat, *Rat:
		return fmt.Sprintf("DECIMAL(%d, %d)", decimalPrecision, decimalScale)
	case Float32, *Float32, Float64, *Float64:
		return "DOUBLE"
	case float32, *float32, float64, *float64, sql.NullFloat64:
		panic(ErrUsingFloatType)
	}
	panic(fmt.Errorf("MySQLDialect: unsupported SQL type: %T", v))
}

func (d *MySQLDialect) AutoIncrement() string {
	return "AUTO_INCREMENT"
}

// FormatBool returns "TRUE" or "FALSE" according to the value of b as boolean for MySQL.
func (d *MySQLDialect) FormatBool(b bool) string {
	if b {
		return "TRUE"
	} else {
		return "FALSE"
	}
}

// PostgresDialect represents a dialect of the PostgreSQL.
// It implements the Dialect interface.
type PostgresDialect struct{}

// Name returns name of the PostgresDialect.
func (d *PostgresDialect) Name() string {
	return "postgres"
}

// Quote returns a quoted s for a column name.
func (d *PostgresDialect) Quote(s string) string {
	return fmt.Sprintf(`"%s"`, strings.Replace(s, `"`, `""`, -1))
}

// PlaceHolder returns the placeholder character of the PostgreSQL.
func (d *PostgresDialect) PlaceHolder(i int) string {
	return fmt.Sprintf("$%d", i+1)
}

// SQLType returns the SQL type of the v for PostgreSQL.
func (d *PostgresDialect) SQLType(v interface{}, autoIncrement bool, size uint64) string {
	switch v.(type) {
	case bool, *bool, sql.NullBool:
		return "boolean"
	case int8, *int8, int16, *int16, uint8, *uint8, uint16, *uint16:
		if autoIncrement {
			return "smallserial"
		} else {
			return "smallint"
		}
	case int, *int, int32, *int32, uint, *uint, uint32, *uint32:
		if autoIncrement {
			return "serial"
		} else {
			return "integer"
		}
	case int64, *int64, uint64, *uint64, sql.NullInt64:
		if autoIncrement {
			return "bigserial"
		} else {
			return "bigint"
		}
	case string, *string, sql.NullString:
		switch {
		case size == 0:
			return "varchar(255)" // default.
		case size < (1<<16)-1-2: // approximate 64KB.
			// This isn't required in PostgreSQL, but defined in order to match to the MySQLDialect.
			return fmt.Sprintf("varchar(%d)", size)
		}
		return "text"
	case []byte:
		return "bytea"
	case time.Time, *time.Time:
		return "timestamp with time zone"
	case Rat, *Rat:
		return fmt.Sprintf("numeric(%d, %d)", decimalPrecision, decimalScale)
	case Float32, *Float32, Float64, *Float64:
		return "double precision"
	case float32, *float32, float64, *float64, sql.NullFloat64:
		panic(ErrUsingFloatType)
	}
	panic(fmt.Errorf("PostgresDialect: unsupported SQL type: %T", v))
}

func (d *PostgresDialect) AutoIncrement() string {
	return ""
}

// FormatBool returns "TRUE" or "FALSE" according to the value of b as boolean for PostgreSQL.
func (d *PostgresDialect) FormatBool(b bool) string {
	if b {
		return "TRUE"
	} else {
		return "FALSE"
	}
}
