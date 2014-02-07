package genmai

import (
	"fmt"
	"strings"
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
}

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
