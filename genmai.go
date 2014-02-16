// Copyright 2014 Naoya Inada. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

// Package genmai provides simple, better and easy-to-use Object-Relational Mapper.
package genmai

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB represents a database object.
type DB struct {
	db      *sql.DB
	dialect Dialect
	tx      *sql.Tx
	m       sync.Mutex
	logger  logger
}

// New returns a new DB.
// If any error occurs, it returns nil and error.
func New(dialect Dialect, dsn string) (*DB, error) {
	db, err := sql.Open(dialect.Name(), dsn)
	if err != nil {
		return nil, err
	}
	return &DB{db: db, dialect: dialect, logger: defaultLogger}, nil
}

// Select fetch data into the output from the database.
// output argument must be pointer to a slice of struct. If not a pointer or not a slice of struct, It returns error.
// The table name of the database will be determined from name of struct. e.g. If *[]ATableName passed to output argument, table name will be "a_table_name".
// If args are not given, fetch the all data like "SELECT * FROM table" SQL.
func (db *DB) Select(output interface{}, args ...interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			err = fmt.Errorf("%v\n%v", e, string(buf[:n]))
		}
	}()
	rv := reflect.ValueOf(output)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("Select: first argument must be a pointer")
	}
	rv = rv.Elem()
	var tableName string
	for _, arg := range args {
		if f, ok := arg.(*From); ok {
			if tableName != "" {
				return fmt.Errorf("Select: From statement specified more than once")
			}
			tableName = f.TableName
		}
	}
	var selectFunc selectFunc
	switch rv.Kind() {
	case reflect.Slice:
		t := rv.Type().Elem()
		if t.Kind() != reflect.Struct {
			return fmt.Errorf("Select: argument of slice must be slice of struct, but %v", rv.Type())
		}
		if tableName == "" {
			tableName = ToSnakeCase(t.Name())
		}
		selectFunc = db.selectToSlice
	default:
		if tableName == "" {
			return fmt.Errorf("Select: From statement must be given if any Function is given")
		}
		selectFunc = db.selectToValue
	}
	col, from, conditions, err := db.classify(tableName, args)
	if err != nil {
		return err
	}
	queries := []string{`SELECT`, col, `FROM`, db.dialect.Quote(from)}
	var values []interface{}
	for _, cond := range conditions {
		q, a := cond.build(0, false)
		queries = append(queries, q...)
		values = append(values, a...)
	}
	rows, err := db.query(strings.Join(queries, " "), values...)
	if err != nil {
		return err
	}
	defer rows.Close()
	value, err := selectFunc(rows, &rv)
	if err != nil {
		return err
	}
	rv.Set(*value)
	return nil
}

// From returns a "FROM" statement.
// A table name will be determined from name of struct of arg.
// If arg argument is not struct type, it panics.
func (db *DB) From(arg interface{}) *From {
	t := reflect.Indirect(reflect.ValueOf(arg)).Type()
	if t.Kind() != reflect.Struct {
		panic(fmt.Errorf("From: argument must be struct (or that pointer) type, got %v", t))
	}
	return &From{TableName: ToSnakeCase(t.Name())}
}

// Where returns a new Condition of "WHERE" clause.
func (db *DB) Where(cond interface{}, args ...interface{}) *Condition {
	return newCondition(db.dialect).Where(cond, args...)
}

// OrderBy returns a new Condition of "ORDER BY" clause.
func (db *DB) OrderBy(column string, order Order) *Condition {
	return newCondition(db.dialect).OrderBy(column, order)
}

// Limit returns a new Condition of "LIMIT" clause.
func (db *DB) Limit(lim int) *Condition {
	return newCondition(db.dialect).Limit(lim)
}

// Offset returns a new Condition of "OFFSET" clause.
func (db *DB) Offset(offset int) *Condition {
	return newCondition(db.dialect).Offset(offset)
}

// Distinct returns a representation object of "DISTINCT" statement.
func (db *DB) Distinct(columns ...string) *Distinct {
	return &Distinct{columns: columns}
}

// Join returns a new JoinCondition of "JOIN" clause.
func (db *DB) Join(table interface{}) *JoinCondition {
	return (&JoinCondition{d: db.dialect}).Join(table)
}

func (db *DB) LeftJoin(table interface{}) *JoinCondition {
	return (&JoinCondition{d: db.dialect}).LeftJoin(table)
}

// Count returns "COUNT" function.
func (db *DB) Count(column ...interface{}) *Function {
	switch len(column) {
	case 0, 1:
		// do nothing.
	default:
		panic(fmt.Errorf("Count: a number of argument must be 0 or 1, got %v", len(column)))
	}
	return &Function{
		Name: "COUNT",
		Args: column,
	}
}

const (
	dbTag        = "db"
	dbColumnTag  = "column"
	dbDefaultTag = "default"
	dbSizeTag    = "size"
	skipTag      = "-"
)

// CreateTable creates the table into database.
// If table isn't direct/indirect struct, it returns error.
func (db *DB) CreateTable(table interface{}) error {
	return db.createTable(table, false)
}

// CreateTableIfNotExists creates the table into database if table isn't exists.
// If table isn't direct/indirect struct, it returns error.
func (db *DB) CreateTableIfNotExists(table interface{}) error {
	return db.createTable(table, true)
}

func (db *DB) createTable(table interface{}, ifNotExists bool) error {
	_, t, tableName, err := db.tableValueOf("CreateTable", table)
	if err != nil {
		return err
	}
	var fields []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if db.hasSkipTag(&field) {
			continue
		}
		var options []string
		autoIncrement := false
		for _, tag := range db.tagsFromField(&field) {
			switch tag {
			case "pk":
				options = append(options, "PRIMARY KEY")
				switch field.Type.Kind() {
				case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					options = append(options, db.dialect.AutoIncrement())
					autoIncrement = true
				}
			case "notnull":
				options = append(options, "NOT NULL")
			case "unique":
				options = append(options, "UNIQUE")
			default:
				return fmt.Errorf(`CreateTable: unsupported field tag: "%v"`, tag)
			}
		}
		size, err := db.sizeFromTag(&field)
		if err != nil {
			return err
		}
		line := append([]string{
			db.dialect.Quote(db.columnFromTag(field)),
			db.dialect.SQLType(reflect.Zero(field.Type).Interface(), autoIncrement, size),
		}, options...)
		def, err := db.defaultFromTag(&field)
		if err != nil {
			return err
		}
		if def != "" {
			line = append(line, def)
		}
		fields = append(fields, strings.Join(line, " "))
	}
	var query string
	if ifNotExists {
		query = "CREATE TABLE IF NOT EXISTS %s (%s)"
	} else {
		query = "CREATE TABLE %s (%s)"
	}
	query = fmt.Sprintf(query, db.dialect.Quote(tableName), strings.Join(fields, ", "))
	if _, err := db.exec(query); err != nil {
		return err
	}
	return nil
}

// DropTable removes the table from database.
// If table isn't direct/indirect struct, it returns error.
func (db *DB) DropTable(table interface{}) error {
	_, _, tableName, err := db.tableValueOf("DropTable", table)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("DROP TABLE %s", db.dialect.Quote(tableName))
	if _, err = db.exec(query); err != nil {
		return err
	}
	return nil
}

// Update updates the one record.
// The obj must be struct, and must have field that specified "pk" struct tag.
// Update will try to update record which searched by value of primary key in obj.
// Update returns the number of rows affected by an update.
func (db *DB) Update(obj interface{}) (affected int64, err error) {
	rv, rtype, tableName, err := db.tableValueOf("Update", obj)
	if err != nil {
		return -1, err
	}
	fieldIndexes := db.collectFieldIndexes(rtype)
	pkIdx := db.findPKIndex(rtype)
	if pkIdx == -1 {
		return -1, fmt.Errorf(`Update: fields of struct doesn't have primary key: "pk" struct tag must be specified for update`)
	}
	sets := make([]string, len(fieldIndexes))
	var args []interface{}
	for i, n := range fieldIndexes {
		col := ToSnakeCase(rtype.Field(n).Name)
		sets[i] = fmt.Sprintf("%s = %s", db.dialect.Quote(col), db.dialect.PlaceHolder(i))
		args = append(args, rv.Field(n).Interface())
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s",
		db.dialect.Quote(tableName),
		strings.Join(sets, ", "),
		db.dialect.Quote(db.columnFromTag(rtype.Field(pkIdx))),
		db.dialect.PlaceHolder(len(fieldIndexes)))
	args = append(args, rv.Field(pkIdx).Interface())
	result, err := db.exec(query, args...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

// Insert inserts the records to the database table.
// The obj must be struct (or that pointer) or slice of struct. If struct have a field which specified
// "pk" struct tag, it won't be used to as an insert value.
// Insert returns the number of rows affected by an insert.
func (db *DB) Insert(obj interface{}) (affected int64, err error) {
	objs, rtype, tableName, err := db.tableObjs("Insert", obj)
	if err != nil {
		return -1, err
	}
	if len(objs) < 1 {
		return 0, nil
	}
	fieldIndexes := db.collectFieldIndexes(rtype)
	cols := make([]string, len(fieldIndexes))
	for i, n := range fieldIndexes {
		cols[i] = db.dialect.Quote(ToSnakeCase(rtype.Field(n).Name))
	}
	var args []interface{}
	for _, obj := range objs {
		rv := reflect.Indirect(reflect.ValueOf(obj))
		for _, n := range fieldIndexes {
			args = append(args, rv.Field(n).Interface())
		}
	}
	holders := make([]string, len(cols))
	for i := 0; i < len(holders); i++ {
		holders[i] = db.dialect.PlaceHolder(i)
	}
	values := strings.Repeat(fmt.Sprintf("(%s), ", strings.Join(holders, ", ")), len(objs))
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		db.dialect.Quote(tableName),
		strings.Join(cols, ", "),
		values[:len(values)-2], // truncate the extra ", ".
	)
	result, err := db.exec(query, args...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

// Delete deletes the records from database table.
// The obj must be struct (or that pointer) or slice of struct, and must have field that specified "pk" struct tag.
// Delete will try to delete record which searched by value of primary key in obj.
// Delete returns teh number of rows affected by a delete.
func (db *DB) Delete(obj interface{}) (affected int64, err error) {
	objs, rtype, tableName, err := db.tableObjs("Delete", obj)
	if err != nil {
		return -1, err
	}
	if len(objs) < 1 {
		return 0, nil
	}
	pkIdx := db.findPKIndex(rtype)
	if pkIdx == -1 {
		return -1, fmt.Errorf(`Delete: fields of struct doesn't have primary key: "pk" struct tag must be specified for delete`)
	}
	var args []interface{}
	for _, obj := range objs {
		rv := reflect.Indirect(reflect.ValueOf(obj))
		args = append(args, rv.Field(pkIdx).Interface())
	}
	holders := make([]string, len(args))
	for i := 0; i < len(holders); i++ {
		holders[i] = db.dialect.PlaceHolder(i)
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
		db.dialect.Quote(tableName),
		db.dialect.Quote(db.columnFromTag(rtype.Field(pkIdx))),
		strings.Join(holders, ", "))
	result, err := db.exec(query, args...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

// Begin starts a transaction.
func (db *DB) Begin() error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	db.m.Lock()
	defer db.m.Unlock()
	db.tx = tx
	return nil
}

// Commit commits the transaction.
func (db *DB) Commit() error {
	db.m.Lock()
	defer db.m.Unlock()
	err := db.tx.Commit()
	db.tx = nil
	return err
}

// Rollback rollbacks the transaction.
func (db *DB) Rollback() error {
	db.m.Lock()
	defer db.m.Unlock()
	err := db.tx.Rollback()
	db.tx = nil
	return err
}

// Raw returns a value that is wrapped with Raw.
func (db *DB) Raw(v interface{}) Raw {
	return Raw(&v)
}

// Close closes the database.
func (db *DB) Close() error {
	return db.db.Close()
}

// Quote returns a quoted s.
// It is for a column name, not a value.
func (db *DB) Quote(s string) string {
	return db.dialect.Quote(s)
}

// DB returns a *sql.DB that is associated to DB.
func (db *DB) DB() *sql.DB {
	return db.db
}

// SetLogOutput sets output destination for logging.
// If w is nil, it unsets output of logging.
func (db *DB) SetLogOutput(w io.Writer) {
	if w == nil {
		db.logger = defaultLogger
	} else {
		db.logger = &templateLogger{w: w, t: defaultLoggerTemplate}
	}
}

// SetLogFormat sets format for logging.
//
// Format syntax uses Go's template. And you can use the following data object in that template.
//
//     - .time        time.Time object in current time.
//     - .duration    Processing time of SQL. It will format to "%.2fms".
//     - .query       string of SQL query. If it using placeholder,
//                    placeholder parameters will append to the end of query.
//
// The default format is:
//
//     [{{.time.Format "2006-01-02 15:04:05"}}] [{{.duration}}] {{.query}}
func (db *DB) SetLogFormat(format string) error {
	return db.logger.SetFormat(format)
}

// selectToSlice returns a slice value fetched from rows.
func (db *DB) selectToSlice(rows *sql.Rows, rv *reflect.Value) (*reflect.Value, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	t := rv.Type().Elem()
	names := make([]string, len(columns))
	for i, column := range columns {
		name := db.fieldName(t, column)
		if name == "" {
			return nil, fmt.Errorf("`%v` field isn't defined in %v", ToCamelCase(column), t)
		}
		names[i] = name
	}
	dest := make([]interface{}, len(columns))
	var result []reflect.Value
	for rows.Next() {
		v := reflect.New(t).Elem()
		for i, name := range names {
			field := v.FieldByName(name)
			dest[i] = field.Addr().Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slice := reflect.MakeSlice(reflect.SliceOf(t), len(result), len(result))
	for i, v := range result {
		slice.Index(i).Set(v)
	}
	return &slice, nil
}

// selectToValue returns a single value fetched from rows.
func (db *DB) selectToValue(rows *sql.Rows, rv *reflect.Value) (*reflect.Value, error) {
	dest := reflect.New(rv.Type()).Elem()
	if rows.Next() {
		if err := rows.Scan(dest.Addr().Interface()); err != nil {
			return nil, err
		}
	}
	return &dest, nil
}

func (db *DB) classify(tableName string, args []interface{}) (column, from string, conditions []*Condition, err error) {
	if len(args) == 0 {
		return ColumnName(db.dialect, tableName, "*"), tableName, nil, nil
	}
	offset := 1
	switch t := args[0].(type) {
	case string:
		if t != "" {
			column = ColumnName(db.dialect, tableName, t)
		}
	case []string:
		column = db.columns(tableName, ToInterfaceSlice(t))
	case *Distinct:
		column = fmt.Sprintf("DISTINCT %s", db.columns(tableName, ToInterfaceSlice(t.columns)))
	case *Function:
		var col string
		if len(t.Args) == 0 {
			col = "*"
		} else {
			col = db.columns(tableName, t.Args)
		}
		column = fmt.Sprintf("%s(%s)", t.Name, col)
	default:
		offset--
	}
	for i := offset; i < len(args); i++ {
		switch t := args[i].(type) {
		case *Condition:
			t.tableName = tableName
			conditions = append(conditions, t)
		case string, []string:
			return "", "", nil, fmt.Errorf("argument of %T type must be before the *Condition arguments", t)
		case *From:
			// ignore.
		case *Function:
			return "", "", nil, fmt.Errorf("%s function must be specified to the first argument", t.Name)
		default:
			return "", "", nil, fmt.Errorf("unsupported argument type: %T", t)
		}
	}
	if column == "" {
		column = ColumnName(db.dialect, tableName, "*")
	}
	return column, tableName, conditions, nil
}

// columns returns the comma-separated column name with quoted.
func (db *DB) columns(tableName string, columns []interface{}) string {
	if len(columns) == 0 {
		return ColumnName(db.dialect, tableName, "*")
	}
	names := make([]string, len(columns))
	for i, col := range columns {
		switch c := col.(type) {
		case Raw:
			names[i] = fmt.Sprint(*c)
		case string:
			names[i] = ColumnName(db.dialect, tableName, c)
		case *Distinct:
			names[i] = fmt.Sprintf("DISTINCT %s", db.columns(tableName, ToInterfaceSlice(c.columns)))
		default:
			panic(fmt.Errorf("column name must be string, Raw or *Distinct, got %T", c))
		}
	}
	return strings.Join(names, ", ")
}

// fieldName returns an actual field name from column name.
func (db *DB) fieldName(t reflect.Type, columnName string) string {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if candidate := db.columnFromTag(field); candidate == columnName {
			return ToCamelCase(field.Name)
		}
	}
	return ""
}

// tagsFromField returns a slice of option strings.
func (db *DB) tagsFromField(field *reflect.StructField) (options []string) {
	if db.hasSkipTag(field) {
		return nil
	}
	for _, tag := range strings.Split(field.Tag.Get(dbTag), ",") {
		if t := strings.ToLower(strings.TrimSpace(tag)); t != "" {
			options = append(options, t)
		}
	}
	return options
}

// hasSkipTag returns whether the struct field has the "-" tag.
func (db *DB) hasSkipTag(field *reflect.StructField) bool {
	if field.Tag.Get(dbTag) == skipTag {
		return true
	}
	return false
}

// hasPKTag returns whether the struct field has the "pk" tag.
func (db *DB) hasPKTag(field *reflect.StructField) bool {
	for _, tag := range db.tagsFromField(field) {
		if tag == "pk" {
			return true
		}
	}
	return false
}

// collectFieldIndexes returns the indexes of field which doesn't have skip tag and pk tag.
func (db *DB) collectFieldIndexes(typ reflect.Type) (indexes []int) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !(db.hasSkipTag(&field) || db.hasPKTag(&field)) {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

// findPKIndex returns the index of field of primary key.
// If not found, it returns -1.
func (db *DB) findPKIndex(typ reflect.Type) int {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if db.hasPKTag(&field) {
			return i
		}
	}
	return -1
}

// sizeFromTag returns a size from tag.
// If "size" tag specified to struct field, it will converted to uint64 and returns it.
// If it doesn't specify, it returns 0.
// If value of "size" tag cannot convert to uint64, it returns 0 and error.
func (db *DB) sizeFromTag(field *reflect.StructField) (size uint64, err error) {
	if s := field.Tag.Get(dbSizeTag); s != "" {
		size, err = strconv.ParseUint(s, 10, 64)
	}
	return size, err
}

// columnFromTag returns the column name.
// If "column" tag specified to struct field, returns it.
// Otherwise, it returns snake-cased field name as column name.
func (db *DB) columnFromTag(field reflect.StructField) string {
	col := field.Tag.Get(dbColumnTag)
	if col == "" {
		return ToSnakeCase(field.Name)
	}
	return col
}

// defaultFromTag returns a "DEFAULT ..." keyword.
// If "default" tag specified to struct field, it use as the default value.
// If it doesn't specify, it returns empty string.
func (db *DB) defaultFromTag(field *reflect.StructField) (string, error) {
	def := field.Tag.Get(dbDefaultTag)
	if def == "" {
		return "", nil
	}
	switch field.Type.Kind() {
	case reflect.Bool:
		b, err := strconv.ParseBool(def)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("DEFAULT %v", db.dialect.FormatBool(b)), nil
	}
	return fmt.Sprintf("DEFAULT %v", def), nil
}

func (db *DB) tableObjs(name string, obj interface{}) (objs []interface{}, rtype reflect.Type, tableName string, err error) {
	switch v := reflect.Indirect(reflect.ValueOf(obj)); v.Kind() {
	case reflect.Slice:
		if v.Len() < 1 {
			return objs, nil, "", nil
		}
		for i := 0; i < v.Len(); i++ {
			sv := v.Index(i)
			if sv.Kind() != reflect.Struct {
				return nil, nil, "", fmt.Errorf("%s: type of slice must be struct or that pointer if slice argument given, got %v", name, sv.Type())
			}
			objs = append(objs, sv.Interface())
		}
	case reflect.Struct:
		objs = append(objs, v.Interface())
	default:
		return nil, nil, "", fmt.Errorf("%s: argument must be struct (or that pointer) or slice of struct, got %T", name, obj)
	}
	_, rtype, tableName, err = db.tableValueOf(name, objs[0])
	return objs, rtype, tableName, err
}

func (db *DB) tableValueOf(name string, table interface{}) (rv reflect.Value, rt reflect.Type, tableName string, err error) {
	rv = reflect.Indirect(reflect.ValueOf(table))
	rt = rv.Type()
	if rt.Kind() != reflect.Struct {
		return rv, rt, "", fmt.Errorf("%s: a table must be struct type, got %v", name, rt)
	}
	tableName = ToSnakeCase(rt.Name())
	if tableName == "" {
		return rv, rt, "", fmt.Errorf("%s: a table isn't named", name)
	}
	return rv, rt, tableName, nil
}

func (db *DB) query(query string, args ...interface{}) (*sql.Rows, error) {
	defer db.logger.Print(now(), query, args...)
	stmt, err := db.prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Query(args...)
}

func (db *DB) exec(query string, args ...interface{}) (sql.Result, error) {
	defer db.logger.Print(now(), query, args...)
	stmt, err := db.prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Exec(args...)
}

func (db *DB) prepare(query string) (*sql.Stmt, error) {
	db.m.Lock()
	defer db.m.Unlock()
	if db.tx == nil {
		return db.db.Prepare(query)
	} else {
		return db.tx.Prepare(query)
	}
}

type selectFunc func(*sql.Rows, *reflect.Value) (*reflect.Value, error)

// Raw represents a raw value.
// Raw value won't quoted.
type Raw *interface{}

// From represents a "FROM" statement.
type From struct {
	TableName string
}

// Distinct represents a "DISTINCT" statement.
type Distinct struct {
	columns []string
}

// Function represents a function of SQL.
type Function struct {
	// A function name.
	Name string

	// function arguments (optional).
	Args []interface{}
}

// Order represents a keyword for the "ORDER" clause of SQL.
type Order string

const (
	ASC  Order = "ASC"
	DESC Order = "DESC"
)

func (o Order) String() string {
	return string(o)
}

// Clause represents a clause of SQL.
type Clause uint

const (
	Where Clause = iota
	And
	Or
	OrderBy
	Limit
	Offset
	In
	Like
	Between
	Join
	LeftJoin
	IsNull
	IsNotNull
)

func (c Clause) String() string {
	if int(c) >= len(clauseStrings) {
		panic(fmt.Errorf("Clause %v is not defined", c))
	}
	return clauseStrings[c]
}

var clauseStrings = []string{
	Where:     "WHERE",
	And:       "AND",
	Or:        "OR",
	OrderBy:   "ORDER BY",
	Limit:     "LIMIT",
	Offset:    "OFFSET",
	In:        "IN",
	Like:      "LIKE",
	Between:   "BETWEEN",
	Join:      "JOIN",
	LeftJoin:  "LEFT JOIN",
	IsNull:    "IS NULL",
	IsNotNull: "IS NOT NULL",
}

// column represents a column name in query.
type column struct {
	table string // table name (optional).
	name  string // column name.
}

// expr represents a expression in query.
type expr struct {
	op     string      // operator.
	column *column     // column name.
	value  interface{} // value.
}

// orderBy represents a "ORDER BY" query.
type orderBy struct {
	column string // column name.
	order  Order  // direction.
}

// between represents a "BETWEEN" query.
type between struct {
	from interface{}
	to   interface{}
}

// Condition represents a condition for query.
type Condition struct {
	d         Dialect
	parts     parts  // parts of the query.
	tableName string // table name (optional).
}

// newCondition returns a new Condition with Dialect.
func newCondition(d Dialect) *Condition {
	return &Condition{d: d}
}

// Where adds "WHERE" clause to the Condition and returns it for method chain.
func (c *Condition) Where(cond interface{}, args ...interface{}) *Condition {
	return c.appendQueryByCondOrExpr("Where", 0, Where, cond, args...)
}

// And adds "AND" operator to the Condition and returns it for method chain.
func (c *Condition) And(cond interface{}, args ...interface{}) *Condition {
	return c.appendQueryByCondOrExpr("And", 100, And, cond, args...)
}

// Or adds "OR" operator to the Condition and returns it for method chain.
func (c *Condition) Or(cond interface{}, args ...interface{}) *Condition {
	return c.appendQueryByCondOrExpr("Or", 100, Or, cond, args...)
}

// In adds "IN" clause to the Condition and returns it for method chain.
func (c *Condition) In(arg interface{}, args ...interface{}) *Condition {
	return c.appendQuery(100, In, append([]interface{}{arg}, args...))
}

// Like adds "LIKE" clause to the Condition and returns it for method chain.
func (c *Condition) Like(arg string) *Condition {
	return c.appendQuery(100, Like, arg)
}

// Between adds "BETWEEN ... AND ..." clause to the Condition and returns it for method chain.
func (c *Condition) Between(from, to interface{}) *Condition {
	return c.appendQuery(100, Between, &between{from, to})
}

// IsNull adds "IS NULL" clause to the Condition and returns it for method chain.
func (c *Condition) IsNull() *Condition {
	return c.appendQuery(100, IsNull, nil)
}

// IsNotNull adds "IS NOT NULL" clause to the Condition and returns it for method chain.
func (c *Condition) IsNotNull() *Condition {
	return c.appendQuery(100, IsNotNull, nil)
}

// OrderBy adds "ORDER BY" clause to the Condition and returns it for method chain.
func (c *Condition) OrderBy(column string, order Order) *Condition {
	return c.appendQuery(300, OrderBy, &orderBy{column: column, order: order})
}

// Limit adds "LIMIT" clause to the Condition and returns it for method chain.
func (c *Condition) Limit(lim int) *Condition {
	return c.appendQuery(500, Limit, lim)
}

// Offset adds "OFFSET" clause to the Condition and returns it for method chain.
func (c *Condition) Offset(offset int) *Condition {
	return c.appendQuery(700, Offset, offset)
}

func (c *Condition) appendQuery(priority int, clause Clause, expr interface{}, args ...interface{}) *Condition {
	c.parts = append(c.parts, part{
		clause:   clause,
		expr:     expr,
		priority: priority,
	})
	return c
}

func (c *Condition) appendQueryByCondOrExpr(name string, order int, clause Clause, cond interface{}, args ...interface{}) *Condition {
	switch t := cond.(type) {
	case string, *Condition:
		args = append([]interface{}{t}, args...)
	default:
		v := reflect.Indirect(reflect.ValueOf(t))
		if v.Kind() != reflect.Struct {
			panic(fmt.Errorf("%s: first argument must be string or struct, got %T", name, t))
		}
		args = append([]interface{}{ToSnakeCase(v.Type().Name())}, args...)
	}
	switch len(args) {
	case 1: // Where(Where("id", "=", 1))
		switch t := args[0].(type) {
		case *Condition:
			cond = t
		case string:
			cond = &column{name: t}
		default:
			panic(fmt.Errorf("%s: first argument must be string or *Condition if args not given, got %T", name, t))
		}
	case 2: // Where(&Table{}, "id")
		cond = &column{
			table: fmt.Sprint(args[0]),
			name:  fmt.Sprint(args[1]),
		}
	case 3: // Where("id", "=", 1)
		cond = &expr{
			op: fmt.Sprint(args[1]),
			column: &column{
				name: fmt.Sprint(args[0]),
			},
			value: args[2],
		}
	case 4: // Where(&Table{}, "id", "=", 1)
		cond = &expr{
			op: fmt.Sprint(args[2]),
			column: &column{
				table: fmt.Sprint(args[0]),
				name:  fmt.Sprint(args[1]),
			},
			value: args[3],
		}
	default:
		panic(fmt.Errorf("%s: arguments expect between 1 and 4, got %v", name, len(args)))
	}
	return c.appendQuery(order, clause, cond)
}

func (c *Condition) build(numHolders int, inner bool) (queries []string, args []interface{}) {
	sort.Sort(c.parts)
	for _, p := range c.parts {
		if !(inner && p.clause == Where) {
			queries = append(queries, p.clause.String())
		}
		switch e := p.expr.(type) {
		case *expr:
			numHolders++
			col := ColumnName(c.d, e.column.table, e.column.name)
			queries = append(queries, col, e.op, c.d.PlaceHolder(numHolders))
			args = append(args, e.value)
		case *orderBy:
			queries = append(queries, c.d.Quote(e.column), e.order.String())
		case *column:
			col := ColumnName(c.d, e.table, e.name)
			queries = append(queries, col)
		case []interface{}:
			holders := make([]string, len(e))
			for i := 0; i < len(e); i++ {
				numHolders++
				holders[i] = c.d.PlaceHolder(numHolders)
			}
			queries = append(queries, "(", strings.Join(holders, ", "), ")")
			args = append(args, e...)
		case *between:
			queries = append(queries, c.d.PlaceHolder(numHolders+1), "AND", c.d.PlaceHolder(numHolders+2))
			args = append(args, e.from, e.to)
			numHolders += 2
		case *Condition:
			q, a := e.build(numHolders, true)
			queries = append(append(append(queries, "("), q...), ")")
			args = append(args, a...)
		case *JoinCondition:
			queries = append(queries,
				c.d.Quote(e.tableName), "ON",
				ColumnName(c.d, c.tableName, e.left), e.op, ColumnName(c.d, e.tableName, e.right))
		case nil:
			// ignore.
		default:
			numHolders++
			queries = append(queries, c.d.PlaceHolder(numHolders))
			args = append(args, e)
		}
	}
	return queries, args
}

// JoinCondition represents a condition of "JOIN" query.
type JoinCondition struct {
	d         Dialect
	tableName string // A table name of 'to join'.
	op        string // A operator of expression in "ON" clause.
	left      string // A left column name of operator.
	right     string // A right column name of operator.
	clause    Clause // A type of join clause ("JOIN" or "LEFT JOIN")
}

// Join adds table name to the JoinCondition of "JOIN".
// If table isn't direct/indirect struct type, it panics.
func (jc *JoinCondition) Join(table interface{}) *JoinCondition {
	return jc.join(Join, table)
}

// LeftJoin adds table name to the JoinCondition of "LEFT JOIN".
// If table isn't direct/indirect struct type, it panics.
func (jc *JoinCondition) LeftJoin(table interface{}) *JoinCondition {
	return jc.join(LeftJoin, table)
}

// On adds "[LEFT] JOIN ... ON" clause to the Condition and returns it for method chain.
func (jc *JoinCondition) On(lcolumn string, args ...string) *Condition {
	switch len(args) {
	case 0:
		jc.left, jc.op, jc.right = lcolumn, "=", lcolumn
	case 2:
		jc.left, jc.op, jc.right = lcolumn, args[0], args[1]
	default:
		panic(fmt.Errorf("On: arguments expect 1 or 3, got %v", len(args)+1))
	}
	c := newCondition(jc.d)
	c.parts = append(c.parts, part{
		clause:   jc.clause,
		expr:     jc,
		priority: -100,
	})
	return c
}

func (jc *JoinCondition) join(joinClause Clause, table interface{}) *JoinCondition {
	t := reflect.Indirect(reflect.ValueOf(table)).Type()
	if t.Kind() != reflect.Struct {
		panic(fmt.Errorf("%v: a table must be struct type, got %v", joinClause, t))
	}
	jc.tableName = ToSnakeCase(t.Name())
	jc.clause = joinClause
	return jc
}

// part represents a part of query.
type part struct {
	clause Clause
	expr   interface{}

	// a order for sort. A lower value is a high-priority.
	priority int
}

// parts is for sort.Interface.
type parts []part

func (ps parts) Len() int {
	return len(ps)
}

func (ps parts) Less(i, j int) bool {
	return ps[i].priority < ps[j].priority
}

func (ps parts) Swap(i, j int) {
	ps[i], ps[j] = ps[j], ps[i]
}
