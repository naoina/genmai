// Copyright 2014 Naoya Inada. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

// Package genmai provides simple, better and easy-to-use Object-Relational Mapper.
package genmai

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/naoina/go-stringutil"
)

var ErrTxDone = errors.New("genmai: transaction hasn't been started or already committed or rolled back")

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
	for rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
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
	ptrN := 0
	switch rv.Kind() {
	case reflect.Slice:
		t := rv.Type().Elem()
		for ; t.Kind() == reflect.Ptr; ptrN++ {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return fmt.Errorf("Select: argument of slice must be slice of struct, but %v", rv.Type())
		}
		if tableName == "" {
			tableName = db.tableName(t)
		}
		selectFunc = db.selectToSlice
	case reflect.Invalid:
		return fmt.Errorf("Select: nil pointer dereference")
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
	query := strings.Join(queries, " ")
	stmt, err := db.prepare(query, values...)
	if err != nil {
		return err
	}
	defer stmt.Close()
	rows, err := stmt.Query(values...)
	if err != nil {
		return err
	}
	defer rows.Close()
	value, err := selectFunc(rows, rv.Type())
	if err != nil {
		return err
	}
	rv.Set(value)
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
	return &From{TableName: db.tableName(t)}
}

// Where returns a new Condition of "WHERE" clause.
func (db *DB) Where(cond interface{}, args ...interface{}) *Condition {
	return newCondition(db).Where(cond, args...)
}

// OrderBy returns a new Condition of "ORDER BY" clause.
func (db *DB) OrderBy(table interface{}, column interface{}, order ...interface{}) *Condition {
	return newCondition(db).OrderBy(table, column, order...)
}

// Limit returns a new Condition of "LIMIT" clause.
func (db *DB) Limit(lim int) *Condition {
	return newCondition(db).Limit(lim)
}

// Offset returns a new Condition of "OFFSET" clause.
func (db *DB) Offset(offset int) *Condition {
	return newCondition(db).Offset(offset)
}

// Distinct returns a representation object of "DISTINCT" statement.
func (db *DB) Distinct(columns ...string) *Distinct {
	return &Distinct{columns: columns}
}

// Join returns a new JoinCondition of "JOIN" clause.
func (db *DB) Join(table interface{}) *JoinCondition {
	return (&JoinCondition{db: db}).Join(table)
}

func (db *DB) LeftJoin(table interface{}) *JoinCondition {
	return (&JoinCondition{db: db}).LeftJoin(table)
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
	fields, err := db.collectTableFields(t)
	if err != nil {
		return err
	}
	var query string
	if ifNotExists {
		query = "CREATE TABLE IF NOT EXISTS %s (%s)"
	} else {
		query = "CREATE TABLE %s (%s)"
	}
	query = fmt.Sprintf(query, db.dialect.Quote(tableName), strings.Join(fields, ", "))
	stmt, err := db.prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if _, err := stmt.Exec(); err != nil {
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
	stmt, err := db.prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if _, err = stmt.Exec(); err != nil {
		return err
	}
	return nil
}

// CreateIndex creates the index into database.
// If table isn't direct/indirect struct, it returns error.
func (db *DB) CreateIndex(table interface{}, name string, names ...string) error {
	return db.createIndex(table, false, name, names...)
}

// CreateUniqueIndex creates the unique index into database.
// If table isn't direct/indirect struct, it returns error.
func (db *DB) CreateUniqueIndex(table interface{}, name string, names ...string) error {
	return db.createIndex(table, true, name, names...)
}

func (db *DB) createIndex(table interface{}, unique bool, name string, names ...string) error {
	_, _, tableName, err := db.tableValueOf("CreateIndex", table)
	if err != nil {
		return err
	}
	names = append([]string{name}, names...)
	indexes := make([]string, len(names))
	for i, name := range names {
		indexes[i] = db.dialect.Quote(name)
	}
	indexName := strings.Join(append([]string{"index", tableName}, names...), "_")
	var query string
	if unique {
		query = "CREATE UNIQUE INDEX %s ON %s (%s)"
	} else {
		query = "CREATE INDEX %s ON %s (%s)"
	}
	query = fmt.Sprintf(query,
		db.dialect.Quote(indexName),
		db.dialect.Quote(tableName),
		strings.Join(indexes, ", "))
	stmt, err := db.prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if _, err := stmt.Exec(); err != nil {
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
	if hook, ok := obj.(BeforeUpdater); ok {
		if err := hook.BeforeUpdate(); err != nil {
			return -1, err
		}
	}
	fieldIndexes := db.collectFieldIndexes(rtype, nil)
	pkIdx := db.findPKIndex(rtype, nil)
	if len(pkIdx) < 1 {
		return -1, fmt.Errorf(`Update: fields of struct doesn't have primary key: "pk" struct tag must be specified for update`)
	}
	sets := make([]string, len(fieldIndexes))
	var args []interface{}
	for i, index := range fieldIndexes {
		col := db.columnFromTag(rtype.FieldByIndex(index))
		sets[i] = fmt.Sprintf("%s = %s", db.dialect.Quote(col), db.dialect.PlaceHolder(i))
		args = append(args, rv.FieldByIndex(index).Interface())
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s",
		db.dialect.Quote(tableName),
		strings.Join(sets, ", "),
		db.dialect.Quote(db.columnFromTag(rtype.FieldByIndex(pkIdx))),
		db.dialect.PlaceHolder(len(fieldIndexes)))
	args = append(args, rv.FieldByIndex(pkIdx).Interface())
	stmt, err := db.prepare(query, args...)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		return -1, err
	}
	affected, _ = result.RowsAffected()
	if hook, ok := obj.(AfterUpdater); ok {
		if err := hook.AfterUpdate(); err != nil {
			return affected, err
		}
	}
	return affected, nil
}

// Insert inserts one or more records to the database table.
// The obj must be pointer to struct or slice of struct. If a struct have a
// field which specified "pk" struct tag on type of autoincrementable, it
// won't be used to as an insert value.
// Insert sets the last inserted id to the primary key of the instance of the given obj if obj is single.
// Insert returns the number of rows affected by insert.
func (db *DB) Insert(obj interface{}) (affected int64, err error) {
	objs, rtype, tableName, err := db.tableObjs("Insert", obj)
	if err != nil {
		return -1, err
	}
	if len(objs) < 1 {
		return 0, nil
	}
	for _, obj := range objs {
		if hook, ok := obj.(BeforeInserter); ok {
			if err := hook.BeforeInsert(); err != nil {
				return -1, err
			}
		}
	}
	fieldIndexes := db.collectFieldIndexes(rtype, nil)
	cols := make([]string, len(fieldIndexes))
	for i, index := range fieldIndexes {
		cols[i] = db.dialect.Quote(db.columnFromTag(rtype.FieldByIndex(index)))
	}
	var args []interface{}
	for _, obj := range objs {
		rv := reflect.Indirect(reflect.ValueOf(obj))
		for _, index := range fieldIndexes {
			args = append(args, rv.FieldByIndex(index).Interface())
		}
	}
	numHolders := 0
	values := make([]string, len(objs))
	holders := make([]string, len(cols))
	for i := 0; i < len(values); i++ {
		for j := 0; j < len(holders); j++ {
			holders[j] = db.dialect.PlaceHolder(numHolders)
			numHolders++
		}
		values[i] = fmt.Sprintf("(%s)", strings.Join(holders, ", "))
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		db.dialect.Quote(tableName),
		strings.Join(cols, ", "),
		strings.Join(values, ", "),
	)
	stmt, err := db.prepare(query, args...)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		return -1, err
	}
	affected, _ = result.RowsAffected()
	if len(objs) == 1 {
		if pkIdx := db.findPKIndex(rtype, nil); len(pkIdx) > 0 {
			field := rtype.FieldByIndex(pkIdx)
			if db.isAutoIncrementable(&field) {
				id, err := db.LastInsertId()
				if err != nil {
					return affected, err
				}
				rv := reflect.Indirect(reflect.ValueOf(objs[0])).FieldByIndex(pkIdx)
				for rv.Kind() == reflect.Ptr {
					rv = rv.Elem()
				}
				rv.Set(reflect.ValueOf(id).Convert(rv.Type()))
			}
		}
	}
	for _, obj := range objs {
		if hook, ok := obj.(AfterInserter); ok {
			if err := hook.AfterInsert(); err != nil {
				return affected, err
			}
		}
	}
	return affected, nil
}

// Delete deletes the records from database table.
// The obj must be pointer to struct or slice of struct, and must have field that specified "pk" struct tag.
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
	for _, obj := range objs {
		if hook, ok := obj.(BeforeDeleter); ok {
			if err := hook.BeforeDelete(); err != nil {
				return -1, err
			}
		}
	}
	pkIdx := db.findPKIndex(rtype, nil)
	if len(pkIdx) < 1 {
		return -1, fmt.Errorf(`Delete: fields of struct doesn't have primary key: "pk" struct tag must be specified for delete`)
	}
	var args []interface{}
	for _, obj := range objs {
		rv := reflect.Indirect(reflect.ValueOf(obj))
		args = append(args, rv.FieldByIndex(pkIdx).Interface())
	}
	holders := make([]string, len(args))
	for i := 0; i < len(holders); i++ {
		holders[i] = db.dialect.PlaceHolder(i)
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
		db.dialect.Quote(tableName),
		db.dialect.Quote(db.columnFromTag(rtype.FieldByIndex(pkIdx))),
		strings.Join(holders, ", "))
	stmt, err := db.prepare(query, args...)
	if err != nil {
		return -1, err
	}
	defer stmt.Close()
	result, err := stmt.Exec(args...)
	if err != nil {
		return -1, err
	}
	affected, _ = result.RowsAffected()
	for _, obj := range objs {
		if hook, ok := obj.(AfterDeleter); ok {
			if err := hook.AfterDelete(); err != nil {
				return affected, err
			}
		}
	}
	return affected, nil
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
// If Begin still not called, or Commit or Rollback already called, Commit returns ErrTxDone.
func (db *DB) Commit() error {
	db.m.Lock()
	defer db.m.Unlock()
	if db.tx == nil {
		return ErrTxDone
	}
	err := db.tx.Commit()
	db.tx = nil
	return err
}

// Rollback rollbacks the transaction.
// If Begin still not called, or Commit or Rollback already called, Rollback returns ErrTxDone.
func (db *DB) Rollback() error {
	db.m.Lock()
	defer db.m.Unlock()
	if db.tx == nil {
		return ErrTxDone
	}
	err := db.tx.Rollback()
	db.tx = nil
	return err
}

func (db *DB) LastInsertId() (int64, error) {
	stmt, err := db.prepare(db.dialect.LastInsertId())
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	var id int64
	return id, stmt.QueryRow().Scan(&id)
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
func (db *DB) selectToSlice(rows *sql.Rows, t reflect.Type) (reflect.Value, error) {
	columns, err := rows.Columns()
	if err != nil {
		return reflect.Value{}, err
	}
	t = t.Elem()
	ptrN := 0
	for ; t.Kind() == reflect.Ptr; ptrN++ {
		t = t.Elem()
	}
	fieldIndexes := make([][]int, len(columns))
	for i, column := range columns {
		index := db.fieldIndexByName(t, column, nil)
		if len(index) < 1 {
			return reflect.Value{}, fmt.Errorf("`%v` field isn't defined in %v or embedded struct", stringutil.ToUpperCamelCase(column), t)
		}
		fieldIndexes[i] = index
	}
	dest := make([]interface{}, len(columns))
	var result []reflect.Value
	for rows.Next() {
		v := reflect.New(t).Elem()
		for i, index := range fieldIndexes {
			field := v.FieldByIndex(index)
			dest[i] = field.Addr().Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return reflect.Value{}, err
		}
		result = append(result, v)
	}
	if err := rows.Err(); err != nil {
		return reflect.Value{}, err
	}
	for i := 0; i < ptrN; i++ {
		t = reflect.PtrTo(t)
	}
	slice := reflect.MakeSlice(reflect.SliceOf(t), len(result), len(result))
	for i, v := range result {
		for j := 0; j < ptrN; j++ {
			v = v.Addr()
		}
		slice.Index(i).Set(v)
	}
	return slice, nil
}

// selectToValue returns a single value fetched from rows.
func (db *DB) selectToValue(rows *sql.Rows, t reflect.Type) (reflect.Value, error) {
	ptrN := 0
	for ; t.Kind() == reflect.Ptr; ptrN++ {
		t = t.Elem()
	}
	dest := reflect.New(t).Elem()
	if rows.Next() {
		if err := rows.Scan(dest.Addr().Interface()); err != nil {
			return reflect.Value{}, err
		}
	}
	for i := 0; i < ptrN; i++ {
		dest = dest.Addr()
	}
	return dest, nil
}

// fieldIndexByName returns the nested field corresponding to the index sequence.
func (db *DB) fieldIndexByName(t reflect.Type, name string, index []int) []int {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if candidate := db.columnFromTag(field); candidate == name {
			return append(index, i)
		}
		if field.Anonymous {
			if idx := db.fieldIndexByName(field.Type, name, append(index, i)); len(idx) > 0 {
				return append(index, idx...)
			}
		}
	}
	return nil
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

func (db *DB) collectTableFields(t reflect.Type) (fields []string, err error) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if IsUnexportedField(field) {
			continue
		}
		if db.hasSkipTag(&field) {
			continue
		}
		if field.Anonymous {
			fs, err := db.collectTableFields(field.Type)
			if err != nil {
				return nil, err
			}
			fields = append(fields, fs...)
			continue
		}
		var options []string
		autoIncrement := false
		for _, tag := range db.tagsFromField(&field) {
			switch tag {
			case "pk":
				options = append(options, "PRIMARY KEY")
				if db.isAutoIncrementable(&field) {
					options = append(options, db.dialect.AutoIncrement())
					autoIncrement = true
				}
			case "unique":
				options = append(options, "UNIQUE")
			default:
				return nil, fmt.Errorf(`CreateTable: unsupported field tag: "%v"`, tag)
			}
		}
		size, err := db.sizeFromTag(&field)
		if err != nil {
			return nil, err
		}
		typName, allowNull := db.dialect.SQLType(reflect.Zero(field.Type).Interface(), autoIncrement, size)
		if !allowNull {
			options = append(options, "NOT NULL")
		}
		line := append([]string{db.dialect.Quote(db.columnFromTag(field)), typName}, options...)
		def, err := db.defaultFromTag(&field)
		if err != nil {
			return nil, err
		}
		if def != "" {
			line = append(line, def)
		}
		fields = append(fields, strings.Join(line, " "))
	}
	return fields, nil
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

// isAutoIncrementable returns whether the struct field is integer.
func (db *DB) isAutoIncrementable(field *reflect.StructField) bool {
	switch field.Type.Kind() {
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

// collectFieldIndexes returns the indexes of field which doesn't have skip tag and pk tag.
func (db *DB) collectFieldIndexes(typ reflect.Type, index []int) (indexes [][]int) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if IsUnexportedField(field) {
			continue
		}
		if !(db.hasSkipTag(&field) || (db.hasPKTag(&field) && db.isAutoIncrementable(&field))) {
			tmp := make([]int, len(index)+1)
			copy(tmp, index)
			tmp[len(tmp)-1] = i
			if field.Anonymous {
				indexes = append(indexes, db.collectFieldIndexes(field.Type, tmp)...)
			} else {
				indexes = append(indexes, tmp)
			}
		}
	}
	return indexes
}

// findPKIndex returns the nested field corresponding to the index sequence of field of primary key.
func (db *DB) findPKIndex(typ reflect.Type, index []int) []int {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if IsUnexportedField(field) {
			continue
		}
		if field.Anonymous {
			if idx := db.findPKIndex(field.Type, append(index, i)); idx != nil {
				return append(index, idx...)
			}
			continue
		}
		if db.hasPKTag(&field) {
			return append(index, i)
		}
	}
	return nil
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

func (db *DB) tableName(t reflect.Type) string {
	if table, ok := reflect.New(t).Interface().(TableNamer); ok {
		return table.TableName()
	}
	return stringutil.ToSnakeCase(t.Name())
}

// columnFromTag returns the column name.
// If "column" tag specified to struct field, returns it.
// Otherwise, it returns snake-cased field name as column name.
func (db *DB) columnFromTag(field reflect.StructField) string {
	col := field.Tag.Get(dbColumnTag)
	if col == "" {
		return stringutil.ToSnakeCase(field.Name)
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
			for sv.Kind() == reflect.Ptr {
				sv = sv.Elem()
			}
			if sv.Kind() == reflect.Interface {
				svk := reflect.Indirect(reflect.ValueOf(sv)).Kind()
				if svk != reflect.Struct {
					goto Error
				}
				objs = append(objs, sv.Interface())
			} else {
				if sv.Kind() != reflect.Struct {
					goto Error
				}
				objs = append(objs, sv.Addr().Interface())
			}
		}
	case reflect.Struct:
		if !v.CanAddr() {
			goto Error
		}
		objs = append(objs, v.Addr().Interface())
	}
	_, rtype, tableName, err = db.tableValueOf(name, objs[0])
	return objs, rtype, tableName, err
Error:
	return nil, nil, "", fmt.Errorf("%s: argument must be pointer to struct or slice of struct, got %T", name, obj)
}

func (db *DB) tableValueOf(name string, table interface{}) (rv reflect.Value, rt reflect.Type, tableName string, err error) {
	rv = reflect.Indirect(reflect.ValueOf(table))
	rt = rv.Type()
	if rt.Kind() != reflect.Struct {
		return rv, rt, "", fmt.Errorf("%s: a table must be struct type, got %v", name, rt)
	}
	tableName = db.tableName(rt)
	if tableName == "" {
		return rv, rt, "", fmt.Errorf("%s: a table isn't named", name)
	}
	return rv, rt, tableName, nil
}

func (db *DB) prepare(query string, args ...interface{}) (*sql.Stmt, error) {
	defer db.logger.Print(now(), query, args...)
	db.m.Lock()
	defer db.m.Unlock()
	if db.tx == nil {
		return db.db.Prepare(query)
	} else {
		return db.tx.Prepare(query)
	}
}

type selectFunc func(*sql.Rows, reflect.Type) (reflect.Value, error)

// TableNamer is an interface that is used to use a different table name.
type TableNamer interface {
	// TableName returns the table name on DB.
	TableName() string
}

// BeforeUpdater is an interface that hook for before Update.
type BeforeUpdater interface {
	// BeforeUpdate called before an update by DB.Update.
	// If it returns error, the update will be cancelled.
	BeforeUpdate() error
}

// AfterUpdater is an interface that hook for after Update.
type AfterUpdater interface {
	// AfterUpdate called after an update by DB.Update.
	AfterUpdate() error
}

// BeforeInserter is an interface that hook for before Insert.
type BeforeInserter interface {
	// BeforeInsert called before an insert by DB.Insert.
	// If it returns error, the insert will be cancelled.
	BeforeInsert() error
}

// AfterInserter is an interface that hook for after Insert.
type AfterInserter interface {
	// AfterInsert called after an insert by DB.Insert.
	AfterInsert() error
}

// BeforeDeleter is an interface that hook for before Delete.
type BeforeDeleter interface {
	// BeforeDelete called before a delete by DB.Delete.
	// If it returns error, the delete will be cancelled.
	BeforeDelete() error
}

// AfterDeleter is an interface that hook for after Delete.
type AfterDeleter interface {
	// AfterDelete called after a delete by DB.Delete.
	AfterDelete() error
}

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
		panic(fmt.Errorf("Clause %v is not defined", uint(c)))
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
	column column // column name.
	order  Order  // direction.
}

// between represents a "BETWEEN" query.
type between struct {
	from interface{}
	to   interface{}
}

// Condition represents a condition for query.
type Condition struct {
	db        *DB
	parts     parts  // parts of the query.
	tableName string // table name (optional).
}

// newCondition returns a new Condition with Dialect.
func newCondition(db *DB) *Condition {
	return &Condition{db: db}
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
func (c *Condition) In(args ...interface{}) *Condition {
	return c.appendQuery(100, In, args)
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
func (c *Condition) OrderBy(table, col interface{}, order ...interface{}) *Condition {
	order = append([]interface{}{table, col}, order...)
	orderbys := make([]orderBy, 0, 1)
	for len(order) > 0 {
		o, rest := order[0], order[1:]
		if _, ok := o.(string); ok {
			if len(rest) < 1 {
				panic(fmt.Errorf("OrderBy: few arguments"))
			}
			// OrderBy("column", genmai.DESC)
			orderbys = append(orderbys, c.orderBy(nil, o, rest[0]))
			order = rest[1:]
			continue
		}
		if len(rest) < 2 {
			panic(fmt.Errorf("OrderBy: few arguments"))
		}
		// OrderBy(tbl{}, "column", genmai.DESC)
		orderbys = append(orderbys, c.orderBy(o, rest[0], rest[1]))
		order = rest[2:]
	}
	return c.appendQuery(300, OrderBy, orderbys)
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
		args = append([]interface{}{c.db.tableName(v.Type())}, args...)
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

func (c *Condition) orderBy(table, col, order interface{}) orderBy {
	o := orderBy{
		column: column{
			name: fmt.Sprint(col),
		},
		order: Order(fmt.Sprint(order)),
	}
	if table != nil {
		rt := reflect.TypeOf(table)
		for rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
		o.column.table = c.db.tableName(rt)
	}
	return o
}

func (c *Condition) build(numHolders int, inner bool) (queries []string, args []interface{}) {
	sort.Sort(c.parts)
	for _, p := range c.parts {
		if !(inner && p.clause == Where) {
			queries = append(queries, p.clause.String())
		}
		switch e := p.expr.(type) {
		case *expr:
			col := ColumnName(c.db.dialect, e.column.table, e.column.name)
			queries = append(queries, col, e.op, c.db.dialect.PlaceHolder(numHolders))
			args = append(args, e.value)
			numHolders++
		case []orderBy:
			o := e[0]
			queries = append(queries, ColumnName(c.db.dialect, o.column.table, o.column.name), o.order.String())
			if len(e) > 1 {
				for _, o := range e[1:] {
					queries = append(queries, ",", ColumnName(c.db.dialect, o.column.table, o.column.name), o.order.String())
				}
			}
		case *column:
			col := ColumnName(c.db.dialect, e.table, e.name)
			queries = append(queries, col)
		case []interface{}:
			e = flatten(e)
			holders := make([]string, len(e))
			for i := 0; i < len(e); i++ {
				holders[i] = c.db.dialect.PlaceHolder(numHolders)
				numHolders++
			}
			queries = append(queries, "(", strings.Join(holders, ", "), ")")
			args = append(args, e...)
		case *between:
			queries = append(queries, c.db.dialect.PlaceHolder(numHolders), "AND", c.db.dialect.PlaceHolder(numHolders+1))
			args = append(args, e.from, e.to)
			numHolders += 2
		case *Condition:
			q, a := e.build(numHolders, true)
			queries = append(append(append(queries, "("), q...), ")")
			args = append(args, a...)
		case *JoinCondition:
			var leftTableName string
			if e.leftTableName == "" {
				leftTableName = c.tableName
			} else {
				leftTableName = e.leftTableName
			}
			queries = append(queries,
				c.db.dialect.Quote(e.tableName), "ON",
				ColumnName(c.db.dialect, leftTableName, e.left), e.op, ColumnName(c.db.dialect, e.tableName, e.right))
		case nil:
			// ignore.
		default:
			queries = append(queries, c.db.dialect.PlaceHolder(numHolders))
			args = append(args, e)
			numHolders++
		}
	}
	return queries, args
}

// JoinCondition represents a condition of "JOIN" query.
type JoinCondition struct {
	db            *DB
	leftTableName string // A table name of 'to be joined'.
	tableName     string // A table name of 'to join'.
	op            string // A operator of expression in "ON" clause.
	left          string // A left column name of operator.
	right         string // A right column name of operator.
	clause        Clause // A type of join clause ("JOIN" or "LEFT JOIN")
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
func (jc *JoinCondition) On(larg interface{}, args ...string) *Condition {
	var lcolumn string
	switch rv := reflect.ValueOf(larg); rv.Kind() {
	case reflect.String:
		lcolumn = rv.String()
	default:
		for rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() != reflect.Struct {
			panic(fmt.Errorf("On: first argument must be string or struct, got %v", rv.Type()))
		}
		jc.leftTableName = jc.db.tableName(rv.Type())
		lcolumn, args = args[0], args[1:]
	}
	switch len(args) {
	case 0:
		jc.left, jc.op, jc.right = lcolumn, "=", lcolumn
	case 2:
		jc.left, jc.op, jc.right = lcolumn, args[0], args[1]
	default:
		panic(fmt.Errorf("On: arguments expect 1 or 3, got %v", len(args)+1))
	}
	c := newCondition(jc.db)
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
	jc.tableName = jc.db.tableName(t)
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
