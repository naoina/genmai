// Copyright 2014 Naoya Inada. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

// Package genmai provides simple, better and easy-to-use Object-Relational Mapper.
package genmai

import (
	"database/sql"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
)

// DB represents a database object.
type DB struct {
	db      *sql.DB
	dialect Dialect
}

// New returns a new DB.
// If any error occurs, it returns nil and error.
func New(dialect Dialect, dsn string) (*DB, error) {
	db, err := sql.Open(dialect.Name(), dsn)
	if err != nil {
		return nil, err
	}
	return &DB{db: db, dialect: dialect}, nil
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
	stmt, err := db.db.Prepare(strings.Join(queries, " "))
	if err != nil {
		return err
	}
	defer stmt.Close()
	rows, err := stmt.Query(values...)
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
	t := reflect.TypeOf(arg)
	if t.Kind() != reflect.Struct {
		panic(fmt.Errorf("From: argument must be struct type, got %v", t))
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

// selectToSlice returns a slice value fetched from rows.
func (db *DB) selectToSlice(rows *sql.Rows, rv *reflect.Value) (*reflect.Value, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = ToCamelCase(column)
	}
	t := rv.Type().Elem()
	for _, name := range names {
		if _, found := t.FieldByName(name); !found {
			return nil, fmt.Errorf("`%v` field is not defined in %v", name, t)
		}
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
)

func (c Clause) String() string {
	if int(c) >= len(clauseStrings) {
		panic(fmt.Errorf("Clause %v is not defined", c))
	}
	return clauseStrings[c]
}

var clauseStrings = []string{
	Where:    "WHERE",
	And:      "AND",
	Or:       "OR",
	OrderBy:  "ORDER BY",
	Limit:    "LIMIT",
	Offset:   "OFFSET",
	In:       "IN",
	Like:     "LIKE",
	Between:  "BETWEEN",
	Join:     "JOIN",
	LeftJoin: "LEFT JOIN",
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
