package genmai

import (
	"database/sql"
	"fmt"
	"reflect"
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
			err = e.(error)
		}
	}()
	rv := reflect.ValueOf(output)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("first argument must be a pointer")
	}
	rv = rv.Elem()
	t := rv.Type().Elem()
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("first argument must be pointer to slice of struct, but %T", output)
	}
	col, conditions, err := db.classify(args)
	if err != nil {
		return err
	}
	queries := []string{`SELECT`, col, `FROM`, db.dialect.Quote(ToSnakeCase(t.Name()))}
	var values []interface{}
	for _, cond := range conditions {
		q, a := cond.build(0, false)
		queries = append(queries, q...)
		values = append(values, a...)
	}
	rows, err := db.db.Query(strings.Join(queries, " "), values...)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = ToCamelCase(column)
	}
	for _, name := range names {
		if _, found := t.FieldByName(name); !found {
			return fmt.Errorf("`%v` field is not defined in %v", name, t)
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
			return err
		}
		result = append(result, v)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	slice := reflect.MakeSlice(reflect.SliceOf(t), len(result), len(result))
	for i, v := range result {
		slice.Index(i).Set(v)
	}
	rv.Set(slice)
	return nil
}

// Where returns a new Condition of "WHERE" clause.
func (db *DB) Where(column string, args ...interface{}) *Condition {
	return newCondition(db.dialect).Where(column, args...)
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

// Close closes the database.
func (db *DB) Close() error {
	return db.db.Close()
}

// Quote returns a quoted s.
// It is for a column name, not a value.
func (db *DB) Quote(s string) string {
	return db.dialect.Quote(s)
}

func (db *DB) classify(args []interface{}) (column string, conditions []*Condition, err error) {
	column = "*" // default.
	if len(args) == 0 {
		return column, nil, nil
	}
	offset := 0
	switch t := args[0].(type) {
	case string:
		if t == "" {
			break
		}
		column = db.dialect.Quote(t)
		offset++
	case []string:
		if len(t) == 0 {
			break
		}
		names := make([]string, len(t))
		for i, name := range t {
			names[i] = db.dialect.Quote(name)
		}
		column = strings.Join(names, ", ")
		offset++
	}
	for i := offset; i < len(args); i++ {
		switch t := args[i].(type) {
		case *Condition:
			conditions = append(conditions, t)
		case string, []string:
			return "", nil, fmt.Errorf("argument of %T type must be before the *Condition arguments", t)
		default:
			return "", nil, fmt.Errorf("all argument types expect string, []string or *Condition, got %T type", t)
		}
	}
	return column, conditions, nil
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
)

func (c Clause) String() string {
	if int(c) >= len(clauseStrings) {
		panic(fmt.Errorf("Clause %v is not defined", c))
	}
	return clauseStrings[c]
}

var clauseStrings = []string{
	Where:   "WHERE",
	And:     "AND",
	Or:      "OR",
	OrderBy: "ORDER BY",
	Limit:   "LIMIT",
	Offset:  "OFFSET",
	In:      "IN",
	Like:    "LIKE",
	Between: "BETWEEN",
}

// column represents a column name in query.
type column string

func (c column) String() string {
	return string(c)
}

// expr represents a expression in query.
type expr struct {
	op     string      // operator.
	column string      // column name.
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
	d     Dialect
	parts parts // parts of the query.
}

// newCondition returns a new Condition with Dialect.
func newCondition(d Dialect) *Condition {
	return &Condition{d: d}
}

// Where adds "WHERE" clause to the Condition and returns it for method chain.
func (c *Condition) Where(col string, args ...interface{}) *Condition {
	var e interface{}
	switch len(args) {
	case 0:
		e = column(col)
	case 2:
		e = &expr{op: fmt.Sprint(args[0]), column: col, value: args[1]}
	default:
		panic(fmt.Errorf("Where arguments expect 1 or 3, got %v", len(args)+1))
	}
	return c.appendQuery(0, Where, e)
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
	switch len(args) {
	case 0:
		if _, ok := cond.(*Condition); !ok {
			panic(fmt.Errorf("`cond` argument must be type of *Condition if args not given"))
		}
	case 2:
		cond = &expr{op: fmt.Sprint(args[0]), column: fmt.Sprint(cond), value: args[1]}
	default:
		panic(fmt.Errorf("%v arguments expect 1 or 3, got %v", name, len(args)+1))
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
			queries = append(queries, c.d.Quote(e.column), e.op, c.d.PlaceHolder(numHolders))
			args = append(args, e.value)
		case *orderBy:
			queries = append(queries, c.d.Quote(e.column), e.order.String())
		case column:
			queries = append(queries, c.d.Quote(e.String()))
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
		default:
			numHolders++
			queries = append(queries, c.d.PlaceHolder(numHolders))
			args = append(args, e)
		}
	}
	return queries, args
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
