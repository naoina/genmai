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
	col, from, conditions, err := db.classify(args)
	if err != nil {
		return err
	}
	var selectFunc selectFunc
	switch rv.Kind() {
	case reflect.Slice:
		t := rv.Type().Elem()
		if t.Kind() != reflect.Struct {
			return fmt.Errorf("argument of slice must be slice of struct, but %v", rv.Type())
		}
		if from == "" {
			from = ToSnakeCase(t.Name())
		}
		selectFunc = db.selectToSlice
	default:
		if from == "" {
			return fmt.Errorf("From statement must be given if any Function is given")
		}
		selectFunc = db.selectToValue
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
		panic(fmt.Errorf("From argument must be struct type, got %v", t))
	}
	return &From{TableName: ToSnakeCase(t.Name())}
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

// Distinct returns a representation object of "DISTINCT" statement.
func (db *DB) Distinct(columns ...string) *Distinct {
	return &Distinct{columns: columns}
}

// Count returns "COUNT" function.
func (db *DB) Count(column ...interface{}) *Function {
	switch len(column) {
	case 0:
		// do nothing.
	case 1:
		if d, ok := column[0].(*Distinct); ok {
			column = []interface{}{db.Raw(fmt.Sprintf("DISTINCT %s", db.columns(ToInterfaceSlice(d.columns))))}
		}
	default:
		panic(fmt.Errorf("a number of argument must be 0 or 1, got %v", len(column)))
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

func (db *DB) classify(args []interface{}) (column, from string, conditions []*Condition, err error) {
	column = "*" // default.
	if len(args) == 0 {
		return column, "", nil, nil
	}
	offset := 1
	switch t := args[0].(type) {
	case string:
		if t != "" {
			column = db.dialect.Quote(t)
		}
	case []string:
		column = db.columns(ToInterfaceSlice(t))
	case *Distinct:
		column = fmt.Sprintf("DISTINCT %s", db.columns(ToInterfaceSlice(t.columns)))
	case *From:
		from = t.TableName
	case *Function:
		column = fmt.Sprintf("%s(%s)", t.Name, db.columns(t.Args))
	default:
		offset--
	}
	for i := offset; i < len(args); i++ {
		switch t := args[i].(type) {
		case *Condition:
			conditions = append(conditions, t)
		case string, []string:
			return "", "", nil, fmt.Errorf("argument of %T type must be before the *Condition arguments", t)
		case *From:
			if from != "" {
				return "", "", nil, fmt.Errorf("From statement specified more than once")
			}
			from = t.TableName
		case *Function:
			return "", "", nil, fmt.Errorf("%s function must be specified to the first argument", t.Name)
		default:
			return "", "", nil, fmt.Errorf("all argument types expect string, []string or *Condition, got %T type", t)
		}
	}
	return column, from, conditions, nil
}

// columns returns the comma-separated column name with quoted.
func (db *DB) columns(columns []interface{}) string {
	if len(columns) == 0 {
		return "*"
	}
	names := make([]string, len(columns))
	for i, col := range columns {
		switch c := col.(type) {
		case Raw:
			names[i] = fmt.Sprint(*c)
		case string:
			names[i] = db.dialect.Quote(c)
		default:
			panic(fmt.Errorf("column name must be type of string or Raw, got %T", c))
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
