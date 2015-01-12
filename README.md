# Genmai [![Build Status](https://travis-ci.org/naoina/genmai.png?branch=master)](https://travis-ci.org/naoina/genmai)

Simple, better and easy-to-use ORM library for [Golang](http://golang.org/).

## Overview

* flexibility with SQL-like API
* Transaction support
* Database dialect interface
* Query logging
* Update/Insert/Delete hooks
* Embedded struct

Database dialect currently supported are:

* MySQL
* PostgreSQL
* SQLite3

## Installation

    go get -u github.com/naoina/genmai

## Schema

Schema of the table will be defined as a struct.

```go
// The struct "User" is the table name "user".
// The field name will be converted to lowercase/snakecase, and used as a column name in table.
// e.g. If field name is "CreatedAt", column name is "created_at".
type User struct {
    // PRIMARY KEY. and column name will use "tbl_id" instead of "id".
    Id int64 `db:"pk" column:"tbl_id"`

    // NOT NULL and Default is "me".
    Name string `default:"me"`

    // Nullable column must use a pointer type, or sql.Null* types.
    CreatedAt *time.Time

    // UNIQUE column if specify the db:"unique" tag.
    ScreenName string `db:"unique"`

    // Ignore column if specify the db:"-" tag.
    Active bool `db:"-"`
}
```

## Query API

### Create table

```go
package main

import (
    "time"

    _ "github.com/mattn/go-sqlite3"
    // _ "github.com/go-sql-driver/mysql"
    // _ "github.com/lib/pq"
    "github.com/naoina/genmai"
)

// define a table schema.
type TestTable struct {
    Id        int64      `db:"pk" column:"tbl_id"`
    Name      string     `default:"me"`
    CreatedAt *time.Time
    UserName  string     `db:"unique" size:"255"`
    Active    bool       `db:"-"`
}

func main() {
    db, err := genmai.New(&genmai.SQLite3Dialect{}, ":memory:")
    // db, err := genmai.New(&genmai.MySQLDialect{}, "dsn")
    // db, err := genmai.New(&genmai.PostgresDialect{}, "dsn")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    if err := db.CreateTable(&TestTable{}); err != nil {
        panic(err)
    }
}
```

### Insert

A single insert:

```go
obj := &TestTable{
    Name: "alice",
    Active: true,
}
n, err := db.Insert(obj)
if err != nil {
    panic(err)
}
fmt.Printf("inserted rows: %d\n", n)
```

Or bulk-insert:

```go
objs := []TestTable{
    {Name: "alice", Active: true},
    {Name: "bob", Active: true},
}
n, err := db.Insert(objs)
if err != nil {
    panic(err)
}
fmt.Printf("inserted rows: %d\n", n)
```

### Select

```go
var results []TestTable
if err := db.Select(&results); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### Where

```go
var results []TestTable
if err := db.Select(&results, db.Where("tbl_id", "=", 1)); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### And/Or

```go
var results []TestTable
if err := db.Select(&results, db.Where("tbl_id", "=", 1).And(db.Where("name", "=", "alice").Or("name", "=", "bob"))); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### In

```go
var results []TestTable
if err := db.Select(&results, db.Where("tbl_id").In(1, 2, 4)); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### Like

```go
var results []TestTable
if err := db.Select(&results, db.Where("name").Like("%li%")); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### Between

```go
var results []TestTable
if err := db.Select(&results, db.Where("name").Between(1, 3)); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### Is Null/Is Not Null

```go
var results []TestTable
if err := db.Select(&results, db.Where("created_at").IsNull()); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
results = []TestTable{}
if err := db.Select(&results, db.Where("created_at").IsNotNull()); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### Order by/Offset/Limit

```go
var results []TestTable
if err := db.Select(&results, db.OrderBy("id", genmai.ASC).Offset(2).Limit(10)); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### Distinct

```go
var results []TestTable
if err := db.Select(&results, db.Distinct("name"), db.Where("name").Like("%")); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

### Count

```go
var n int64
if err := db.Select(&n, db.Count(), db.From(&TestTable{})); err != nil {
    panic(err)
}
fmt.Printf("%v\n", n)
```

With condition:

```go
var n int64
if err := db.Select(&n, db.Count(), db.From(&TestTable{}), db.Where("id", ">", 100)); err != nil {
    panic(err)
}
fmt.Printf("%v\n", n)
```

### Join

Inner Join:

```go
package main

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/mattn/go-sqlite3"
    // _ "github.com/go-sql-driver/mysql"
    // _ "github.com/lib/pq"
    "github.com/naoina/genmai"
)

type TestTable struct {
    Id        int64  `db:"pk" column:"tbl_id"`
    Name      string `default:"me"`
    CreatedAt *time.Time
    Active    bool `db:"-"` // column to ignore.
}

type Table2 struct {
    Id   int64 `db:"pk" column:"tbl_id"`
    Body sql.NullString
}

func main() {
    db, err := genmai.New(&genmai.SQLite3Dialect{}, ":memory:")
    // db, err := genmai.New(&genmai.MySQLDialect{}, "dsn")
    // db, err := genmai.New(&genmai.PostgresDialect{}, "dsn")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    if err := db.CreateTable(&TestTable{}); err != nil {
        panic(err)
    }
    if err := db.CreateTable(&Table2{}); err != nil {
        panic(err)
    }
    objs1 := []TestTable{
        {Name: "alice", Active: true},
        {Name: "bob", Active: true},
    }
    objs2 := []Table2{
        {Body: sql.NullString{String: "something"}},
    }
    if _, err = db.Insert(objs1); err != nil {
        panic(err)
    }
    if _, err := db.Insert(objs2); err != nil {
        panic(err)
    }
    // fmt.Printf("inserted rows: %d\n", n)
    var results []TestTable
    if err := db.Select(&results, db.Join(&Table2{}).On("tbl_id")); err != nil {
        panic(err)
    }
    fmt.Printf("%v\n", results)
}
```

Left Join:

```go
var results []TestTable
if err := db.Select(&results, db.LeftJoin(&Table2{}).On("name", "=", "body")); err != nil {
    panic(err)
}
fmt.Printf("%v\n", results)
```

`RIGHT OUTER JOIN` and `FULL OUTER JOIN` are still unsupported.

### Update

```go
var results []TestTable
if err := db.Select(&results); err != nil {
    panic(err)
}
obj := results[0]
obj.Name = "nico"
if _, err := db.Update(&obj); err != nil {
    panic(err)
}
```

### Delete

A single delete:

```go
obj := TestTable{Id: 1}
if _, err := db.Delete(&obj); err != nil {
    panic(err)
}
```

Or bulk-delete:

```go
objs := []TestTable{
    {Id: 1}, {Id: 3},
}
if _, err := db.Delete(objs); err != nil {
    panic(err)
}
```

### Transaction

```go
defer func() {
    if err := recover(); err != nil {
        db.Rollback()
    } else {
        db.Commit()
    }
}()
if err := db.Begin(); err != nil {
    panic(err)
}
// do something.
```

### Using any table name

You can implement [TableNamer](https://godoc.org/github.com/naoina/genmai#TableNamer) interface to use any table name.

```go
type UserTable struct {
    Id int64 `db:"pk"`
}

func (u *UserTable) TableName() string {
    return "user"
}
```

In the above example, the table name `user` is used instead of `user_table`.

### Using raw database/sql interface

```go
rawDB := db.DB()
// do something with using raw database/sql interface...
```

## Query logging

By default, query logging is disabled.
You can enable Query logging as follows.

```go
db.SetLogOutput(os.Stdout) // Or any io.Writer can be passed.
```

Also you can change the format of output as follows.

```go
db.SetLogFormat("format string")
```

Format syntax uses Go's template. And you can use the following data object in that template.

```
- .time        time.Time object in current time.
- .duration    Processing time of SQL. It will format to "%.2fms".
- .query       string of SQL query. If it using placeholder,
               placeholder parameters will append to the end of query.
```

The default format is:

    [{{.time.Format "2006-01-02 15:04:05"}}] [{{.duration}}] {{.query}}

In production, it is recommended to disable this feature in order to somewhat affect performance.

```go
db.SetLogOutput(nil) // To disable logging by nil.
```

## Update/Insert/Delete hooks

Genmai calls `Before`/`After` hook method if defined in model struct.

```go
func (t *TestTable) BeforeInsert() error {
    t.CreatedAt = time.Now()
    return nil
}
```

If `Before` prefixed hook returns an error, it query won't run.

All hooks are:

* `BeforeInsert/AfterInsert`
* `BeforeUpdate/AfterUpdate`
* `BeforeDelete/AfterDelete`

If use bulk-insert or bulk-delete, hooks method run for each object.

## Embedded struct

Common fields can be defined on struct and embed that.

```go
package main

import "time"

type TimeStamp struct {
    CreatedAt time.Time
    UpdatedAt time.Time
}

type User struct {
    Id   int64
    Name string

    TimeStamp
}
```

Also Genmai has defined `TimeStamp` struct for commonly used fields.

```
type User struct {
    Id int64

    genmai.TimeStamp
}
```

See the Godoc of [TimeStamp](http://godoc.org/github.com/naoina/genmai#TimeStamp) for more information.

If you'll override hook method defined in embedded struct, you'll should call the that hook in overridden method.
For example in above struct case:

```go
func (u *User) BeforeInsert() error {
	if err := u.TimeStamp.BeforeInsert(); err != nil {
		return err
	}
	// do something.
	return nil
}
```

## Documentation

API document and more examples are available here:

http://godoc.org/github.com/naoina/genmai

## TODO

* Benchmark
* More SQL support
* Migration

## License

Genmai is licensed under the MIT
