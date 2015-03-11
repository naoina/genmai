package genmai

import (
	"fmt"
	"reflect"
	"time"
	"unicode"
)

var now = time.Now // for test.

// ToInterfaceSlice convert to []interface{} from []string.
func ToInterfaceSlice(slice []string) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// columnName returns the column name that added the table name with quoted if needed.
func ColumnName(d Dialect, tname, cname string) string {
	if cname != "*" {
		cname = d.Quote(cname)
	}
	if tname == "" {
		return cname
	}
	return fmt.Sprintf("%s.%s", d.Quote(tname), cname)
}

// IsUnexportedField returns whether the field is unexported.
// This function is to avoid the bug in versions older than Go1.3.
// See following links:
//     https://code.google.com/p/go/issues/detail?id=7247
//     http://golang.org/ref/spec#Exported_identifiers
func IsUnexportedField(field reflect.StructField) bool {
	return !(field.PkgPath == "" && unicode.IsUpper(rune(field.Name[0])))
}

func flatten(args []interface{}) []interface{} {
	result := make([]interface{}, 0, len(args))
	for _, v := range args {
		switch rv := reflect.ValueOf(v); rv.Kind() {
		case reflect.Slice, reflect.Array:
			for i := 0; i < rv.Len(); i++ {
				result = append(result, rv.Index(i).Interface())
			}
		default:
			result = append(result, v)
		}
	}
	return result
}
