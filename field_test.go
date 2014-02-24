package genmai

import (
	"reflect"
	"testing"
	"time"
)

func TestTimeStamp_BeforeInsert(t *testing.T) {
	createdAt, err := time.Parse("2006-01-02 15:04:05", "2014-02-24 22:36:56")
	if err != nil {
		t.Fatal(err)
	}
	updatedAt, err := time.Parse("2006-01-02 15:04:05", "2014-02-24 23:51:26")
	if err != nil {
		t.Fatal(err)
	}
	n, err := time.Parse("2006-01-02 15:04:05", "2000-02-02 16:57:38")
	if err != nil {
		t.Fatal(err)
	}
	baknow := now
	now = func() time.Time {
		return n
	}
	defer func() {
		now = baknow
	}()
	tm := &TimeStamp{
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	if err := tm.BeforeInsert(); err != nil {
		t.Fatal(err)
	}
	actual := tm.CreatedAt
	expected := n
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
	actual = tm.UpdatedAt
	expected = n
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestTimeStamp_BeforeUpdate(t *testing.T) {
	createdAt, err := time.Parse("2006-01-02 15:04:05", "2014-02-24 22:36:56")
	if err != nil {
		t.Fatal(err)
	}
	updatedAt, err := time.Parse("2006-01-02 15:04:05", "2014-02-24 23:51:26")
	if err != nil {
		t.Fatal(err)
	}
	n, err := time.Parse("2006-01-02 15:04:05", "2000-02-02 16:57:38")
	if err != nil {
		t.Fatal(err)
	}
	baknow := now
	now = func() time.Time {
		return n
	}
	defer func() {
		now = baknow
	}()
	tm := &TimeStamp{
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	if err := tm.BeforeUpdate(); err != nil {
		t.Fatal(err)
	}
	actual := tm.CreatedAt
	expected := createdAt
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
	actual = tm.UpdatedAt
	expected = n
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}
