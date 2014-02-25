package genmai

import "time"

// TimeStamp is fields for timestamps that commonly used.
type TimeStamp struct {
	// Time of creation. This field will be set automatically by BeforeInsert.
	CreatedAt time.Time `json:"created_at"`

	// Time of update. This field will be set by BeforeInsert or BeforeUpdate.
	UpdatedAt time.Time `json:"updated_at"`
}

// BeforeInsert sets current time to CreatedAt and UpdatedAt field.
// It always returns nil.
func (ts *TimeStamp) BeforeInsert() error {
	n := now()
	ts.CreatedAt = n
	ts.UpdatedAt = n
	return nil
}

// BeforeUpdate sets current time to UpdatedAt field.
// It always returns nil.
func (ts *TimeStamp) BeforeUpdate() error {
	ts.UpdatedAt = now()
	return nil
}
