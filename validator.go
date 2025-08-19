package where

import (
	"strings"
)

// Validator provides field and function allowlisting for security.
// It can be used to restrict which fields and functions are allowed in filter expressions.
type Validator struct {
	allowedFields    map[string]bool
	allowedFunctions map[string]bool
	allowAll         bool
}

// NewValidator creates a new validator with empty allowlists.
// By default, all fields and functions are denied unless explicitly allowed.
func NewValidator() *Validator {
	return &Validator{
		allowedFields:    make(map[string]bool),
		allowedFunctions: make(map[string]bool),
		allowAll:         false,
	}
}

// AllowAll configures the validator to allow all fields and functions.
// This disables security restrictions and should be used with caution.
func (v *Validator) AllowAll() *Validator {
	v.allowAll = true
	return v
}

// AllowFields adds the specified fields to the allowlist.
// Field names are case-insensitive.
func (v *Validator) AllowFields(fields ...string) *Validator {
	for _, field := range fields {
		v.allowedFields[strings.ToLower(field)] = true
	}
	return v
}

// AllowFunctions adds the specified functions to the allowlist.
// Function names are case-insensitive.
func (v *Validator) AllowFunctions(functions ...string) *Validator {
	for _, fn := range functions {
		v.allowedFunctions[strings.ToUpper(fn)] = true
	}
	return v
}

// IsFieldAllowed returns true if the field is allowed by this validator.
func (v *Validator) IsFieldAllowed(field string) bool {
	if v.allowAll {
		return true
	}
	return v.allowedFields[strings.ToLower(field)]
}

// IsFunctionAllowed returns true if the function is allowed by this validator.
func (v *Validator) IsFunctionAllowed(function string) bool {
	if v.allowAll {
		return true
	}
	return v.allowedFunctions[strings.ToUpper(function)]
}
