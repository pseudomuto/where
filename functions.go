package where

import (
	"strings"
)

const (
	// FunctionType constants define categories of SQL functions.
	FunctionTypeScalar     FunctionType = "scalar"
	FunctionTypeAggregate  FunctionType = "aggregate"
	FunctionTypeWindow     FunctionType = "window"
	FunctionTypeDate       FunctionType = "date"
	FunctionTypeString     FunctionType = "string"
	FunctionTypeMath       FunctionType = "math"
	FunctionTypeConversion FunctionType = "conversion"
)

type (
	// FunctionType categorizes SQL functions by their purpose.
	FunctionType string

	// FunctionDef defines metadata for a SQL function including argument validation.
	FunctionDef struct {
		Name        string
		Type        FunctionType
		MinArgs     int
		MaxArgs     int // -1 means unlimited
		Description string
	}
)

// StandardFunctions contains metadata for commonly supported SQL functions.
// This is optional documentation - all functions are supported by default regardless of this list.
var StandardFunctions = map[string]FunctionDef{
	"LOWER": {
		Name:        "LOWER",
		Type:        FunctionTypeString,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Converts string to lowercase",
	},
	"UPPER": {
		Name:        "UPPER",
		Type:        FunctionTypeString,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Converts string to uppercase",
	},
	"LENGTH": {
		Name:        "LENGTH",
		Type:        FunctionTypeString,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Returns length of string",
	},
	"TRIM": {
		Name:        "TRIM",
		Type:        FunctionTypeString,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Removes leading and trailing whitespace",
	},
	"LTRIM": {
		Name:        "LTRIM",
		Type:        FunctionTypeString,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Removes leading whitespace",
	},
	"RTRIM": {
		Name:        "RTRIM",
		Type:        FunctionTypeString,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Removes trailing whitespace",
	},
	"SUBSTRING": {
		Name:        "SUBSTRING",
		Type:        FunctionTypeString,
		MinArgs:     2,
		MaxArgs:     3,
		Description: "Extracts substring from string",
	},
	"CONCAT": {
		Name:        "CONCAT",
		Type:        FunctionTypeString,
		MinArgs:     2,
		MaxArgs:     -1,
		Description: "Concatenates strings",
	},
	"COALESCE": {
		Name:        "COALESCE",
		Type:        FunctionTypeScalar,
		MinArgs:     2,
		MaxArgs:     -1,
		Description: "Returns first non-null value",
	},
	"GREATEST": {
		Name:        "GREATEST",
		Type:        FunctionTypeScalar,
		MinArgs:     2,
		MaxArgs:     -1,
		Description: "Returns greatest value",
	},
	"LEAST": {
		Name:        "LEAST",
		Type:        FunctionTypeScalar,
		MinArgs:     2,
		MaxArgs:     -1,
		Description: "Returns smallest value",
	},
	"NOW": {
		Name:        "NOW",
		Type:        FunctionTypeDate,
		MinArgs:     0,
		MaxArgs:     0,
		Description: "Returns current timestamp",
	},
	"CURRENT_DATE": {
		Name:        "CURRENT_DATE",
		Type:        FunctionTypeDate,
		MinArgs:     0,
		MaxArgs:     0,
		Description: "Returns current date",
	},
	"CURRENT_TIME": {
		Name:        "CURRENT_TIME",
		Type:        FunctionTypeDate,
		MinArgs:     0,
		MaxArgs:     0,
		Description: "Returns current time",
	},
	"CURRENT_TIMESTAMP": {
		Name:        "CURRENT_TIMESTAMP",
		Type:        FunctionTypeDate,
		MinArgs:     0,
		MaxArgs:     0,
		Description: "Returns current timestamp",
	},
	"DATE": {
		Name:        "DATE",
		Type:        FunctionTypeDate,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extracts date from timestamp",
	},
	"TIME": {
		Name:        "TIME",
		Type:        FunctionTypeDate,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extracts time from timestamp",
	},
	"YEAR": {
		Name:        "YEAR",
		Type:        FunctionTypeDate,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extracts year from date",
	},
	"MONTH": {
		Name:        "MONTH",
		Type:        FunctionTypeDate,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extracts month from date",
	},
	"DAY": {
		Name:        "DAY",
		Type:        FunctionTypeDate,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extracts day from date",
	},
	"HOUR": {
		Name:        "HOUR",
		Type:        FunctionTypeDate,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extracts hour from time",
	},
	"MINUTE": {
		Name:        "MINUTE",
		Type:        FunctionTypeDate,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extracts minute from time",
	},
	"SECOND": {
		Name:        "SECOND",
		Type:        FunctionTypeDate,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extracts second from time",
	},
	"EXTRACT": {
		Name:        "EXTRACT",
		Type:        FunctionTypeDate,
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Extracts date part from date/time",
	},
	"IF": {
		Name:        "IF",
		Type:        FunctionTypeScalar,
		MinArgs:     3,
		MaxArgs:     3,
		Description: "Conditional expression",
	},
	"CAST": {
		Name:        "CAST",
		Type:        FunctionTypeConversion,
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Type conversion",
	},
	"ABS": {
		Name:        "ABS",
		Type:        FunctionTypeMath,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Absolute value",
	},
	"ROUND": {
		Name:        "ROUND",
		Type:        FunctionTypeMath,
		MinArgs:     1,
		MaxArgs:     2,
		Description: "Rounds a number",
	},
	"FLOOR": {
		Name:        "FLOOR",
		Type:        FunctionTypeMath,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Rounds down to integer",
	},
	"CEIL": {
		Name:        "CEIL",
		Type:        FunctionTypeMath,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Rounds up to integer",
	},
	"SQRT": {
		Name:        "SQRT",
		Type:        FunctionTypeMath,
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Square root",
	},
	"POWER": {
		Name:        "POWER",
		Type:        FunctionTypeMath,
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Raises to power",
	},
}

// GetFunctionDef retrieves the function definition for the given function name.
// Function names are case-insensitive. Returns false if the function is not in StandardFunctions.
// Note: All functions are supported by default - this is only for documentation purposes.
func GetFunctionDef(name string) (FunctionDef, bool) {
	def, ok := StandardFunctions[strings.ToUpper(name)]
	return def, ok
}

// ValidateFunctionArgs validates that the given argument count is valid for the function.
// Returns true if the argument count is within the allowed range for functions in StandardFunctions.
// Note: This is optional validation - all functions are supported by default regardless of arity.
func ValidateFunctionArgs(name string, argCount int) bool {
	def, ok := GetFunctionDef(name)
	if !ok {
		return false
	}

	if def.MaxArgs == -1 {
		return argCount >= def.MinArgs
	}

	return argCount >= def.MinArgs && argCount <= def.MaxArgs
}
