package where_test

import (
	"testing"

	"github.com/pseudomuto/where"
	"github.com/stretchr/testify/require"
)

func TestGetFunctionDef(t *testing.T) {
	tests := []struct {
		name         string
		functionName string
		expectFound  bool
		expectedDef  where.FunctionDef
	}{
		{
			name:         "LOWER function",
			functionName: "LOWER",
			expectFound:  true,
			expectedDef: where.FunctionDef{
				Name:        "LOWER",
				Type:        "string",
				MinArgs:     1,
				MaxArgs:     1,
				Description: "Converts string to lowercase",
			},
		},
		{
			name:         "UPPER function",
			functionName: "UPPER",
			expectFound:  true,
			expectedDef: where.FunctionDef{
				Name:        "UPPER",
				Type:        "string",
				MinArgs:     1,
				MaxArgs:     1,
				Description: "Converts string to uppercase",
			},
		},
		{
			name:         "COALESCE function",
			functionName: "COALESCE",
			expectFound:  true,
			expectedDef: where.FunctionDef{
				Name:        "COALESCE",
				Type:        "scalar",
				MinArgs:     2,
				MaxArgs:     -1,
				Description: "Returns first non-null value",
			},
		},
		{
			name:         "NOW function",
			functionName: "NOW",
			expectFound:  true,
			expectedDef: where.FunctionDef{
				Name:        "NOW",
				Type:        "date",
				MinArgs:     0,
				MaxArgs:     0,
				Description: "Returns current timestamp",
			},
		},
		{
			name:         "case insensitive lookup",
			functionName: "lower",
			expectFound:  true,
			expectedDef: where.FunctionDef{
				Name:        "LOWER",
				Type:        "string",
				MinArgs:     1,
				MaxArgs:     1,
				Description: "Converts string to lowercase",
			},
		},
		{
			name:         "nonexistent function",
			functionName: "NONEXISTENT",
			expectFound:  false,
		},
		{
			name:         "empty function name",
			functionName: "",
			expectFound:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, found := where.GetFunctionDef(tt.functionName)
			require.Equal(t, tt.expectFound, found)

			if tt.expectFound {
				require.Equal(t, tt.expectedDef.Name, def.Name)
				require.Equal(t, tt.expectedDef.Type, def.Type)
				require.Equal(t, tt.expectedDef.MinArgs, def.MinArgs)
				require.Equal(t, tt.expectedDef.MaxArgs, def.MaxArgs)
				require.Equal(t, tt.expectedDef.Description, def.Description)
			}
		})
	}
}

func TestValidateFunctionArgs(t *testing.T) {
	tests := []struct {
		name         string
		functionName string
		argCount     int
		expectValid  bool
	}{
		// Fixed argument count functions
		{
			name:         "LOWER with correct args",
			functionName: "LOWER",
			argCount:     1,
			expectValid:  true,
		},
		{
			name:         "LOWER with too few args",
			functionName: "LOWER",
			argCount:     0,
			expectValid:  false,
		},
		{
			name:         "LOWER with too many args",
			functionName: "LOWER",
			argCount:     2,
			expectValid:  false,
		},
		// No argument functions
		{
			name:         "NOW with correct args",
			functionName: "NOW",
			argCount:     0,
			expectValid:  true,
		},
		{
			name:         "NOW with extra args",
			functionName: "NOW",
			argCount:     1,
			expectValid:  false,
		},
		// Variable argument functions (MaxArgs = -1)
		{
			name:         "COALESCE with minimum args",
			functionName: "COALESCE",
			argCount:     2,
			expectValid:  true,
		},
		{
			name:         "COALESCE with more args",
			functionName: "COALESCE",
			argCount:     5,
			expectValid:  true,
		},
		{
			name:         "COALESCE with too few args",
			functionName: "COALESCE",
			argCount:     1,
			expectValid:  false,
		},
		{
			name:         "CONCAT with minimum args",
			functionName: "CONCAT",
			argCount:     2,
			expectValid:  true,
		},
		{
			name:         "CONCAT with many args",
			functionName: "CONCAT",
			argCount:     10,
			expectValid:  true,
		},
		{
			name:         "CONCAT with too few args",
			functionName: "CONCAT",
			argCount:     1,
			expectValid:  false,
		},
		// Range argument functions
		{
			name:         "SUBSTRING with min args",
			functionName: "SUBSTRING",
			argCount:     2,
			expectValid:  true,
		},
		{
			name:         "SUBSTRING with max args",
			functionName: "SUBSTRING",
			argCount:     3,
			expectValid:  true,
		},
		{
			name:         "SUBSTRING with too few args",
			functionName: "SUBSTRING",
			argCount:     1,
			expectValid:  false,
		},
		{
			name:         "SUBSTRING with too many args",
			functionName: "SUBSTRING",
			argCount:     4,
			expectValid:  false,
		},
		{
			name:         "ROUND with min args",
			functionName: "ROUND",
			argCount:     1,
			expectValid:  true,
		},
		{
			name:         "ROUND with max args",
			functionName: "ROUND",
			argCount:     2,
			expectValid:  true,
		},
		{
			name:         "ROUND with too many args",
			functionName: "ROUND",
			argCount:     3,
			expectValid:  false,
		},
		// Case insensitive
		{
			name:         "lowercase function name",
			functionName: "lower",
			argCount:     1,
			expectValid:  true,
		},
		{
			name:         "mixed case function name",
			functionName: "Lower",
			argCount:     1,
			expectValid:  true,
		},
		// Nonexistent function
		{
			name:         "nonexistent function",
			functionName: "NONEXISTENT",
			argCount:     1,
			expectValid:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := where.ValidateFunctionArgs(tt.functionName, tt.argCount)
			require.Equal(t, tt.expectValid, valid)
		})
	}
}

func TestAllStandardFunctions(t *testing.T) {
	// Test that all functions in StandardFunctions can be retrieved
	expectedFunctions := []string{
		"LOWER", "UPPER", "LENGTH", "TRIM", "LTRIM", "RTRIM", "SUBSTRING", "CONCAT",
		"COALESCE", "GREATEST", "LEAST", "NOW", "CURRENT_DATE", "CURRENT_TIME",
		"CURRENT_TIMESTAMP", "DATE", "TIME", "YEAR", "MONTH", "DAY", "HOUR",
		"MINUTE", "SECOND", "EXTRACT", "IF", "CAST", "ABS", "ROUND", "FLOOR",
		"CEIL", "SQRT", "POWER",
	}

	for _, funcName := range expectedFunctions {
		t.Run(funcName, func(t *testing.T) {
			def, found := where.GetFunctionDef(funcName)
			require.True(t, found, "Function %s should be found", funcName)
			require.Equal(t, funcName, def.Name)
			require.NotEmpty(t, def.Type)
			require.NotEmpty(t, def.Description)
			require.GreaterOrEqual(t, def.MinArgs, 0)
			require.True(t, def.MaxArgs >= def.MinArgs || def.MaxArgs == -1,
				"MaxArgs should be >= MinArgs or -1 for unlimited")
		})
	}
}

func TestFunctionTypes(t *testing.T) {
	tests := []struct {
		functionName string
		expectedType where.FunctionType
	}{
		{"LOWER", where.FunctionTypeString},
		{"UPPER", where.FunctionTypeString},
		{"LENGTH", where.FunctionTypeString},
		{"COALESCE", where.FunctionTypeScalar},
		{"GREATEST", where.FunctionTypeScalar},
		{"NOW", where.FunctionTypeDate},
		{"DATE", where.FunctionTypeDate},
		{"CAST", where.FunctionTypeConversion},
		{"ABS", where.FunctionTypeMath},
		{"ROUND", where.FunctionTypeMath},
	}

	for _, tt := range tests {
		t.Run(tt.functionName, func(t *testing.T) {
			def, found := where.GetFunctionDef(tt.functionName)
			require.True(t, found)
			require.Equal(t, tt.expectedType, def.Type)
		})
	}
}
