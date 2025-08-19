package postgres_test

import (
	"testing"

	"github.com/pseudomuto/where"
	// Import the PostgreSQL driver
	_ "github.com/pseudomuto/where/drivers/postgres"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLDateTimeFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "NOW function",
			expression:     "created_at > NOW()",
			expectedSQL:    "created_at > NOW()",
			expectedParams: []any{},
		},
		{
			name:           "AGE function",
			expression:     "AGE(created_at) > '1 year'",
			expectedSQL:    "AGE(created_at) > $1",
			expectedParams: []any{"1 year"},
		},
		{
			name:           "DATE_TRUNC function",
			expression:     "DATE_TRUNC('month', created_at) = '2024-01-01'",
			expectedSQL:    "DATE_TRUNC($1, created_at) = $2",
			expectedParams: []any{"month", "2024-01-01"},
		},
		{
			name:           "EXTRACT function",
			expression:     "EXTRACT('year', created_at) = 2024",
			expectedSQL:    "EXTRACT($1 FROM created_at) = $2",
			expectedParams: []any{"year", float64(2024)},
		},
		{
			name:           "TO_CHAR function",
			expression:     "TO_CHAR(created_at, 'YYYY-MM-DD') = '2024-01-15'",
			expectedSQL:    "TO_CHAR(created_at, $1) = $2",
			expectedParams: []any{"YYYY-MM-DD", "2024-01-15"},
		},
		{
			name:           "MAKE_DATE function",
			expression:     "MAKE_DATE(2024, 1, 15) > CURRENT_DATE()",
			expectedSQL:    "MAKE_DATE($1, $2, $3) > CURRENT_DATE()",
			expectedParams: []any{float64(2024), float64(1), float64(15)},
		},
		{
			name:           "ISFINITE function",
			expression:     "ISFINITE(timestamp_column) = true",
			expectedSQL:    "ISFINITE(timestamp_column) = TRUE",
			expectedParams: []any{},
		},
	}

	runTests(t, tests)
}

func TestPostgreSQLStringFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "CONCAT function",
			expression:     "CONCAT(first_name, ' ', last_name) = 'John Doe'",
			expectedSQL:    "CONCAT(first_name, $1, last_name) = $2",
			expectedParams: []any{" ", "John Doe"},
		},
		{
			name:           "SUBSTRING function",
			expression:     "SUBSTRING(description, 1, 10) = 'Important'",
			expectedSQL:    "SUBSTRING(description FROM $1 FOR $2) = $3",
			expectedParams: []any{float64(1), float64(10), "Important"},
		},
		{
			name:           "INITCAP function",
			expression:     "INITCAP(name) = 'John Doe'",
			expectedSQL:    "INITCAP(name) = $1",
			expectedParams: []any{"John Doe"},
		},
		{
			name:           "SPLIT_PART function",
			expression:     "SPLIT_PART(email, '@', 1) = 'admin'",
			expectedSQL:    "SPLIT_PART(email, $1, $2) = $3",
			expectedParams: []any{"@", float64(1), "admin"},
		},
		{
			name:           "REGEXP_MATCH function",
			expression:     "REGEXP_MATCH(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$') IS NOT NULL",
			expectedSQL:    "REGEXP_MATCH(email, $1) IS NOT NULL",
			expectedParams: []any{"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
		},
		{
			name:           "STARTS_WITH function",
			expression:     "STARTS_WITH(url, 'https://') = true",
			expectedSQL:    "STARTS_WITH(url, $1) = TRUE",
			expectedParams: []any{"https://"},
		},
		{
			name:           "STRING_AGG function",
			expression:     "STRING_AGG(tag, ', ') LIKE '%admin%'",
			expectedSQL:    "STRING_AGG(tag, $1) LIKE $2",
			expectedParams: []any{", ", "%admin%"},
		},
		{
			name:           "MD5 function",
			expression:     "MD5(password) = 'abc123'",
			expectedSQL:    "MD5(password) = $1",
			expectedParams: []any{"abc123"},
		},
		{
			name:           "ENCODE and DECODE functions",
			expression:     "ENCODE(data, 'base64') = 'abc' OR DECODE(encoded, 'base64') != ''",
			expectedSQL:    "(ENCODE(data, $1) = $2 OR DECODE(encoded, $3) != $4)",
			expectedParams: []any{"base64", "abc", "base64", ""},
		},
	}

	runTests(t, tests)
}

func TestPostgreSQLMathFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "CBRT function",
			expression:     "CBRT(volume) > 10",
			expectedSQL:    "CBRT(volume) > $1",
			expectedParams: []any{float64(10)},
		},
		{
			name:           "TRUNC function",
			expression:     "TRUNC(amount, 2) > 100",
			expectedSQL:    "TRUNC(amount, $1) > $2",
			expectedParams: []any{float64(2), float64(100)},
		},
		{
			name:           "MOD function",
			expression:     "MOD(number, 2) = 0",
			expectedSQL:    "MOD(number, $1) = $2",
			expectedParams: []any{float64(2), float64(0)},
		},
		{
			name:           "POWER function",
			expression:     "POWER(base, 2) = 100",
			expectedSQL:    "POWER(base, $1) = $2",
			expectedParams: []any{float64(2), float64(100)},
		},
		{
			name:           "trigonometric functions in degrees",
			expression:     "SIND(45) > 0.7 AND COSD(0) = 1",
			expectedSQL:    "(SIND($1) > $2 AND COSD($3) = $4)",
			expectedParams: []any{float64(45), float64(0.7), float64(0), float64(1)},
		},
		{
			name:           "WIDTH_BUCKET function",
			expression:     "WIDTH_BUCKET(score, 0, 100, 10) = 5",
			expectedSQL:    "WIDTH_BUCKET(score, $1, $2, $3) = $4",
			expectedParams: []any{float64(0), float64(100), float64(10), float64(5)},
		},
		{
			name:           "GCD and LCM functions",
			expression:     "GCD(12, 18) = 6 AND LCM(4, 6) = 12",
			expectedSQL:    "(GCD($1, $2) = $3 AND LCM($4, $5) = $6)",
			expectedParams: []any{float64(12), float64(18), float64(6), float64(4), float64(6), float64(12)},
		},
	}

	runTests(t, tests)
}

func TestPostgreSQLConditionalFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "NULLIF function",
			expression:     "NULLIF(value, 0) > 100",
			expectedSQL:    "NULLIF(value, $1) > $2",
			expectedParams: []any{float64(0), float64(100)},
		},
		{
			name:           "COALESCE function",
			expression:     "COALESCE(email, phone, 'N/A') != 'N/A'",
			expectedSQL:    "COALESCE(email, phone, $1) != $2",
			expectedParams: []any{"N/A", "N/A"},
		},
		{
			name:           "NUM_NULLS function",
			expression:     "NUM_NULLS(field1, field2, field3) > 0",
			expectedSQL:    "NUM_NULLS(field1, field2, field3) > $1",
			expectedParams: []any{float64(0)},
		},
	}

	runTests(t, tests)
}

func TestPostgreSQLJSONFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "JSONB_EXTRACT_PATH function",
			expression:     "JSONB_EXTRACT_PATH(metadata, 'user', 'id') = '12345'",
			expectedSQL:    "JSONB_EXTRACT_PATH(metadata, $1, $2) = $3",
			expectedParams: []any{"user", "id", "12345"},
		},
		{
			name:           "JSONB_BUILD_OBJECT function",
			expression:     "JSONB_BUILD_OBJECT('name', name, 'age', age) IS NOT NULL",
			expectedSQL:    "JSONB_BUILD_OBJECT($1, name, $2, age) IS NOT NULL",
			expectedParams: []any{"name", "age"},
		},
		{
			name:           "JSONB_AGG function",
			expression:     "JSONB_AGG(data) IS NOT NULL",
			expectedSQL:    "JSONB_AGG(data) IS NOT NULL",
			expectedParams: []any{},
		},
		{
			name:           "JSONB_SET function",
			expression:     "JSONB_SET(config, '{settings,theme}', '\"dark\"') != '{}'",
			expectedSQL:    "JSONB_SET(config, $1, $2) != $3",
			expectedParams: []any{"{settings,theme}", "\"dark\"", "{}"},
		},
		{
			name:           "TO_JSONB function",
			expression:     "TO_JSONB(row_data) IS NOT NULL",
			expectedSQL:    "TO_JSONB(row_data) IS NOT NULL",
			expectedParams: []any{},
		},
	}

	runTests(t, tests)
}

func TestPostgreSQLArrayFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "ARRAY_LENGTH function",
			expression:     "ARRAY_LENGTH(tags, 1) > 0",
			expectedSQL:    "ARRAY_LENGTH(tags, $1) > $2",
			expectedParams: []any{float64(1), float64(0)},
		},
		{
			name:           "ARRAY_APPEND function",
			expression:     "ARRAY_APPEND(tags, 'new') != '{}'",
			expectedSQL:    "ARRAY_APPEND(tags, $1) != $2",
			expectedParams: []any{"new", "{}"},
		},
		{
			name:           "ARRAY_POSITION function",
			expression:     "ARRAY_POSITION(tags, 'admin') > 0",
			expectedSQL:    "ARRAY_POSITION(tags, $1) > $2",
			expectedParams: []any{"admin", float64(0)},
		},
		{
			name:           "ARRAY_TO_STRING function",
			expression:     "ARRAY_TO_STRING(tags, ', ') LIKE '%admin%'",
			expectedSQL:    "ARRAY_TO_STRING(tags, $1) LIKE $2",
			expectedParams: []any{", ", "%admin%"},
		},
		{
			name:           "CARDINALITY function",
			expression:     "CARDINALITY(items) > 10",
			expectedSQL:    "CARDINALITY(items) > $1",
			expectedParams: []any{float64(10)},
		},
	}

	runTests(t, tests)
}

func TestPostgreSQLAggregateFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "STDDEV and VARIANCE functions",
			expression:     "STDDEV(score) < 10 AND VARIANCE(score) < 100",
			expectedSQL:    "(STDDEV(score) < $1 AND VARIANCE(score) < $2)",
			expectedParams: []any{float64(10), float64(100)},
		},
		{
			name:           "BOOL_AND and BOOL_OR functions",
			expression:     "BOOL_AND(is_active) = true OR BOOL_OR(is_premium) = true",
			expectedSQL:    "(BOOL_AND(is_active) = TRUE OR BOOL_OR(is_premium) = TRUE)",
			expectedParams: []any{},
		},
	}

	runTests(t, tests)
}

func TestPostgreSQLComplexExpressions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name: "complex PostgreSQL expression",
			expression: `
				DATE_TRUNC('month', created_at) = '2024-01-01' AND
				JSONB_EXTRACT_PATH(metadata, 'user', 'role') = 'admin' AND
				ARRAY_LENGTH(tags, 1) > 0 AND
				REGEXP_MATCH(email, '^admin@') IS NOT NULL
			`,
			expectedSQL:    "(DATE_TRUNC($1, created_at) = $2 AND JSONB_EXTRACT_PATH(metadata, $3, $4) = $5 AND ARRAY_LENGTH(tags, $6) > $7 AND REGEXP_MATCH(email, $8) IS NOT NULL)",
			expectedParams: []any{"month", "2024-01-01", "user", "role", "admin", float64(1), float64(0), "^admin@"},
		},
	}

	runTests(t, tests)
}

func TestPostgreSQLILIKESupport(t *testing.T) {
	// PostgreSQL natively supports ILIKE
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "ILIKE operator",
			expression:     "name ILIKE '%john%'",
			expectedSQL:    "name ILIKE $1",
			expectedParams: []any{"%john%"},
		},
		{
			name:           "NOT ILIKE operator",
			expression:     "email NOT ILIKE '%spam%'",
			expectedSQL:    "email NOT ILIKE $1",
			expectedParams: []any{"%spam%"},
		},
	}

	runTests(t, tests)
}

// Helper function to run tests
func runTests(t *testing.T, tests []struct {
	name           string
	expression     string
	expectedSQL    string
	expectedParams []any
},
) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.expression)
			require.NoError(t, err, "Failed to parse expression: %s", tt.expression)

			sql, params, err := filter.ToSQL("postgres")
			require.NoError(t, err, "Failed to generate PostgreSQL SQL")

			require.Equal(t, tt.expectedSQL, sql, "SQL mismatch for expression: %s", tt.expression)
			require.Equal(t, tt.expectedParams, params, "Parameters mismatch for expression: %s", tt.expression)
		})
	}
}
