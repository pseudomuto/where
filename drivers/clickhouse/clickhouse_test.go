package clickhouse_test

import (
	"testing"

	"github.com/pseudomuto/where"
	// Import the ClickHouse driver
	_ "github.com/pseudomuto/where/drivers/clickhouse"
	"github.com/stretchr/testify/require"
)

func TestClickHouseFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		// Date/Time formatting functions (case-sensitive)
		{
			name:           "toYYYYMM function",
			expression:     "toYYYYMM(created_at) = 202401",
			expectedSQL:    "toYYYYMM(created_at) = ?",
			expectedParams: []any{float64(202401)},
		},
		{
			name:           "toYYYYMMDD function",
			expression:     "toYYYYMMDD(timestamp) >= 20240115",
			expectedSQL:    "toYYYYMMDD(`timestamp`) >= ?",
			expectedParams: []any{float64(20240115)},
		},
		{
			name:           "toYYYYMMDDhhmmss function",
			expression:     "toYYYYMMDDhhmmss(event_time) > 20240115120000",
			expectedSQL:    "toYYYYMMDDhhmmss(event_time) > ?",
			expectedParams: []any{float64(20240115120000)},
		},

		// Date extraction functions
		{
			name:           "toYear function",
			expression:     "toYear(created_at) = 2024",
			expectedSQL:    "toYear(created_at) = ?",
			expectedParams: []any{float64(2024)},
		},
		{
			name:           "toMonth function",
			expression:     "toMonth(created_at) IN (1, 2, 3)",
			expectedSQL:    "toMonth(created_at) IN (?, ?, ?)",
			expectedParams: []any{float64(1), float64(2), float64(3)},
		},
		{
			name:           "toDayOfWeek function",
			expression:     "toDayOfWeek(event_date) = 1",
			expectedSQL:    "toDayOfWeek(event_date) = ?",
			expectedParams: []any{float64(1)},
		},

		// Date truncation functions
		{
			name:           "toStartOfMonth function",
			expression:     "toStartOfMonth(created_at) = '2024-01-01'",
			expectedSQL:    "toStartOfMonth(created_at) = ?",
			expectedParams: []any{"2024-01-01"},
		},
		{
			name:           "toStartOfWeek function with timezone",
			expression:     "toStartOfWeek(timestamp, 'UTC') >= '2024-01-01'",
			expectedSQL:    "toStartOfWeek(`timestamp`, ?) >= ?",
			expectedParams: []any{"UTC", "2024-01-01"},
		},

		// String functions
		{
			name:           "toString function",
			expression:     "toString(user_id) = '12345'",
			expectedSQL:    "toString(user_id) = ?",
			expectedParams: []any{"12345"},
		},
		{
			name:           "startsWith function",
			expression:     "startsWith(email, 'admin') = true",
			expectedSQL:    "startsWith(email, ?) = TRUE",
			expectedParams: []any{"admin"},
		},
		{
			name:           "endsWith function",
			expression:     "endsWith(domain, '.com') = true",
			expectedSQL:    "endsWith(domain, ?) = TRUE",
			expectedParams: []any{".com"},
		},
		{
			name:           "leftPad function",
			expression:     "leftPad(id, 10, '0') = '0000012345'",
			expectedSQL:    "leftPad(`id`, ?, ?) = ?",
			expectedParams: []any{float64(10), "0", "0000012345"},
		},

		// Array functions
		{
			name:           "arrayLength function",
			expression:     "arrayLength(tags) > 0",
			expectedSQL:    "arrayLength(tags) > ?",
			expectedParams: []any{float64(0)},
		},
		{
			name:           "has function",
			expression:     "has(categories, 'electronics') = true",
			expectedSQL:    "has(categories, ?) = TRUE",
			expectedParams: []any{"electronics"},
		},
		{
			name:           "arrayElement function",
			expression:     "arrayElement(tags, 1) = 'important'",
			expectedSQL:    "arrayElement(tags, ?) = ?",
			expectedParams: []any{float64(1), "important"},
		},
		{
			name:           "arraySlice function",
			expression:     "arrayLength(arraySlice(items, 1, 5)) = 5",
			expectedSQL:    "arrayLength(arraySlice(items, ?, ?)) = ?",
			expectedParams: []any{float64(1), float64(5), float64(5)},
		},

		// Mathematical functions
		{
			name:           "abs function",
			expression:     "abs(balance) > 100",
			expectedSQL:    "abs(balance) > ?",
			expectedParams: []any{float64(100)},
		},
		{
			name:           "round function",
			expression:     "round(price, 2) = 19.99",
			expectedSQL:    "round(price, ?) = ?",
			expectedParams: []any{float64(2), float64(19.99)},
		},
		{
			name:           "sqrt function",
			expression:     "sqrt(area) > 10",
			expectedSQL:    "sqrt(area) > ?",
			expectedParams: []any{float64(10)},
		},
		{
			name:           "pow function",
			expression:     "pow(base, 2) = 100",
			expectedSQL:    "pow(base, ?) = ?",
			expectedParams: []any{float64(2), float64(100)},
		},

		// Conditional functions
		{
			name:           "isNull function",
			expression:     "isNull(optional_field) = true",
			expectedSQL:    "isNull(optional_field) = TRUE",
			expectedParams: []any{},
		},
		{
			name:           "ifNull function",
			expression:     "ifNull(description, 'No description') != 'No description'",
			expectedSQL:    "ifNull(description, ?) != ?",
			expectedParams: []any{"No description", "No description"},
		},

		// Hash functions
		{
			name:           "MD5 function",
			expression:     "MD5(password) = 'abc123'",
			expectedSQL:    "MD5(password) = ?",
			expectedParams: []any{"abc123"},
		},
		{
			name:           "SHA256 function",
			expression:     "SHA256(token) != ''",
			expectedSQL:    "SHA256(token) != ?",
			expectedParams: []any{""},
		},

		// JSON functions
		{
			name:           "JSONExtract function",
			expression:     "JSONExtract(metadata, '$.user_id') = '12345'",
			expectedSQL:    "JSONExtract(metadata, ?) = ?",
			expectedParams: []any{"$.user_id", "12345"},
		},
		{
			name:           "JSONExtractString function",
			expression:     "JSONExtractString(config, '$.environment') = 'production'",
			expectedSQL:    "JSONExtractString(config, ?) = ?",
			expectedParams: []any{"$.environment", "production"},
		},

		// URL functions
		{
			name:           "domain function",
			expression:     "domain(url) = 'example.com'",
			expectedSQL:    "domain(url) = ?",
			expectedParams: []any{"example.com"},
		},
		{
			name:           "protocol function",
			expression:     "protocol(url) = 'https'",
			expectedSQL:    "protocol(url) = ?",
			expectedParams: []any{"https"},
		},

		// Encoding functions
		{
			name:           "base64Encode function",
			expression:     "base64Encode(data) != ''",
			expectedSQL:    "base64Encode(data) != ?",
			expectedParams: []any{""},
		},
		{
			name:           "hex function",
			expression:     "hex(binary_data) LIKE 'FF%'",
			expectedSQL:    "hex(binary_data) LIKE ?",
			expectedParams: []any{"FF%"},
		},

		// Complex expressions with multiple ClickHouse functions
		{
			name: "complex ClickHouse expression",
			expression: `
				toYYYYMM(event_time) = 202401 AND
				has(tags, 'conversion') = true AND
				abs(revenue) > 0 AND
				startsWith(domain(referrer), 'google') = true
			`,
			expectedSQL:    "(toYYYYMM(event_time) = ? AND has(tags, ?) = TRUE AND abs(revenue) > ? AND startsWith(domain(referrer), ?) = TRUE)",
			expectedParams: []any{float64(202401), "conversion", float64(0), "google"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.expression)
			require.NoError(t, err, "Failed to parse expression: %s", tt.expression)

			sql, params, err := filter.ToSQL("clickhouse")
			require.NoError(t, err, "Failed to generate ClickHouse SQL")

			require.Equal(t, tt.expectedSQL, sql, "SQL mismatch for expression: %s", tt.expression)
			require.Equal(t, tt.expectedParams, params, "Parameters mismatch for expression: %s", tt.expression)
		})
	}
}

func TestClickHouseCaseSensitivity(t *testing.T) {
	tests := []struct {
		name             string
		expression       string
		shouldWork       bool
		expectedFunction string
	}{
		{
			name:             "toYYYYMM - correct case",
			expression:       "toYYYYMM(date) = 202401",
			shouldWork:       true,
			expectedFunction: "toYYYYMM",
		},
		{
			name:             "toString - correct case",
			expression:       "toString(id) = '123'",
			shouldWork:       true,
			expectedFunction: "toString",
		},
		{
			name:             "arrayLength - correct case",
			expression:       "arrayLength(items) > 0",
			shouldWork:       true,
			expectedFunction: "arrayLength",
		},
		{
			name:             "startsWith - correct case",
			expression:       "startsWith(text, 'hello') = true",
			shouldWork:       true,
			expectedFunction: "startsWith",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.expression)
			require.NoError(t, err, "Failed to parse expression")

			sql, _, err := filter.ToSQL("clickhouse")
			if tt.shouldWork {
				require.NoError(t, err, "Expected expression to work")
				require.Contains(t, sql, tt.expectedFunction, "Expected function name to be preserved")
			} else {
				require.Error(t, err, "Expected expression to fail")
			}
		})
	}
}

func TestClickHouseArrayOperations(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "array element access",
			expression:     "arrayElement(tags, 1) = 'primary'",
			expectedSQL:    "arrayElement(tags, ?) = ?",
			expectedParams: []any{float64(1), "primary"},
		},
		{
			name:           "array contains check",
			expression:     "has(categories, 'electronics') = true AND has(features, 'wireless') = true",
			expectedSQL:    "(has(categories, ?) = TRUE AND has(features, ?) = TRUE)",
			expectedParams: []any{"electronics", "wireless"},
		},
		{
			name:           "array operations combination",
			expression:     "arrayLength(tags) > 0 AND has(tags, 'important') = true",
			expectedSQL:    "(arrayLength(tags) > ? AND has(tags, ?) = TRUE)",
			expectedParams: []any{float64(0), "important"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.expression)
			require.NoError(t, err)

			sql, params, err := filter.ToSQL("clickhouse")
			require.NoError(t, err)

			require.Equal(t, tt.expectedSQL, sql)
			require.Equal(t, tt.expectedParams, params)
		})
	}
}
