package mysql_test

import (
	"testing"

	"github.com/pseudomuto/where"
	// Import the MySQL driver
	_ "github.com/pseudomuto/where/drivers/mysql"
	"github.com/stretchr/testify/require"
)

func TestMySQLFunctions(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		// Date/Time functions
		{
			name:           "NOW function",
			expression:     "created_at > NOW()",
			expectedSQL:    "created_at > NOW()",
			expectedParams: []any{},
		},
		{
			name:           "DATE_FORMAT function",
			expression:     "DATE_FORMAT(created_at, '%Y-%m-%d') = '2024-01-15'",
			expectedSQL:    "DATE_FORMAT(created_at, ?) = ?",
			expectedParams: []any{"%Y-%m-%d", "2024-01-15"},
		},
		{
			name:           "TIMESTAMPDIFF function",
			expression:     "TIMESTAMPDIFF('MINUTE', start_time, end_time) > 30",
			expectedSQL:    "TIMESTAMPDIFF(?, start_time, end_time) > ?",
			expectedParams: []any{"MINUTE", float64(30)},
		},
		{
			name:           "STR_TO_DATE function",
			expression:     "STR_TO_DATE(date_string, '%Y-%m-%d') > '2024-01-01'",
			expectedSQL:    "STR_TO_DATE(date_string, ?) > ?",
			expectedParams: []any{"%Y-%m-%d", "2024-01-01"},
		},
		{
			name:           "YEAR and MONTH functions",
			expression:     "YEAR(created_at) = 2024 AND MONTH(created_at) IN (1, 2, 3)",
			expectedSQL:    "(YEAR(created_at) = ? AND MONTH(created_at) IN (?, ?, ?))",
			expectedParams: []any{float64(2024), float64(1), float64(2), float64(3)},
		},
		{
			name:           "DATEDIFF function",
			expression:     "DATEDIFF(end_date, start_date) > 7",
			expectedSQL:    "DATEDIFF(end_date, start_date) > ?",
			expectedParams: []any{float64(7)},
		},

		// String functions
		{
			name:           "CONCAT_WS function",
			expression:     "CONCAT_WS(' ', first_name, last_name) = 'John Doe'",
			expectedSQL:    "CONCAT_WS(?, first_name, last_name) = ?",
			expectedParams: []any{" ", "John Doe"},
		},
		{
			name:           "SUBSTRING_INDEX function",
			expression:     "SUBSTRING_INDEX(email, '@', 1) = 'admin'",
			expectedSQL:    "SUBSTRING_INDEX(email, ?, ?) = ?",
			expectedParams: []any{"@", float64(1), "admin"},
		},
		{
			name:           "LPAD function",
			expression:     "LPAD(id, 10, '0') = '0000000123'",
			expectedSQL:    "LPAD(id, ?, ?) = ?",
			expectedParams: []any{float64(10), "0", "0000000123"},
		},
		{
			name:           "LOCATE function",
			expression:     "LOCATE('admin', username) > 0",
			expectedSQL:    "LOCATE(?, username) > ?",
			expectedParams: []any{"admin", float64(0)},
		},
		{
			name:           "MD5 function",
			expression:     "MD5(password) = 'abc123'",
			expectedSQL:    "MD5(password) = ?",
			expectedParams: []any{"abc123"},
		},
		{
			name:           "HEX and UNHEX functions",
			expression:     "HEX(data) = 'FF00' OR UNHEX(hex_string) != ''",
			expectedSQL:    "(HEX(data) = ? OR UNHEX(hex_string) != ?)",
			expectedParams: []any{"FF00", ""},
		},

		// Mathematical functions
		{
			name:           "ROUND function",
			expression:     "ROUND(price, 2) = 19.99",
			expectedSQL:    "ROUND(price, ?) = ?",
			expectedParams: []any{float64(2), float64(19.99)},
		},
		{
			name:           "TRUNCATE function",
			expression:     "TRUNCATE(amount, 2) > 100",
			expectedSQL:    "TRUNCATE(amount, ?) > ?",
			expectedParams: []any{float64(2), float64(100)},
		},
		{
			name:           "MOD function",
			expression:     "MOD(number, 2) = 0",
			expectedSQL:    "MOD(number, ?) = ?",
			expectedParams: []any{float64(2), float64(0)},
		},
		{
			name:           "POW function",
			expression:     "POW(base, 2) = 100",
			expectedSQL:    "POW(base, ?) = ?",
			expectedParams: []any{float64(2), float64(100)},
		},
		{
			name:           "SQRT function",
			expression:     "SQRT(area) > 10",
			expectedSQL:    "SQRT(area) > ?",
			expectedParams: []any{float64(10)},
		},
		{
			name:           "trigonometric functions",
			expression:     "SIN(angle) > 0 AND COS(angle) < 1",
			expectedSQL:    "(SIN(angle) > ? AND COS(angle) < ?)",
			expectedParams: []any{float64(0), float64(1)},
		},

		// Conditional functions
		// Note: IF is not supported as it requires complex conditional expression parsing
		{
			name:           "IFNULL function",
			expression:     "IFNULL(nickname, username) != ''",
			expectedSQL:    "IFNULL(nickname, username) != ?",
			expectedParams: []any{""},
		},
		{
			name:           "NULLIF function",
			expression:     "NULLIF(value, 0) > 100",
			expectedSQL:    "NULLIF(value, ?) > ?",
			expectedParams: []any{float64(0), float64(100)},
		},
		{
			name:           "COALESCE function",
			expression:     "COALESCE(email, phone, 'N/A') != 'N/A'",
			expectedSQL:    "COALESCE(email, phone, ?) != ?",
			expectedParams: []any{"N/A", "N/A"},
		},

		// JSON functions (MySQL 5.7+)
		{
			name:           "JSON_EXTRACT function",
			expression:     "JSON_EXTRACT(metadata, '$.user_id') = '12345'",
			expectedSQL:    "JSON_EXTRACT(metadata, ?) = ?",
			expectedParams: []any{"$.user_id", "12345"},
		},
		{
			name:           "JSON_CONTAINS function",
			expression:     "JSON_CONTAINS(tags, '\"admin\"', '$') = 1",
			expectedSQL:    "JSON_CONTAINS(tags, ?, ?) = ?",
			expectedParams: []any{"\"admin\"", "$", float64(1)},
		},
		{
			name:           "JSON_LENGTH function",
			expression:     "JSON_LENGTH(items) > 0",
			expectedSQL:    "JSON_LENGTH(items) > ?",
			expectedParams: []any{float64(0)},
		},
		{
			name:           "JSON_VALID function",
			expression:     "JSON_VALID(data) = 1",
			expectedSQL:    "JSON_VALID(data) = ?",
			expectedParams: []any{float64(1)},
		},

		// Aggregate functions
		{
			name:           "GROUP_CONCAT function",
			expression:     "GROUP_CONCAT(tag) LIKE '%admin%'",
			expectedSQL:    "GROUP_CONCAT(tag) LIKE ?",
			expectedParams: []any{"%admin%"},
		},

		// Complex expressions
		{
			name: "complex MySQL expression",
			expression: `
				DATE_FORMAT(created_at, '%Y-%m') = '2024-01' AND
				CONCAT(first_name, ' ', last_name) LIKE '%Smith%' AND
				ROUND(price, 2) > 100 AND
				IFNULL(discount, 0) > 5
			`,
			expectedSQL:    "(DATE_FORMAT(created_at, ?) = ? AND CONCAT(first_name, ?, last_name) LIKE ? AND ROUND(price, ?) > ? AND IFNULL(discount, ?) > ?)",
			expectedParams: []any{"%Y-%m", "2024-01", " ", "%Smith%", float64(2), float64(100), float64(0), float64(5)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.expression)
			require.NoError(t, err, "Failed to parse expression: %s", tt.expression)

			sql, params, err := filter.ToSQL("mysql")
			require.NoError(t, err, "Failed to generate MySQL SQL")

			require.Equal(t, tt.expectedSQL, sql, "SQL mismatch for expression: %s", tt.expression)
			require.Equal(t, tt.expectedParams, params, "Parameters mismatch for expression: %s", tt.expression)
		})
	}
}

func TestMySQLILIKETranslation(t *testing.T) {
	// MySQL doesn't support ILIKE, it should be translated to LOWER + LIKE
	tests := []struct {
		name           string
		expression     string
		expectedSQL    string
		expectedParams []any
	}{
		{
			name:           "ILIKE operator",
			expression:     "name ILIKE '%john%'",
			expectedSQL:    "LOWER(name) LIKE LOWER(?)",
			expectedParams: []any{"%john%"},
		},
		{
			name:           "NOT ILIKE operator",
			expression:     "email NOT ILIKE '%spam%'",
			expectedSQL:    "LOWER(email) NOT LIKE LOWER(?)",
			expectedParams: []any{"%spam%"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.expression)
			require.NoError(t, err)

			sql, params, err := filter.ToSQL("mysql")
			require.NoError(t, err)

			require.Equal(t, tt.expectedSQL, sql)
			require.Equal(t, tt.expectedParams, params)
		})
	}
}
