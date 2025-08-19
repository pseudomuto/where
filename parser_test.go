package where_test

import (
	"testing"

	"github.com/pseudomuto/where"
	_ "github.com/pseudomuto/where/drivers/clickhouse"
	_ "github.com/pseudomuto/where/drivers/mysql"
	_ "github.com/pseudomuto/where/drivers/postgres"
	"github.com/stretchr/testify/require"
)

func TestParseSimpleComparison(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"equal", "age = 18"},
		{"not equal", "status != 'active'"},
		{"less than", "count < 100"},
		{"greater than", "price > 99.99"},
		{"less or equal", "quantity <= 10"},
		{"greater or equal", "score >= 80"},
		{"case insensitive keywords", "AGE = 18 AND STATUS != 'active'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseLikeExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple like", "name LIKE '%john%'"},
		{"not like", "email NOT LIKE '%spam%'"},
		{"ilike", "description ILIKE '%search%'"},
		{"not ilike", "title NOT ILIKE '%test%'"},
		{"case variations", "name like '%john%' AND email NOT LIKE '%spam%'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseBetweenExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple between", "age BETWEEN 18 AND 65"},
		{"not between", "price NOT BETWEEN 10 AND 100"},
		{"with decimals", "score BETWEEN 0.0 AND 100.0"},
		{"case insensitive", "age between 18 and 65"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseInExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple in", "status IN ('active', 'pending')"},
		{"not in", "type NOT IN ('test', 'demo')"},
		{"numeric in", "id IN (1, 2, 3, 4, 5)"},
		{"single value", "category IN ('news')"},
		{"case insensitive", "status in ('active', 'pending')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseIsNullExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"is null", "deleted_at IS NULL"},
		{"is not null", "created_at IS NOT NULL"},
		{"case insensitive", "deleted_at is null"},
		{"mixed case", "created_at Is Not Null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseLogicalOperators(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple and", "age > 18 AND status = 'active'"},
		{"simple or", "type = 'admin' OR type = 'moderator'"},
		{"not expression", "NOT (status = 'deleted')"},
		{"complex mix", "(age >= 18 AND verified = true) OR role = 'admin'"},
		{"nested parentheses", "((a = 1 OR b = 2) AND c = 3) OR d = 4"},
		{"case insensitive", "age > 18 and status = 'active' or role = 'admin'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"no args", "created_at > NOW()"},
		{"single arg", "LOWER(name) = 'john'"},
		{"multiple args", "COALESCE(nickname, username, 'anonymous') != ''"},
		{"nested function", "LENGTH(TRIM(name)) > 0"},
		{"case insensitive", "lower(name) = 'john'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseFieldReferences(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple field", "age > 18"},
		{"qualified field", "users.age > 18"},
		{"fully qualified", "db.users.age > 18"},
		{"quoted field", "`order` > 100"},
		{"double quoted", `"select" = 'test'`},
		{"mixed quotes", "`user`.`order` > 100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseLiterals(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"string single quote", "name = 'John'"},
		{"string double quote", `email = "john@example.com"`},
		{"integer", "age = 25"},
		{"float", "price = 99.99"},
		{"negative number", "balance = -100.50"},
		{"scientific notation", "value = 1.5e10"},
		{"boolean true", "active = true"},
		{"boolean false", "deleted = false"},
		{"null", "parent_id = null"},
		{"case insensitive bool", "active = TRUE AND deleted = FALSE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseComplexExpressions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			"kitchen sink",
			`(age BETWEEN 18 AND 65 OR is_verified = true) AND 
			 LOWER(email) NOT LIKE '%spam%' AND 
			 status IN ('active', 'premium') AND 
			 NOT (country = 'XX' OR ip_address IS NULL)`,
		},
		{
			"functions and operators",
			`DATE(created_at) = '2024-01-01' AND 
			 COALESCE(nickname, username) LIKE '%admin%' AND
			 LENGTH(password) >= 8`,
		},
		{
			"nested conditions",
			`(
				(category = 'electronics' AND price BETWEEN 100 AND 1000) OR
				(category = 'books' AND price < 50)
			 ) AND 
			 stock > 0 AND
			 vendor_id NOT IN (1, 2, 3)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{"empty input", "", "empty filter expression"},
		{"invalid operator", "age >> 18", ""},
		{"unclosed parenthesis", "(age > 18", ""},
		{"empty in list", "id IN ()", "unexpected token \")\""},
		{"invalid syntax", "age > > 18", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.Error(t, err)
			require.Nil(t, filter)
			if tt.error != "" {
				require.Contains(t, err.Error(), tt.error)
			}
		})
	}
}

func TestParserOptions(t *testing.T) {
	t.Run("max depth", func(t *testing.T) {
		parser, err := where.NewParser(where.WithMaxDepth(2))
		require.NoError(t, err)

		filter, err := parser.Parse("(((age > 18)))")
		require.Error(t, err)
		require.Nil(t, filter)
		require.Contains(t, err.Error(), "depth exceeds maximum")
	})

	t.Run("max IN items", func(t *testing.T) {
		parser, err := where.NewParser(where.WithMaxINItems(3))
		require.NoError(t, err)

		filter, err := parser.Parse("id IN (1, 2, 3, 4)")
		require.Error(t, err)
		require.Nil(t, filter)
		require.Contains(t, err.Error(), "exceeds maximum")
	})

	t.Run("allowed functions", func(t *testing.T) {
		parser, err := where.NewParser(where.WithFunctions("LOWER", "UPPER"))
		require.NoError(t, err)

		_, err = parser.Parse("LOWER(name) = 'john'")
		require.NoError(t, err)

		_, err = parser.Parse("LENGTH(name) > 0")
		require.Error(t, err)
		require.Contains(t, err.Error(), "not allowed")
	})
}
