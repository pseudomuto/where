package where_test

import (
	"testing"

	"github.com/pseudomuto/where"
	"github.com/stretchr/testify/require"
)

func TestNewLexer(t *testing.T) {
	t.Run("creates lexer without error", func(t *testing.T) {
		lexer, err := where.NewLexer()
		require.NoError(t, err)
		require.NotNil(t, lexer)
	})

	t.Run("lexer can be used to create parser", func(t *testing.T) {
		lexer, err := where.NewLexer()
		require.NoError(t, err)
		require.NotNil(t, lexer)

		// The real test is whether the lexer can be used by the parser
		// We test this indirectly by verifying the parser can parse expressions
		// This ensures the lexer rules are correctly defined

		testCases := []string{
			"age > 18",
			"name = 'John'",
			"age > 18 AND status = 'active'",
			"type = 'admin' OR role = 'user'",
			"NOT (active = true)",
			"age BETWEEN 18 AND 65",
			"id IN (1, 2, 3)",
			"name LIKE '%john%'",
			"email ILIKE '%@gmail.com'",
			"deleted_at IS NULL",
			"created_at IS NOT NULL",
			"LOWER(name) = 'test'",
			"users.name = 'john'",
			"(age > 18)",
			"active = true AND deleted = false",
			"parent_id = null",
			"`order` > 100",
			`"select" = 'test'`,
			"value = 1.5e10",
			"temperature = -15.5",
		}

		for _, testCase := range testCases {
			t.Run("can parse: "+testCase, func(t *testing.T) {
				filter, err := where.Parse(testCase)
				require.NoError(t, err, "Failed to parse: %s", testCase)
				require.NotNil(t, filter)
			})
		}
	})

	t.Run("lexer handles case insensitive keywords", func(t *testing.T) {
		// Test that the lexer properly handles case insensitive keywords
		// by verifying they can be parsed correctly
		testCases := []struct {
			name  string
			input string
		}{
			{"uppercase keywords", "AGE > 18 AND STATUS = 'ACTIVE'"},
			{"lowercase keywords", "age > 18 and status = 'active'"},
			{"mixed case keywords", "Age > 18 And Status = 'Active'"},
			{"BETWEEN variations", "age BETWEEN 18 AND 65"},
			{"between variations", "age between 18 and 65"},
			{"IN variations", "id IN (1, 2)"},
			{"in variations", "id in (1, 2)"},
			{"LIKE variations", "name LIKE '%test%'"},
			{"like variations", "name like '%test%'"},
			{"ILIKE variations", "name ILIKE '%test%'"},
			{"ilike variations", "name ilike '%test%'"},
			{"IS NULL variations", "field IS NULL"},
			{"is null variations", "field is null"},
			{"boolean variations", "active = TRUE AND deleted = FALSE"},
			{"boolean variations lower", "active = true and deleted = false"},
		}

		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				filter, err := where.Parse(tt.input)
				require.NoError(t, err, "Failed to parse case insensitive expression: %s", tt.input)
				require.NotNil(t, filter)
			})
		}
	})

	t.Run("lexer handles quoted identifiers", func(t *testing.T) {
		testCases := []struct {
			name  string
			input string
		}{
			{"backtick identifier", "`order` > 100"},
			{"backtick with spaces", "`user name` = 'test'"},
			{"double quoted identifier", `"select" = 'test'`},
			{"qualified backtick", "`users`.`order` > 100"},
			{"mixed quotes", `"user"."order" > 100`},
		}

		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				filter, err := where.Parse(tt.input)
				require.NoError(t, err, "Failed to parse quoted identifier expression: %s", tt.input)
				require.NotNil(t, filter)
			})
		}
	})

	t.Run("lexer handles different number formats", func(t *testing.T) {
		testCases := []struct {
			name  string
			input string
		}{
			{"positive integer", "age = 123"},
			{"negative integer", "balance = -456"},
			{"positive float", "price = 123.456"},
			{"negative float", "balance = -123.456"},
			{"scientific notation", "value = 1.5e10"},
			{"scientific notation negative", "value = -1.5e-10"},
			{"scientific notation capital E", "value = 1.5E10"},
			{"zero", "count = 0"},
			{"zero float", "rate = 0.0"},
		}

		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				filter, err := where.Parse(tt.input)
				require.NoError(t, err, "Failed to parse number expression: %s", tt.input)
				require.NotNil(t, filter)
			})
		}
	})

	t.Run("lexer handles string literals", func(t *testing.T) {
		testCases := []struct {
			name  string
			input string
		}{
			{"single quoted string", "name = 'hello'"},
			{"single quoted with spaces", "name = 'hello world'"},
			{"double quoted string", `name = "hello"`},
			{"double quoted with spaces", `name = "hello world"`},
			{"empty single quoted", "name = ''"},
			{"empty double quoted", `name = ""`},
			{"escaped single quote", `name = 'don\'t'`},
			{"escaped double quote", `name = "say \"hello\""`},
		}

		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				filter, err := where.Parse(tt.input)
				require.NoError(t, err, "Failed to parse string literal expression: %s", tt.input)
				require.NotNil(t, filter)
			})
		}
	})

	t.Run("lexer handles complex expressions", func(t *testing.T) {
		testCases := []string{
			`(age BETWEEN 18 AND 65 OR verified = true) AND 
			 LOWER(email) NOT LIKE '%spam%' AND 
			 status IN ('active', 'premium') AND 
			 NOT (country = 'XX' OR ip_address IS NULL)`,

			`LENGTH(TRIM(users.name)) > 0 AND 
			 accounts.balance >= -100.50 AND
			 UPPER(profiles.status) != 'DELETED'`,

			`value = 1.5e10 AND active = true AND 
			 threshold <= -2.3E-5 AND deleted = false`,
		}

		for i, testCase := range testCases {
			t.Run("complex expression "+string(rune('A'+i)), func(t *testing.T) {
				filter, err := where.Parse(testCase)
				require.NoError(t, err, "Failed to parse complex expression: %s", testCase)
				require.NotNil(t, filter)
			})
		}
	})
}
