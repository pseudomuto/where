package where_test

import (
	"testing"

	"github.com/pseudomuto/where"
	_ "github.com/pseudomuto/where/drivers/clickhouse"
	_ "github.com/pseudomuto/where/drivers/mysql"
	_ "github.com/pseudomuto/where/drivers/postgres"
	"github.com/stretchr/testify/require"
)

func TestSQLGenerationPostgreSQL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "simple comparison",
			input:    "age > 18",
			wantSQL:  "age > $1",
			wantArgs: []any{float64(18)},
		},
		{
			name:     "string comparison",
			input:    "name = 'John'",
			wantSQL:  "name = $1",
			wantArgs: []any{"John"},
		},
		{
			name:     "AND condition",
			input:    "age > 18 AND status = 'active'",
			wantSQL:  "(age > $1 AND status = $2)",
			wantArgs: []any{float64(18), "active"},
		},
		{
			name:     "OR condition",
			input:    "type = 'admin' OR type = 'moderator'",
			wantSQL:  "(type = $1 OR type = $2)",
			wantArgs: []any{"admin", "moderator"},
		},
		{
			name:     "BETWEEN",
			input:    "age BETWEEN 18 AND 65",
			wantSQL:  "age BETWEEN $1 AND $2",
			wantArgs: []any{float64(18), float64(65)},
		},
		{
			name:     "IN list",
			input:    "status IN ('active', 'pending', 'approved')",
			wantSQL:  "status IN ($1, $2, $3)",
			wantArgs: []any{"active", "pending", "approved"},
		},
		{
			name:     "IS NULL",
			input:    "deleted_at IS NULL",
			wantSQL:  "deleted_at IS NULL",
			wantArgs: []any{},
		},
		{
			name:     "IS NOT NULL",
			input:    "created_at IS NOT NULL",
			wantSQL:  "created_at IS NOT NULL",
			wantArgs: []any{},
		},
		{
			name:     "LIKE",
			input:    "name LIKE '%john%'",
			wantSQL:  "name LIKE $1",
			wantArgs: []any{"%john%"},
		},
		{
			name:     "ILIKE",
			input:    "email ILIKE '%@gmail.com'",
			wantSQL:  "email ILIKE $1",
			wantArgs: []any{"%@gmail.com"},
		},
		{
			name:     "NOT expression",
			input:    "NOT (status = 'deleted')",
			wantSQL:  "NOT (status = $1)",
			wantArgs: []any{"deleted"},
		},
		{
			name:     "function call",
			input:    "LOWER(name) = 'john'",
			wantSQL:  "LOWER(name) = $1",
			wantArgs: []any{"john"},
		},
		{
			name:     "reserved keyword as column",
			input:    "user = 'admin'",
			wantSQL:  `"user" = $1`,
			wantArgs: []any{"admin"},
		},
		{
			name:     "qualified column",
			input:    "users.order > 100",
			wantSQL:  `users."order" > $1`,
			wantArgs: []any{float64(100)},
		},
		{
			name:     "complex expression",
			input:    "(age >= 18 AND status = 'active') OR role = 'admin'",
			wantSQL:  "((age >= $1 AND status = $2) OR role = $3)",
			wantArgs: []any{float64(18), "active", "admin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)

			sql, args, err := filter.ToSQL("postgres")
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, sql)
			require.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestSQLGenerationMySQL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "simple comparison",
			input:    "age > 18",
			wantSQL:  "age > ?",
			wantArgs: []any{float64(18)},
		},
		{
			name:     "reserved keyword as column",
			input:    "select = 'test'",
			wantSQL:  "`select` = ?",
			wantArgs: []any{"test"},
		},
		{
			name:     "ILIKE becomes LIKE",
			input:    "name ILIKE '%john%'",
			wantSQL:  "LOWER(name) LIKE LOWER(?)",
			wantArgs: []any{"%john%"},
		},
		{
			name:     "IN list",
			input:    "id IN (1, 2, 3)",
			wantSQL:  "id IN (?, ?, ?)",
			wantArgs: []any{float64(1), float64(2), float64(3)},
		},
		{
			name:     "qualified reserved keyword",
			input:    "db.table.order > 100",
			wantSQL:  "db.`table`.`order` > ?",
			wantArgs: []any{float64(100)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)

			sql, args, err := filter.ToSQL("mysql")
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, sql)
			require.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestSQLGenerationClickHouse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "simple comparison",
			input:    "age > 18",
			wantSQL:  "age > ?",
			wantArgs: []any{float64(18)},
		},
		{
			name:     "reserved keyword",
			input:    "cluster = 'main'",
			wantSQL:  "`cluster` = ?",
			wantArgs: []any{"main"},
		},
		{
			name:     "ILIKE native support",
			input:    "name ILIKE '%test%'",
			wantSQL:  "name ILIKE ?",
			wantArgs: []any{"%test%"},
		},
		{
			name:     "array syntax",
			input:    "id IN (1, 2, 3)",
			wantSQL:  "`id` IN (?, ?, ?)",
			wantArgs: []any{float64(1), float64(2), float64(3)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)

			sql, args, err := filter.ToSQL("clickhouse")
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, sql)
			require.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestDriverAliases(t *testing.T) {
	filter, err := where.Parse("age > 18")
	require.NoError(t, err)

	t.Run("PostgreSQL aliases", func(t *testing.T) {
		sql1, _, err1 := filter.ToSQL("postgres")
		require.NoError(t, err1)

		sql2, _, err2 := filter.ToSQL("postgresql")
		require.NoError(t, err2)

		sql3, _, err3 := filter.ToSQL("pg")
		require.NoError(t, err3)

		require.Equal(t, sql1, sql2)
		require.Equal(t, sql1, sql3)
	})

	t.Run("MySQL aliases", func(t *testing.T) {
		sql1, _, err1 := filter.ToSQL("mysql")
		require.NoError(t, err1)

		sql2, _, err2 := filter.ToSQL("mariadb")
		require.NoError(t, err2)

		require.Equal(t, sql1, sql2)
	})

	t.Run("ClickHouse aliases", func(t *testing.T) {
		sql1, _, err1 := filter.ToSQL("clickhouse")
		require.NoError(t, err1)

		sql2, _, err2 := filter.ToSQL("ch")
		require.NoError(t, err2)

		require.Equal(t, sql1, sql2)
	})
}

func TestValidator(t *testing.T) {
	t.Run("field validation", func(t *testing.T) {
		filter, err := where.Parse("age > 18 AND name = 'John'")
		require.NoError(t, err)

		validator := where.NewValidator().AllowFields("age")

		_, _, err = filter.ToSQL("postgres", where.WithValidator(validator))
		require.Error(t, err)
		require.Contains(t, err.Error(), "field \"name\" is not allowed")

		validator.AllowFields("name")
		_, _, err = filter.ToSQL("postgres", where.WithValidator(validator))
		require.NoError(t, err)
	})

	t.Run("function validation", func(t *testing.T) {
		filter, err := where.Parse("LOWER(name) = 'john'")
		require.NoError(t, err)

		validator := where.NewValidator().AllowFields("name")

		_, _, err = filter.ToSQL("postgres", where.WithValidator(validator))
		require.Error(t, err)
		require.Contains(t, err.Error(), "function \"LOWER\" is not allowed")

		validator.AllowFunctions("LOWER")
		_, _, err = filter.ToSQL("postgres", where.WithValidator(validator))
		require.NoError(t, err)
	})

	t.Run("allow all", func(t *testing.T) {
		filter, err := where.Parse("LOWER(secret_field) = 'test'")
		require.NoError(t, err)

		validator := where.NewValidator().AllowAll()

		_, _, err = filter.ToSQL("postgres", where.WithValidator(validator))
		require.NoError(t, err)
	})
}

func TestBooleanAndNullLiterals(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "boolean true",
			input:    "active = true",
			wantSQL:  "active = TRUE",
			wantArgs: []any{},
		},
		{
			name:     "boolean false",
			input:    "deleted = false",
			wantSQL:  "deleted = FALSE",
			wantArgs: []any{},
		},
		{
			name:     "null comparison",
			input:    "parent_id = null",
			wantSQL:  "parent_id = NULL",
			wantArgs: []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)

			sql, args, err := filter.ToSQL("postgres")
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, sql)
			require.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestInvalidDriver(t *testing.T) {
	filter, err := where.Parse("age > 18")
	require.NoError(t, err)

	_, _, err = filter.ToSQL("nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "driver \"nonexistent\" not registered")
}

func TestQuotedIdentifiers(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		driver  string
		wantSQL string
	}{
		{
			name:    "backtick to postgres",
			input:   "`order` > 100",
			driver:  "postgres",
			wantSQL: `"order" > $1`,
		},
		{
			name:    "double quote to mysql",
			input:   `"select" = 'test'`,
			driver:  "mysql",
			wantSQL: "`select` = ?",
		},
		{
			name:    "mixed quotes qualified",
			input:   "`users`.`order` > 100",
			driver:  "postgres",
			wantSQL: `users."order" > $1`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := where.Parse(tt.input)
			require.NoError(t, err)

			sql, _, err := filter.ToSQL(tt.driver)
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, sql)
		})
	}
}
