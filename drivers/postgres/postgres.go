package postgres

import (
	"fmt"
	"strings"

	"github.com/pseudomuto/where"
)

// PostgreSQLDriver implements the where.Driver interface for PostgreSQL databases.
type PostgreSQLDriver struct {
	keywords map[string]bool
}

// NewPostgreSQLDriver creates a new PostgreSQL driver instance.
//
// Example:
//
//	import (
//		"github.com/pseudomuto/where"
//		_ "github.com/pseudomuto/where/drivers/postgres"
//	)
//
//	filter, params, _ := where.Build("age > 18", "postgres")
//	// SELECT * FROM users WHERE age > $1
func NewPostgreSQLDriver() *PostgreSQLDriver {
	return &PostgreSQLDriver{
		keywords: PostgreSQLKeywords,
	}
}

func (d *PostgreSQLDriver) Name() string {
	return "postgres"
}

func (d *PostgreSQLDriver) QuoteIdentifier(name string) string {
	if name == "" {
		return name
	}

	name = strings.TrimSpace(name)

	if strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
		return name
	}
	if strings.HasPrefix(name, "`") && strings.HasSuffix(name, "`") {
		name = name[1 : len(name)-1]
	}

	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		quoted := make([]string, len(parts))
		for i, part := range parts {
			quoted[i] = d.quoteSimpleIdentifier(part)
		}
		return strings.Join(quoted, ".")
	}

	return d.quoteSimpleIdentifier(name)
}

func (d *PostgreSQLDriver) quoteSimpleIdentifier(name string) string {
	if d.needsQuoting(name) {
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
	}
	return name
}

func (d *PostgreSQLDriver) needsQuoting(name string) bool {
	if d.IsReservedKeyword(name) {
		return true
	}

	if name == "" {
		return false
	}

	if name[0] >= '0' && name[0] <= '9' {
		return true
	}

	for _, ch := range name {
		if (ch < 'a' || ch > 'z') &&
			(ch < 'A' || ch > 'Z') &&
			(ch < '0' || ch > '9') &&
			ch != '_' {
			return true
		}
	}

	return false
}

func (d *PostgreSQLDriver) Placeholder(position int) string {
	return fmt.Sprintf("$%d", position)
}

func (d *PostgreSQLDriver) IsReservedKeyword(word string) bool {
	return d.keywords[strings.ToUpper(word)]
}

func (d *PostgreSQLDriver) TranslateOperator(op string) (string, bool) {
	upperOp := strings.ToUpper(op)
	switch upperOp {
	case "=", "!=", "<>", "<", ">", "<=", ">=":
		return op, true
	case "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE":
		return upperOp, true
	case "IN", "NOT IN":
		return upperOp, true
	case "IS NULL", "IS NOT NULL":
		return upperOp, true
	case "BETWEEN", "NOT BETWEEN":
		return upperOp, true
	default:
		return "", false
	}
}

func (d *PostgreSQLDriver) SupportsFeature(feature string) bool {
	switch strings.ToUpper(feature) {
	case "ILIKE", "ARRAY", "JSON", "JSONB", "RETURNING", "CTE", "WINDOW":
		return true
	default:
		return false
	}
}

func init() {
	driver := NewPostgreSQLDriver()
	where.RegisterDriver("postgres", driver)
	where.RegisterDriver("postgresql", driver)
	where.RegisterDriver("pg", driver)
}
