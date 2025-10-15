package postgres

import (
	"fmt"
	"slices"
	"strings"

	"github.com/pseudomuto/where"
)

var supportedFeatures = []string{
	"ARRAY",
	"CTE",
	"ILIKE",
	"JSON",
	"JSONB",
	"RETURNING",
	"WINDOW",
}

type (
	// PostgreSQLDriver implements the where.Driver interface for PostgreSQL databases.
	PostgreSQLDriver struct{}
)

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
	return &PostgreSQLDriver{}
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
	if where.NeedsQuoting(name, d) {
		return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
	}
	return name
}

func (d *PostgreSQLDriver) Placeholder(position int) string {
	return fmt.Sprintf("$%d", position)
}

func (d *PostgreSQLDriver) Keywords() []string {
	return keywords
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
	return slices.Contains(supportedFeatures, strings.ToUpper(feature))
}

func init() {
	driver := NewPostgreSQLDriver()
	where.RegisterDriver("postgres", driver)
	where.RegisterDriver("postgresql", driver)
	where.RegisterDriver("pg", driver)
}
