package mysql

import (
	"fmt"
	"slices"
	"strings"

	"github.com/pseudomuto/where"
)

var supportedFeatures = []string{
	"CTE",
	"FULLTEXT",
	"JSON",
	"PARTITION",
	"SPATIAL",
}

type (
	// MySQLDriver implements the where.Driver interface for MySQL and MariaDB databases.
	MySQLDriver struct {
		keywords map[string]bool
	}
)

// NewMySQLDriver creates a new MySQL driver instance.
//
// Example:
//
//	import (
//		"github.com/pseudomuto/where"
//		_ "github.com/pseudomuto/where/drivers/mysql"
//	)
//
//	filter, params, _ := where.Build("age > 18", "mysql")
//	// SELECT * FROM users WHERE age > ?
func NewMySQLDriver() *MySQLDriver {
	return &MySQLDriver{
		keywords: MySQLKeywords,
	}
}

func (d *MySQLDriver) Name() string {
	return "mysql"
}

func (d *MySQLDriver) QuoteIdentifier(name string) string {
	if name == "" {
		return name
	}

	name = strings.TrimSpace(name)

	if strings.HasPrefix(name, "`") && strings.HasSuffix(name, "`") {
		return name
	}
	if strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
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

func (d *MySQLDriver) quoteSimpleIdentifier(name string) string {
	if where.NeedsQuoting(name, d) {
		return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
	}
	return name
}

func (d *MySQLDriver) Placeholder(position int) string {
	return "?"
}

func (d *MySQLDriver) IsReservedKeyword(word string) bool {
	return d.keywords[strings.ToUpper(word)]
}

func (d *MySQLDriver) TranslateOperator(op string) (string, bool) {
	upperOp := strings.ToUpper(op)
	switch upperOp {
	case "=", "!=", "<>", "<", ">", "<=", ">=":
		return op, true
	case "LIKE", "NOT LIKE":
		return upperOp, true
	case "ILIKE":
		return "LIKE", true
	case "NOT ILIKE":
		return "NOT LIKE", true
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

func (d *MySQLDriver) SupportsFeature(feature string) bool {
	return slices.Contains(supportedFeatures, strings.ToUpper(feature))
}

func init() {
	driver := NewMySQLDriver()
	where.RegisterDriver("mysql", driver)
	where.RegisterDriver("mariadb", driver)
}
