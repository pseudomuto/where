package mysql

import (
	"fmt"
	"slices"
	"strings"

	"github.com/pseudomuto/where"
)

var (
	supportedFeatures = []string{
		"CTE",
		"FULLTEXT",
		"JSON",
		"PARTITION",
		"SPATIAL",
	}

	supportedOperations = []string{
		"=", "!=", "<>", "<", ">", "<=", ">=",
		"LIKE", "NOT LIKE",
		"IN", "NOT IN",
		"IS NULL", "IS NOT NULL",
		"BETWEEN", "NOT BETWEEN",
	}
)

type (
	// MySQLDriver implements the where.Driver interface for MySQL and MariaDB databases.
	MySQLDriver struct{}
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
	return &MySQLDriver{}
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

func (d *MySQLDriver) Keywords() []string {
	return keywords
}

func (d *MySQLDriver) TranslateOperator(op string) (string, bool) {
	upperOp := strings.ToUpper(op)
	if slices.Contains(supportedOperations, upperOp) {
		return upperOp, true
	}

	if upperOp == "ILIKE" || upperOp == "NOT ILIKE" {
		return strings.Replace(upperOp, "ILIKE", "LIKE", 1), true
	}

	return "", false
}

func (d *MySQLDriver) SupportsFeature(feature string) bool {
	return slices.Contains(supportedFeatures, strings.ToUpper(feature))
}

func init() {
	driver := NewMySQLDriver()
	where.RegisterDriver("mysql", driver)
	where.RegisterDriver("mariadb", driver)
}
