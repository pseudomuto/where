package mysql

import (
	"fmt"
	"strings"

	"github.com/pseudomuto/where"
)

// MySQLDriver implements the where.Driver interface for MySQL and MariaDB databases.
type MySQLDriver struct {
	keywords map[string]bool
}

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
	if d.needsQuoting(name) {
		return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
	}
	return name
}

func (d *MySQLDriver) needsQuoting(name string) bool {
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
	switch strings.ToUpper(feature) {
	case "JSON", "FULLTEXT", "SPATIAL", "PARTITION", "CTE":
		return true
	case "ILIKE":
		return false
	default:
		return false
	}
}

func init() {
	driver := NewMySQLDriver()
	where.RegisterDriver("mysql", driver)
	where.RegisterDriver("mariadb", driver)
}
