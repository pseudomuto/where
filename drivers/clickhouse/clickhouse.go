package clickhouse

import (
	"fmt"
	"strings"

	"github.com/pseudomuto/where"
)

// ClickHouseDriver implements the where.Driver interface for ClickHouse databases.
type ClickHouseDriver struct {
	keywords map[string]bool
}

// NewClickHouseDriver creates a new ClickHouse driver instance.
//
// Example:
//
//	import (
//		"github.com/pseudomuto/where"
//		_ "github.com/pseudomuto/where/drivers/clickhouse"
//	)
//
//	filter, params, _ := where.Build("age > 18", "clickhouse")
//	// SELECT * FROM users WHERE age > ?
func NewClickHouseDriver() *ClickHouseDriver {
	return &ClickHouseDriver{
		keywords: ClickHouseKeywords,
	}
}

func (d *ClickHouseDriver) Name() string {
	return "clickhouse"
}

func (d *ClickHouseDriver) QuoteIdentifier(name string) string {
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

func (d *ClickHouseDriver) quoteSimpleIdentifier(name string) string {
	if d.needsQuoting(name) {
		return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
	}
	return name
}

func (d *ClickHouseDriver) needsQuoting(name string) bool {
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

func (d *ClickHouseDriver) Placeholder(position int) string {
	return "?"
}

func (d *ClickHouseDriver) IsReservedKeyword(word string) bool {
	return d.keywords[strings.ToUpper(word)]
}

func (d *ClickHouseDriver) TranslateOperator(op string) (string, bool) {
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

func (d *ClickHouseDriver) SupportsFeature(feature string) bool {
	switch strings.ToUpper(feature) {
	case "ILIKE", "ARRAY", "TUPLE", "WITH", "SAMPLE", "PREWHERE", "FINAL", "GLOBAL":
		return true
	case "JSON":
		return true
	default:
		return false
	}
}

func init() {
	driver := NewClickHouseDriver()
	where.RegisterDriver("clickhouse", driver)
	where.RegisterDriver("ch", driver)
}
