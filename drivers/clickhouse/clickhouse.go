package clickhouse

import (
	"fmt"
	"slices"
	"strings"

	"github.com/pseudomuto/where"
)

var (
	supportedFeatures = []string{
		"ARRAY",
		"FINAL",
		"GLOBAL",
		"ILIKE",
		"JSON",
		"PREWHERE",
		"SAMPLE",
		"TUPLE",
		"WITH",
	}

	supportedOperations = []string{
		"=", "!=", "<>", "<", ">", "<=", ">=",
		"LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE",
		"IN", "NOT IN",
		"IS NULL", "IS NOT NULL",
		"BETWEEN", "NOT BETWEEN",
	}
)

type (
	// ClickHouseDriver implements the where.Driver interface for ClickHouse databases.
	ClickHouseDriver struct{}
)

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
	return &ClickHouseDriver{}
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
	if where.NeedsQuoting(name, d) {
		return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
	}
	return name
}

func (d *ClickHouseDriver) Placeholder(position int) string {
	return "?"
}

func (d *ClickHouseDriver) Keywords() []string {
	return keywords
}

func (d *ClickHouseDriver) TranslateOperator(op string) (string, bool) {
	upperOp := strings.ToUpper(op)
	if slices.Contains(supportedOperations, upperOp) {
		return upperOp, true
	}

	return "", false
}

func (d *ClickHouseDriver) SupportsFeature(feature string) bool {
	return slices.Contains(supportedFeatures, strings.ToUpper(feature))
}

func init() {
	driver := NewClickHouseDriver()
	where.RegisterDriver("clickhouse", driver)
	where.RegisterDriver("ch", driver)
}
