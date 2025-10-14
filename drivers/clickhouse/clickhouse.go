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

func (d *ClickHouseDriver) TranslateFunction(name string, argCount int) (string, bool) {
	// Check for exact case-sensitive matches first (ClickHouse-specific functions)
	if template, ok := d.translateCaseSensitiveFunction(name, argCount); ok {
		return template, true
	}

	// Fall back to case-insensitive standard functions
	upperName := strings.ToUpper(name)
	switch upperName {
	case "NOW", "TODAY", "YESTERDAY":
		return upperName + "()", true
	case "LOWER", "UPPER", "LENGTH", "TRIM", "LTRIM", "RTRIM", "REVERSE":
		return upperName + "(%s)", true
	case "TO_DATE", "TO_DATETIME", "TO_DATETIME64", "TO_TIME":
		return upperName + "(%s)", true
	case "DATE", "DATE_TRUNC":
		return upperName + "(%s)", true
	case "YEAR", "QUARTER", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND":
		return upperName + "(%s)", true
	case "FORMAT_DATETIME":
		if argCount == 2 {
			return "formatDateTime(%s, %s)", true
		}
		return "", false
	case "SUBSTRING", "SUBSTR":
		switch argCount {
		case 2:
			return "substring(%s, %s)", true
		case 3:
			return "substring(%s, %s, %s)", true
		default:
			return "", false
		}
	case "COALESCE", "GREATEST", "LEAST":
		placeholders := make([]string, argCount)
		for i := range argCount {
			placeholders[i] = "%s"
		}
		return strings.ToLower(upperName) + "(" + strings.Join(placeholders, ", ") + ")", true
	case "CONCAT":
		placeholders := make([]string, argCount)
		for i := range argCount {
			placeholders[i] = "%s"
		}
		return "concat(" + strings.Join(placeholders, ", ") + ")", true
	case "IF":
		if argCount == 3 {
			return "if(%s, %s, %s)", true
		}
		return "", false
	case "ARRAY":
		placeholders := make([]string, argCount)
		for i := range argCount {
			placeholders[i] = "%s"
		}
		return "[" + strings.Join(placeholders, ", ") + "]", true
	case "TUPLE":
		placeholders := make([]string, argCount)
		for i := range argCount {
			placeholders[i] = "%s"
		}
		return "(" + strings.Join(placeholders, ", ") + ")", true
	default:
		return "", false
	}
}

// translateCaseSensitiveFunction handles ClickHouse-specific case-sensitive functions
func (d *ClickHouseDriver) translateCaseSensitiveFunction(name string, argCount int) (string, bool) {
	// Delegate to category-specific functions
	if template, ok := d.translateDateTimeFunctions(name, argCount); ok {
		return template, true
	}
	if template, ok := d.translateStringFunctions(name, argCount); ok {
		return template, true
	}
	if template, ok := d.translateArrayFunctions(name, argCount); ok {
		return template, true
	}
	if template, ok := d.translateMathFunctions(name, argCount); ok {
		return template, true
	}
	if template, ok := d.translateConditionalFunctions(name, argCount); ok {
		return template, true
	}
	if template, ok := d.translateHashFunctions(name, argCount); ok {
		return template, true
	}
	if template, ok := d.translateJSONFunctions(name, argCount); ok {
		return template, true
	}
	if template, ok := d.translateURLFunctions(name, argCount); ok {
		return template, true
	}
	if template, ok := d.translateEncodingFunctions(name, argCount); ok {
		return template, true
	}

	return "", false
}

func (d *ClickHouseDriver) translateDateTimeFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "toDate", "toDateTime", "toDateTime64":
		if argCount > 0 && argCount < 4 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "toYYYYMM", "toYYYYMMDD", "toYYYYMMDDhhmmss":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "toYear", "toQuarter", "toMonth", "toDayOfMonth", "toDayOfWeek", "toHour", "toMinute", "toSecond":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "toStartOfYear", "toStartOfQuarter", "toStartOfMonth", "toStartOfWeek", "toStartOfDay", "toStartOfHour", "toStartOfMinute":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	}
	return "", false
}

func (d *ClickHouseDriver) translateStringFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "toString", "toFixedString":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "leftPad", "rightPad":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "startsWith", "endsWith":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	}
	return "", false
}

func (d *ClickHouseDriver) translateArrayFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "arrayElement", "arraySlice":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "arrayLength", "arrayReverse", "arraySort", "arrayUniq", "arrayCompact":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "arrayConcat", "arrayIntersect", "arrayExcept":
		if argCount >= 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "has", "hasAll", "hasAny", "indexOf":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	}
	return "", false
}

func (d *ClickHouseDriver) translateMathFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "abs", "sign", "sqrt", "cbrt", "exp", "exp2", "exp10", "log", "log2", "log10", "ln":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "pow", "power", "round", "roundBankers", "floor", "ceil", "trunc":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "sin", "cos", "tan", "asin", "acos", "atan":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "atan2":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	}
	return "", false
}

func (d *ClickHouseDriver) translateConditionalFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "multiIf":
		if argCount >= 3 && argCount%2 == 1 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "isNull", "isNotNull":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "ifNull":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	}
	return "", false
}

func (d *ClickHouseDriver) translateHashFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "MD5", "SHA1", "SHA224", "SHA256":
		if argCount == 1 {
			return name + "(%s)", true
		}
	}
	return "", false
}

func (d *ClickHouseDriver) translateJSONFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "JSONExtract", "JSONExtractString", "JSONExtractInt", "JSONExtractFloat", "JSONExtractBool":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "JSONHas", "JSONLength", "JSONType":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	}
	return "", false
}

func (d *ClickHouseDriver) translateURLFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "protocol", "domain", "topLevelDomain", "firstSignificantSubdomain", "cutToFirstSignificantSubdomain":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "extractURLParameter", "extractURLParameterNames":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	}
	return "", false
}

func (d *ClickHouseDriver) translateEncodingFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "base64Encode", "base64Decode", "hex", "unhex":
		if argCount == 1 {
			return name + "(%s)", true
		}
	}
	return "", false
}

// buildPlaceholders creates a comma-separated list of %s placeholders
func (d *ClickHouseDriver) buildPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := range count {
		placeholders[i] = "%s"
	}
	return strings.Join(placeholders, ", ")
}

func (d *ClickHouseDriver) ConcatOperator() string {
	return "||"
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
