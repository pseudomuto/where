package postgres

import (
	"fmt"
	"strings"

	"github.com/pseudomuto/where"
)

func init() {
	driver := NewPostgreSQLDriver()
	where.RegisterDriver("postgres", driver)
	where.RegisterDriver("postgresql", driver)
	where.RegisterDriver("pg", driver)
}

type PostgreSQLDriver struct {
	keywords map[string]bool
}

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

func (d *PostgreSQLDriver) TranslateFunction(name string, argCount int) (string, bool) {
	upperName := strings.ToUpper(name)

	// Date/Time functions
	if template, ok := d.translateDateTimeFunctions(upperName, argCount); ok {
		return template, true
	}

	// String functions
	if template, ok := d.translateStringFunctions(upperName, argCount); ok {
		return template, true
	}

	// Mathematical functions
	if template, ok := d.translateMathFunctions(upperName, argCount); ok {
		return template, true
	}

	// Conditional and logical functions
	if template, ok := d.translateConditionalFunctions(upperName, argCount); ok {
		return template, true
	}

	// JSON/JSONB functions
	if template, ok := d.translateJSONFunctions(upperName, argCount); ok {
		return template, true
	}

	// Array functions
	if template, ok := d.translateArrayFunctions(upperName, argCount); ok {
		return template, true
	}

	// Aggregate functions
	if template, ok := d.translateAggregateFunctions(upperName, argCount); ok {
		return template, true
	}

	return "", false
}

func (d *PostgreSQLDriver) translateDateTimeFunctions(name string, argCount int) (string, bool) {
	// Basic date/time functions
	if template, ok := d.translateBasicDateTimeFunctions(name, argCount); ok {
		return template, true
	}

	// Advanced date/time functions
	if template, ok := d.translateAdvancedDateTimeFunctions(name, argCount); ok {
		return template, true
	}

	return "", false
}

func (d *PostgreSQLDriver) translateBasicDateTimeFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "NOW", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP":
		return name + "()", true
	case "CLOCK_TIMESTAMP", "STATEMENT_TIMESTAMP", "TRANSACTION_TIMESTAMP", "TIMEOFDAY":
		return name + "()", true
	case "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND":
		// PostgreSQL doesn't have direct YEAR/MONTH functions, use EXTRACT
		if argCount == 1 {
			return "EXTRACT(" + name + " FROM %s)", true
		}
	case "AGE":
		if argCount == 1 || argCount == 2 {
			return "AGE(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "ISFINITE":
		if argCount == 1 {
			return "ISFINITE(%s)", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateAdvancedDateTimeFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "DATE_PART", "EXTRACT":
		if argCount == 2 {
			if name == "EXTRACT" {
				return "EXTRACT(%s FROM %s)", true
			}
			return "DATE_PART(%s, %s)", true
		}
	case "DATE_TRUNC":
		if argCount == 2 {
			return "DATE_TRUNC(%s, %s)", true
		}
	case "TO_CHAR", "TO_DATE", "TO_TIMESTAMP":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "JUSTIFY_DAYS", "JUSTIFY_HOURS", "JUSTIFY_INTERVAL":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "MAKE_DATE":
		if argCount == 3 {
			return name + "(%s, %s, %s)", true
		}
	case "MAKE_TIME":
		if argCount == 3 || argCount == 6 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "MAKE_TIMESTAMP", "MAKE_TIMESTAMPTZ":
		if argCount == 6 || argCount == 7 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "MAKE_INTERVAL":
		if argCount >= 0 && argCount <= 7 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateStringFunctions(name string, argCount int) (string, bool) {
	// Basic string functions
	if template, ok := d.translateBasicStringFunctions(name, argCount); ok {
		return template, true
	}

	// Advanced string functions
	if template, ok := d.translateAdvancedStringFunctions(name, argCount); ok {
		return template, true
	}

	// Regular expression and conversion functions
	if template, ok := d.translateRegexAndConversionFunctions(name, argCount); ok {
		return template, true
	}

	return "", false
}

func (d *PostgreSQLDriver) translateBasicStringFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "LOWER", "UPPER", "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "BIT_LENGTH", "OCTET_LENGTH":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "REVERSE", "INITCAP", "ASCII", "CHR", "QUOTE_IDENT", "QUOTE_LITERAL", "QUOTE_NULLABLE", "MD5":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "TRIM", "LTRIM", "RTRIM", "BTRIM":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateAdvancedStringFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "CONCAT", "CONCAT_WS":
		if argCount >= 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "FORMAT":
		if argCount >= 1 {
			return "FORMAT(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "SUBSTRING", "SUBSTR":
		switch argCount {
		case 2:
			return "SUBSTRING(%s FROM %s)", true
		case 3:
			return "SUBSTRING(%s FROM %s FOR %s)", true
		}
	case "LEFT", "RIGHT":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "LPAD", "RPAD":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "POSITION", "STRPOS":
		if argCount == 2 {
			return name + "(%s IN %s)", true
		}
	case "REPLACE", "TRANSLATE":
		if argCount == 3 {
			return name + "(%s, %s, %s)", true
		}
	case "REPEAT":
		if argCount == 2 {
			return "REPEAT(%s, %s)", true
		}
	case "SPLIT_PART":
		if argCount == 3 {
			return "SPLIT_PART(%s, %s, %s)", true
		}
	case "STARTS_WITH":
		if argCount == 2 {
			return "STARTS_WITH(%s, %s)", true
		}
	case "STRING_AGG":
		if argCount == 2 {
			return "STRING_AGG(%s, %s)", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateRegexAndConversionFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "REGEXP_MATCH", "REGEXP_MATCHES", "REGEXP_REPLACE", "REGEXP_SPLIT_TO_ARRAY", "REGEXP_SPLIT_TO_TABLE":
		if argCount >= 2 && argCount <= 4 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "STRING_TO_ARRAY", "STRING_TO_TABLE":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "ENCODE", "DECODE", "CONVERT", "CONVERT_FROM", "CONVERT_TO":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateMathFunctions(name string, argCount int) (string, bool) {
	// Basic math functions
	if template, ok := d.translateBasicMathFunctions(name, argCount); ok {
		return template, true
	}

	// Trigonometric functions
	if template, ok := d.translateTrigFunctions(name, argCount); ok {
		return template, true
	}

	// Advanced math functions
	if template, ok := d.translateAdvancedMathFunctions(name, argCount); ok {
		return template, true
	}

	return "", false
}

func (d *PostgreSQLDriver) translateBasicMathFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "ABS", "SIGN", "SQRT", "CBRT", "EXP", "LN", "LOG10":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "LOG":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "CEIL", "CEILING", "FLOOR":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "ROUND", "TRUNC":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "POWER", "POW":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "MOD", "DIV":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateTrigFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "COT", "SIND", "COSD", "TAND", "ASIND", "ACOSD", "ATAND", "COTD":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "ATAN2", "ATAN2D":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "DEGREES", "RADIANS":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "PI":
		if argCount == 0 {
			return "PI()", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateAdvancedMathFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "RANDOM":
		if argCount == 0 {
			return "RANDOM()", true
		}
	case "SETSEED":
		if argCount == 1 {
			return "SETSEED(%s)", true
		}
	case "WIDTH_BUCKET":
		if argCount == 4 {
			return "WIDTH_BUCKET(%s, %s, %s, %s)", true
		}
	case "SCALE":
		if argCount == 1 {
			return "SCALE(%s)", true
		}
	case "GCD", "LCM":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "FACTORIAL":
		if argCount == 1 {
			return "FACTORIAL(%s)", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateConditionalFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "NULLIF":
		if argCount == 2 {
			return "NULLIF(%s, %s)", true
		}
	case "COALESCE", "GREATEST", "LEAST":
		if argCount >= 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "NUM_NULLS", "NUM_NONNULLS":
		if argCount >= 1 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateJSONFunctions(name string, argCount int) (string, bool) {
	// JSON construction functions
	if template, ok := d.translateJSONConstructionFunctions(name, argCount); ok {
		return template, true
	}

	// JSON extraction and query functions
	if template, ok := d.translateJSONExtractionFunctions(name, argCount); ok {
		return template, true
	}

	// JSON aggregation and conversion functions
	if template, ok := d.translateJSONAggregationFunctions(name, argCount); ok {
		return template, true
	}

	return "", false
}

func (d *PostgreSQLDriver) translateJSONConstructionFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "JSON_BUILD_ARRAY", "JSONB_BUILD_ARRAY":
		if argCount >= 0 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "JSON_BUILD_OBJECT", "JSONB_BUILD_OBJECT":
		if argCount >= 0 && argCount%2 == 0 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "JSON_OBJECT", "JSONB_OBJECT":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "JSONB_SET", "JSONB_INSERT":
		if argCount == 3 || argCount == 4 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateJSONExtractionFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "JSON_ARRAY_LENGTH", "JSONB_ARRAY_LENGTH":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "JSON_EACH", "JSONB_EACH", "JSON_EACH_TEXT", "JSONB_EACH_TEXT":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "JSON_EXTRACT_PATH", "JSONB_EXTRACT_PATH", "JSON_EXTRACT_PATH_TEXT", "JSONB_EXTRACT_PATH_TEXT":
		if argCount >= 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "JSON_OBJECT_KEYS", "JSONB_OBJECT_KEYS":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "JSON_POPULATE_RECORD", "JSONB_POPULATE_RECORD", "JSON_POPULATE_RECORDSET", "JSONB_POPULATE_RECORDSET":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "JSON_TO_RECORD", "JSONB_TO_RECORD", "JSON_TO_RECORDSET", "JSONB_TO_RECORDSET":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "JSONB_STRIP_NULLS", "JSON_STRIP_NULLS", "JSONB_PRETTY":
		if argCount == 1 {
			return name + "(%s)", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateJSONAggregationFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "JSON_AGG", "JSONB_AGG":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "JSON_OBJECT_AGG", "JSONB_OBJECT_AGG":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "TO_JSON", "TO_JSONB", "ROW_TO_JSON", "ARRAY_TO_JSON":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateArrayFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "ARRAY_LENGTH", "ARRAY_NDIMS", "ARRAY_DIMS", "ARRAY_LOWER", "ARRAY_UPPER":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "ARRAY_APPEND", "ARRAY_PREPEND", "ARRAY_CAT":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "ARRAY_REMOVE", "ARRAY_REPLACE":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "ARRAY_POSITION", "ARRAY_POSITIONS":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "ARRAY_TO_STRING":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "ARRAY_AGG":
		if argCount == 1 {
			return "ARRAY_AGG(%s)", true
		}
	case "UNNEST":
		if argCount >= 1 {
			return "UNNEST(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "CARDINALITY":
		if argCount == 1 {
			return "CARDINALITY(%s)", true
		}
	}
	return "", false
}

func (d *PostgreSQLDriver) translateAggregateFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "BIT_AND", "BIT_OR", "BIT_XOR", "BOOL_AND", "BOOL_OR":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "CORR", "COVAR_POP", "COVAR_SAMP", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "MODE":
		if argCount == 1 {
			return "MODE() WITHIN GROUP (ORDER BY %s)", true
		}
	case "PERCENTILE_CONT", "PERCENTILE_DISC":
		if argCount == 2 {
			return name + "(%s) WITHIN GROUP (ORDER BY %s)", true
		}
	}
	return "", false
}

// buildPlaceholders creates a comma-separated list of %s placeholders
func (d *PostgreSQLDriver) buildPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "%s"
	}
	return strings.Join(placeholders, ", ")
}

func (d *PostgreSQLDriver) ConcatOperator() string {
	return "||"
}

func (d *PostgreSQLDriver) SupportsFeature(feature string) bool {
	switch strings.ToUpper(feature) {
	case "ILIKE", "ARRAY", "JSON", "JSONB", "RETURNING", "CTE", "WINDOW":
		return true
	default:
		return false
	}
}
