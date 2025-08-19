package mysql

import (
	"fmt"
	"strings"

	"github.com/pseudomuto/where"
)

func init() {
	driver := NewMySQLDriver()
	where.RegisterDriver("mysql", driver)
	where.RegisterDriver("mariadb", driver)
}

type MySQLDriver struct {
	keywords map[string]bool
}

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

func (d *MySQLDriver) TranslateFunction(name string, argCount int) (string, bool) {
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

	// JSON functions
	if template, ok := d.translateJSONFunctions(upperName, argCount); ok {
		return template, true
	}

	// Aggregate functions
	if template, ok := d.translateAggregateFunctions(upperName, argCount); ok {
		return template, true
	}

	return "", false
}

func (d *MySQLDriver) translateDateTimeFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "NOW", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "CURDATE", "CURTIME", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP":
		return name + "()", true
	case "DATE", "TIME", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "MICROSECOND":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "WEEK", "WEEKDAY", "DAYOFWEEK", "DAYOFMONTH", "DAYOFYEAR", "QUARTER":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "MONTHNAME", "DAYNAME":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "DATE_FORMAT", "TIME_FORMAT":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "STR_TO_DATE":
		if argCount == 2 {
			return "STR_TO_DATE(%s, %s)", true
		}
	case "DATE_ADD", "DATE_SUB", "ADDDATE", "SUBDATE":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "DATEDIFF", "TIMEDIFF":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "TIMESTAMPDIFF", "TIMESTAMPADD":
		if argCount == 3 {
			return name + "(%s, %s, %s)", true
		}
	case "EXTRACT":
		if argCount == 2 {
			return "EXTRACT(%s FROM %s)", true
		}
	case "LAST_DAY", "FROM_DAYS", "TO_DAYS", "FROM_UNIXTIME", "UNIX_TIMESTAMP":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "MAKEDATE", "MAKETIME":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "CONVERT_TZ":
		if argCount == 3 {
			return "CONVERT_TZ(%s, %s, %s)", true
		}
	}
	return "", false
}

func (d *MySQLDriver) translateStringFunctions(name string, argCount int) (string, bool) {
	// Basic single-argument string functions
	if template, ok := d.translateBasicStringFunctions(name, argCount); ok {
		return template, true
	}

	// Multi-argument string functions
	if template, ok := d.translateAdvancedStringFunctions(name, argCount); ok {
		return template, true
	}

	return "", false
}

func (d *MySQLDriver) translateBasicStringFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "LOWER", "LCASE", "UPPER", "UCASE", "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "BIT_LENGTH", "OCTET_LENGTH":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "TRIM", "LTRIM", "RTRIM", "REVERSE", "SOUNDEX", "SPACE", "HEX", "UNHEX", "MD5", "SHA1":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "SHA2":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "ASCII", "ORD", "QUOTE":
		if argCount == 1 {
			return name + "(%s)", true
		}
	}
	return "", false
}

func (d *MySQLDriver) translateAdvancedStringFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "CONCAT", "CONCAT_WS":
		if argCount >= 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "SUBSTRING", "SUBSTR", "MID":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "SUBSTRING_INDEX":
		if argCount == 3 {
			return "SUBSTRING_INDEX(%s, %s, %s)", true
		}
	case "LEFT", "RIGHT":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "LPAD", "RPAD":
		if argCount == 3 {
			return name + "(%s, %s, %s)", true
		}
	case "LOCATE", "POSITION", "INSTR":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "REPLACE":
		if argCount == 3 {
			return name + "(%s, %s, %s)", true
		}
	case "INSERT":
		if argCount == 4 {
			return name + "(%s, %s, %s, %s)", true
		}
	case "REPEAT":
		if argCount == 2 {
			return "REPEAT(%s, %s)", true
		}
	case "STRCMP", "FIELD", "ELT", "FIND_IN_SET":
		if argCount >= 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "FORMAT":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "REGEXP", "RLIKE":
		if argCount == 2 {
			return "%s " + name + " %s", true
		}
	}
	return "", false
}

func (d *MySQLDriver) translateMathFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "ABS", "SIGN", "SQRT", "EXP", "LN", "LOG", "LOG10", "LOG2":
		if argCount == 1 || (name == "LOG" && argCount == 2) {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "COT":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "ATAN2":
		if argCount == 2 {
			return "ATAN2(%s, %s)", true
		}
	case "DEGREES", "RADIANS", "PI":
		if argCount == 0 || (name != "PI" && argCount == 1) {
			if name == "PI" {
				return "PI()", true
			}
			return name + "(%s)", true
		}
	case "CEIL", "CEILING", "FLOOR":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "ROUND", "TRUNCATE":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "POW", "POWER":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "MOD":
		if argCount == 2 {
			return "MOD(%s, %s)", true
		}
	case "RAND":
		if argCount == 0 || argCount == 1 {
			if argCount == 0 {
				return "RAND()", true
			}
			return "RAND(%s)", true
		}
	case "CONV":
		if argCount == 3 {
			return "CONV(%s, %s, %s)", true
		}
	}
	return "", false
}

func (d *MySQLDriver) translateConditionalFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "IF":
		if argCount == 3 {
			return "IF(%s, %s, %s)", true
		}
	case "IFNULL", "NULLIF":
		if argCount == 2 {
			return name + "(%s, %s)", true
		}
	case "ISNULL":
		if argCount == 1 {
			return "ISNULL(%s)", true
		}
	case "COALESCE", "GREATEST", "LEAST":
		if argCount >= 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "CASE":
		// Handle CASE expressions separately in the parser
		return "", false
	}
	return "", false
}

func (d *MySQLDriver) translateJSONFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "JSON_EXTRACT", "JSON_UNQUOTE", "JSON_TYPE", "JSON_VALID", "JSON_LENGTH", "JSON_DEPTH", "JSON_KEYS":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "JSON_SET", "JSON_INSERT", "JSON_REPLACE", "JSON_REMOVE":
		if argCount >= 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "JSON_ARRAY", "JSON_OBJECT":
		if argCount >= 0 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "JSON_CONTAINS", "JSON_CONTAINS_PATH":
		if argCount == 2 || argCount == 3 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "JSON_ARRAYAGG", "JSON_OBJECTAGG":
		if argCount == 1 || argCount == 2 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	}
	return "", false
}

func (d *MySQLDriver) translateAggregateFunctions(name string, argCount int) (string, bool) {
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "STD", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP":
		if argCount == 1 {
			return name + "(%s)", true
		}
	case "GROUP_CONCAT":
		if argCount >= 1 {
			return name + "(" + d.buildPlaceholders(argCount) + ")", true
		}
	case "BIT_AND", "BIT_OR", "BIT_XOR":
		if argCount == 1 {
			return name + "(%s)", true
		}
	}
	return "", false
}

// buildPlaceholders creates a comma-separated list of %s placeholders
func (d *MySQLDriver) buildPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "%s"
	}
	return strings.Join(placeholders, ", ")
}

func (d *MySQLDriver) ConcatOperator() string {
	return "CONCAT"
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
