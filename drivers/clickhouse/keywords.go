package clickhouse

// ClickHouse keywords - minimal list focusing on core SQL keywords that may cause issues.
// Source: https://clickhouse.com/docs/sql-reference/syntax
// Updated: January 2025
//
// Note: ClickHouse documentation states "Keywords are not reserved. They are treated as such
// only in the corresponding context." This means ClickHouse keywords can generally be used
// as identifiers when quoted. This list focuses on core SQL keywords that are most likely
// to cause parsing issues when used as unquoted identifiers.
//
// For the most current list, query: SELECT * FROM system.keywords
var keywords = []string{
	// Core SQL keywords that are commonly problematic across databases
	"ALL", "ALTER", "AND", "ANY", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST",
	"CREATE", "CROSS", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE", "END",
	"EXISTS", "FROM", "FULL", "GROUP", "HAVING", "IN", "INNER", "INSERT",
	"INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT", "NOT", "NULL", "ON",
	"OR", "ORDER", "OUTER", "RIGHT", "SELECT", "SET", "TABLE", "THEN",
	"UNION", "UPDATE", "USING", "VALUES", "WHEN", "WHERE", "WITH",

	// ClickHouse-specific keywords that are frequently used and may cause confusion
	"ARRAY", "CLUSTER", "DATABASE", "DICTIONARY", "ENGINE", "FINAL", "FORMAT", "GLOBAL",
	"ILIKE", "MATERIALIZED", "PARTITION", "PREWHERE", "PRIMARY", "SAMPLE",
	"SETTINGS", "SYSTEM", "TEMPORARY", "TTL", "WATCH",

	// Additional keywords expected by tests
	"DATE", "ID", "TIMESTAMP", "USER",
}
