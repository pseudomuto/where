package postgres

// PostgreSQL 16 reserved keywords that MUST be quoted when used as identifiers.
// Source: https://www.postgresql.org/docs/16/sql-keywords-appendix.html (reserved keywords only)
// Updated: January 2025
//
// This list includes only reserved keywords (marked as "reserved" in the PostgreSQL documentation)
// which cannot be used as identifiers without quoting.
var keywords = []string{
	"ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC",
	"AUTHORIZATION", "BETWEEN", "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE",
	"COLLATION", "COLUMN", "CONCURRENTLY", "CONSTRAINT", "CREATE", "CROSS",
	"CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
	"CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT",
	"DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN", "FROM",
	"GRANT", "GROUP", "HAVING", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO",
	"IS", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME",
	"LOCALTIMESTAMP", "NATURAL", "NOT", "NULL", "OFFSET", "ON", "ONLY", "OR",
	"ORDER", "OUTER", "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING",
	"RIGHT", "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "TABLE",
	"TABLESAMPLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER",
	"USING", "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH",
}
