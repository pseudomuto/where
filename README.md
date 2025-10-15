# Where

[![Build Status](https://github.com/pseudomuto/where/workflows/ci/badge.svg)](https://github.com/pseudomuto/where/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/pseudomuto/where)](https://goreportcard.com/report/github.com/pseudomuto/where)
[![GoDoc](https://godoc.org/github.com/pseudomuto/where?status.svg)](https://godoc.org/github.com/pseudomuto/where)
[![Codecov](https://codecov.io/gh/pseudomuto/where/branch/main/graph/badge.svg)](https://codecov.io/gh/pseudomuto/where)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

A SQL filter expression parser that converts human-readable filter expressions into parameterized SQL queries for multiple database backends.

## Why Where?

When building web APIs or data processing systems, you often need to allow users to filter data with expressions like:

```
age >= 18 AND status = 'active' AND name ILIKE '%john%'
```

Converting these expressions safely into SQL queries while preventing SQL injection attacks is complex and error-prone. Where solves this by:

- **Parsing human-readable expressions** into Abstract Syntax Trees (AST)
- **Generating parameterized SQL** that prevents injection attacks
- **Supporting multiple databases** with their specific SQL dialects
- **Providing validation** to restrict allowed fields and functions
- **Handling database-specific features** like PostgreSQL's JSONB or ClickHouse's arrays

## Installation

```bash
go get github.com/pseudomuto/where
```

Import the package and the database drivers you need:

```go
import (
    "github.com/pseudomuto/where"
    _ "github.com/pseudomuto/where/drivers/postgres"
    _ "github.com/pseudomuto/where/drivers/mysql"
    _ "github.com/pseudomuto/where/drivers/clickhouse"
)
```

## Quick Start

```go
// Parse a filter expression
filter, err := where.Parse("age >= 18 AND status = 'active'")
if err != nil {
    log.Fatal(err)
}

// Convert to SQL for PostgreSQL
sql, params, err := filter.ToSQL("postgres")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("SQL: %s\n", sql)     // (age >= $1 AND status = $2)
fmt.Printf("Params: %v\n", params) // [18 active]
```

## Supported Database Drivers

### PostgreSQL (`postgres`, `postgresql`, `pg`)
- **Features**: Native ILIKE support, JSONB path extraction, array functions
- **Functions**: All PostgreSQL functions supported (e.g., DATE_TRUNC, EXTRACT, ARRAY_LENGTH, JSONB_EXTRACT_PATH)
- **Placeholders**: `$1`, `$2`, `$3`...
- **Identifiers**: Double quotes (`"field"`)

### MySQL (`mysql`)
- **Features**: ILIKE converted to LOWER() + LIKE, comprehensive date/time functions
- **Functions**: All MySQL functions supported (e.g., DATE_FORMAT, TIMESTAMPDIFF, JSON_EXTRACT)
- **Placeholders**: `?`
- **Identifiers**: Backticks (`` `field` ``)

### ClickHouse (`clickhouse`)
- **Features**: Case-sensitive functions, array operations, time-series optimized
- **Functions**: All ClickHouse functions supported (e.g., toYYYYMM, arrayLength, startsWith)
- **Placeholders**: `?`
- **Identifiers**: Backticks (`` `field` ``)

## Supported Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=`, `!=`, `<>` | Equality and inequality | `status = 'active'` |
| `<`, `>`, `<=`, `>=` | Comparison | `age >= 18` |
| `LIKE`, `NOT LIKE` | Pattern matching | `name LIKE 'John%'` |
| `ILIKE`, `NOT ILIKE` | Case-insensitive pattern matching | `email ILIKE '%gmail%'` |
| `IN`, `NOT IN` | List membership | `status IN ('active', 'pending')` |
| `BETWEEN`, `NOT BETWEEN` | Range checks | `age BETWEEN 18 AND 65` |
| `IS NULL`, `IS NOT NULL` | Null checks | `deleted_at IS NULL` |
| `AND`, `OR`, `NOT` | Logical operators | `age > 18 AND verified = true` |

## Advanced Usage

### Parser Configuration

```go
// Create parser with validation and limits
parser, err := where.NewParser(
    where.WithMaxDepth(3),              // Limit nesting depth
    where.WithMaxINItems(10),           // Limit IN clause items
    where.WithFunctions("LOWER", "UPPER"), // Restrict at parse-time (optional)
)

filter, err := parser.Parse("LOWER(email) = 'admin@example.com'")
```

### Function Validation

There are two levels of function validation available:

1. **Parse-time validation** (optional): Restrict functions during parsing
2. **Runtime validation** (recommended): Use Validator for comprehensive security

### Field and Function Allowlists

```go
// Create validator with allowlists
validator := where.NewValidator().
    AllowFields("email", "age", "status").
    AllowFunctions("LOWER", "UPPER", "LENGTH")

sql, params, err := filter.ToSQL("postgres", where.WithValidator(validator))
```

### Cross-Database Compatibility

```go
filter, _ := where.Parse("name ILIKE '%john%' AND age > 21")

// PostgreSQL - native ILIKE
sql, _ := filter.ToSQL("postgres")
// Result: (name ILIKE $1 AND age > $2)

// MySQL - ILIKE converted to LOWER() + LIKE
sql, _ = filter.ToSQL("mysql")
// Result: (LOWER(name) LIKE LOWER(?) AND age > ?)

// ClickHouse - native ILIKE
sql, _ = filter.ToSQL("clickhouse")
// Result: (name ILIKE ? AND age > ?)
```

### Complex Expressions

```go
filter, err := where.Parse(`
    (age BETWEEN 18 AND 65 OR is_verified = true) AND
    email NOT LIKE '%spam%' AND
    status IN ('active', 'premium', 'vip') AND
    NOT (country = 'XX' OR ip_address IS NULL)
`)

sql, params, _ := filter.ToSQL("postgres")
// Generates properly parenthesized SQL with correct operator precedence
```

### Database-Specific Functions

```go
// PostgreSQL JSONB operations
pgFilter, _ := where.Parse(`
    JSONB_EXTRACT_PATH(metadata, 'user', 'role') = 'admin' AND
    ARRAY_LENGTH(tags, 1) > 0
`)

// MySQL date formatting
mysqlFilter, _ := where.Parse(`
    DATE_FORMAT(created_at, '%Y-%m-%d') = '2024-01-15' AND
    TIMESTAMPDIFF('DAY', start_date, end_date) > 7
`)

// ClickHouse time-series functions
chFilter, _ := where.Parse(`
    toYYYYMM(event_time) = 202401 AND
    has(categories, 'analytics') = true
`)

// Any function with any number of arguments is supported
customFilter, _ := where.Parse(`
    toDateTime64(timestamp, 3, 'UTC') > '2024-01-01' AND
    MY_CUSTOM_FUNCTION(a, b, c, d, e) = 42
`)
```

### Universal Function Support

Where supports **all functions** available in your target database without requiring pre-configuration:

- **No function whitelists** - Any function your database supports can be used
- **Variable arity support** - Functions can accept any number of arguments (e.g., `toDateTime64(value, precision, timezone)`)
- **Database-native syntax** - Functions are passed through directly to the database for validation
- **Custom functions** - User-defined functions work immediately without code changes

Function validation happens at **database execution time** rather than parse time, providing maximum flexibility while maintaining safety through parameterization.

## Security Features

### SQL Injection Prevention
All values are properly parameterized, preventing SQL injection attacks:

```go
// Safe - values become parameters
filter, _ := where.Parse("name = 'Robert'); DROP TABLE users; --'")
sql, params, _ := filter.ToSQL("postgres")
// Result: name = $1 with params ["Robert'); DROP TABLE users; --"]
```

### Field and Function Allowlists
Restrict which fields and functions users can access:

```go
validator := where.NewValidator().
    AllowFields("public_field1", "public_field2").
    AllowFunctions("LOWER", "UPPER")

// This will fail validation
_, _, err := badFilter.ToSQL("postgres", where.WithValidator(validator))
// Error: field "private_field" is not allowed
```

### Reserved Keyword Handling
Automatically quotes reserved keywords for each database:

```go
filter, _ := where.Parse("user = 'admin' AND order > 100")

// PostgreSQL: ("user" = $1 AND "order" > $2)
// MySQL: (`user` = ? AND `order` > ?)
// ClickHouse: (`user` = ? AND `order` > ?)
```

## Performance Considerations

- **Parsing is cached**: Identical expressions are parsed once and reused
- **Minimal allocations**: Optimized for high-throughput scenarios  
- **Database-specific optimizations**: Each driver leverages database-specific features
- **Configurable limits**: Prevent resource exhaustion with depth and item limits

## Grammar and Precedence

The parser follows standard SQL operator precedence:

1. **Predicates**: `field = value`, `field IS NULL`
2. **NOT**: `NOT condition`
3. **AND**: `condition1 AND condition2`
4. **OR**: `condition1 OR condition2`

Parentheses can override precedence: `(A OR B) AND C` vs `A OR (B AND C)`

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests: `task test`
4. Run linting: `task lint`
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Development

```bash
# Run tests
task test

# Run linting
task lint

# Auto-fix linting issues
task lint:fix

# Update dependencies
task up

# Create a release tag
task tag TAG=v1.2.3
```

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.