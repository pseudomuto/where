package where_test

import (
	"fmt"
	"log"

	"github.com/pseudomuto/where"
	_ "github.com/pseudomuto/where/drivers/clickhouse"
	_ "github.com/pseudomuto/where/drivers/mysql"
	_ "github.com/pseudomuto/where/drivers/postgres"
)

// ExampleParse demonstrates basic usage of the filter parser.
func ExampleParse() {
	filter, err := where.Parse("age >= 18 AND status = 'active'")
	if err != nil {
		log.Fatal(err)
	}

	sql, params, err := filter.ToSQL("postgres")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Params: %v\n", params)
	// Output:
	// SQL: (age >= $1 AND status = $2)
	// Params: [18 active]
}

// ExampleFilter_ToSQL demonstrates converting parsed filters to SQL for different databases.
func ExampleFilter_ToSQL() {
	filter, err := where.Parse("name ILIKE '%john%' AND age > 21")
	if err != nil {
		log.Fatal(err)
	}

	// PostgreSQL - native ILIKE support
	sql, params, _ := filter.ToSQL("postgres")
	fmt.Printf("PostgreSQL: %s | %v\n", sql, params)

	// MySQL - ILIKE converted to LOWER() + LIKE
	sql, params, _ = filter.ToSQL("mysql")
	fmt.Printf("MySQL: %s | %v\n", sql, params)

	// ClickHouse - native ILIKE support
	sql, params, _ = filter.ToSQL("clickhouse")
	fmt.Printf("ClickHouse: %s | %v\n", sql, params)

	// Output:
	// PostgreSQL: (name ILIKE $1 AND age > $2) | [%john% 21]
	// MySQL: (LOWER(name) LIKE LOWER(?) AND age > ?) | [%john% 21]
	// ClickHouse: (name ILIKE ? AND age > ?) | [%john% 21]
}

// ExampleNewParser demonstrates creating a parser with validation options.
func ExampleNewParser() {
	// Create parser with restrictions
	parser, err := where.NewParser(
		where.WithMaxDepth(3),
		where.WithMaxINItems(5),
		where.WithFunctions("LOWER", "UPPER", "LENGTH"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// This will succeed - uses allowed function
	filter, err := parser.Parse("LOWER(email) = 'admin@example.com'")
	if err != nil {
		log.Fatal(err)
	}

	sql, params, _ := filter.ToSQL("postgres")
	fmt.Printf("Allowed: %s | %v\n", sql, params)

	// This will fail - uses disallowed function
	_, err = parser.Parse("TRIM(name) = 'test'")
	fmt.Printf("Error: %v\n", err)

	// Output:
	// Allowed: LOWER(email) = $1 | [admin@example.com]
	// Error: filter validation failed: function "TRIM" is not allowed
}

// ExampleValidator demonstrates field and function validation.
func ExampleValidator() {
	filter, err := where.Parse("LOWER(email) = 'admin@example.com' AND age > 18")
	if err != nil {
		log.Fatal(err)
	}

	// Create validator with allowlists
	validator := where.NewValidator().
		AllowFields("email", "age", "status").
		AllowFunctions("LOWER", "UPPER")

	sql, params, err := filter.ToSQL("postgres", where.WithValidator(validator))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Params: %v\n", params)

	// Try with disallowed field
	badFilter, _ := where.Parse("password = 'secret'")
	_, _, err = badFilter.ToSQL("postgres", where.WithValidator(validator))
	fmt.Printf("Error: %v\n", err)

	// Output:
	// SQL: (LOWER(email) = $1 AND age > $2)
	// Params: [admin@example.com 18]
	// Error: field "password" is not allowed
}

// ExampleNewParser_complexFilter demonstrates parsing complex expressions.
func ExampleNewParser_complexFilter() {
	filter, err := where.Parse(`
		(age BETWEEN 18 AND 65 OR is_verified = true) AND
		email NOT LIKE '%spam%' AND
		status IN ('active', 'premium', 'vip') AND
		NOT (country = 'XX' OR ip_address IS NULL)
	`)
	if err != nil {
		log.Fatal(err)
	}

	sql, params, _ := filter.ToSQL("postgres")
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Param count: %d\n", len(params))

	// Output:
	// SQL: ((age BETWEEN $1 AND $2 OR is_verified = TRUE) AND email NOT LIKE $3 AND status IN ($4, $5, $6) AND NOT ((country = $7 OR ip_address IS NULL)))
	// Param count: 7
}

// ExampleParse_clickHouse demonstrates comprehensive ClickHouse usage with various features.
func ExampleParse_clickHouse() {
	// Complex ClickHouse query with various data types and functions
	filter, err := where.Parse(`
		user_id IN (1001, 1002, 1003) AND
		event_time >= '2024-01-01 00:00:00' AND
		event_name = 'page_view' AND
		properties.utm_source IS NOT NULL AND
		LOWER(properties.country) IN ('us', 'ca', 'gb') AND
		session_duration BETWEEN 30 AND 3600
	`)
	if err != nil {
		log.Fatal(err)
	}

	sql, params, err := filter.ToSQL("clickhouse")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("ClickHouse SQL: %s\n", sql)
	fmt.Printf("Parameters: %v\n", params)

	// Demonstrate ClickHouse-specific features
	arrayFilter, _ := where.Parse("tags IN ('analytics', 'marketing') AND score > 0.85")
	sql, params, _ = arrayFilter.ToSQL("clickhouse")
	fmt.Printf("\nArray filter: %s\n", sql)
	fmt.Printf("Parameters: %v\n", params)

	// Output:
	// ClickHouse SQL: (user_id IN (?, ?, ?) AND event_time >= ? AND event_name = ? AND properties.utm_source IS NOT NULL AND LOWER(properties.country) IN (?, ?, ?) AND session_duration BETWEEN ? AND ?)
	// Parameters: [1001 1002 1003 2024-01-01 00:00:00 page_view us ca gb 30 3600]
	//
	// Array filter: (tags IN (?, ?) AND score > ?)
	// Parameters: [analytics marketing 0.85]
}

// ExampleParse_functions demonstrates various SQL function usage.
func ExampleParse_functions() {
	examples := []struct {
		name   string
		filter string
	}{
		{"String functions", "LOWER(name) = 'admin' AND LENGTH(password) >= 8"},
		{"Date functions", "DATE(created_at) = '2024-01-01' AND created_at > NOW()"},
		{"Null handling", "COALESCE(nickname, username, email) != ''"},
		{"Substring", "SUBSTRING(description, 1, 10) = 'Important'"},
	}

	for _, ex := range examples {
		fmt.Printf("%s:\n", ex.name)
		filter, err := where.Parse(ex.filter)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		sql, params, err := filter.ToSQL("postgres")
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		fmt.Printf("  SQL: %s\n", sql)
		fmt.Printf("  Params: %v\n\n", params)
	}

	// Output:
	// String functions:
	//   SQL: (LOWER(name) = $1 AND LENGTH(password) >= $2)
	//   Params: [admin 8]
	//
	// Date functions:
	//   SQL: (DATE(created_at) = $1 AND created_at > NOW())
	//   Params: [2024-01-01]
	//
	// Null handling:
	//   SQL: COALESCE(nickname, username, email) != $1
	//   Params: []
	//
	// Substring:
	//   SQL: SUBSTRING(description FROM $1 FOR $2) = $3
	//   Params: [1 10 Important]
}

// ExampleParse_reservedKeywords demonstrates handling of reserved keywords across databases.
func ExampleParse_reservedKeywords() {
	filter, err := where.Parse("user = 'admin' AND order > 100 AND select NOT IN ('draft', 'deleted')")
	if err != nil {
		log.Fatal(err)
	}

	databases := []string{"postgres", "mysql", "clickhouse"}

	for _, db := range databases {
		sql, params, _ := filter.ToSQL(db)
		fmt.Printf("%s: %s\n", db, sql)
		fmt.Printf("Params: %v\n\n", params)
	}

	// Output:
	// postgres: ("user" = $1 AND "order" > $2 AND "select" NOT IN ($3, $4))
	// Params: [admin 100 draft deleted]
	//
	// mysql: (`user` = ? AND `order` > ? AND `select` NOT IN (?, ?))
	// Params: [admin 100 draft deleted]
	//
	// clickhouse: (`user` = ? AND `order` > ? AND `select` NOT IN (?, ?))
	// Params: [admin 100 draft deleted]
}

// ExampleParse_advancedClickHouse demonstrates advanced ClickHouse-specific patterns.
func ExampleParse_advancedClickHouse() {
	// Simulate time-series data filtering common in ClickHouse
	filter, err := where.Parse(`
		timestamp >= '2024-01-15' AND
		user_id IN (1001, 1002, 1003, 1004, 1005) AND
		event_type = 'conversion' AND
		properties.channel IN ('organic', 'paid', 'social') AND
		revenue > 0 AND
		NOT (test_group = 'control' AND variant IS NULL)
	`)
	if err != nil {
		log.Fatal(err)
	}

	sql, params, err := filter.ToSQL("clickhouse")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Advanced ClickHouse Query:\n")
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Parameter count: %d\n", len(params))
	fmt.Printf("Sample params: %v...\n", params[:3])

	// Demonstrate aggregation-friendly filtering
	aggFilter, _ := where.Parse("date >= '2024-01-01' AND country IN ('US', 'CA') AND active = true")
	sql, params, _ = aggFilter.ToSQL("clickhouse")
	fmt.Printf("\nAggregation filter: %s\n", sql)
	fmt.Printf("Parameters: %v\n", params)

	// Output:
	// Advanced ClickHouse Query:
	// SQL: (`timestamp` >= ? AND user_id IN (?, ?, ?, ?, ?) AND event_type = ? AND properties.channel IN (?, ?, ?) AND revenue > ? AND NOT ((test_group = ? AND variant IS NULL)))
	// Parameter count: 12
	// Sample params: [2024-01-15 1001 1002]...
	//
	// Aggregation filter: (`date` >= ? AND country IN (?, ?) AND active = TRUE)
	// Parameters: [2024-01-01 US CA]
}

// ExampleParse_crossDatabaseFunctions demonstrates how the same filter expression works across different databases.
func ExampleParse_crossDatabaseFunctions() {
	// Date and time functions work across all databases
	dateFilter, err := where.Parse(`
		created_at >= '2024-01-01' AND
		YEAR(created_at) = 2024 AND
		MONTH(created_at) IN (1, 2, 3)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Generate SQL for different databases
	databases := []string{"postgres", "mysql", "clickhouse"}
	fmt.Println("Date functions across databases:")
	for _, db := range databases {
		sql, params, _ := dateFilter.ToSQL(db)
		fmt.Printf("%s: %s\n", db, sql)
		fmt.Printf("  Params: %v\n", params)
	}

	// String functions are also portable
	stringFilter, _ := where.Parse(`
		LOWER(email) LIKE '%admin%' AND
		LENGTH(password) >= 8 AND
		CONCAT(first_name, ' ', last_name) != ''
	`)

	fmt.Println("\nString functions across databases:")
	for _, db := range databases {
		sql, _, _ := stringFilter.ToSQL(db)
		if len(sql) > 50 {
			fmt.Printf("%s: %s...\n", db, sql[:50])
		} else {
			fmt.Printf("%s: %s\n", db, sql)
		}
	}

	// Output:
	// Date functions across databases:
	// postgres: (created_at >= $1 AND EXTRACT(YEAR FROM created_at) = $2 AND EXTRACT(MONTH FROM created_at) IN ($3, $4, $5))
	//   Params: [2024-01-01 2024 1 2 3]
	// mysql: (created_at >= ? AND YEAR(created_at) = ? AND MONTH(created_at) IN (?, ?, ?))
	//   Params: [2024-01-01 2024 1 2 3]
	// clickhouse: (created_at >= ? AND YEAR(created_at) = ? AND MONTH(created_at) IN (?, ?, ?))
	//   Params: [2024-01-01 2024 1 2 3]
	//
	// String functions across databases:
	// postgres: (LOWER(email) LIKE $1 AND LENGTH(password) >= $2 A...
	// mysql: (LOWER(email) LIKE ? AND LENGTH(password) >= ? AND...
	// clickhouse: (LOWER(email) LIKE ? AND LENGTH(password) >= ? AND...
}

// ExampleParse_databaseSpecificFunctions demonstrates database-specific function usage.
func ExampleParse_databaseSpecificFunctions() {
	// MySQL-specific date formatting
	mysqlFilter, err := where.Parse(`
		DATE_FORMAT(created_at, '%Y-%m-%d') = '2024-01-15' AND
		TIMESTAMPDIFF('DAY', start_date, end_date) > 7
	`)
	if err != nil {
		log.Fatal(err)
	}

	sql, params, _ := mysqlFilter.ToSQL("mysql")
	fmt.Println("MySQL-specific functions:")
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Params: %v\n", params)

	// PostgreSQL-specific array and JSON functions
	pgFilter, _ := where.Parse(`
		ARRAY_LENGTH(tags, 1) > 0 AND
		JSONB_EXTRACT_PATH(metadata, 'user', 'role') = 'admin'
	`)

	sql, params, _ = pgFilter.ToSQL("postgres")
	fmt.Println("\nPostgreSQL-specific functions:")
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Params: %v\n", params)

	// ClickHouse-specific date functions
	chFilter, _ := where.Parse(`
		toYYYYMM(event_time) = 202401 AND
		has(categories, 'analytics') = true
	`)

	sql, params, _ = chFilter.ToSQL("clickhouse")
	fmt.Println("\nClickHouse-specific functions:")
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Params: %v\n", params)

	// Output:
	// MySQL-specific functions:
	// SQL: (DATE_FORMAT(created_at, ?) = ? AND TIMESTAMPDIFF(?, start_date, end_date) > ?)
	// Params: [%Y-%m-%d 2024-01-15 DAY 7]
	//
	// PostgreSQL-specific functions:
	// SQL: (ARRAY_LENGTH(tags, $1) > $2 AND JSONB_EXTRACT_PATH(metadata, $3, $4) = $5)
	// Params: [1 0 user role admin]
	//
	// ClickHouse-specific functions:
	// SQL: (toYYYYMM(event_time) = ? AND has(categories, ?) = TRUE)
	// Params: [202401 analytics]
}

// ExampleParse_clickHouseStandardFunctions demonstrates ClickHouse-specific functions including case-sensitive ones.
func ExampleParse_clickHouseStandardFunctions() {
	// Date/time functions with case-sensitive syntax
	dateFilter, err := where.Parse(`
		toYYYYMM(event_time) = 202401 AND
		toYear(created_at) = 2024 AND
		toStartOfMonth(timestamp) >= '2024-01-01'
	`)
	if err != nil {
		log.Fatal(err)
	}

	sql, params, _ := dateFilter.ToSQL("clickhouse")
	fmt.Printf("ClickHouse Date Functions:\n")
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Parameters: %v\n", params)

	// Array operations
	arrayFilter, _ := where.Parse(`
		arrayLength(tags) > 0 AND
		has(categories, 'analytics') = true AND
		arrayElement(segments, 1) = 'premium'
	`)
	sql, params, _ = arrayFilter.ToSQL("clickhouse")
	fmt.Printf("\nArray Operations:\n")
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Parameters: %v\n", params)

	// String and encoding functions
	stringFilter, _ := where.Parse(`
		startsWith(url, 'https') = true AND
		domain(referrer) = 'google.com' AND
		hex(user_hash) LIKE 'A%'
	`)
	sql, params, _ = stringFilter.ToSQL("clickhouse")
	fmt.Printf("\nString Functions:\n")
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Parameters: %v\n", params)

	// Output:
	// ClickHouse Date Functions:
	// SQL: (toYYYYMM(event_time) = ? AND toYear(created_at) = ? AND toStartOfMonth(`timestamp`) >= ?)
	// Parameters: [202401 2024 2024-01-01]
	//
	// Array Operations:
	// SQL: (arrayLength(tags) > ? AND has(categories, ?) = TRUE AND arrayElement(segments, ?) = ?)
	// Parameters: [0 analytics 1 premium]
	//
	// String Functions:
	// SQL: (startsWith(url, ?) = TRUE AND domain(referrer) = ? AND hex(user_hash) LIKE ?)
	// Parameters: [https google.com A%]
}
