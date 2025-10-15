package where

import (
	"sync"

	"github.com/pkg/errors"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

type (
	// Driver interface defines the contract for database-specific implementations.
	// Each database driver handles SQL dialect differences, keyword quoting, and placeholders.
	// Functions are handled generically by the SQL builder without driver-specific translation.
	Driver interface {
		// Name returns the driver name (e.g., "postgres", "mysql", "clickhouse").
		Name() string

		// QuoteIdentifier quotes database identifiers to handle reserved keywords and special characters.
		QuoteIdentifier(name string) string

		// Placeholder returns the placeholder syntax for the given parameter position.
		Placeholder(position int) string

		// IsReservedKeyword returns true if the word is a reserved keyword in this database.
		IsReservedKeyword(word string) bool

		// TranslateOperator translates an operator to database-specific syntax.
		TranslateOperator(op string) (translated string, supported bool)

		// SupportsFeature returns true if the database supports the named feature.
		SupportsFeature(feature string) bool
	}
)

// RegisterDriver registers a database driver with the given name.
// This function is typically called from driver package init() functions.
func RegisterDriver(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()

	if driver == nil {
		panic("where: RegisterDriver driver is nil")
	}
	if name == "" {
		panic("where: RegisterDriver name is empty")
	}

	drivers[name] = driver
}

// GetDriver retrieves a registered driver by name.
// Returns an error if the driver is not found.
func GetDriver(name string) (Driver, error) {
	driversMu.RLock()
	defer driversMu.RUnlock()

	driver, ok := drivers[name]
	if !ok {
		return nil, errors.Errorf("driver %q not registered", name)
	}
	return driver, nil
}

// ListDrivers returns a list of all registered driver names.
func ListDrivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()

	names := make([]string, 0, len(drivers))
	for name := range drivers {
		names = append(names, name)
	}
	return names
}

// NeedsQuoting determines if an identifier needs to be quoted.
// This implements the common SQL identifier quoting rules used across all database drivers.
func NeedsQuoting(name string, driver Driver) bool {
	if driver.IsReservedKeyword(name) {
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
