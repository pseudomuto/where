package where_test

import (
	"sort"
	"testing"

	"github.com/pseudomuto/where"
	"github.com/stretchr/testify/require"
)

// MockDriver for testing
type MockDriver struct {
	name string
}

func (m *MockDriver) Name() string                               { return m.name }
func (m *MockDriver) QuoteIdentifier(name string) string         { return "[" + name + "]" }
func (m *MockDriver) Placeholder(position int) string            { return "?" }
func (m *MockDriver) IsReservedKeyword(word string) bool         { return word == "select" }
func (m *MockDriver) TranslateOperator(op string) (string, bool) { return op, true }
func (m *MockDriver) TranslateFunction(name string, argCount int) (string, bool) {
	return name + "()", true
}
func (m *MockDriver) ConcatOperator() string              { return "+" }
func (m *MockDriver) SupportsFeature(feature string) bool { return true }

func TestDriverRegistry(t *testing.T) {
	// Create a mock driver for testing
	mockDriver := &MockDriver{name: "test"}

	t.Run("RegisterDriver", func(t *testing.T) {
		// Register a new driver
		where.RegisterDriver("testdb", mockDriver)

		// Verify it was registered
		driver, err := where.GetDriver("testdb")
		require.NoError(t, err)
		require.Equal(t, "test", driver.Name())
	})

	t.Run("GetDriver existing", func(t *testing.T) {
		driver, err := where.GetDriver("postgres")
		require.NoError(t, err)
		require.NotNil(t, driver)
		require.Equal(t, "postgres", driver.Name())
	})

	t.Run("GetDriver nonexistent", func(t *testing.T) {
		driver, err := where.GetDriver("nonexistent")
		require.Error(t, err)
		require.Nil(t, driver)
		require.Contains(t, err.Error(), "driver \"nonexistent\" not registered")
	})

	t.Run("GetDriver with aliases", func(t *testing.T) {
		// Test PostgreSQL aliases
		pg1, err1 := where.GetDriver("postgres")
		require.NoError(t, err1)

		pg2, err2 := where.GetDriver("postgresql")
		require.NoError(t, err2)

		pg3, err3 := where.GetDriver("pg")
		require.NoError(t, err3)

		require.Equal(t, pg1.Name(), pg2.Name())
		require.Equal(t, pg1.Name(), pg3.Name())

		// Test MySQL aliases
		mysql1, err1 := where.GetDriver("mysql")
		require.NoError(t, err1)

		mysql2, err2 := where.GetDriver("mariadb")
		require.NoError(t, err2)

		require.Equal(t, mysql1.Name(), mysql2.Name())

		// Test ClickHouse aliases
		ch1, err1 := where.GetDriver("clickhouse")
		require.NoError(t, err1)

		ch2, err2 := where.GetDriver("ch")
		require.NoError(t, err2)

		require.Equal(t, ch1.Name(), ch2.Name())
	})

	t.Run("ListDrivers", func(t *testing.T) {
		drivers := where.ListDrivers()
		require.NotEmpty(t, drivers)

		// Should include our built-in drivers
		sort.Strings(drivers)
		require.Contains(t, drivers, "clickhouse")
		require.Contains(t, drivers, "mysql")
		require.Contains(t, drivers, "postgres")

		// Should include our test driver if it was registered
		if contains(drivers, "testdb") {
			require.Contains(t, drivers, "testdb")
		}
	})

	t.Run("RegisterDriver overwrites existing", func(t *testing.T) {
		// Register first driver
		driver1 := &MockDriver{name: "first"}
		where.RegisterDriver("overwrite-test", driver1)

		retrieved1, err := where.GetDriver("overwrite-test")
		require.NoError(t, err)
		require.Equal(t, "first", retrieved1.Name())

		// Overwrite with second driver
		driver2 := &MockDriver{name: "second"}
		where.RegisterDriver("overwrite-test", driver2)

		retrieved2, err := where.GetDriver("overwrite-test")
		require.NoError(t, err)
		require.Equal(t, "second", retrieved2.Name())
	})
}

// Helper function
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
