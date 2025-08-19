package where_test

import (
	"testing"

	"github.com/pseudomuto/where"
	"github.com/stretchr/testify/require"
)

func TestNewValidator(t *testing.T) {
	validator := where.NewValidator()
	require.NotNil(t, validator)

	// By default, nothing should be allowed
	require.False(t, validator.IsFieldAllowed("any_field"))
	require.False(t, validator.IsFunctionAllowed("ANY_FUNCTION"))
}

func TestValidatorAllowAll(t *testing.T) {
	validator := where.NewValidator()

	// Before AllowAll - should deny everything
	require.False(t, validator.IsFieldAllowed("test_field"))
	require.False(t, validator.IsFunctionAllowed("TEST_FUNCTION"))

	// After AllowAll - should allow everything
	validator.AllowAll()
	require.True(t, validator.IsFieldAllowed("test_field"))
	require.True(t, validator.IsFieldAllowed("another_field"))
	require.True(t, validator.IsFunctionAllowed("TEST_FUNCTION"))
	require.True(t, validator.IsFunctionAllowed("another_function"))

	// Should work with any case
	require.True(t, validator.IsFieldAllowed("UPPER_CASE"))
	require.True(t, validator.IsFieldAllowed("lower_case"))
	require.True(t, validator.IsFunctionAllowed("UPPER_CASE"))
	require.True(t, validator.IsFunctionAllowed("lower_case"))
}

func TestValidatorAllowFields(t *testing.T) {
	validator := where.NewValidator()

	// Allow specific fields
	validator.AllowFields("name", "age", "email")

	// Should allow specified fields (case insensitive)
	require.True(t, validator.IsFieldAllowed("name"))
	require.True(t, validator.IsFieldAllowed("age"))
	require.True(t, validator.IsFieldAllowed("email"))
	require.True(t, validator.IsFieldAllowed("NAME"))
	require.True(t, validator.IsFieldAllowed("Age"))
	require.True(t, validator.IsFieldAllowed("EMAIL"))

	// Should deny non-specified fields
	require.False(t, validator.IsFieldAllowed("password"))
	require.False(t, validator.IsFieldAllowed("secret"))

	// Functions should still be denied
	require.False(t, validator.IsFunctionAllowed("LOWER"))
}

func TestValidatorAllowFunctions(t *testing.T) {
	validator := where.NewValidator()

	// Allow specific functions
	validator.AllowFunctions("LOWER", "UPPER", "LENGTH")

	// Should allow specified functions (case insensitive)
	require.True(t, validator.IsFunctionAllowed("LOWER"))
	require.True(t, validator.IsFunctionAllowed("UPPER"))
	require.True(t, validator.IsFunctionAllowed("LENGTH"))
	require.True(t, validator.IsFunctionAllowed("lower"))
	require.True(t, validator.IsFunctionAllowed("upper"))
	require.True(t, validator.IsFunctionAllowed("length"))

	// Should deny non-specified functions
	require.False(t, validator.IsFunctionAllowed("TRIM"))
	require.False(t, validator.IsFunctionAllowed("CONCAT"))

	// Fields should still be denied
	require.False(t, validator.IsFieldAllowed("name"))
}

func TestValidatorChaining(t *testing.T) {
	// Test method chaining
	validator := where.NewValidator().
		AllowFields("id", "name", "email").
		AllowFunctions("LOWER", "UPPER")

	// Should allow chained fields
	require.True(t, validator.IsFieldAllowed("id"))
	require.True(t, validator.IsFieldAllowed("name"))
	require.True(t, validator.IsFieldAllowed("email"))

	// Should allow chained functions
	require.True(t, validator.IsFunctionAllowed("LOWER"))
	require.True(t, validator.IsFunctionAllowed("UPPER"))

	// Should deny others
	require.False(t, validator.IsFieldAllowed("password"))
	require.False(t, validator.IsFunctionAllowed("LENGTH"))
}

func TestValidatorCumulativeAllows(t *testing.T) {
	validator := where.NewValidator()

	// Add fields incrementally
	validator.AllowFields("name")
	require.True(t, validator.IsFieldAllowed("name"))
	require.False(t, validator.IsFieldAllowed("email"))

	validator.AllowFields("email", "age")
	require.True(t, validator.IsFieldAllowed("name"))
	require.True(t, validator.IsFieldAllowed("email"))
	require.True(t, validator.IsFieldAllowed("age"))
	require.False(t, validator.IsFieldAllowed("password"))

	// Add functions incrementally
	validator.AllowFunctions("LOWER")
	require.True(t, validator.IsFunctionAllowed("LOWER"))
	require.False(t, validator.IsFunctionAllowed("UPPER"))

	validator.AllowFunctions("UPPER", "LENGTH")
	require.True(t, validator.IsFunctionAllowed("LOWER"))
	require.True(t, validator.IsFunctionAllowed("UPPER"))
	require.True(t, validator.IsFunctionAllowed("LENGTH"))
	require.False(t, validator.IsFunctionAllowed("TRIM"))
}

func TestValidatorCaseInsensitivity(t *testing.T) {
	validator := where.NewValidator()

	// Allow with mixed case
	validator.AllowFields("UserName", "Email_Address")
	validator.AllowFunctions("lower", "UPPER")

	// Test field case insensitivity
	require.True(t, validator.IsFieldAllowed("username"))
	require.True(t, validator.IsFieldAllowed("USERNAME"))
	require.True(t, validator.IsFieldAllowed("UserName"))
	require.True(t, validator.IsFieldAllowed("email_address"))
	require.True(t, validator.IsFieldAllowed("EMAIL_ADDRESS"))
	require.True(t, validator.IsFieldAllowed("Email_Address"))

	// Test function case insensitivity
	require.True(t, validator.IsFunctionAllowed("LOWER"))
	require.True(t, validator.IsFunctionAllowed("lower"))
	require.True(t, validator.IsFunctionAllowed("Lower"))
	require.True(t, validator.IsFunctionAllowed("UPPER"))
	require.True(t, validator.IsFunctionAllowed("upper"))
	require.True(t, validator.IsFunctionAllowed("Upper"))
}

func TestValidatorQualifiedFields(t *testing.T) {
	validator := where.NewValidator()

	// Allow qualified field names
	validator.AllowFields("users.name", "orders.id", "table.column")

	require.True(t, validator.IsFieldAllowed("users.name"))
	require.True(t, validator.IsFieldAllowed("orders.id"))
	require.True(t, validator.IsFieldAllowed("table.column"))

	// Case insensitive for qualified names too
	require.True(t, validator.IsFieldAllowed("USERS.NAME"))
	require.True(t, validator.IsFieldAllowed("Orders.Id"))

	// Should not allow parts individually
	require.False(t, validator.IsFieldAllowed("users"))
	require.False(t, validator.IsFieldAllowed("name"))
}

func TestValidatorEmptyInputs(t *testing.T) {
	validator := where.NewValidator()

	// Empty strings should be handled gracefully
	require.False(t, validator.IsFieldAllowed(""))
	require.False(t, validator.IsFunctionAllowed(""))

	// Allow empty string explicitly
	validator.AllowFields("")
	validator.AllowFunctions("")
	require.True(t, validator.IsFieldAllowed(""))
	require.True(t, validator.IsFunctionAllowed(""))
}

func TestValidatorWithAllowAllOverride(t *testing.T) {
	validator := where.NewValidator()

	// Set up specific allows first
	validator.AllowFields("name", "email")
	validator.AllowFunctions("LOWER", "UPPER")

	require.True(t, validator.IsFieldAllowed("name"))
	require.False(t, validator.IsFieldAllowed("password"))
	require.True(t, validator.IsFunctionAllowed("LOWER"))
	require.False(t, validator.IsFunctionAllowed("TRIM"))

	// AllowAll should override specific restrictions
	validator.AllowAll()
	require.True(t, validator.IsFieldAllowed("name"))
	require.True(t, validator.IsFieldAllowed("password"))
	require.True(t, validator.IsFieldAllowed("anything"))
	require.True(t, validator.IsFunctionAllowed("LOWER"))
	require.True(t, validator.IsFunctionAllowed("TRIM"))
	require.True(t, validator.IsFunctionAllowed("ANYTHING"))
}

func TestValidatorIntegrationWithFilters(t *testing.T) {
	// Test validator integration with actual filter parsing and SQL generation
	t.Run("allowed fields and functions", func(t *testing.T) {
		filter, err := where.Parse("LOWER(name) = 'john' AND age > 18")
		require.NoError(t, err)

		validator := where.NewValidator().
			AllowFields("name", "age").
			AllowFunctions("LOWER")

		sql, args, err := filter.ToSQL("postgres", where.WithValidator(validator))
		require.NoError(t, err)
		require.Equal(t, "(LOWER(name) = $1 AND age > $2)", sql)
		require.Equal(t, []any{"john", float64(18)}, args)
	})

	t.Run("disallowed field", func(t *testing.T) {
		filter, err := where.Parse("name = 'john' AND secret = 'password'")
		require.NoError(t, err)

		validator := where.NewValidator().AllowFields("name")

		_, _, err = filter.ToSQL("postgres", where.WithValidator(validator))
		require.Error(t, err)
		require.Contains(t, err.Error(), "field \"secret\" is not allowed")
	})

	t.Run("disallowed function", func(t *testing.T) {
		filter, err := where.Parse("LENGTH(name) > 5")
		require.NoError(t, err)

		validator := where.NewValidator().
			AllowFields("name").
			AllowFunctions("LOWER", "UPPER") // LENGTH not allowed

		_, _, err = filter.ToSQL("postgres", where.WithValidator(validator))
		require.Error(t, err)
		require.Contains(t, err.Error(), "function \"LENGTH\" is not allowed")
	})
}
