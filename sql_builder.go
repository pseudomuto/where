package where

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type (
	// SQLBuilder builds SQL queries from parsed filter expressions.
	SQLBuilder struct {
		driver    Driver
		params    []any
		validator *Validator
	}

	// BuildOption is a function type for configuring SQL building options.
	BuildOption func(*SQLBuilder)
)

// WithValidator returns a BuildOption that sets a validator for field and function restrictions.
func WithValidator(v *Validator) BuildOption {
	return func(b *SQLBuilder) {
		b.validator = v
	}
}

// ToSQL converts the filter to SQL with parameterized values for the specified database driver.
// Returns the SQL string, parameter values, and any error encountered during conversion.
func (f *Filter) ToSQL(driverName string, options ...BuildOption) (string, []any, error) {
	driver, err := GetDriver(driverName)
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to get driver %q", driverName)
	}

	builder := &SQLBuilder{
		driver: driver,
		params: make([]any, 0),
	}

	for _, opt := range options {
		opt(builder)
	}

	if f == nil || f.Expression == nil {
		return "", nil, errors.New("empty filter")
	}

	sql, err := builder.buildExpression(f.Expression)
	if err != nil {
		return "", nil, err
	}

	return sql, builder.params, nil
}

func (b *SQLBuilder) buildExpression(expr *Expression) (string, error) {
	if expr == nil || len(expr.Or) == 0 {
		return "", errors.New("empty expression")
	}

	if len(expr.Or) == 1 {
		return b.buildTerm(expr.Or[0])
	}

	parts := make([]string, len(expr.Or))
	for i, term := range expr.Or {
		part, err := b.buildTerm(term)
		if err != nil {
			return "", err
		}
		parts[i] = part
	}

	return "(" + strings.Join(parts, " OR ") + ")", nil
}

func (b *SQLBuilder) buildTerm(term *Term) (string, error) {
	if term == nil || len(term.And) == 0 {
		return "", errors.New("empty term")
	}

	if len(term.And) == 1 {
		return b.buildFactor(term.And[0])
	}

	parts := make([]string, len(term.And))
	for i, factor := range term.And {
		part, err := b.buildFactor(factor)
		if err != nil {
			return "", err
		}
		parts[i] = part
	}

	return "(" + strings.Join(parts, " AND ") + ")", nil
}

func (b *SQLBuilder) buildFactor(factor *Factor) (string, error) {
	if factor == nil {
		return "", errors.New("empty factor")
	}

	var result string
	var err error

	if factor.SubExpr != nil {
		result, err = b.buildExpression(factor.SubExpr)
	} else if factor.Predicate != nil {
		result, err = b.buildPredicate(factor.Predicate)
	} else {
		return "", errors.New("empty factor content")
	}

	if err != nil {
		return "", err
	}

	if factor.Not {
		return "NOT (" + result + ")", nil
	}

	return result, nil
}

func (b *SQLBuilder) buildPredicate(pred *Predicate) (string, error) {
	if pred == nil {
		return "", errors.New("empty predicate")
	}

	leftVal, err := b.buildValue(pred.Left)
	if err != nil {
		return "", err
	}

	if pred.Operation == nil {
		return "", errors.New("predicate missing operation")
	}

	return b.buildOperation(leftVal, pred.Operation)
}

func (b *SQLBuilder) buildOperation(leftVal string, op *Operation) (string, error) {
	if op == nil {
		return "", errors.New("empty operation")
	}

	if op.Compare != nil {
		return b.buildCompare(leftVal, op.Compare)
	}
	if op.Like != nil {
		return b.buildLike(leftVal, op.Like)
	}
	if op.Between != nil {
		return b.buildBetween(leftVal, op.Between)
	}
	if op.In != nil {
		return b.buildIn(leftVal, op.In)
	}
	if op.IsNull != nil {
		return b.buildIsNull(leftVal, op.IsNull)
	}

	return "", errors.New("unrecognized operation type")
}

func (b *SQLBuilder) buildCompare(leftVal string, comp *CompareOp) (string, error) {
	rightVal, err := b.buildValue(comp.Right)
	if err != nil {
		return "", err
	}
	sqlOp := comp.Operator.String()
	return fmt.Sprintf("%s %s %s", leftVal, sqlOp, rightVal), nil
}

func (b *SQLBuilder) buildLike(leftVal string, like *LikeOp) (string, error) {
	pattern, err := b.buildValue(like.Pattern)
	if err != nil {
		return "", err
	}

	operator := strings.ToUpper(like.Type.Operator)
	if like.Not {
		operator = "NOT " + operator
	}

	translated, supported := b.driver.TranslateOperator(operator)
	if !supported {
		return "", fmt.Errorf("operator %s not supported by driver %s", operator, b.driver.Name())
	}

	if b.driver.Name() == "mysql" && strings.Contains(strings.ToUpper(like.Type.Operator), "ILIKE") {
		leftVal = fmt.Sprintf("LOWER(%s)", leftVal)
		pattern = fmt.Sprintf("LOWER(%s)", pattern)
	}

	return fmt.Sprintf("%s %s %s", leftVal, translated, pattern), nil
}

func (b *SQLBuilder) buildBetween(leftVal string, between *BetweenOp) (string, error) {
	lower, err := b.buildValue(between.Lower)
	if err != nil {
		return "", err
	}

	upper, err := b.buildValue(between.Upper)
	if err != nil {
		return "", err
	}

	sqlOp := "BETWEEN"
	if between.Not {
		sqlOp = "NOT BETWEEN"
	}

	return fmt.Sprintf("%s %s %s AND %s", leftVal, sqlOp, lower, upper), nil
}

func (b *SQLBuilder) buildIn(leftVal string, in *InOp) (string, error) {
	if len(in.Values) == 0 {
		return "", errors.New("IN expression requires at least one value")
	}

	items := make([]string, len(in.Values))
	var err error
	for i, item := range in.Values {
		items[i], err = b.buildValue(item)
		if err != nil {
			return "", err
		}
	}

	sqlOp := "IN"
	if in.Not {
		sqlOp = "NOT IN"
	}

	return fmt.Sprintf("%s %s (%s)", leftVal, sqlOp, strings.Join(items, ", ")), nil
}

func (b *SQLBuilder) buildIsNull(leftVal string, isNull *IsNullOp) (string, error) {
	sqlOp := "IS NULL"
	if isNull.Not {
		sqlOp = "IS NOT NULL"
	}

	return fmt.Sprintf("%s %s", leftVal, sqlOp), nil
}

func (b *SQLBuilder) buildValue(val *Value) (string, error) {
	if val == nil {
		return "", errors.New("nil value")
	}

	if val.Function != nil {
		return b.buildFunctionCall(val.Function)
	}

	if val.Field != nil {
		return b.buildFieldRef(val.Field)
	}

	if val.Literal != nil {
		return b.buildLiteralValue(val.Literal)
	}

	if val.SubExpr != nil {
		return b.buildExpression(val.SubExpr)
	}

	return "", errors.New("unrecognized value type")
}

func (b *SQLBuilder) buildFunctionCall(fn *FunctionCall) (string, error) {
	if b.validator != nil && !b.validator.IsFunctionAllowed(fn.Name) {
		return "", fmt.Errorf("function %q is not allowed", fn.Name)
	}

	template, supported := b.driver.TranslateFunction(fn.Name, len(fn.Args))
	if !supported {
		return "", fmt.Errorf("function %q with %d arguments not supported by driver %s",
			fn.Name, len(fn.Args), b.driver.Name())
	}

	if len(fn.Args) == 0 {
		return template, nil
	}

	args := make([]any, len(fn.Args))
	for i, arg := range fn.Args {
		argStr, err := b.buildValue(arg)
		if err != nil {
			return "", err
		}
		args[i] = argStr
	}

	return fmt.Sprintf(template, args...), nil
}

func (b *SQLBuilder) buildFieldRef(field *FieldRef) (string, error) {
	if len(field.Parts) == 0 {
		return "", errors.New("empty field")
	}

	if b.validator != nil && !b.validator.IsFieldAllowed(strings.Join(field.Parts, ".")) {
		return "", fmt.Errorf("field %q is not allowed", strings.Join(field.Parts, "."))
	}

	parts := make([]string, len(field.Parts))
	for i, part := range field.Parts {
		part = strings.TrimSpace(part)

		if strings.HasPrefix(part, "`") && strings.HasSuffix(part, "`") {
			part = part[1 : len(part)-1]
		} else if strings.HasPrefix(part, `"`) && strings.HasSuffix(part, `"`) {
			part = part[1 : len(part)-1]
		}

		parts[i] = b.driver.QuoteIdentifier(part)
	}

	return strings.Join(parts, "."), nil
}

func (b *SQLBuilder) buildLiteralValue(lit *LiteralValue) (string, error) {
	if lit.Null {
		return "NULL", nil
	}

	if lit.Boolean != nil {
		if lit.Boolean.Value() {
			return "TRUE", nil
		}
		return "FALSE", nil
	}

	if lit.Number != nil {
		b.params = append(b.params, *lit.Number)
		return b.driver.Placeholder(len(b.params)), nil
	}

	if lit.String != nil {
		str := *lit.String
		if len(str) >= 2 && ((str[0] == '\'' && str[len(str)-1] == '\'') ||
			(str[0] == '"' && str[len(str)-1] == '"')) {
			str = str[1 : len(str)-1]
		}
		b.params = append(b.params, str)
		return b.driver.Placeholder(len(b.params)), nil
	}

	return "", errors.New("unrecognized literal type")
}
