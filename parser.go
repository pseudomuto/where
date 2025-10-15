package where

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/pkg/errors"
)

type (
	// Parser represents a configured filter expression parser with validation options.
	Parser struct {
		parser *participle.Parser[Filter]
		opts   *parserOptions
	}

	// parserOptions holds configuration options for the parser.
	parserOptions struct {
		maxDepth     int
		maxINItems   int
		allowedFuncs map[string]bool
	}

	// ParserOption is a function type for configuring parser options.
	ParserOption func(*parserOptions)
)

// WithMaxDepth returns a ParserOption that sets the maximum nesting depth for expressions.
func WithMaxDepth(depth int) ParserOption {
	return func(o *parserOptions) {
		o.maxDepth = depth
	}
}

// WithMaxINItems returns a ParserOption that sets the maximum number of items allowed in IN expressions.
func WithMaxINItems(max int) ParserOption {
	return func(o *parserOptions) {
		o.maxINItems = max
	}
}

// WithFunctions returns a ParserOption that restricts which functions are allowed in expressions.
// This provides parse-time validation - note that all functions are supported at the driver level.
// Use the Validator for runtime validation instead for more comprehensive security.
func WithFunctions(names ...string) ParserOption {
	return func(o *parserOptions) {
		if o.allowedFuncs == nil {
			o.allowedFuncs = make(map[string]bool)
		}
		for _, name := range names {
			o.allowedFuncs[strings.ToUpper(name)] = true
		}
	}
}

// NewParser creates a new parser with the specified options.
func NewParser(opts ...ParserOption) (*Parser, error) {
	options := &parserOptions{
		maxDepth:   10,
		maxINItems: 1000,
	}

	for _, opt := range opts {
		opt(options)
	}

	lex, err := NewLexer()
	if err != nil {
		return nil, fmt.Errorf("failed to create lexer: %w", err)
	}

	parser, err := participle.Build[Filter](
		participle.Lexer(lex),
		participle.Elide("Whitespace"),
		participle.UseLookahead(5),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build parser: %w", err)
	}

	return &Parser{
		parser: parser,
		opts:   options,
	}, nil
}

// Parse parses a filter expression string and returns the parsed Filter AST.
// The input is validated according to the parser's configured options.
func (p *Parser) Parse(input string) (*Filter, error) {
	if input == "" {
		return nil, errors.New("empty filter expression")
	}

	filter, err := p.parser.ParseString("", input)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse filter expression")
	}

	if err := p.validate(filter); err != nil {
		return nil, errors.Wrapf(err, "filter validation failed")
	}

	return filter, nil
}

func (p *Parser) validate(filter *Filter) error {
	return p.validateExpression(filter.Expression, 0)
}

func (p *Parser) validateExpression(expr *Expression, depth int) error {
	if depth > p.opts.maxDepth {
		return fmt.Errorf("expression depth exceeds maximum of %d", p.opts.maxDepth)
	}

	if expr == nil || len(expr.Or) == 0 {
		return nil
	}

	for _, term := range expr.Or {
		if err := p.validateTerm(term, depth); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) validateTerm(term *Term, depth int) error {
	if term == nil || len(term.And) == 0 {
		return nil
	}

	for _, factor := range term.And {
		if err := p.validateFactor(factor, depth); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) validateFactor(factor *Factor, depth int) error {
	if factor == nil {
		return errors.New("empty factor")
	}

	if factor.SubExpr != nil {
		return p.validateExpression(factor.SubExpr, depth+1)
	}

	if factor.Predicate != nil {
		return p.validatePredicate(factor.Predicate)
	}

	return errors.New("empty factor content")
}

func (p *Parser) validatePredicate(pred *Predicate) error {
	if pred == nil {
		return errors.New("empty predicate")
	}

	// Validate the left side (field/function/literal)
	if err := p.validateValue(pred.Left); err != nil {
		return err
	}

	if pred.Operation == nil {
		return errors.New("predicate missing operation")
	}

	return p.validateOperation(pred.Operation)
}

func (p *Parser) validateOperation(op *Operation) error {
	if op == nil {
		return errors.New("empty operation")
	}

	if op.Compare != nil {
		return p.validateValue(op.Compare.Right)
	}

	if op.Like != nil {
		return p.validateValue(op.Like.Pattern)
	}

	if op.Between != nil {
		if err := p.validateValue(op.Between.Lower); err != nil {
			return err
		}
		return p.validateValue(op.Between.Upper)
	}

	if op.In != nil {
		if len(op.In.Values) == 0 {
			return errors.New("IN expression requires at least one value")
		}
		if len(op.In.Values) > p.opts.maxINItems {
			return fmt.Errorf("IN expression exceeds maximum of %d items", p.opts.maxINItems)
		}

		for _, value := range op.In.Values {
			if err := p.validateValue(value); err != nil {
				return err
			}
		}
		return nil
	}

	if op.IsNull != nil {
		// IS NULL doesn't require additional validation
		return nil
	}

	return errors.New("operation has no valid type")
}

func (p *Parser) validateValue(val *Value) error {
	if val == nil {
		return nil
	}

	if val.Function != nil {
		if p.opts.allowedFuncs != nil {
			if !p.opts.allowedFuncs[strings.ToUpper(val.Function.Name)] {
				return fmt.Errorf("function %q is not allowed", val.Function.Name)
			}
		}

		for _, arg := range val.Function.Args {
			if err := p.validateValue(arg); err != nil {
				return err
			}
		}
	}

	if val.SubExpr != nil {
		return p.validateExpression(val.SubExpr, 0)
	}

	return nil
}

// Parse is a convenience function that creates a default parser and parses the input.
// For more control over parsing options, create a parser with NewParser.
func Parse(input string) (*Filter, error) {
	parser, err := NewParser()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create parser")
	}
	return parser.Parse(input)
}
