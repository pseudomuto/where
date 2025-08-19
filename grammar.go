package where

import (
	"github.com/alecthomas/participle/v2/lexer"
)

type (
	// Filter represents the root AST node for a parsed filter expression.
	Filter struct {
		Pos        lexer.Position
		Expression *Expression `parser:"@@"`
	}

	// Expression represents logical expressions with proper precedence (OR has lower precedence than AND).
	Expression struct {
		Or []*Term `parser:"@@ ( Or @@ )*"`
	}

	// Term represents a logical AND expression containing one or more factors.
	Term struct {
		And []*Factor `parser:"@@ ( And @@ )*"`
	}

	// Factor represents a single factor in a logical expression, which can be negated.
	Factor struct {
		Not       bool        `parser:"@Not?"`
		SubExpr   *Expression `parser:"( LParen @@ RParen )"`
		Predicate *Predicate  `parser:"| @@"`
	}

	// Predicate represents the core predicate AST node containing a left value and an operation.
	Predicate struct {
		Left      *Value     `parser:"@@"`
		Operation *Operation `parser:"@@"`
	}

	// Operation represents different types of operations with clean separation of each operation type.
	Operation struct {
		Between *BetweenOp `parser:"@@"`
		In      *InOp      `parser:"| @@"`
		Like    *LikeOp    `parser:"| @@"`
		Compare *CompareOp `parser:"| @@"`
		IsNull  *IsNullOp  `parser:"| @@"`
	}

	// CompareOp represents comparison operations (=, !=, <, >, <=, >=).
	CompareOp struct {
		Operator CompareOperator `parser:"@@"`
		Right    *Value          `parser:"@@"`
	}

	// CompareOperator represents the type of comparison operator.
	CompareOperator struct {
		Type string `parser:"@( Equal | NotEqual | LessOrEqual | GreaterOrEqual | Less | Greater )"`
	}

	// LikeOp represents LIKE and ILIKE operations with optional NOT.
	LikeOp struct {
		Not     bool     `parser:"@Not?"`
		Type    LikeType `parser:"@@"`
		Pattern *Value   `parser:"@@"`
	}

	// LikeType represents the type of LIKE operation (LIKE or ILIKE).
	LikeType struct {
		Operator string `parser:"@( Like | ILike )"`
	}

	// BetweenOp represents BETWEEN operations with optional NOT.
	BetweenOp struct {
		Not     bool   `parser:"@Not?"`
		Between string `parser:"@Between"`
		Lower   *Value `parser:"@@"`
		And     string `parser:"@And"`
		Upper   *Value `parser:"@@"`
	}

	// InOp represents IN operations with optional NOT.
	InOp struct {
		Not    bool     `parser:"@Not?"`
		In     string   `parser:"@In"`
		Values []*Value `parser:"LParen @@ ( Comma @@ )* RParen"`
	}

	// IsNullOp represents IS NULL operations with optional NOT.
	IsNullOp struct {
		Is   string `parser:"@Is"`
		Not  bool   `parser:"@Not?"`
		Null string `parser:"@Null"`
	}

	// Value represents different types of values that can appear in expressions.
	Value struct {
		Function *FunctionCall `parser:"@@"`
		Field    *FieldRef     `parser:"| @@"`
		Literal  *LiteralValue `parser:"| @@"`
		SubExpr  *Expression   `parser:"| LParen @@ RParen"`
	}

	// FunctionCall represents a function call with a name and arguments.
	FunctionCall struct {
		Name string   `parser:"@Ident"`
		Args []*Value `parser:"LParen ( @@ ( Comma @@ )* )? RParen"`
	}

	// FieldRef represents a field reference with support for qualified names (table.column).
	FieldRef struct {
		Parts []string `parser:"@( QuotedIdent | BacktickIdent | Ident ) ( Dot @( QuotedIdent | BacktickIdent | Ident ) )*"`
	}

	// LiteralValue represents literal values (strings, numbers, booleans, null).
	LiteralValue struct {
		String  *string     `parser:"@( String | DoubleQuotedString )"`
		Number  *float64    `parser:"| @Number"`
		Boolean *BooleanLit `parser:"| @@"`
		Null    bool        `parser:"| @Null"`
	}

	// BooleanLit represents boolean literal values (true/false).
	BooleanLit struct {
		True  bool `parser:"@True"`
		False bool `parser:"| @False"`
	}
)

// Value returns the boolean value represented by the BooleanLit.
func (b *BooleanLit) Value() bool {
	return b.True // True if True token found, False if False token found
}

// Value returns the Go value represented by the LiteralValue, with strings having quotes stripped.
func (l *LiteralValue) Value() any {
	if l.String != nil {
		s := *l.String
		if len(s) >= 2 {
			s = s[1 : len(s)-1]
		}
		return s
	}
	if l.Number != nil {
		return *l.Number
	}
	if l.Boolean != nil {
		return l.Boolean.Value()
	}
	return nil
}

// IsNull returns true if the LiteralValue represents a NULL value.
func (l *LiteralValue) IsNull() bool {
	return l.Null
}

// String returns the SQL operator string representation of the CompareOperator.
func (op *CompareOperator) String() string {
	switch op.Type {
	case "Equal":
		return "="
	case "NotEqual":
		return "!="
	case "Less":
		return "<"
	case "Greater":
		return ">"
	case "LessOrEqual":
		return "<="
	case "GreaterOrEqual":
		return ">="
	default:
		return op.Type
	}
}
