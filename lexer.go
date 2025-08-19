package where

import (
	"github.com/alecthomas/participle/v2/lexer"
)

// NewLexer creates a new lexer for parsing SQL filter expressions.
// The lexer supports case-insensitive keywords, quoted identifiers, and various operators.
func NewLexer() (*lexer.StatefulDefinition, error) {
	return lexer.NewSimple([]lexer.SimpleRule{
		{Name: "Whitespace", Pattern: `\s+`},

		{Name: "And", Pattern: `(?i)\bAND\b`},
		{Name: "Or", Pattern: `(?i)\bOR\b`},
		{Name: "Not", Pattern: `(?i)\bNOT\b`},
		{Name: "Between", Pattern: `(?i)\bBETWEEN\b`},
		{Name: "In", Pattern: `(?i)\bIN\b`},
		{Name: "Like", Pattern: `(?i)\bLIKE\b`},
		{Name: "ILike", Pattern: `(?i)\bILIKE\b`},
		{Name: "Is", Pattern: `(?i)\bIS\b`},
		{Name: "Null", Pattern: `(?i)\bNULL\b`},
		{Name: "True", Pattern: `(?i)\bTRUE\b`},
		{Name: "False", Pattern: `(?i)\bFALSE\b`},

		{Name: "NotEqual", Pattern: `!=|<>`},
		{Name: "LessOrEqual", Pattern: `<=`},
		{Name: "GreaterOrEqual", Pattern: `>=`},
		{Name: "Equal", Pattern: `=`},
		{Name: "Less", Pattern: `<`},
		{Name: "Greater", Pattern: `>`},

		{Name: "String", Pattern: `'([^'\\]|\\.)*'`},

		{Name: "BacktickIdent", Pattern: "`[^`]+`"},
		{Name: "QuotedIdent", Pattern: `"[a-zA-Z_][a-zA-Z0-9_]*"`},
		{Name: "DoubleQuotedString", Pattern: `"([^"\\]|\\.)*"`},

		{Name: "Number", Pattern: `[-+]?\d+(\.\d+)?([eE][-+]?\d+)?`},

		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},

		{Name: "Dot", Pattern: `\.`},
		{Name: "LParen", Pattern: `\(`},
		{Name: "RParen", Pattern: `\)`},
		{Name: "Comma", Pattern: `,`},
	})
}
