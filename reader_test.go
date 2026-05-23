package tlvm

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenize(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want []string // expected token .value sequence
	}{
		{"Empty", "", nil},
		{"OnlyWhitespace", "   \t\n  ", nil},
		{"Atoms", "abc 123 1.5 true false", []string{"abc", "123", "1.5", "true", "false"}},
		{"NegativeNumber", "-1 -2.5", []string{"-1", "-2.5"}},
		{"PositiveNumber", "+1 +2.5", []string{"+1", "+2.5"}},
		{"OperatorTokens", "+ - * /", []string{"+", "-", "*", "/"}},
		{"Parens", "(foo)", []string{"(", "foo", ")"}},
		{"QuoteFamily", "' ` , ,@", []string{"'", "`", ",", ",@"}},
		{
			"Comment",
			"; hi there\n42",
			[]string{"; hi there", "42"},
		},
		{
			"CommentAtEOF",
			"(+ 1 2) ; trailing",
			[]string{"(", "+", "1", "2", ")", "; trailing"},
		},
		{
			"DottedIdentifier",
			"p.X.Y",
			[]string{"p.X.Y"},
		},
		{
			"StringWithEscapes",
			`"a\"b\n"`,
			[]string{`"a\"b\n"`},
		},
		{
			"NestedCall",
			"(+ 1 (* 2 3))",
			[]string{"(", "+", "1", "(", "*", "2", "3", ")", ")"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			toks, err := tokenize(tc.in)
			require.NoError(t, err)
			var got []string
			for _, tk := range toks {
				got = append(got, tk.value)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestTokenize_UnterminatedString(t *testing.T) {
	_, err := tokenize(`"never closed`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unterminated string")
}

func TestValidateBrackets(t *testing.T) {
	t.Run("Balanced", func(t *testing.T) {
		require.NoError(t, validateBrackets("(foo (bar) baz)"))
	})
	t.Run("Empty", func(t *testing.T) {
		require.NoError(t, validateBrackets(""))
	})
	t.Run("MissingClose", func(t *testing.T) {
		err := validateBrackets("(foo")
		require.Error(t, err)
		require.Contains(t, err.Error(), "right bracket")
	})
	t.Run("ExtraClose", func(t *testing.T) {
		err := validateBrackets("foo)")
		require.Error(t, err)
		require.Contains(t, err.Error(), "redundant bracket")
	})
	t.Run("DeepNesting", func(t *testing.T) {
		require.NoError(t, validateBrackets(strings.Repeat("(", 100)+strings.Repeat(")", 100)))
	})
	t.Run("MultiByteUnicode_Balanced", func(t *testing.T) {
		// Iteration over bytes (not runes) must still see balanced parens
		// when the file contains multi-byte characters.
		require.NoError(t, validateBrackets(`("你好 ééé")`))
	})
}

// TestRead exercises the higher-level Reader that converts tokens into
// the cons / literal / number / str AST consumed by the compiler.
func TestRead(t *testing.T) {
	t.Run("EmptyInput", func(t *testing.T) {
		got, err := Read("")
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("CommentOnly", func(t *testing.T) {
		got, err := Read("; just a comment")
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("SingleAtom_Int", func(t *testing.T) {
		got, err := Read("42")
		require.NoError(t, err)
		require.Len(t, got, 1)
		n, ok := got[0].(number)
		require.True(t, ok, "expected number, got %T", got[0])
		require.EqualValues(t, 42, n.value)
	})

	t.Run("SingleAtom_Float", func(t *testing.T) {
		got, err := Read("3.14")
		require.NoError(t, err)
		f, ok := got[0].(float)
		require.True(t, ok)
		require.InDelta(t, 3.14, f.value, 1e-9)
	})

	t.Run("SingleAtom_NegativeInt", func(t *testing.T) {
		got, err := Read("-7")
		require.NoError(t, err)
		n, ok := got[0].(number)
		require.True(t, ok, "expected number, got %T", got[0])
		require.EqualValues(t, -7, n.value)
	})

	t.Run("SingleAtom_Bool", func(t *testing.T) {
		got, err := Read("true false")
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.True(t, got[0].(boolean).value)
		require.False(t, got[1].(boolean).value)
	})

	t.Run("SingleAtom_Literal", func(t *testing.T) {
		got, err := Read("foo")
		require.NoError(t, err)
		l, ok := got[0].(literal)
		require.True(t, ok)
		require.Equal(t, Label("foo"), l.value)
	})

	t.Run("StringLiteralUnquoted", func(t *testing.T) {
		got, err := Read(`"hello\tworld"`)
		require.NoError(t, err)
		s, ok := got[0].(str)
		require.True(t, ok)
		require.Equal(t, "hello\tworld", s.value)
	})

	t.Run("EmptyList", func(t *testing.T) {
		got, err := Read("()")
		require.NoError(t, err)
		c, ok := got[0].(*cons)
		require.True(t, ok)
		require.Empty(t, c.expr)
	})

	t.Run("NestedList", func(t *testing.T) {
		got, err := Read("(a (b c) d)")
		require.NoError(t, err)
		require.Equal(t, "(a (b c) d)", fmt.Sprintf("%v", got[0]))
	})

	t.Run("QuoteSugar", func(t *testing.T) {
		// 'x  ->  (x quote)  (note: head is last element of cons.expr)
		got, err := Read("'x")
		require.NoError(t, err)
		c, ok := got[0].(*cons)
		require.True(t, ok)
		// The wrapper is built as expr: [x, quote-literal] — first() returns the keyword.
		first, ok := c.first().(literal)
		require.True(t, ok)
		require.Equal(t, Label(keywordQuote), first.value)
	})

	t.Run("BacktickSugar", func(t *testing.T) {
		got, err := Read("`x")
		require.NoError(t, err)
		c := got[0].(*cons)
		require.Equal(t, Label(keywordBacktick), c.first().(literal).value)
	})

	t.Run("CommaSugar", func(t *testing.T) {
		got, err := Read(",x")
		require.NoError(t, err)
		c := got[0].(*cons)
		require.Equal(t, Label(keywordComma), c.first().(literal).value)
	})

	t.Run("SpliceSugar", func(t *testing.T) {
		got, err := Read(",@x")
		require.NoError(t, err)
		c := got[0].(*cons)
		require.Equal(t, Label(keywordSplice), c.first().(literal).value)
	})

	t.Run("UnbalancedReturnsError", func(t *testing.T) {
		_, err := Read("(foo")
		require.Error(t, err)
	})
}

// TestStringerImplementations is a smoke test for the various String() methods
// defined on AST node types. They are mostly used for debugging / pretty-printing
// and easy to break unnoticed.
func TestStringerImplementations(t *testing.T) {
	t.Run("literal", func(t *testing.T) {
		require.Equal(t, "foo", literal{value: "foo"}.String())
	})
	t.Run("number", func(t *testing.T) {
		require.Equal(t, "42", number{value: 42}.String())
	})
	t.Run("float", func(t *testing.T) {
		require.Contains(t, float{value: 1.5}.String(), "1.5")
	})
	t.Run("boolean", func(t *testing.T) {
		require.Equal(t, "true", boolean{value: true}.String())
		require.Equal(t, "false", boolean{value: false}.String())
	})
	t.Run("cons", func(t *testing.T) {
		// build (1 2 3) via the consBuilder.
		b := newConsBuilder()
		b.append(number{value: 1})
		b.append(number{value: 2})
		b.append(number{value: 3})
		require.Equal(t, "(1 2 3)", b.build(0).String())
	})
	t.Run("Label", func(t *testing.T) {
		require.Equal(t, "x", Label("x").String())
		require.Equal(t, "S", StructLabel("S").String())
	})
}

func TestParseInvalidNumber(t *testing.T) {
	_, err := Read("+1.2.3")
	require.Error(t, err)
}
