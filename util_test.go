package tlvm

import (
	"errors"
	"testing"

	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/require"
)

func TestLazyString(t *testing.T) {
	calls := 0
	ls := LazyString(func() string {
		calls++
		return "lazy-value"
	})
	require.Equal(t, "lazy-value", ls.String())
	require.Equal(t, "lazy-value", ls.String())
	require.Equal(t, 2, calls, "LazyString must call its closure each time it is evaluated")
}

func TestShowErrorLine(t *testing.T) {
	t.Run("PositionPastEnd", func(t *testing.T) {
		require.Equal(t, "", ShowErrorLine("abc", 100))
	})

	t.Run("SingleLine", func(t *testing.T) {
		got := ShowErrorLine("hello", 2)
		require.Equal(t, "line 2: he^llo", got)
	})

	t.Run("MultiLine_PointsToCorrectLine", func(t *testing.T) {
		text := "line1\nline2\nline3"
		pos := len("line1\n") + 2 // inside "line2", before 'n'
		got := ShowErrorLine(text, pos)
		require.Equal(t, "line 2: li^ne2", got)
	})

	t.Run("AtNewline", func(t *testing.T) {
		// position exactly on the trailing newline of the first line
		text := "ab\ncd"
		got := ShowErrorLine(text, 2)
		require.NotEmpty(t, got)
	})
}

func TestFormatErrorWithTextPosition(t *testing.T) {
	t.Run("WithProperty", func(t *testing.T) {
		err := errorx.IllegalArgument.New("boom").
			WithProperty(errRawTextPositionProperty, 2)
		got := FormatErrorWithTextPosition(err, "hello")
		require.Contains(t, got, "boom")
		require.Contains(t, got, "^llo")
	})

	t.Run("WithoutProperty", func(t *testing.T) {
		err := errors.New("plain error")
		got := FormatErrorWithTextPosition(err, "hello")
		require.Equal(t, "plain error", got)
	})

	t.Run("PropertyOutOfRange", func(t *testing.T) {
		// Position past end of text: ShowErrorLine returns "" and we still
		// get the underlying error message.
		err := errorx.IllegalArgument.New("boom").
			WithProperty(errRawTextPositionProperty, 999)
		got := FormatErrorWithTextPosition(err, "hello")
		require.Contains(t, got, "boom")
	})
}
