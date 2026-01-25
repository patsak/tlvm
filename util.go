package tlvm

import (
	"fmt"
	"strings"

	"github.com/joomcode/errorx"
)

type LazyString func() string

func (ls LazyString) String() string {
	return ls()
}

func FormatErrorWithTextPosition(err error, rawText string) string {
	pos, ok := errorx.ExtractProperty(err, errRawTextPositionProperty)
	if !ok {
		return err.Error()
	}

	return fmt.Sprintf("%s\n%s", err, ShowErrorLine(rawText, pos.(int)))
}

func ShowErrorLine(rawText string, posInt int) string {
	if posInt >= len(rawText) {
		return ""
	}
	lineStart := strings.LastIndexByte(rawText[:posInt], '\n')
	if lineStart == -1 {
		lineStart = 0
	}

	lineEnd := strings.IndexByte(rawText[posInt:], '\n')
	if lineEnd == -1 {
		lineEnd = len(rawText)
	} else {
		lineEnd = posInt + lineEnd
	}

	lineNumber := strings.Count(rawText[:lineStart], "\n") + 2

	return fmt.Sprintf("line %d: %s",
		lineNumber,
		strings.TrimSpace(rawText[lineStart:posInt]+"^"+rawText[posInt:lineEnd]))
}
