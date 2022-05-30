package tlvm

import (
	"fmt"
	"strings"
)

type SExpressions []any

type valueAndPosition interface {
	valueAndPosition() (any, int)
}

type literal struct {
	value string
	pos   int
}

func (l literal) String() string {
	return l.value
}

func (l literal) valueAndPosition() (any, int) {
	return l.value, l.pos
}

type str struct {
	value string
	pos   int
}

func (l str) valueAndPosition() (any, int) {
	return l.value, l.pos
}

type number struct {
	value int64
	pos   int
}

func (l number) String() string {
	return fmt.Sprintf("%d", l.value)
}

func (l number) valueAndPosition() (any, int) {
	return l.value, l.pos
}

type float struct {
	value float64
	pos   int
}

func (l float) String() string {
	return fmt.Sprintf("%f", l.value)
}

func (l float) valueAndPosition() (any, int) {
	return l.value, l.pos
}

type cons struct {
	second any
	first  any
	pos    int
}

func (l *cons) valueAndPosition() (any, int) {
	return l, l.pos
}

func (c *cons) String() string {
	l := consToList(c)
	var res []string
	for _, cc := range l {
		res = append(res, fmt.Sprintf("%v", cc))
	}
	return "(" + strings.Join(res, " ") + ")"
}
