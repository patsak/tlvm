package tlvm

import (
	"fmt"
	"strings"
)

type SExpressions []any

func (e SExpressions) headLiteralValue() string {
	return e[0].(literal).value
}

func (e SExpressions) head() any {
	return e[0]
}

func (e SExpressions) tail() SExpressions {
	return e[1:]
}

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

type boolean struct {
	value bool
	pos   int
}

func (l boolean) String() string {
	return fmt.Sprintf("%t", l.value)
}

func (l boolean) valueAndPosition() (any, int) {
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

type btUint [2]byte

type ptr int

func offsetAddress(offset int) ptr {
	if offset < 0 {
		return ptr(int(addrShiftLeftFlag) | -offset)
	} else {
		return ptr(int(addrShiftRightFlag) | offset)
	}
}

func (p ptr) format(basePointerName string) string {
	if addrShiftLeftFlag&p > 0 {
		return fmt.Sprintf("%s-%d", basePointerName, p&^addrShiftLeftFlag)
	} else if addrShiftRightFlag&p > 0 {
		return fmt.Sprintf("%s+%d", basePointerName, p&^addrShiftRightFlag)
	} else {
		return fmt.Sprintf("%d", p)
	}
}

func (p ptr) abs(base int) ptr {
	if addrShiftLeftFlag&p > 0 {
		return ptr(base) - p&^addrShiftLeftFlag
	} else if addrShiftRightFlag&p > 0 {
		return ptr(base) + p&^addrShiftRightFlag
	} else {
		return p
	}
}

type closure struct {
	name      string            // function name
	addr      ptr               // function code pointer
	constAddr ptr               // static function code pointer
	nargs     int               // number of function arguments
	varargs   bool              // variable arguments exists
	values    []closureVariable // bound variables from external scope
}

type closureVariable struct {
	value *any
	vt    valType
	addr  ptr
}
