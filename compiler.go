package tlvm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/joomcode/errorx"
)

type VMByteCode struct {
	consts               map[any]int // pointers to constants
	constList            []any
	labels               map[string]int // pointers to labels in code
	scope                *scope         // current variables scope
	macrosByName         map[string]macros
	externalFunctions    map[string]reflect.Value
	autoIncrementLabelID int

	debugInfo           map[int]string
	origPositionPointer map[int]int

	definedFunctions []byte
	code             []byte // result code
}

type scope struct {
	parentScope  *scope
	offset       int
	varAddresses map[any]int
}

var emptyAddr = []byte{0, 0}

type macros struct {
	args []string
	code []byte
}

type CompileOption func(bt *VMByteCode)

func ExtFunctionsOrPanic(funcs map[string]any) CompileOption {
	o, err := ExtFunctions(funcs)
	if err != nil {
		errorx.Panic(err)
	}
	return o
}

func ExtFunctions(funcs map[string]any) (CompileOption, error) {
	res := map[string]reflect.Value{}
	for k, v := range funcs {
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Func {
			return nil, errors.New(fmt.Sprintf("value for key %s must be function ", k))
		}
		if rv.Type().NumOut() != 1 {
			return nil, errors.New(fmt.Sprintf("function must return single value"))
		}

		res[k] = rv
	}
	return func(bt *VMByteCode) {
		bt.externalFunctions = res
	}, nil
}

func EnvVariables(env ...string) CompileOption {
	return func(bt *VMByteCode) {
		for _, k := range env {
			bt.constAddr(k)
		}
	}
}

func Compile(text string, options ...CompileOption) (_ *VMByteCode, err error) {
	expressions, err := Read(text)
	if err != nil {
		return nil, err
	}

	vmByteCode := VMByteCode{
		consts:    map[any]int{},
		labels:    map[string]int{},
		debugInfo: map[int]string{},
		scope: &scope{
			varAddresses: map[any]int{},
		},
		origPositionPointer: map[int]int{},
		macrosByName:        map[string]macros{},
	}
	for _, opt := range options {
		opt(&vmByteCode)
	}

	defer func() {
		errRec := recover()
		if errRec == nil {
			return
		}
		var ok bool
		err, ok = errRec.(error)
		if !ok {
			err = errorx.IllegalArgument.New("%v", errRec)
			return
		}
		err = wrapCompilationError(err, text)

	}()

	for _, e := range expressions {
		emit(e, &vmByteCode)
	}

	return &vmByteCode, nil
}

func (c *VMByteCode) constAddr(v any) []byte {
	_, ok := c.consts[v]
	if ok {
		return c.addr(c.consts[v])
	}
	c.consts[v] = len(c.consts)
	c.constList = append(c.constList, v)
	return c.addr(c.consts[v])
}

func (c *VMByteCode) findConstAddr(v any) ([]byte, bool) {
	_, ok := c.consts[v]
	if ok {
		return c.addr(c.consts[v]), true
	}
	return nil, false
}

func (c *VMByteCode) stackAddr(v any) []byte {
	res, ok := c.tryGetStackAddr(v)
	if ok {
		return res
	}
	return c.createNextStackAddr(v)
}

func (c *VMByteCode) createNextStackAddr(v any) []byte {
	c.scope.varAddresses[v] = addrShiftRightFlag | c.scope.offset
	c.scope.offset++
	return c.addr(c.scope.varAddresses[v])
}

func (c *VMByteCode) tryGetStackAddr(v any) ([]byte, bool) {
	laddr, ok := c.findStackAddress(v)
	if ok {
		return laddr, true
	}
	return nil, false

}

func (c *VMByteCode) storeStackAddr(v any, pos int) []byte {
	c.scope.varAddresses[v] = pos
	return c.addr(pos)
}

func (c *VMByteCode) findStackAddress(v any) ([]byte, bool) {
	var findAddr func(*scope) ([]byte, bool)
	findAddr = func(s *scope) ([]byte, bool) {
		_, ok := s.varAddresses[v]
		if ok {
			return c.addr(s.varAddresses[v]), true
		}
		if s.parentScope != nil {
			return findAddr(s.parentScope)
		}

		return nil, false
	}
	return findAddr(c.scope)
}

func (c *VMByteCode) storeLabelAddress(cv string, pos int) []byte {
	_, ok := c.labels[cv]
	if ok {
		return c.addr(c.labels[cv])

	}
	c.labels[cv] = pos
	return c.addr(pos)
}

func (c *VMByteCode) findLabelAddress(cv string) ([]byte, bool) {
	v, ok := c.labels[cv]
	if !ok {
		return nil, false
	}
	return c.addr(v), true
}

func (c *VMByteCode) addr(addr int) []byte {
	res := addr
	var out [2]byte
	binary.BigEndian.PutUint16(out[:], uint16(res))
	return out[:]
}

func (c *VMByteCode) origPos(pos int) {
	c.origPositionPointer[len(c.code)-1] = pos
}

func (c *VMByteCode) b(ops ...byte) *VMByteCode {
	c.code = append(c.code, ops...)
	return c
}

func (c *VMByteCode) newLabelID() string {
	c.autoIncrementLabelID++
	return "__" + strconv.Itoa(c.autoIncrementLabelID)
}

func (c *VMByteCode) writeOpCode(op opCode) *VMByteCode {
	return c.b(byte(op))
}

func (c *VMByteCode) writeAddr(addr int) *VMByteCode {
	return c.b(c.addr(addr)...)
}

func (c *VMByteCode) writeConstAddr(v any) *VMByteCode {
	return c.b(c.constAddr(v)...)
}

func (c *VMByteCode) writeEmptyAddress() *VMByteCode {
	return c.b(emptyAddr...)
}

func (c *VMByteCode) iptr(p *int) *VMByteCode {
	*p = c.pos()
	return c
}

func (c *VMByteCode) pos() int {
	return len(c.code)
}

func (c *VMByteCode) modify(i int, o []byte) {
	for j := range o {
		c.code[i+j] = o[j]
	}
}

func (c *VMByteCode) inNewScope(f func()) {
	c.scope = &scope{
		parentScope:  c.scope,
		offset:       c.scope.offset,
		varAddresses: map[any]int{},
	}
	defer func() {
		c.scope = c.scope.parentScope
	}()
	f()
}

func (c *VMByteCode) debug(msg string, args ...any) {
	c.debugInfo[len(c.definedFunctions)+c.pos()] = fmt.Sprintf(msg, args...)
}

func emit(node any, cur *VMByteCode) {
	switch v := node.(type) {
	case *cons:
		cur.origPos(v.pos)
		switch first := v.first.(type) {
		case literal:
			switch first.value {
			case "and":
				emitAnd(v, cur)
			case "or":
				emitOr(v, cur)
			case "not":
				emitNot(v, cur)
			case "if":
				emitIf(v, cur)
			case "+":
				emitMultiOp(v, cur, opAdd)
			case "-":
				emitBinaryOp(v, cur, opSub)
			case "/":
				emitBinaryOp(v, cur, opDiv)
			case "*":
				emitBinaryOp(v, cur, opMul)
			case "eq":
				emitBinaryOp(v, cur, opCmp, cmpFlagEq)
			case "lt":
				emitBinaryOp(v, cur, opCmp, cmpFlagLt)
			case "gt":
				emitBinaryOp(v, cur, opCmp, cmpFlagGt)
			case "setq":
				emitSetq(v, cur)
			case "list":
				emitList(v, cur)
			case "dolist":
				emitDoList(v, cur)
			case "defun":
				emitDefineFunction(v, cur)
			case "defmacro":
				emitDefineMacros(v, cur)
			case "macroexpand":
				emitMacroExpand(v, cur)
			case "backtick":
				emitBacktick(v.second, cur)
			case "quote":
				emitQuote(v.second, cur)
			case "lambda":
				emitLambda(v, cur)
			case "progn":
				emitProgn(v, cur)
			case "print":
				emitPrint(v, cur)
			default:
				l := consToList(v)
				if _, ok := cur.macrosByName[l[0].(literal).value]; ok {
					emitMacroCall(v, cur)
				} else {
					emitCallFunction(v, cur)
				}
			}
		}
	case int64, float64, string, str, float, number:
		vt := v
		var ok bool
		valueWithPos, ok := v.(valueAndPosition)
		if ok {
			var pos int
			vt, pos = valueWithPos.valueAndPosition()
			cur.origPos(pos)
		}
		cur.writeOpCode(opPush).writeConstAddr(vt)
	case literal:
		parts := variableParts(v.value)
		addr, hasAddr := cur.findStackAddress(parts[0])
		if !hasAddr {
			addr, hasAddr = cur.findConstAddr(parts[0])
			if !hasAddr {
				panic(errorx.IllegalFormat.New("variable '%s' is not defined", v.value).WithProperty(errRawTextPositionProperty, v.pos))
			}
		}
		if len(parts) == 1 {
			cur.writeOpCode(opPush).b(addr...)
		} else {
			cur.writeOpCode(opPushField).b(addr...).writeConstAddr(strings.Join(parts[1:], "."))

		}

	default:
		panic(errorx.IllegalFormat.New("unexpected value %v with type %t", v, v))
	}
}

func variableParts(s string) []string {
	return strings.Split(s, ".")
}

func emitAnd(cc *cons, cur *VMByteCode) {
	l := consToList(cc)
	andExpressions := l[1:]
	var indexes []int
	for _, o := range andExpressions {
		emit(o, cur)
		var i int
		cur.writeOpCode(opBr).iptr(&i).b(emptyAddr...)

		indexes = append(indexes, i)
	}
	var jmpAddr, falsePos int
	cur.writeOpCode(opPush).writeConstAddr(boolTrue).
		writeOpCode(opJmp).iptr(&jmpAddr).writeEmptyAddress().
		iptr(&falsePos).writeOpCode(opPush).writeConstAddr(boolFalse)

	cur.modify(jmpAddr, cur.addr(cur.pos()))

	for _, i := range indexes {
		cur.modify(i, cur.addr(falsePos))
	}
}

func emitNot(cc *cons, cur *VMByteCode) {
	l := consToList(cc)
	emit(l[1], cur)
	cur.writeOpCode(opNot)
}

func emitOr(cc *cons, cur *VMByteCode) {
	l := consToList(cc)

	boolExpressions := l[1:]
	var indexes []int
	for _, e := range boolExpressions {
		emit(e, cur)
		var i, next int
		cur.writeOpCode(opBr).iptr(&next).writeEmptyAddress()
		cur.writeOpCode(opJmp).iptr(&i).writeEmptyAddress()
		cur.modify(next, cur.addr(cur.pos()))
		indexes = append(indexes, i)
	}

	var truePos, lastJump int
	cur.writeOpCode(opPush).writeConstAddr(boolFalse)
	cur.writeOpCode(opJmp).iptr(&lastJump).writeEmptyAddress()
	cur.iptr(&truePos).writeOpCode(opPush).writeConstAddr(boolTrue)

	cur.modify(lastJump, cur.addr(cur.pos()))
	for _, i := range indexes {
		cur.modify(i, cur.addr(truePos))
	}
}

func emitBinaryOp(cc *cons, cur *VMByteCode, op opCode, bts ...byte) {
	l := consToList(cc)

	emit(l[1], cur)
	emit(l[2], cur)

	cur.writeOpCode(op).b(bts...)
}

func emitMultiOp(cc *cons, cur *VMByteCode, op opCode) {
	l := consToList(cc)

	emit(l[1], cur)
	for _, e := range l[2:] {
		emit(e, cur)

		cur.writeOpCode(op)
	}
}

func (cc *cons) emitSum(cur *VMByteCode) {
	l := consToList(cc)
	emit(l[0], cur)

	for _, o := range l[1:] {
		emit(o, cur)
		cur.writeOpCode(opAdd)
	}
}

func emitDefineFunction(cc *cons, cur *VMByteCode) {
	l := consToList(cc)

	code := cur.code
	cur.code = cur.definedFunctions

	defer func() {
		cur.definedFunctions = cur.code
		cur.code = code
	}()

	emitFunction(l[1].(literal).value, l[2:], cur)
}

func emitFunction(name string, expr SExpressions, cur *VMByteCode) {
	cur.inNewScope(func() {
		cur.scope.offset = 4 // skip stack entries stored by CALL opcode

		cur.debug("define function %s", name)
		cur.storeLabelAddress(name, cur.pos())

		args := consToList(expr[0].(*cons))
		for i, a := range args {
			cur.storeStackAddr(a.(literal).value, addrShiftLeftFlag|i) // grow to stack bottom from base pointer
		}

		for _, e := range expr[1:] {
			emit(e, cur)
		}

		cur.debug("end define function %s", name)
		cur.writeOpCode(opRet)
	})
}

func emitCallFunction(cc *cons, cur *VMByteCode) {
	args := consToList(cc)
	for i := len(args) - 1; i >= 1; i-- {
		emit(args[i], cur)
	}
	nargs := len(args[1:])

	extFunc, ok := cur.externalFunctions[args[0].(literal).value]
	if ok {

		cur.writeOpCode(opPush).b(cur.constAddr(extFunc)...)
		cur.debug("call external function %s", args[0].(literal).value)
		cur.writeOpCode(opExtCall).b(cur.addr(nargs)...)
		return
	}

	functionName := args[0].(literal).value
	fVariable, hasVariable := cur.tryGetStackAddr(functionName)
	if hasVariable {
		cur.debug("call dynamic function %s", functionName)
		cur.writeOpCode(opDynamicCall).b(fVariable...).b(cur.addr(nargs)...)
		return
	}
	fAddress, hasLabel := cur.findLabelAddress(functionName)
	if hasLabel {
		cur.writeOpCode(opCall).b(fAddress...).b(cur.addr(nargs)...)
		cur.debug("call function %s", functionName)
		return
	}

	panic(errorx.IllegalArgument.New("unknown function name %s", args[0].(literal).value).WithProperty(errRawTextPositionProperty, cc.pos))
}

func emitIf(cc *cons, cur *VMByteCode) {
	l := consToList(cc)
	condition := l[1]
	thenBlock := l[2]

	emit(condition, cur)

	var elseStart, elseEnd int

	cur.writeOpCode(opBr).iptr(&elseStart).writeEmptyAddress()

	emit(thenBlock, cur)
	if len(l) > 3 { // with else
		cur.writeOpCode(opJmp).iptr(&elseEnd).writeEmptyAddress()
	}
	cur.modify(elseStart, cur.addr(cur.pos()))
	if len(l) > 3 { // with else
		elseBlock := l[3]
		emit(elseBlock, cur)
		cur.modify(elseEnd, cur.addr(cur.pos()))
	}
}

func emitSetq(cc *cons, cur *VMByteCode) {
	l := consToList(cc)
	variableName := l[1].(literal).value
	rightValue := l[2]
	emit(rightValue, cur)

	addr := cur.stackAddr(variableName)

	cur.writeOpCode(opStore).b(addr...)
	cur.writeOpCode(opPush).b(addr...)
}

func emitList(cc *cons, cur *VMByteCode) {
	l := consToList(cc)
	cur.writeOpCode(opPush).b(cur.constAddr(nil)...)

	for i := len(l) - 1; i >= 1; i-- {
		emit(l[i], cur)
		cur.writeOpCode(opCons)
	}
}

func emitDoList(cc *cons, cur *VMByteCode) {
	l := consToList(cc)
	loopParams := consToList(l[1].(*cons))
	loopVar := loopParams[0]
	inputList := loopParams[1]
	body := l[2:]

	cur.inNewScope(func() {
		emit(inputList, cur)

		addr := cur.createNextStackAddr(loopVar.(literal).value)

		var begin int
		cur.iptr(&begin)
		cur.writeOpCode(opPush).b(addr...)
		cur.writeOpCode(opNil)
		cur.writeOpCode(opNot)
		var end int
		cur.writeOpCode(opBr).iptr(&end).b(emptyAddr...)
		cur.writeOpCode(opPush).b(addr...)
		cur.writeOpCode(opPush).b(addr...)
		cur.writeOpCode(opCar)
		cur.writeOpCode(opStore).b(addr...)

		for _, e := range body {
			emit(e, cur)
		}
		cur.writeOpCode(opPop)
		cur.writeOpCode(opCdr)
		cur.writeOpCode(opStore).b(addr...)
		cur.writeOpCode(opJmp).writeAddr(begin)
		cur.modify(end, cur.addr(cur.pos()))
	})
}

func emitMacroCall(c *cons, cur *VMByteCode) {
	emit(expandMacros(c, cur), cur)
}

func emitMacroExpand(c *cons, cur *VMByteCode) {
	l := consToList(c)
	emit(l[1], cur)

	vmToProduceArgument := NewVM(cur)
	if err := vmToProduceArgument.Execute(); err != nil {
		errorx.Panic(err)
	}

	cur.writeOpCode(opPush).writeConstAddr(expandMacros(vmToProduceArgument.Result().(*cons), cur))
}

func expandMacros(expr *cons, cur *VMByteCode) any {
	l := consToList(expr)
	vm := NewVM(cur)

	name := l[0].(literal).value
	args := l[1:]
	macros := cur.macrosByName[name]
	vm.ip = len(vm.code)
	vm.code = append(vm.code, macros.code...)
	for i := range args { // push arguments in reverse order for stack
		vm.push(args[len(args)-i-1])
	}
	vm.bp = vm.sp // prepare base pointer
	if err := vm.Execute(); err != nil {
		errorx.Panic(err)
	}

	return vm.Result()
}

func emitDefineMacros(cc *cons, cur *VMByteCode) {
	l := consToList(cc)

	m := macros{}

	lastInstruction := len(cur.code)
	name := l[1].(literal).value
	args := consToList(l[2].(*cons))
	body := l[3:]

	stashedFrame := *cur.scope
	code := cur.code
	defer func() {
		cur.code = code
		cur.scope = &stashedFrame
	}()
	for i, a := range args {
		cur.storeStackAddr(a.(literal).value, addrShiftLeftFlag|i)
		m.args = append(m.args, a.(literal).value)
	}
	for _, b := range body {
		emit(b, cur)
	}

	m.code = make([]byte, len(cur.code[lastInstruction:]))
	copy(m.code, cur.code[lastInstruction:])
	cur.macrosByName[name] = m
}

func emitBacktick(v any, cur *VMByteCode) {
	switch vv := v.(type) {
	case *cons:
		if fl, ok := vv.first.(literal); ok && fl.value == "comma" {
			emit(vv.second, cur)
		} else if fl, ok := vv.first.(literal); ok && fl.value == "splice" {
			l := consToList(vv.second.(*cons))[1:]

			for i := len(l) - 1; i >= 0; i-- {
				emitBacktick(l[i], cur)
				if i > 0 {
					cur.writeOpCode(opCons)
				}
			}
		} else {
			emitBacktick(vv.second, cur)
			emitBacktick(vv.first, cur)
			cur.writeOpCode(opCons)
		}
	default:
		cur.writeOpCode(opPush).writeConstAddr(vv)
	}
}

func emitQuote(v any, cur *VMByteCode) {
	cur.writeOpCode(opPush).writeConstAddr(v)
}

func emitLambda(v *cons, cur *VMByteCode) {
	labelID := cur.newLabelID()

	l := consToList(v)
	var endLambdaAddress int
	var startDefinitionAddress int

	cur.writeOpCode(opJmp).iptr(&endLambdaAddress).writeEmptyAddress().iptr(&startDefinitionAddress)

	emitFunction(labelID, l[1:], cur)

	funcDefinitionLength := cur.pos() - startDefinitionAddress
	cur.modify(endLambdaAddress, cur.addr(addrShiftRightFlag|funcDefinitionLength))
	cur.debug("label %s", labelID)
	cur.writeOpCode(opPushAddr).writeAddr(addrShiftLeftFlag | funcDefinitionLength + 3 /*opcode + address */)
}

func emitProgn(v *cons, cur *VMByteCode) {
	l := consToList(v)
	for _, e := range l[1:] {
		emit(e, cur)
	}
}

func emitPrint(v *cons, cur *VMByteCode) {
	args := consToList(v)
	emit(args[1], cur)
	cur.writeOpCode(opPrint)
}

func consToList(c *cons) SExpressions {
	var res []any

	cur := c
	for {
		if c.first == nil {
			return res
		}
		res = append(res, c.first)

		switch v := c.second.(type) {
		case *cons:
			c = v
		case nil:
			return res
		default:
			res = append(res, cur.second)
			return res
		}
	}
}

func wrapCompilationError(err error, rawText string) error {
	pos, ok := errorx.ExtractProperty(err, errRawTextPositionProperty)
	if !ok {
		return err
	}
	posInt := pos.(int)
	if posInt >= len(rawText) {
		return err
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

	return errorx.Decorate(err, "Code: %s", rawText[lineStart:posInt]+"^"+rawText[posInt:lineEnd])
}
