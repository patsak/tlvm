package tlvm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/joomcode/errorx"
)

type VMByteCode struct {
	consts               map[any]ptr // pointers to constants
	constList            []any
	labels               map[string]*closure // pointers to labels in code
	scope                *scope              // current variables scope
	macrosByName         map[string]macros
	externalFunctions    map[string]reflect.Value
	autoIncrementLabelID int

	debugInfo           map[int]string
	origPositionPointer map[int]int

	definedFunctions []byte
	code             []byte // result code
}

type scopeType int

const (
	scopeTypeLexical scopeType = iota
	scopeTypeStackFrame
)

type scope struct {
	parentScope  *scope
	offset       int
	closure      *scope
	scopeType    scopeType
	varAddresses map[any]ptr
}

var emptyAddr = []byte{0, 0}

type macros struct {
	code  []byte
	rest  bool
	nargs int
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

	expressions, err := Read(text)
	if err != nil {
		return nil, wrapCompilationError(err, text)
	}

	vmByteCode := VMByteCode{
		consts:    map[any]ptr{},
		labels:    map[string]*closure{},
		debugInfo: map[int]string{},
		scope: &scope{
			varAddresses: map[any]ptr{},
			closure: &scope{
				varAddresses: map[any]ptr{},
			},
		},
		origPositionPointer: map[int]int{},
		macrosByName:        map[string]macros{},
	}
	for _, opt := range options {
		opt(&vmByteCode)
	}

	for _, e := range expressions {
		emit(e, &vmByteCode)
	}

	return &vmByteCode, nil
}

func (c *VMByteCode) constAddr(v any) btUint {
	_, ok := c.consts[v]
	if ok {
		return makeByteUint(c.consts[v])
	}
	c.consts[v] = ptr(len(c.consts))
	c.constList = append(c.constList, v)
	return makeByteUint(c.consts[v])
}

func (c *VMByteCode) findConstAddr(v any) (btUint, bool) {
	_, ok := c.consts[v]
	if ok {
		return makeByteUint(c.consts[v]), true
	}
	return btUint{}, false
}

func (c *scope) createNextAddr(v any) btUint {
	c.varAddresses[v] = offsetAddress(c.offset)
	c.offset++
	return makeByteUint(c.varAddresses[v])
}

func (c *scope) storeAddr(v any, pos ptr) btUint {
	c.varAddresses[v] = pos
	return makeByteUint(pos)
}

func (c *scope) findLocalAddress(v any) (btUint, valType, bool) {
	if c == nil {
		return btUint{}, 0, false
	}
	localAddr, ok := c.varAddresses[v]
	if ok {
		return makeByteUint(localAddr), valTypeLocal, true
	}
	closureAddr, ok := c.closure.varAddresses[v]
	if ok {
		return makeByteUint(closureAddr), valTypeClosure, true
	}

	return btUint{}, 0, false
}

func (c *scope) closureAddress(v any) (btUint, bool) {
	a, ok := c.closure.varAddresses[v]
	if ok {
		return makeByteUint(a), ok
	}
	return btUint{}, false
}

type valType int

const (
	valTypeConst valType = iota
	valTypeClosure
	valTypeLocal
)

func (c *scope) resolveAddress(v any) (btUint, valType, bool) {
	localAddr, ok := c.varAddresses[v]
	if ok {
		return makeByteUint(localAddr), valTypeLocal, true
	}
	closureAddr, ok := c.closure.varAddresses[v]
	if ok {
		return makeByteUint(closureAddr), valTypeClosure, true
	}

	var findAddr func(*scope) (btUint, valType, bool)
	findAddr = func(s *scope) (btUint, valType, bool) {
		if s == nil {
			return btUint{}, 0, false
		}
		_, ok := s.varAddresses[v]
		if ok {
			if s.parentScope != nil && c.scopeType == scopeTypeStackFrame {
				return c.closure.createNextAddr(v), valTypeClosure, true
			} else {
				return makeByteUint(s.varAddresses[v]), valTypeLocal, true
			}
		}
		if s.parentScope != nil {
			res, vt, ok := findAddr(s.parentScope)

			if ok && vt == valTypeClosure && s.scopeType == scopeTypeStackFrame { // close variable in parent stack frames
				if _, _, ok := s.findLocalAddress(v); !ok {
					s.closure.createNextAddr(v)
				}
			}
			return res, vt, ok
		}

		return btUint{}, 0, false
	}
	return findAddr(c.parentScope)
}

func (c *VMByteCode) storeFunction(cv string, cl *closure) {
	c.labels[cv] = cl

}

func (c *VMByteCode) findFunction(cv string) (*closure, bool) {
	v, ok := c.labels[cv]
	if !ok {
		return nil, false
	}
	return v, true
}

func makeByteUint(addr ptr) btUint {
	res := addr
	var out [2]byte
	binary.BigEndian.PutUint16(out[:], uint16(res))
	return out
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

func (c *VMByteCode) writePointer(a ptr) *VMByteCode {
	return c.writeAddress(makeByteUint(a))
}

func (c *VMByteCode) writeAddress(a btUint) *VMByteCode {
	bts := a[:]
	return c.b(bts...)
}

func (c *VMByteCode) writeInt(a int) *VMByteCode {
	return c.writePointer(ptr(a))
}
func (c *VMByteCode) writeBool(b bool) *VMByteCode {
	v := 0
	if b {
		v = 1
	}
	return c.b(byte(v))
}

func (c *VMByteCode) writeConstAddr(v any) *VMByteCode {
	return c.writeAddress(c.constAddr(v))
}

func (c *VMByteCode) writeEmptyAddress() *VMByteCode {
	return c.b(emptyAddr...)
}

func (c *VMByteCode) iptr(p *ptr) *VMByteCode {
	*p = c.pos()
	return c
}

func (c *VMByteCode) pos() ptr {
	return ptr(len(c.code))
}

func (c *VMByteCode) modify(i ptr, o btUint) {
	for j := range o {
		c.code[int(i+ptr(j))] = o[j]
	}
}

func (c *VMByteCode) inNewScope(typ scopeType, f func()) {
	c.scope = &scope{
		parentScope:  c.scope,
		offset:       c.scope.offset,
		varAddresses: map[any]ptr{},
		closure: &scope{
			varAddresses: map[any]ptr{},
		},
		scopeType: typ,
	}
	defer func() {
		c.scope = c.scope.parentScope
	}()
	f()
}

func (c *VMByteCode) debug(msg string, args ...any) {
	c.debugInfo[len(c.definedFunctions)+int(c.pos())] = fmt.Sprintf(msg, args...)
}

func emit(node any, cur *VMByteCode) {
	switch v := node.(type) {
	case *cons:
		cur.origPos(v.pos)
		switch first := v.first.(type) {
		case *cons:
			args := consToList(v.second.(*cons))
			for _, a := range args {
				emit(a, cur)
			}

			emit(v.first, cur)
			cur.writeOpCode(opPopCall).writeInt(len(args))
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
				emitList(consToList(v), cur)
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
			case "while":
				emitWhile(v, cur)
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
		emitLiteral(v, cur)
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
	var indexes []ptr
	for _, o := range andExpressions {
		emit(o, cur)
		var i ptr
		cur.writeOpCode(opBr).iptr(&i).b(emptyAddr...)

		indexes = append(indexes, i)
	}
	var jmpAddr, falsePos ptr
	cur.writeOpCode(opPush).writeConstAddr(boolTrue).
		writeOpCode(opJmp).iptr(&jmpAddr).writeEmptyAddress().
		iptr(&falsePos).writeOpCode(opPush).writeConstAddr(boolFalse)

	cur.modify(jmpAddr, makeByteUint(cur.pos()))

	for _, i := range indexes {
		cur.modify(i, makeByteUint(falsePos))
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
	var indexes []ptr
	for _, e := range boolExpressions {
		emit(e, cur)
		var i, next ptr
		cur.writeOpCode(opBr).iptr(&next).writeEmptyAddress()
		cur.writeOpCode(opJmp).iptr(&i).writeEmptyAddress()
		cur.modify(next, makeByteUint(cur.pos()))
		indexes = append(indexes, i)
	}

	var truePos, lastJump ptr
	cur.writeOpCode(opPush).writeConstAddr(boolFalse)
	cur.writeOpCode(opJmp).iptr(&lastJump).writeEmptyAddress()
	cur.iptr(&truePos).writeOpCode(opPush).writeConstAddr(boolTrue)

	cur.modify(lastJump, makeByteUint(cur.pos()))
	for _, i := range indexes {
		cur.modify(i, makeByteUint(truePos))
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
	cur.inNewScope(scopeTypeStackFrame, func() {
		emitFunction(l[1].(literal).value, l[2:], cur)
	})
}

func emitFunction(name string, expr SExpressions, cur *VMByteCode) closure {
	cur.scope.offset = callFrameOffset + 1 // skip stack entries stored by CALL opcode

	cur.debug("define function %s", name)
	fn := closure{
		name: name,
		addr: cur.pos(),
	}
	cur.storeFunction(name, &fn)

	args := consToList(expr[0].(*cons))
	var restArg bool

	var actualArgs SExpressions
	for i, a := range args {
		v := a.(literal).value
		if v == "&rest" {
			restArg = true
			actualArgs = append(actualArgs, args[i+1])
			break
		}
		actualArgs = append(actualArgs, args[i])
	}

	for i, a := range actualArgs {
		v := a.(literal).value
		cur.scope.storeAddr(v, offsetAddress(-(len(actualArgs) - i - 1))) // grow to stack bottom from base pointer
	}

	fn.rest = restArg
	fn.nargs = len(actualArgs)

	for _, e := range expr[1:] {
		emit(e, cur)
	}

	emitReturn(fn, cur)

	return fn
}

func emitReturn(cl closure, cur *VMByteCode) {
	labelAddr := makeByteUint(cl.addr)

	const opCallOffset = 5
	isRecursiveCall := opCode(cur.code[cur.pos()-opCallOffset]) == opCall &&
		cur.code[cur.pos()-opCallOffset+1] == labelAddr[0] &&
		cur.code[cur.pos()-opCallOffset+2] == labelAddr[1]

	if !isRecursiveCall {
		cur.debug("end define function %s", cl.name)
		cur.writeOpCode(opRet)
		return
	}

	cur.code = cur.code[:len(cur.code)-opCallOffset]

	var retIndex, jumpPrepareIndex ptr

	cur.writeOpCode(opJmp).iptr(&jumpPrepareIndex).writeEmptyAddress()

	for i := 0; i < opCallOffset-3; i++ { // padding to preserve code length
		cur.writeOpCode(opNoOp)
	}
	cur.writeOpCode(opJmp).iptr(&retIndex).writeEmptyAddress()

	cur.modify(jumpPrepareIndex, makeByteUint(cur.pos()))
	for i := 0; i < cl.nargs; i++ {
		cur.writeOpCode(opStore).writePointer(offsetAddress(-i))
	}
	cur.writeOpCode(opJmp).writeAddress(labelAddr)
	cur.debug("end define function %s", cl.name)
	cur.writeOpCode(opRet)
	cur.modify(retIndex, makeByteUint(cur.pos()-1))
}

func emitCallFunction(cc *cons, cur *VMByteCode) {
	args := consToList(cc)

	if extFunc, ok := cur.externalFunctions[args[0].(literal).value]; ok {
		nargs := emitArgs(args[1:], cur)
		cur.writeOpCode(opPush).writeAddress(cur.constAddr(extFunc))
		cur.debug("call external function %s", args[0].(literal).value)
		cur.writeOpCode(opExtCall).writeInt(nargs)
		return
	}

	functionName := args[0].(literal).value
	if fVariable, _, hasVariable := cur.scope.resolveAddress(functionName); hasVariable {
		nargs := emitArgs(args[1:], cur)
		cur.debug("call closure %s", functionName)
		cur.writeOpCode(opClosureCall).writeAddress(fVariable).writeInt(nargs)
		return
	}

	if fAddress, hasLabel := cur.findFunction(functionName); hasLabel {
		fargs := consToListN(cc, fAddress.nargs+1)
		emitArgs(fargs[1:], cur)
		cur.debug("call function %s", functionName)
		cur.writeOpCode(opCall).writePointer(fAddress.addr).writeInt(fAddress.nargs)
		return
	}

	panic(errorx.IllegalArgument.New("unknown function name %s", args[0].(literal).value).WithProperty(errRawTextPositionProperty, cc.pos))
}

func emitArgs(args SExpressions, cur *VMByteCode) int {
	for i := range args {
		emit(args[i], cur)
	}

	return len(args)
}

func emitIf(cc *cons, cur *VMByteCode) {
	l := consToList(cc)
	condition := l[1]
	thenBlock := l[2]

	emit(condition, cur)

	var elseStart, elseEnd ptr

	cur.writeOpCode(opBr).iptr(&elseStart).writeEmptyAddress()

	emit(thenBlock, cur)
	if len(l) > 3 { // with else
		cur.writeOpCode(opJmp).iptr(&elseEnd).writeEmptyAddress()
	}
	cur.modify(elseStart, makeByteUint(cur.pos()))
	if len(l) > 3 { // with else
		elseBlock := l[3]
		emit(elseBlock, cur)
		cur.modify(elseEnd, makeByteUint(cur.pos()))
	}
}

func emitSetq(cc *cons, cur *VMByteCode) {
	l := consToList(cc)
	variableName := l[1].(literal).value
	rightValue := l[2]
	emit(rightValue, cur)

	addr, vt, ok := cur.scope.resolveAddress(variableName)
	if !ok {
		addr = cur.scope.createNextAddr(variableName)
		vt = valTypeLocal
	}

	switch vt {
	case valTypeClosure:
		cur.writeOpCode(opStoreClosureVal).writeAddress(addr)
		cur.writeOpCode(opPushClosureVal).writeAddress(addr)
	case valTypeLocal:
		cur.writeOpCode(opStore).writeAddress(addr)
		cur.writeOpCode(opPush).writeAddress(addr)
	default:
		errorx.Panic(errorx.IllegalArgument.New("unknown value type %d", valTypeLocal))
	}
}

func emitList(l SExpressions, cur *VMByteCode) {
	cur.writeOpCode(opPush).writeAddress(cur.constAddr(nil))

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

	cur.inNewScope(scopeTypeLexical, func() {
		emit(inputList, cur)

		ad := cur.scope.createNextAddr(loopVar.(literal).value)

		var begin ptr
		cur.iptr(&begin)
		cur.writeOpCode(opPush).writeAddress(ad)
		cur.writeOpCode(opNil)
		cur.writeOpCode(opNot)
		var end ptr
		cur.writeOpCode(opBr).iptr(&end).b(emptyAddr...)
		cur.writeOpCode(opPush).writeAddress(ad)
		cur.writeOpCode(opPush).writeAddress(ad)
		cur.writeOpCode(opCar)
		cur.writeOpCode(opStore).writeAddress(ad)

		for _, e := range body {
			emit(e, cur)
		}
		cur.writeOpCode(opPop)
		cur.writeOpCode(opCdr)
		cur.writeOpCode(opStore).writeAddress(ad)
		cur.writeOpCode(opJmp).writePointer(begin)
		cur.modify(end, makeByteUint(cur.pos()))
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
	if macros.rest && len(args) < macros.nargs {
		errorx.Panic(errorx.IllegalArgument.New("number of arguments for macros %s must be greater than %d", name, macros.nargs))
	}
	if !macros.rest && len(args) != macros.nargs {
		errorx.Panic(errorx.IllegalArgument.New("number of arguments for macros %s must be equal to %d", name, macros.nargs))
	}
	vm.ip = len(vm.code)
	vm.code = append(vm.code, macros.code...)
	if macros.rest {
		restArgs := args[macros.nargs-1:]
		c := &cons{first: restArgs[0]}
		for i := 1; i < len(restArgs); i++ {
			next := &cons{first: restArgs[i]}
			c.second = next
			c = next
		}
		args[macros.nargs-1] = c
		args = args[:macros.nargs]
	}
	for i := range args { // push arguments in reverse order for stack
		vm.push(args[i])
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

	var restArg bool
	var actualArgs SExpressions
	for i, a := range args {
		v := a.(literal).value
		if v == "&rest" {
			restArg = true
			actualArgs = append(actualArgs, args[i+1])
			break
		}
		actualArgs = append(actualArgs, args[i])
	}

	for i, a := range actualArgs {
		v := a.(literal).value
		cur.scope.storeAddr(v, offsetAddress(-(len(actualArgs) - i - 1))) // grow to stack bottom from base pointer
	}

	for _, b := range body {
		emit(b, cur)
	}

	m.code = make([]byte, len(cur.code[lastInstruction:]))
	m.rest = restArg
	m.nargs = len(actualArgs)
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

type closure struct {
	name   string
	addr   ptr
	nargs  int
	rest   bool
	values []*any
}

func emitLambda(v *cons, cur *VMByteCode) {
	labelID := cur.newLabelID()

	l := consToList(v)
	var endLambdaAddress ptr
	var startDefinitionAddress ptr

	cur.writeOpCode(opJmp).iptr(&endLambdaAddress).writeEmptyAddress().iptr(&startDefinitionAddress)

	cur.inNewScope(scopeTypeStackFrame, func() {
		fn := emitFunction(labelID, l[1:], cur)

		funcDefinitionLength := cur.pos() - startDefinitionAddress
		cur.modify(endLambdaAddress, makeByteUint(offsetAddress(int(funcDefinitionLength))))
		cur.debug("label %s", labelID)

		cur.writeOpCode(opPushClosure).
			writePointer(offsetAddress(-int(funcDefinitionLength) - 3 /*opcode + address */)).
			writeInt(fn.nargs). // lambda arguments count
			writeBool(fn.rest) // rest args flag

		type closureVar struct {
			stackAddr  btUint
			vt         valType
			closurePtr ptr
		}
		var localAddresses []closureVar
		for v, closureValPtr := range cur.scope.closure.varAddresses {
			localAddress, vt, ok := cur.scope.parentScope.findLocalAddress(v)
			if !ok {
				errorx.Panic(errorx.IllegalState.New("closure variable must exists in local variables in parent scope"))
			}
			localAddresses = append(localAddresses, closureVar{localAddress, vt, closureValPtr})
		}

		sort.Slice(localAddresses, func(i, j int) bool {
			return localAddresses[i].closurePtr < localAddresses[j].closurePtr
		})

		// write closure variables count
		cur.writeInt(len(localAddresses))
		for _, a := range localAddresses {
			cur.b(byte(a.vt))
			cur.writeAddress(a.stackAddr)
		}
	})
}

func emitProgn(v *cons, cur *VMByteCode) {
	l := consToList(v)
	cur.inNewScope(scopeTypeLexical, func() {
		for _, e := range l[1:] {
			emit(e, cur)
		}
	})
}

func emitPrint(v *cons, cur *VMByteCode) {
	args := consToList(v)
	emit(args[1], cur)
	cur.writeOpCode(opPrint)
}

func emitWhile(v *cons, cur *VMByteCode) {
	l := consToList(v)
	condition := l[1]
	body := l[2:]
	var checkConditionPtr, breakAddress ptr

	checkConditionPtr = cur.pos()
	emit(condition, cur)
	cur.writeOpCode(opBr).iptr(&breakAddress).writeEmptyAddress()
	for _, b := range body {
		emit(b, cur)
	}
	cur.writeOpCode(opJmp).writePointer(checkConditionPtr)
	cur.modify(breakAddress, makeByteUint(cur.pos()))
}

func emitLiteral(v literal, cur *VMByteCode) {
	parts := variableParts(v.value)
	addr, vt, ok := cur.scope.resolveAddress(parts[0])
	if !ok {
		addr, ok = cur.findConstAddr(parts[0])
		if !ok {
			errorx.Panic(errorx.IllegalArgument.New("unknown literal '%s'", v.value))
		}
	}
	switch vt {
	case valTypeClosure:
		cur.writeOpCode(opPushClosureVal).writeAddress(addr)
	case valTypeLocal, valTypeConst:
		if len(parts) == 1 {
			cur.writeOpCode(opPush).writeAddress(addr)
		} else {
			cur.writeOpCode(opPushField).writeAddress(addr).writeConstAddr(strings.Join(parts[1:], "."))
		}
	default:
		errorx.Panic(errorx.IllegalArgument.New("unknown value type '%d'", vt))
	}
}

func consToList(c *cons) SExpressions {
	return consToListN(c, -1)
}

func consToListN(c *cons, n int) SExpressions {
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
		if n > 0 && len(res) == n {
			break
		}
	}
	return res
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

var forRangeMacro = `
(defmacro forRange (i from to &rest body) 
` + "`(progn (setq ,i ,from) (while (lt ,i ,to) ,@body)))" + `
`
