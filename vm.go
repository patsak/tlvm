package tlvm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/joomcode/errorx"
	"golang.org/x/exp/constraints"
)

var (
	errRawTextPositionProperty = errorx.RegisterProperty("rawTextPosition")
)

type opCode byte

const (
	opPush opCode = iota
	opPushClosureVal
	opStoreClosureVal
	opPushClosure
	opPushField
	opStore
	opStoreField
	opPop
	opCmp
	opAdd
	opSub
	opDiv
	opMul
	opCmpBool
	opBr
	opJmp
	opNot
	opTrue
	opCall
	opClosureCall
	opExtCall
	opPopCall
	opRet
	opFrameReset
	opHalt
	opCons
	opCar
	opCdr
	opNil
	opPrint
	opNoOp
	opSplice
	opMakeHashTable
	opSetHashTableValue
	opGetHashTableValue
	opMakeVector
	opSetVectorValue
	opAppend
	opGetVectorValue
	opLen
	opContains
)

const (
	boolTrue  bool = true
	boolFalse bool = false
)

const (
	cmpFlagEq          byte = 1 << 0
	cmpFlagGt          byte = 1 << 1
	cmpFlagLt          byte = 1 << 2
	addrShiftRightFlag ptr  = 1 << 15
	addrShiftLeftFlag  ptr  = 1 << 14
)

type VM struct {
	stack                       [256]stackValue
	code                        []byte         // byte code
	cp                          int            // constants top pointer
	ep                          int            // entry point
	bp                          int            // base pointer
	sp                          int            // stack pointer
	ip                          int            // instruction pointer
	env                         map[any]ptr    // environment variables pointers
	debugInfo                   map[int]string // debug string by instruction position
	originalTextPositionPointer map[int]int    // position in original code text by instruction position
	labels                      map[Label]*closure
}

type Label string

func (l Label) String() string {
	return string(l)
}

const callFrameOffset = 4

func NewVM(output *VMByteCode) *VM {
	vm := &VM{
		code: append(output.definedFunctions, output.code...),
		bp:   -1,
		sp:   -1,
	}
	for i := range output.globalsList {
		vm.stack[i] = output.globalsList[i]
	}
	vm.ip = len(output.definedFunctions)
	vm.ep = vm.ip
	vm.bp = len(output.globalsList)
	vm.sp = len(output.globalsList) - 1
	vm.cp = vm.sp
	vm.env = output.globals

	vm.labels = output.labels
	vm.debugInfo = output.debugInfo
	vm.originalTextPositionPointer = output.origPositionPointer
	return vm
}

func (v *VM) CodeString() string {
	sBp, sSp, sIp := v.bp, v.sp, v.ip
	defer func() {
		v.bp = sBp
		v.sp = sSp
		v.ip = sIp
	}()

	v.ip = 0
	b := strings.Builder{}

	for v.ip < len(v.code) {
		b.WriteString(strconv.Itoa(v.ip))
		b.WriteString(" ")
		origIp := v.ip
		o := v.code[v.ip]
		v.ip++

		switch opCode(o) {
		case opPush:
			b.WriteString(fmt.Sprintf("PUSH %s", v.formatStackAddr()))
		case opPushClosure:
			b.WriteString(fmt.Sprintf("PUSHCLOSURE %s", v.formatIpAddr()))
			nargs := v.readInt()
			rest := v.readBool()
			nclosurevals := v.readInt()
			b.WriteString(fmt.Sprintf(", %d args, %t &rest, %d bound variables:", nargs, rest, nclosurevals))
			for i := 0; i < nclosurevals; i++ {
				b.WriteString(fmt.Sprintf(" %d", v.readByte()))
				b.WriteString(fmt.Sprintf(" %s", v.formatStackAddr()))
			}
		case opPushField:
			b.WriteString(fmt.Sprintf("PUSHFIELD %s %s", v.formatStackAddr(), v.formatStackAddr()))
		case opPushClosureVal:
			b.WriteString(fmt.Sprintf("PUSHCLOSUREVAL %s", v.formatStackAddr()))
		case opStoreClosureVal:
			b.WriteString(fmt.Sprintf("STORECLOSERVAL %s", v.formatStackAddr()))
		case opClosureCall:
			b.WriteString(fmt.Sprintf("CLOSURECALL %s %s", v.formatStackAddr(), v.formatStackAddr()))
		case opStore:
			b.WriteString(fmt.Sprintf("STORE %s", v.formatStackAddr()))
		case opStoreField:
			b.WriteString(fmt.Sprintf("STORE_FIELD %s %s", v.formatStackAddr(), v.formatStackAddr()))
		case opAdd:
			b.WriteString(fmt.Sprintf("ADD"))
		case opSub:
			b.WriteString(fmt.Sprintf("SUB"))
		case opCmp:
			b.WriteString(fmt.Sprintf("CMP %d", v.readByte()))
		case opCmpBool:
			b.WriteString(fmt.Sprintf("CMPBOOL"))
		case opBr:
			b.WriteString(fmt.Sprintf("BR %s", v.formatStackAddr()))
		case opMul:
			b.WriteString(fmt.Sprintf("MUL"))
		case opDiv:
			b.WriteString(fmt.Sprintf("DIV"))
		case opCall:
			b.WriteString(fmt.Sprintf("CALL %d %d", v.readBasePointerAddr(), v.readBasePointerAddr()))
		case opPopCall:
			b.WriteString(fmt.Sprintf("POPCALL %s", v.formatIpAddr()))
		case opExtCall:
			b.WriteString(fmt.Sprintf("EXTCALL %d", v.readBasePointerAddr()))
		case opRet:
			b.WriteString(fmt.Sprintf("RET"))
		case opFrameReset:
			b.WriteString(fmt.Sprintf("FRAME_RESET"))
		case opJmp:
			b.WriteString(fmt.Sprintf("JMP %s", v.formatIpAddr()))
		case opNot:
			b.WriteString("NOT")
		case opCar:
			b.WriteString("CAR")
		case opCons:
			b.WriteString("CONS")
		case opNil:
			b.WriteString("ISNIL")
		case opPop:
			b.WriteString("POP")
		case opCdr:
			b.WriteString("CDR")
		case opPrint:
			b.WriteString("PRINT")
		case opSplice:
			b.WriteString("SPLICE")
		case opNoOp:
			b.WriteString("NOOP")
		case opMakeHashTable:
			b.WriteString("MAKE_HASH_TABLE")
		case opSetHashTableValue:
			b.WriteString("SET_HASH_TABLE_VALUE")
		case opGetHashTableValue:
			b.WriteString("GET_HASH_TABLE_VALUE")
		case opMakeVector:
			b.WriteString("MAKE_VECTOR")
		case opSetVectorValue:
			b.WriteString("SET_VECTOR_VALUE")
		case opGetVectorValue:
			b.WriteString("GET_VECTOR_VALUE")
		case opAppend:
			b.WriteString("APPEND")
		case opLen:
			b.WriteString("LEN")
		case opContains:
			b.WriteString("CONTAINS")
		case opHalt:
		default:
			panic(errorx.IllegalFormat.New("unknown code %d", o))
		}
		debugString, ok := v.debugInfo[origIp]
		if ok {
			b.WriteString(" ; " + debugString)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func (v *VM) Copy() VM {
	nv := *v
	return nv
}

func (vm *VM) EnvInt(k string, v int) {
	vm.Env(Label(k), int64(v))
}

func (vm *VM) EnvInt64(k string, v int64) {
	vm.Env(Label(k), v)
}

func (vm *VM) EnvFloat32(k string, v float32) {
	vm.Env(Label(k), float64(v))
}

func (vm *VM) EnvFloat64(k string, v float64) {
	vm.Env(Label(k), v)
}

func (vm *VM) Env(k Label, v any) {
	pos, ok := vm.env[k]
	if !ok {
		return
	}
	vm.stack[pos] = stackValueFrom(v)
}

func (vm *VM) EnvString(k string, v string) {
	vm.stack[vm.env[Label(k)]] = stackValueFrom(v)
}

func (v *VM) Result() any {
	return v.stack[v.sp].Interface()
}

func (v *VM) Reset() {
	v.ip = v.ep
	v.bp = v.cp + 1
	v.sp = v.cp
}

func (v *VM) Execute() (errRes error) {
	defer func() {
		err := recover()
		if err == nil {
			return
		}

		if v, ok := err.(error); ok {
			errRes = v
		} else {
			errRes = errorx.IllegalState.New("%v", err)
		}

		errRes = errorx.Decorate(errRes, "VM instruction: %v", v.ip)
	}()

	for v.ip < len(v.code) {
		o := v.code[v.ip]
		v.ip++
		switch opCode(o) {
		case opPush:
			vv := v.getStackValueByAddress(v.readBasePointerAddr())
			v.push(vv)
		case opPushClosureVal:
			a := v.readClosureAddr()
			vars := v.getClosureVars()
			closureVar := vars[a]
			v.push(closureVar.value)
		case opStoreClosureVal:
			r := v.pop()
			a := v.readClosureAddr()
			vars := v.getClosureVars()
			vptr := vars[a]
			v.store(vptr.value, r)
		case opPushClosure:
			ip := v.readIpAddrArg()
			n := v.readInt()
			rest := v.readBool()
			nClosureVars := v.readInt()
			closureVars := make([]closureVariable, 0, nClosureVars)
			for i := 0; i < nClosureVars; i++ {
				vt := valType(v.readByte())
				varPtr := v.readPtr()
				closureVar := closureVariable{addr: varPtr, vt: vt}
				switch vt {
				case valTypeClosure:
					clVars := v.getClosureVars()
					closureVar.value = clVars[varPtr.abs(0)].value
				case valTypeLocal:
					// replace stack value with pointer
					stackValue := v.getStackValueByAddress(varPtr.abs(v.bp)) // copy local value
					ptr := reflect.New(stackValue.Type()).Elem()             // create pointer
					ptr.Set(stackValue)
					v.setStackValueByAddress(varPtr.abs(v.bp), ptr)
					closureVar.value = ptr
				default:
					errorx.Panic(errorx.IllegalState.New("unexpected value type %d in closure", vt))
				}
				closureVars = append(closureVars, closureVar)
			}

			v.push(stackValueFrom(&closure{codePointer: ip, nargs: n, varargs: rest, values: closureVars}))
		case opPushField:
			variableAddr := v.readBasePointerAddr()
			fieldPathAddr := v.readBasePointerAddr()
			structValue := v.elem(v.getStackValueByAddress(variableAddr))
			path := v.getStackValueByAddress(fieldPathAddr).Interface().(Label)
			rv := v.fieldByPath(structValue, path)
			v.push(rv)
		case opStore:
			vv := v.pop()
			a := v.readBasePointerAddr()
			v.stack[a] = vv
		case opStoreField:
			vv := v.pop()

			variableAddr := v.readBasePointerAddr()
			fieldPathAddr := v.readBasePointerAddr()

			path := v.getStackValueByAddress(fieldPathAddr).Interface().(Label)
			rv := v.elem(v.getStackValueByAddress(variableAddr))
			if !rv.CanAddr() {
				errorx.Panic(errorx.IllegalArgument.New("can't store field %s in non addressable structure", path))
			}
			rv = v.fieldByPath(rv, path)

			rv.Set(vv)
		case opCmp:
			v2 := v.elem(v.pop())
			v1 := v.elem(v.pop())

			chFl := v.readByte()
			switch v1.Kind() {
			case reflect.Float64, reflect.Float32:
				v.push(stackValueFrom(cmp(v1.Float(), v2.Float(), chFl)))
			case reflect.Int, reflect.Int32, reflect.Int64, reflect.Int8:
				v.push(stackValueFrom(cmp(v1.Int(), v2.Int(), chFl)))
			case reflect.String:
				v.push(stackValueFrom(cmp(v1.String(), v2.String(), chFl)))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %+v for cmp operation", v1.Type()))
			}
		case opAdd:
			v1 := v.elem(v.pop())
			v2 := v.elem(v.pop())
			if v1.Kind() == reflect.String || v2.Kind() == reflect.String {
				if v1.Kind() == reflect.String && v2.Kind() == reflect.String {
					v.push(stackValueFrom(v2.String() + v1.String()))
					break
				}
				errorx.Panic(errorx.IllegalArgument.New("unexpected types %v and %v for ADD operation", v2.Type(), v1.Type()))
			}
			if !isNumberKind(v1.Kind()) || !isNumberKind(v2.Kind()) {
				errorx.Panic(errorx.IllegalArgument.New("unexpected types %v and %v for ADD operation", v2.Type(), v1.Type()))
			}
			if isFloatKind(v1.Kind()) || isFloatKind(v2.Kind()) {
				v.push(stackValueFrom(v2.Convert(floatType).Float() + v1.Convert(floatType).Float()))
			} else {
				v.push(stackValueFrom(v2.Int() + v1.Int()))
			}
		case opSub:
			v1 := v.elem(v.pop())
			v2 := v.elem(v.pop())
			if !isNumberKind(v1.Kind()) || !isNumberKind(v2.Kind()) {
				errorx.Panic(errorx.IllegalArgument.New("unexpected types %v and %v for SUB operation", v2.Type(), v1.Type()))
			}
			if isFloatKind(v1.Kind()) || isFloatKind(v2.Kind()) {
				v.push(stackValueFrom(v2.Convert(floatType).Float() - v1.Convert(floatType).Float()))
			} else {
				v.push(stackValueFrom(v2.Int() - v1.Int()))
			}
		case opDiv:
			v1 := v.elem(v.pop())
			v2 := v.elem(v.pop())
			if !isNumberKind(v1.Kind()) || !isNumberKind(v2.Kind()) {
				errorx.Panic(errorx.IllegalArgument.New("unexpected types %v and %v for DIV operation", v2.Type(), v1.Type()))
			}
			v.push(stackValueFrom(v2.Convert(floatType).Float() / v1.Convert(floatType).Float()))
		case opMul:
			v1 := v.elem(v.pop())
			v2 := v.elem(v.pop())
			if !isNumberKind(v1.Kind()) || !isNumberKind(v2.Kind()) {
				errorx.Panic(errorx.IllegalArgument.New("unexpected types %v and %v for MUL operation", v2.Type(), v1.Type()))
			}
			if isFloatKind(v1.Kind()) || isFloatKind(v2.Kind()) {
				v.push(stackValueFrom(v2.Convert(floatType).Float() * v1.Convert(floatType).Float()))
			} else {
				v.push(stackValueFrom(v2.Int() * v1.Int()))
			}
		case opCmpBool:
			v1 := v.elem(v.pop()).Bool()
			v2 := v.elem(v.pop()).Bool()
			chFl := v.readByte()

			if chFl&cmpFlagEq > 0 {
				v.push(stackValueFrom(v1 == v2))
			}
		case opTrue:
			v1 := v.pop().Bool()
			chFl := v.readByte()
			if chFl&cmpFlagEq > 0 {
				v.push(stackValueFrom(v1))
			}
		case opNil:
			v.push(stackValueFrom(v.pop().IsNil()))
		case opBr:
			condition := v.pop().Bool()
			addr := v.readPtr()
			if !condition {
				v.goTo(addr)
			}
		case opPop:
			v.pop()
		case opExtCall:
			fn := v.elem(v.pop())
			nargs := v.readInt()
			args := make([]stackValue, nargs)
			for i := 0; i < nargs; i++ {
				args[nargs-i-1] = v.elem(v.pop())
			}
			values := fn.Call(args)
			v.push(values[0])
		case opClosureCall:
			a := v.readBasePointerAddr()
			cl := v.getStackValueByAddress(a).Interface().(*closure)
			nargs := v.readInt()
			if nargs != cl.nargs {
				errorx.Panic(errorx.IllegalState.New("illegal arguments count to call function"))
			}
			vars := make([]closureVariable, 0, len(cl.values))
			for _, vv := range cl.values {
				switch vv.vt {
				case valTypeLocal:
					// bind to a stack slot in the caller frame by reference:
					absAddr := vv.addr.abs(v.bp)
					stackValue := v.getStackValueByAddress(absAddr)
					if !stackValue.IsValid() {
						errorx.Panic(errorx.IllegalState.New("invalid stack value for closure binding"))
					}
					if !stackValue.CanSet() {
						cell := reflect.New(stackValue.Type()).Elem()
						cell.Set(stackValue)
						v.setStackValueByAddress(absAddr, cell)
						stackValue = cell
					}
					vv.value = stackValue
				case valTypeClosure:
					// Bind to a closure variable of the caller frame by reference (no copying).
					clVars := v.getClosureVars()
					vv.value = clVars[vv.addr.abs(0)].value
				default:
					errorx.Panic(errorx.IllegalState.New("unexpected value type %d in closure", vv.vt))
				}

				vars = append(vars, vv)
			}
			v.pushRestArgIfNeeded(nargs, cl)

			addr := cl.codePointer
			v.push(stackValueFrom(cl.nargs))
			v.push(stackValueFrom(vars))
			v.push(stackValueFrom(v.bp))
			v.push(stackValueFrom(v.ip))
			v.bp = v.sp - callFrameOffset
			v.goTo(addr)
		case opPopCall:
			clu := v.pop().Interface()
			cl, ok := clu.(*closure)
			if !ok {
				errorx.Panic(errorx.IllegalState.New("can't cast %T to closure", clu))
			}
			nargs := v.readInt()

			if !cl.varargs && nargs != cl.nargs ||
				cl.varargs && nargs < cl.nargs {
				errorx.Panic(errorx.IllegalState.New("illegal arguments count to call function %s", cl.name))
			}

			v.pushRestArgIfNeeded(nargs, cl)
			addr := cl.codePointer
			v.push(stackValueFrom(cl.nargs))
			v.push(stackValueFrom(cl.values))
			v.push(stackValueFrom(v.bp))
			v.push(stackValueFrom(v.ip))
			v.bp = v.sp - callFrameOffset
			v.goTo(addr)
		case opCall:
			addr := v.readPtr()
			nargs := v.readInt()
			v.push(stackValueFrom(nargs))
			v.push(stackValueFrom(nil))
			v.push(stackValueFrom(v.bp))
			v.push(stackValueFrom(v.ip))
			v.bp = v.sp - callFrameOffset
			v.goTo(addr)
		case opRet:
			result := v.pop()
			v.sp = v.bp + callFrameOffset
			v.ip = int(v.pop().Int())
			v.bp = int(v.pop().Int())
			v.pop() // skip closure values
			nargs := v.pop().Int()
			v.sp -= int(nargs)
			v.push(result)
		case opFrameReset:
			v.sp = v.bp + callFrameOffset
		case opJmp:
			v.goTo(v.readPtr())
		case opNot:
			v.push(stackValueFrom(!v.pop().Bool()))
		case opCons:
			first := v.pop()
			second := v.pop()

			var res *cons
			if second.IsValid() {
				res = second.Interface().(*cons)
				res.concat(first.Interface())
			} else {
				res = &cons{
					expr: []any{first.Interface()},
				}
			}

			v.push(stackValueFrom(res))
		case opCar:
			c := (*cons)(v.pop().UnsafePointer())
			v.push(stackValueFrom(c.first()))
		case opCdr:
			c := (*cons)(v.pop().UnsafePointer())
			v.push(stackValueFrom(c.tail()))
		case opSplice:
			nextV := v.pop()
			prevV := v.pop()

			if !prevV.IsValid() {
				v.push(nextV)
				break
			}
			if !nextV.IsValid() {
				v.push(prevV)
				break
			}

			next := (*cons)(nextV.UnsafePointer())
			prev := (*cons)(prevV.UnsafePointer())

			b := &cons{}
			b.expr = append(prev.expr, next.expr...)

			v.push(stackValueFrom(b))
		case opMakeHashTable:
			v.push(stackValueFrom(make(map[any]any)))
		case opSetHashTableValue:
			m := v.pop().Interface().(map[any]any)
			k := v.pop()
			val := v.pop()
			m[k.Interface()] = val.Interface()
		case opGetHashTableValue:
			m := v.pop().Interface().(map[any]any)
			i := v.pop()
			val, ok := m[i.Interface()]
			if !ok {
				v.push(stackValue{})
			} else {
				v.push(stackValueFrom(val))
			}
		case opMakeVector:
			v.push(stackValueFrom(make([]any, 0)))
		case opSetVectorValue:
			m := v.pop().Interface().([]any)
			i := v.pop().Int()
			m[i] = v.pop().Interface()
		case opGetVectorValue:
			vec := v.pop()
			i := v.pop().Convert(intType).Int()
			var pv stackValue
			switch vec.Kind() {
			case reflect.String:
				pv = stackValueFrom(stringRuneAt(vec.String(), i))
			case reflect.Array, reflect.Slice:
				pv = vec.Index(int(i))
			default:
				panic(errorx.Panic(errorx.IllegalArgument.New("unexpected type %+v for GETV operation", vec.Type())))
			}
			v.push(pv)
		case opAppend:
			m := v.pop()
			n := v.pop()

			switch m.Kind() {
			case reflect.Slice:
				m = reflect.Append(m, n)
			default:
				vv := (*cons)(m.UnsafePointer())
				vv.expr = append([]any{n}, vv.expr...)
			}
			v.push(m)
		case opLen:
			var l int
			vv := v.pop()
			switch vv.Kind() {
			case reflect.Slice, reflect.Map, reflect.String:
				l = vv.Len()
			default:
				if vv.Type() == consType {
					l = len((*cons)(vv.UnsafePointer()).expr)
				} else {
					panic(errorx.Panic(errorx.IllegalArgument.New("can't get length from type %+v", vv.Type())))
				}
			}
			v.push(stackValueFrom(l))
		case opContains:
			container := v.pop()
			value := v.pop()

			var res bool
			switch container.Kind() {
			case reflect.Map:
				res = container.MapIndex(value).IsValid()
			case reflect.Slice:
				for i := 0; i < container.Len(); i++ {
					if reflect.DeepEqual(container.Index(i).Interface(), value.Interface()) {
						res = true
						break
					}
				}
			default:
				panic(errorx.Panic(errorx.IllegalArgument.New("can't check contains in type %+v", container.Type())))
			}
			v.push(stackValueFrom(res))
		case opPrint:
			fmt.Printf("%v\n", v.pop())
		case opNoOp:
		case opHalt:
			return
		}
	}

	return nil
}

func (v *VM) pushRestArgIfNeeded(nargs int, cl *closure) {
	if !cl.varargs {
		return
	}
	expr := make([]any, 0, nargs-cl.nargs+1)
	for i := 0; i < nargs-cl.nargs+1; i++ {
		expr = append(expr, v.pop())
	}

	v.push(stackValueFrom(&cons{expr: expr}))
}

func (v *VM) readPtr() ptr {
	ip := v.ip
	code := v.code
	res := ptr(uint16(code[ip])<<8 | uint16(code[ip+1]))
	v.ip = ip + 2
	return res
}

func (v *VM) readInt() int {
	return int(v.readPtr())
}

func (v *VM) readBool() bool {
	return v.readByte() > 0
}

func (v *VM) readBasePointerAddr() ptr {
	return v.readPtr().abs(v.bp)
}

func (v *VM) readClosureAddr() ptr {
	return v.readPtr().abs(0)
}

func (v *VM) getClosureVars() []closureVariable {
	const closureVarsOffset = 2
	return v.stack[v.bp+closureVarsOffset].Interface().([]closureVariable)
}

func (v *VM) getStackValueByAddress(p ptr) stackValue {
	return v.stack[p]
}

func (v *VM) setStackValueByAddress(p ptr, rv stackValue) {
	v.stack[p] = rv
}

func (v *VM) fieldByPath(rv stackValue, path Label) stackValue {
	for {
		part, rest, ok := strings.Cut(path.String(), ".")
		rv = rv.FieldByName(part)
		if !ok {
			return rv
		}
		path = Label(rest)
	}
}

func (v *VM) elem(a stackValue) stackValue {
	for a.IsValid() {
		switch a.Kind() {
		case reflect.Interface:
			if a.IsNil() {
				return stackValue{}
			}
			a = a.Elem()
		case reflect.Ptr:
			if a.IsNil() {
				return stackValue{}
			}
			a = a.Elem()
		default:
			return a
		}
	}
	return a
}

func (v *VM) store(t stackValue, s stackValue) {
	v.elem(t).Set(v.elem(s))
}

func (v *VM) readIpAddrArg() ptr {
	return v.readPtr().abs(v.ip)
}

func (v *VM) formatStackAddr() string {
	return v.readPtr().format("bp")
}

func (v *VM) formatIpAddr() string {
	return fmt.Sprintf("%d", v.readPtr().abs(v.ip))
}

func (v *VM) readByte() byte {
	a := v.code[v.ip]
	v.ip++
	return a
}

func (v *VM) pop() stackValue {
	ret := v.stack[v.sp]
	v.sp--
	return ret
}

func (v *VM) peek() any {
	return v.stack[v.sp]
}

func (v *VM) push(rv stackValue) {
	v.sp++
	v.stack[v.sp] = rv
}

func (v *VM) goTo(p ptr) {
	v.ip = int(p.abs(v.ip))
}

func stringRuneAt(s string, index int64) string {
	if index < 0 {
		panic("index out of range")
	}
	for _, r := range s {
		if index == 0 {
			return string(r)
		}
		index--
	}
	panic("index out of range")
}

func cmp[T constraints.Ordered](v1, v2 T, chFl byte) bool {
	if chFl&cmpFlagEq > 0 {
		return v1 == v2
	}
	if chFl&cmpFlagLt > 0 {
		return v1 < v2
	}
	if chFl&cmpFlagGt > 0 {
		return v1 > v2
	}
	return false
}

var (
	floatType = reflect.TypeOf(float64(0))
	intType   = reflect.TypeOf(0)
	consType  = reflect.TypeOf(&cons{})
)

func isIntKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func isFloatKind(k reflect.Kind) bool {
	switch k {
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func isNumberKind(k reflect.Kind) bool {
	return isIntKind(k) || isFloatKind(k)
}

func stackValueFrom(v any) stackValue {
	return reflect.ValueOf(v)
}
