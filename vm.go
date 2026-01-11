package tlvm

import (
	"encoding/binary"
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
	stack                       [256]reflect.Value
	code                        []byte         // byte code
	cp                          int            // constants top pointer
	ep                          int            // entry point
	bp                          int            // base pointer
	sp                          int            // stack pointer
	ip                          int            // instruction pointer
	env                         map[any]ptr    // environment variables pointers
	debugInfo                   map[int]string // debug string by instruction position
	originalTextPositionPointer map[int]int    // position in original code text by instruction position
	labels                      map[string]*closure
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
			b.WriteString(fmt.Sprintf("PUSH %s", v.strStackAddr()))
		case opPushClosure:
			b.WriteString(fmt.Sprintf("PUSHCLOSURE %s", v.strIpAddr()))
			nargs := v.readInt()
			rest := v.readBool()
			nclosurevals := v.readInt()
			b.WriteString(fmt.Sprintf(", %d args, %t &rest, %d bound variables:", nargs, rest, nclosurevals))
			for i := 0; i < nclosurevals; i++ {
				b.WriteString(fmt.Sprintf(" %d", v.next()))
				b.WriteString(fmt.Sprintf(" %s", v.strStackAddr()))
			}
		case opPushField:
			b.WriteString(fmt.Sprintf("PUSHFIELD %s %s", v.strStackAddr(), v.strStackAddr()))
		case opPushClosureVal:
			b.WriteString(fmt.Sprintf("PUSHCLOSUREVAL %s", v.strStackAddr()))
		case opStoreClosureVal:
			b.WriteString(fmt.Sprintf("STORECLOSERVAL %s", v.strStackAddr()))
		case opClosureCall:
			b.WriteString(fmt.Sprintf("CLOSURECALL %s %s", v.strStackAddr(), v.strStackAddr()))
		case opStore:
			b.WriteString(fmt.Sprintf("STORE %s", v.strStackAddr()))
		case opStoreField:
			b.WriteString(fmt.Sprintf("STORE_FIELD %s %s", v.strStackAddr(), v.strStackAddr()))
		case opAdd:
			b.WriteString(fmt.Sprintf("ADD"))
		case opSub:
			b.WriteString(fmt.Sprintf("SUB"))
		case opCmp:
			b.WriteString(fmt.Sprintf("CMP %d", v.next()))
		case opCmpBool:
			b.WriteString(fmt.Sprintf("CMPBOOL"))
		case opBr:
			b.WriteString(fmt.Sprintf("BR %s", v.strStackAddr()))
		case opMul:
			b.WriteString(fmt.Sprintf("MUL"))
		case opDiv:
			b.WriteString(fmt.Sprintf("DIV"))
		case opCall:
			b.WriteString(fmt.Sprintf("CALL %d %d", v.readBasePointerAddr(), v.readBasePointerAddr()))
		case opPopCall:
			b.WriteString(fmt.Sprintf("POPCALL %s", v.strIpAddr()))
		case opExtCall:
			b.WriteString(fmt.Sprintf("EXTCALL %d", v.readBasePointerAddr()))
		case opRet:
			b.WriteString(fmt.Sprintf("RET"))
	case opFrameReset:
			b.WriteString(fmt.Sprintf("FRAME_RESET"))
		case opJmp:
			b.WriteString(fmt.Sprintf("JMP %s", v.strIpAddr()))
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
	vm.Env(k, int64(v))
}

func (vm *VM) EnvInt64(k string, v int64) {
	vm.Env(k, v)
}

func (vm *VM) EnvFloat32(k string, v float32) {
	vm.Env(k, float64(v))
}

func (vm *VM) EnvFloat64(k string, v float64) {
	vm.Env(k, v)
}

func (vm *VM) Env(k any, v any) {
	pos, ok := vm.env[k]
	if !ok {
		return
	}
	vm.stack[pos] = reflect.ValueOf(v)
}

func (vm *VM) EnvString(k string, v string) {
	vm.stack[vm.env[k]] = reflect.ValueOf(v)
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
			ip := v.ipAddrArg()
			n := v.readInt()
			rest := v.readBool()
			nClosureVars := v.readInt()
			var closureVars []closureVariable
			for i := 0; i < nClosureVars; i++ {
				vt := valType(v.next())
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

			v.push(reflect.ValueOf(&closure{codePointer: ip, nargs: n, varargs: rest, values: closureVars}))
		case opPushField:
			variableAddr := v.readBasePointerAddr()
			fieldPathAddr := v.readBasePointerAddr()
			structValue := v.elem(v.getStackValueByAddress(variableAddr))
			path := v.getStackValueByAddress(fieldPathAddr).Interface().(string)
			rv := structValue
			for _, p := range strings.Split(path, ".") {
				rv = rv.FieldByName(p)
			}
			v.push(rv)
		case opStore:
			vv := v.pop()
			a := v.readBasePointerAddr()
			v.stack[a] = vv
		case opStoreField:
			vv := v.pop()

			variableAddr := v.readBasePointerAddr()
			fieldPathAddr := v.readBasePointerAddr()

			path := v.getStackValueByAddress(fieldPathAddr).Interface().(string)
			rv := v.elem(v.getStackValueByAddress(variableAddr))
			if !rv.CanAddr() {
				errorx.Panic(errorx.IllegalArgument.New("can't store field %s in non addressable structure", path))
			}
			for _, p := range strings.Split(path, ".") {
				rv = rv.FieldByName(p)
			}

			rv.Set(vv)
		case opCmp:
			v2 := v.elem(v.pop())
			v1 := v.elem(v.pop())

			chFl := v.next()
			switch v1.Kind() {
			case reflect.Float64, reflect.Float32:
				v.push(reflect.ValueOf(cmp(v1.Float(), v2.Float(), chFl)))
			case reflect.Int, reflect.Int32, reflect.Int64, reflect.Int8:
				v.push(reflect.ValueOf(cmp(v1.Int(), v2.Int(), chFl)))
			case reflect.String:
				v.push(reflect.ValueOf(cmp(v1.String(), v2.String(), chFl)))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %+v for cmp operation", v1.Type()))
			}
		case opAdd:
			v1 := v.elem(v.pop())
			v2 := v.elem(v.pop())
			switch v1.Kind() {
			case reflect.Float64, reflect.Float32:
				v.push(reflect.ValueOf(v1.Float() + v2.Float()))
			case reflect.Int, reflect.Int32, reflect.Int64, reflect.Int8:
				v.push(reflect.ValueOf(v1.Int() + v2.Int()))
			case reflect.String:
				v.push(reflect.ValueOf(v2.String() + v1.String()))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %+v for ADD operation", v1.Type()))
			}
		case opSub:
			v1 := v.elem(v.pop())
			v2 := v.elem(v.pop())
			switch v1.Kind() {
			case reflect.Float64, reflect.Float32:
				v.push(reflect.ValueOf(v2.Float() - v1.Float()))
			case reflect.Int, reflect.Int32, reflect.Int64, reflect.Int8:
				v.push(reflect.ValueOf(v2.Int() - v1.Int()))

			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %+v for SUB operation", v1.Type()))
			}
		case opDiv:
			v1 := v.elem(v.pop())
			v2 := v.elem(v.pop())
			switch v1.Kind() {
			case reflect.Int, reflect.Int32, reflect.Int64, reflect.Int8, reflect.Float64, reflect.Float32:
				v.push(reflect.ValueOf(v2.Convert(floatType).Float() / v1.Convert(floatType).Float()))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %+v for DIV operation", v1.Type()))
			}
		case opMul:
			v1 := v.elem(v.pop())
			v2 := v.elem(v.pop())
			switch v2.Kind() {
			case reflect.Int32, reflect.Int8, reflect.Int64, reflect.Int:
				v.push(reflect.ValueOf(v2.Int() * v1.Int()))
			case reflect.Float64, reflect.Float32:
				v.push(reflect.ValueOf(v2.Float() * v1.Float()))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %+v for MUL operation", v2.Type()))
			}
		case opCmpBool:
			v1 := v.elem(v.pop()).Bool()
			v2 := v.elem(v.pop()).Bool()
			chFl := v.next()

			if chFl&cmpFlagEq > 0 {
				v.push(reflect.ValueOf(v1 == v2))
			}
		case opTrue:
			v1 := v.pop().Bool()
			chFl := v.next()
			if chFl&cmpFlagEq > 0 {
				v.push(reflect.ValueOf(v1))
			}
		case opNil:
			v.push(reflect.ValueOf(v.pop().IsNil()))
		case opBr:
			condition := v.pop().Bool()
			addr := v.readPtr()
			if !condition {
				v.goTo(addr)
			}
		case opPop:
			v.pop()
		case opExtCall:
			fn := v.pop()
			nargs := v.readInt()
			args := make([]reflect.Value, nargs)
			for i := 0; i < nargs; i++ {
				args[nargs-i-1] = v.pop()
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
			var vars []closureVariable
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
			v.push(reflect.ValueOf(cl.nargs))
			v.push(reflect.ValueOf(vars))
			v.push(reflect.ValueOf(v.bp))
			v.push(reflect.ValueOf(v.ip))
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
			v.push(reflect.ValueOf(cl.nargs))
			v.push(reflect.ValueOf(cl.values))
			v.push(reflect.ValueOf(v.bp))
			v.push(reflect.ValueOf(v.ip))
			v.bp = v.sp - callFrameOffset
			v.goTo(addr)
		case opCall:
			addr := v.readPtr()
			nargs := v.readInt()
			v.push(reflect.ValueOf(nargs))
			v.push(reflect.ValueOf(nil))
			v.push(reflect.ValueOf(v.bp))
			v.push(reflect.ValueOf(v.ip))
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
			v.push(reflect.ValueOf(!v.pop().Bool()))
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

			v.push(reflect.ValueOf(res))
		case opCar:
			c := (*cons)(v.pop().UnsafePointer())
			v.push(reflect.ValueOf(c.first()))
		case opCdr:
			c := (*cons)(v.pop().UnsafePointer())
			v.push(reflect.ValueOf(c.tail()))
		case opSplice:
			next := (*cons)(v.pop().UnsafePointer())
			prev := (*cons)(v.pop().UnsafePointer())

			b := &cons{}
			b.expr = append(prev.expr, next.expr...)

			v.push(reflect.ValueOf(b))
		case opMakeHashTable:
			v.push(reflect.ValueOf(make(map[any]any)))
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
				v.push(reflect.Value{})
			} else {
				v.push(reflect.ValueOf(val))
			}
		case opMakeVector:
			v.push(reflect.ValueOf(make([]any, 0)))
		case opSetVectorValue:
			m := v.pop().Interface().([]any)
			i := v.pop().Int()
			v := v.pop()
			m[i] = v
		case opGetVectorValue:
			vec := v.pop()
			i := v.pop().Convert(intType).Int()
			var pv reflect.Value
			switch vec.Kind() {
			case reflect.String:
				pv = reflect.ValueOf(string([]rune(vec.String())[i]))
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
				if vv.Type() == reflect.TypeOf(&cons{}) {
					l = len((*cons)(vv.UnsafePointer()).expr)
				} else {
					panic(errorx.Panic(errorx.IllegalArgument.New("can't get length from type %+v", vv.Type())))
				}
			}
			v.push(reflect.ValueOf(l))
		case opContains:
			container := v.pop()
			value := v.pop()

			var res bool
			switch container.Kind() {
			case reflect.Map:
				res = container.MapIndex(value).IsValid()
			case reflect.Slice:
				for i := 0; i < container.Len(); i++ {
					if container.Index(i).Equal(value) {
						res = true
						break
					}
				}
			default:
				panic(errorx.Panic(errorx.IllegalArgument.New("can't check contains in type %+v", container.Type())))
			}
			v.push(reflect.ValueOf(res))
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
	var expr []any
	for i := 0; i < nargs-cl.nargs+1; i++ {
		expr = append(expr, v.pop())
	}

	v.push(reflect.ValueOf(&cons{expr: expr}))
}

func (v *VM) readPtr() ptr {
	res := binary.BigEndian.Uint16(v.code[v.ip : v.ip+2])
	v.ip += 2
	return ptr(res)
}

func (v *VM) readInt() int {
	return int(v.readPtr())
}

func (v *VM) readBool() bool {
	return v.next() > 0
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

func (v *VM) getStackValueByAddress(p ptr) reflect.Value {
	return v.stack[p]
}

func (v *VM) setStackValueByAddress(p ptr, rv reflect.Value) {
	v.stack[p] = rv
}

func (v *VM) elem(a reflect.Value) reflect.Value {
	return reflect.Indirect(a)
}

func (v *VM) store(t reflect.Value, s reflect.Value) {
	v.elem(t).Set(v.elem(s))
}

func (v *VM) ipAddrArg() ptr {
	return v.readPtr().abs(v.ip)
}

func (v *VM) strStackAddr() string {
	return v.readPtr().format("bp")
}

func (v *VM) strIpAddr() string {
	return fmt.Sprintf("%d", v.readPtr().abs(v.ip))
}

func (v *VM) next() byte {
	a := v.code[v.ip]
	v.ip++
	return a
}

func (v *VM) pop() reflect.Value {
	ret := v.stack[v.sp]
	v.sp--
	return ret
}

func (v *VM) popRaw() any {
	ret := v.stack[v.sp]
	v.sp--
	return ret
}

func (v *VM) peek() any {
	return v.stack[v.sp]
}

func (v *VM) push(rv reflect.Value) {
	v.sp++
	v.stack[v.sp] = rv
}

func (v *VM) goTo(p ptr) {
	v.ip = int(p.abs(v.ip))
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
)
