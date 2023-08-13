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
	stack                       [256]any
	code                        []byte         // byte code
	cp                          int            // constants top pointer
	ep                          int            // entry point
	bp                          int            // base pointer
	sp                          int            // stack pointer
	ip                          int            // instruction pointer
	env                         map[any]ptr    // environment variables pointers
	debugInfo                   map[int]string // debug string by instruction position
	originalTextPositionPointer map[int]int    // position in original code text by instruction position
}

const callFrameOffset = 4

func NewVM(output *VMByteCode) *VM {
	vm := &VM{
		code: append(output.definedFunctions, output.code...),
		bp:   -1,
		sp:   -1,
	}
	for i := range output.constList {
		vm.stack[i] = output.constList[i]
	}
	vm.ip = len(output.definedFunctions)
	vm.ep = vm.ip
	vm.bp = len(output.constList)
	vm.sp = len(output.constList) - 1
	vm.cp = vm.sp
	vm.env = output.consts
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
			b.WriteString(fmt.Sprintf("CALL %d %d", v.stackAddrArg(), v.stackAddrArg()))
		case opPopCall:
			b.WriteString(fmt.Sprintf("POPCALL %s", v.strIpAddr()))
		case opExtCall:
			b.WriteString(fmt.Sprintf("EXTCALL %d", v.stackAddrArg()))
		case opRet:
			b.WriteString(fmt.Sprintf("RET"))
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
	vm.stack[pos] = v
}

func (vm *VM) EnvString(k string, v string) {
	vm.stack[vm.env[k]] = v
}

func (v *VM) Result() any {
	return v.stack[v.sp]
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
			a := v.stackAddrArg()
			v.push(v.stack[a])
		case opPushClosureVal:
			a := v.closureAddr()
			v.push(*v.closure()[a])
		case opStoreClosureVal:
			vv := v.pop()
			a := v.closureAddr()
			vptr := v.closure()[a]
			*vptr = vv
		case opPushClosure:
			ip := v.ipAddrArg()
			n := v.readInt()
			rest := v.readBool()

			nClosureVars := v.readInt()
			var closureVars []*any
			for i := 0; i < nClosureVars; i++ {
				vt := valType(v.next())
				varPtr := v.readPtr()
				switch vt {
				case valTypeClosure:
					closureVars = append(closureVars, v.closure()[varPtr.abs(0)])
				case valTypeLocal:
					vv := v.stack[varPtr.abs(v.bp)]
					closureVars = append(closureVars, &vv)
				case valTypeGlobal:
					closureVars = append(closureVars, &v.stack[varPtr.abs(v.bp)])
				default:
					errorx.Panic(errorx.IllegalState.New("unexpected value type %d in closure", vt))
				}
			}

			v.push(&closure{addr: ip, nargs: n, varargs: rest, values: closureVars})
		case opPushField:
			variableAddr := v.stackAddrArg()
			fieldPathAddr := v.stackAddrArg()
			vv := v.stack[variableAddr]
			path := v.stack[fieldPathAddr].(string)
			rv := reflect.ValueOf(vv)

			for _, p := range strings.Split(path, ".") {
				rv = rv.FieldByName(p)
			}
			v.push(rv.Interface())
		case opStore:
			vv := v.pop()
			a := v.stackAddrArg()
			v.stack[a] = vv
		case opCmp:
			v2 := v.pop()
			v1 := v.pop()

			chFl := v.next()
			switch v1.(type) {
			case float64, float32:
				v.push(cmp(castFloat(v1), castFloat(v2), chFl))
			case int64, int32, int16, int:
				v.push(cmp(castInt(v1), castInt(v2), chFl))
			case string:
				v.push(cmp(v1.(string), v2.(string), chFl))
			}
		case opAdd:
			v1 := v.pop()
			v2 := v.pop()
			switch vt := v2.(type) {
			case float64, float32:
				v.push(castFloat(vt) + castFloat(v1))
			case int64, int, int32:
				v.push(castInt(vt) + castInt(v1))
			case string:
				v.push(vt + v1.(string))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %T for ADD operation", vt))
			}
		case opSub:
			v1 := v.pop()
			v2 := v.pop()
			switch vt := v2.(type) {
			case float64, float32:
				v.push(castFloat(vt) - castFloat(v1))
			case int64, int, int32:
				v.push(castInt(vt) - castInt(v1))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %T for SUB operation", vt))
			}
		case opDiv:
			v1 := v.pop()
			v2 := v.pop()
			switch vt := v2.(type) {
			case float64, float32:
				v.push(castFloat(vt) / castFloat(v1))
			case int64, int, int32:
				v.push(castInt(vt) / castInt(v1))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %T for DIV operation", vt))
			}
		case opMul:
			v1 := v.pop()
			v2 := v.pop()
			switch vt := v2.(type) {
			case float64, float32:
				v.push(castFloat(vt) * castFloat(v1))
			case int64, int, int32:
				v.push(castInt(vt) * castInt(v1))
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %T for MUL operation", vt))
			}
		case opCmpBool:
			v1 := v.pop().(bool)
			v2 := v.pop().(bool)
			chFl := v.next()

			if chFl&cmpFlagEq > 0 {
				v.push(v1 == v2)
			}
		case opTrue:
			v1 := v.pop().(bool)
			chFl := v.next()
			if chFl&cmpFlagEq > 0 {
				v.push(v1)
			}
		case opNil:
			v.push(v.pop() == nil)
		case opBr:
			v1 := v.pop().(bool)
			addr := v.readPtr()
			if !v1 {
				v.goTo(addr)
			}
		case opPop:
			v.pop()
		case opExtCall:
			fn := v.pop().(reflect.Value)
			nargs := v.readInt()
			args := make([]reflect.Value, nargs)
			for i := 0; i < nargs; i++ {
				args[nargs-i-1] = reflect.ValueOf(v.pop())
			}
			values := fn.Call(args)
			v.push(values[0].Interface())
		case opClosureCall:
			a := v.stackAddrArg()
			cl := v.stack[a].(*closure)
			nargs := v.readInt()
			if nargs != cl.nargs {
				errorx.Panic(errorx.IllegalState.New("illegal arguments count to call function"))
			}
			v.pushRestArgIfNeeded(nargs, cl)

			addr := cl.addr
			v.push(cl.nargs)
			v.push(cl.values)
			v.push(v.bp)
			v.push(v.ip)
			v.bp = v.sp - callFrameOffset
			v.goTo(addr)
		case opPopCall:
			cl := v.pop().(*closure)
			nargs := v.readInt()

			if !cl.varargs && nargs != cl.nargs ||
				cl.varargs && nargs < cl.nargs {
				errorx.Panic(errorx.IllegalState.New("illegal arguments count to call function"))
			}
			v.pushRestArgIfNeeded(nargs, cl)
			addr := cl.addr
			v.push(cl.nargs)
			v.push(cl.values)
			v.push(v.bp)
			v.push(v.ip)
			v.bp = v.sp - callFrameOffset
			v.goTo(addr)
		case opCall:
			addr := v.readPtr()
			nargs := v.readInt()
			v.push(nargs)
			v.push(nil)
			v.push(v.bp)
			v.push(v.ip)
			v.bp = v.sp - callFrameOffset
			v.goTo(addr)
		case opRet:
			result := v.pop()
			v.sp = v.bp + callFrameOffset
			v.ip = v.pop().(int)
			v.bp = v.pop().(int)
			v.pop() // skip closure values
			nargs := v.pop().(int)
			v.sp -= nargs
			v.push(result)
		case opJmp:
			v.goTo(v.readPtr())
		case opNot:
			v.push(!v.pop().(bool))
		case opCons:
			first := v.pop()
			second := v.pop()
			v.push(&cons{
				first:  first,
				second: second,
			})
		case opCar:
			c := v.pop().(*cons)
			v.push(c.first)

		case opCdr:
			c := v.pop().(*cons)
			v.push(c.second)
		case opPrint:
			c := v.pop()
			fmt.Printf("%v\n", c)
		case opSplice:
			c := v.pop().(*cons)
			prev := v.pop()

			l := consToList(c)
			for i := 0; i < len(l); i++ {
				prev = &cons{
					first:  l[len(l)-1-i],
					second: prev,
				}
			}
			v.push(prev)
		case opMakeHashTable:
			v.push(make(map[any]any))
		case opSetHashTableValue:
			m := v.pop().(map[any]any)
			k := v.pop()
			v := v.pop()
			m[k] = v
		case opGetHashTableValue:
			m := v.pop().(map[any]any)
			i := v.pop()
			v.push(m[i])
		case opMakeVector:
			v.push(make([]any, 0))
		case opSetVectorValue:
			m := v.pop().([]any)
			i := v.pop().(int64)
			v := v.pop()
			m[i] = v
		case opGetVectorValue:
			vec := v.pop()
			i := v.pop().(int64)
			var pv any
			switch vect := vec.(type) {
			case string:
				pv = string([]rune(vect)[i])
			case []any:
				pv = vect[i]
			default:
				errorx.Panic(errorx.IllegalArgument.New("unexpected type %T for GETV operation", vect))
			}
			v.push(pv)
		case opAppend:
			m := v.pop()
			n := v.pop()

			switch vv := m.(type) {
			case []any:
				vv = append(vv, n)
				m = vv
			case *cons:
				lastCons(vv).second = &cons{first: n}
			}
			v.push(m)
		case opLen:
			var l int
			switch tv := v.pop().(type) {
			case []any:
				l = len(tv)
			case map[any]any:
				l = len(tv)
			case string:
				l = len(tv)
			case *cons:
				l = len(consToList(tv))
			default:
				panic(errorx.Panic(errorx.IllegalArgument.New("can't get length from type %T", tv)))
			}
			v.push(l)
		case opContains:
			container := v.pop()
			m := v.pop()

			var res bool
			switch c := container.(type) {
			case map[any]any:
				_, res = c[m]
			case []any:
				for _, e := range c {
					if e == m {
						res = true
						break
					}
				}
			}
			v.push(res)
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
	var prev any
	prev = nil
	for i := 0; i < nargs-cl.nargs+1; i++ {
		c := &cons{}
		c.second = prev
		c.first = v.pop()
		prev = c
	}
	v.push(prev)

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

func (v *VM) stackAddrArg() ptr {
	return v.readPtr().abs(v.bp)
}

func (v *VM) closureAddr() ptr {
	return v.readPtr().abs(0)
}

func (v *VM) closure() []*any {
	return v.stack[v.bp+2].([]*any)
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

func (v *VM) pop() any {
	ret := v.stack[v.sp]
	v.sp--
	return ret
}

func (v *VM) peek() any {
	return v.stack[v.sp]
}

func (v *VM) push(b any) {
	v.sp++
	v.stack[v.sp] = b
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

func castInt(v any) int64 {
	switch vt := v.(type) {
	case int64:
		return vt
	case int32:
		return int64(vt)
	case int16:
		return int64(vt)
	case int8:
		return int64(vt)
	case int:
		return int64(vt)
	default:
		panic(errorx.Panic(errorx.IllegalArgument.New("can't cast %T to int", v)))
	}
}

func castFloat(v any) float64 {
	switch vt := v.(type) {
	case int64:
		return float64(vt)
	case int:
		return float64(vt)
	case int32:
		return float64(vt)
	case int16:
		return float64(vt)
	case int8:
		return float64(vt)
	case float64:
		return vt
	case float32:
		return float64(vt)
	default:
		panic(errorx.Panic(errorx.IllegalArgument.New("can't cast %T to int", v)))
	}
}
