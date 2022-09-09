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
	errVMInstructionProperty   = errorx.RegisterProperty("vmInstruction")
)

type opCode byte

const (
	opPush opCode = iota
	opPushAddr
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
	opDynamicCall
	opExtCall
	opRet
	opHalt
	opCons
	opCar
	opCdr
	opNil
	opPrint
)

const (
	boolTrue  bool = true
	boolFalse bool = false
)

const (
	cmpFlagEq          byte = 1 << 0
	cmpFlagGt          byte = 1 << 1
	cmpFlagLt          byte = 1 << 2
	addrShiftRightFlag int  = 1 << 15
	addrShiftLeftFlag  int  = 1 << 14
)

type vm struct {
	stack                       [256]any
	code                        []byte         // byte code
	cp                          int            // constants top pointer
	ep                          int            // entry point
	bp                          int            // base pointer
	sp                          int            // stack pointer
	ip                          int            // instruction pointer
	env                         map[any]int    // environment variables pointers
	debugInfo                   map[int]string // debug string by instruction position
	originalTextPositionPointer map[int]int    // position in original code text by instruction position
}

func NewVM(output *VMByteCode) *vm {
	vm := &vm{
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

func (v *vm) CodeString() string {
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
		case opPushAddr:
			b.WriteString(fmt.Sprintf("PUSHADDR %s", v.strIpAddr()))
		case opPushField:
			b.WriteString(fmt.Sprintf("PUSHFIELD %s %s", v.strStackAddr(), v.strStackAddr()))
		case opDynamicCall:
			b.WriteString(fmt.Sprintf("DYNCALL %s %d", v.strStackAddr(), v.stackAddrArg()))
		case opStore:
			b.WriteString(fmt.Sprintf("STORE %s", v.strStackAddr()))
		case opAdd:
			b.WriteString(fmt.Sprintf("ADD"))
		case opSub:
			b.WriteString(fmt.Sprintf("ADD"))
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

func (v *vm) Copy() vm {
	nv := *v
	return nv
}

func (vm *vm) EnvInt(k string, v int) {
	vm.Env(k, int64(v))
}

func (vm *vm) EnvInt64(k string, v int64) {
	vm.Env(k, v)
}

func (vm *vm) EnvFloat32(k string, v float32) {
	vm.Env(k, float64(v))
}

func (vm *vm) EnvFloat64(k string, v float64) {
	vm.Env(k, v)
}

func (vm *vm) Env(k any, v any) {
	pos, ok := vm.env[k]
	if !ok {
		return
	}
	vm.stack[pos] = v
}

func (vm *vm) EnvString(k string, v string) {
	vm.stack[vm.env[k]] = v
}

func (v *vm) Result() any {
	return v.stack[v.sp]
}

func (v *vm) Reset() {
	v.ip = v.ep
	v.bp = v.cp + 1
	v.sp = v.cp
}

func (v *vm) Execute() (errRes error) {
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
		case opPushAddr:
			v.push(v.ipAddrArg())
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
			v2 := v.pop()
			v1 := v.pop()
			switch vt := v2.(type) {
			case float64, float32:
				v.push(castFloat(vt) + castFloat(v1))
			case int64, int, int32:
				v.push(castInt(vt) + castInt(v1))
			case string:
				v.push(vt + v1.(string))
			default:
				panic(fmt.Sprintf("unexpected type %T", vt))
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
				panic(fmt.Sprintf("unexpected type %T", vt))
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
				panic(fmt.Sprintf("unexpected type %T", vt))
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
				panic(fmt.Sprintf("unexpected type %T", vt))
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
			addr := v.arg()
			if !v1 {
				v.goTo(addr)
			}
		case opPop:
			v.pop()
		case opExtCall:
			fn := v.pop().(reflect.Value)
			nargs := v.arg()
			var args []reflect.Value
			for i := 0; i < int(nargs); i++ {
				args = append(args, reflect.ValueOf(v.pop()))
			}
			values := fn.Call(args)
			v.push(values[0].Interface())
		case opDynamicCall:
			a := v.stackAddrArg()
			addr := v.stack[a].(int)
			nargs := v.arg()
			v.push(nargs)
			v.push(v.bp)
			v.push(v.ip)
			v.bp = v.sp - 3
			v.goTo(addr)
		case opCall:
			addr := v.arg()
			nargs := v.arg()
			v.push(nargs)
			v.push(v.bp)
			v.push(v.ip)
			v.bp = v.sp - 3
			v.goTo(addr)
		case opRet:
			result := v.pop()
			v.sp = v.bp + 3
			v.ip = v.pop().(int)
			v.bp = v.pop().(int)
			nargs := v.pop().(int)
			v.sp -= nargs
			v.push(result)
		case opJmp:
			v.goTo(v.arg())
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
		case opHalt:
			return
		}
	}

	return nil
}

func (v *vm) arg() int {
	res := binary.BigEndian.Uint16(v.code[v.ip : v.ip+2])
	v.ip += 2
	return int(res)
}

func (v *vm) stackAddrArg() int {
	return v.addrFromArg(v.bp, v.arg())
}

func (v *vm) ipAddrArg() int {
	return v.addrFromArg(v.ip, v.arg())
}

func (v *vm) strStackAddr() string {
	a := int(v.arg())
	if addrShiftLeftFlag&a > 0 {
		return fmt.Sprintf("bp-%d", a&^addrShiftLeftFlag)
	} else if addrShiftRightFlag&a > 0 {
		return fmt.Sprintf("bp+%d", a&^addrShiftRightFlag)
	} else {
		return fmt.Sprintf("%d", a)
	}
}

func (v *vm) strIpAddr() string {
	return fmt.Sprintf("%d", v.addrFromArg(v.ip, v.arg()))
}

func (v *vm) addrFromArg(base int, arg int) int {
	if addrShiftLeftFlag&arg > 0 {
		return base - arg&^addrShiftLeftFlag
	} else if addrShiftRightFlag&arg > 0 {
		return base + arg&^addrShiftRightFlag
	} else {
		return arg
	}
}

func (v *vm) next() byte {
	a := v.code[v.ip]
	v.ip++
	return a
}

func (v *vm) pop() any {
	ret := v.stack[v.sp]
	v.sp--
	return ret
}

func (v *vm) push(b any) {
	v.sp++
	v.stack[v.sp] = b
}

func (v *vm) goTo(addr int) {
	if addrShiftRightFlag&addr > 0 {
		v.ip += addr &^ addrShiftRightFlag
	} else if addrShiftLeftFlag&addr > 0 {
		v.ip -= addr &^ addrShiftLeftFlag
	} else {
		v.ip = addr
	}
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
