package tlvm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// Public API tests
// -----------------------------------------------------------------------------

func TestBuild(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		vm, err := Build("(+ 1 2)")
		require.NoError(t, err)
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 3, vm.Result())
	})
	t.Run("CompileError", func(t *testing.T) {
		_, err := Build("(setq")
		require.Error(t, err)
	})
}

func TestVMReset(t *testing.T) {
	vm := mustBuild(t, "(setq x (+ 1 2)) x")
	require.NoError(t, vm.Execute(context.Background()))
	require.EqualValues(t, 3, vm.Result())

	// Reset and rerun must produce the same result without re-compiling.
	vm.Reset()
	require.NoError(t, vm.Execute(context.Background()))
	require.EqualValues(t, 3, vm.Result())
}

func TestVMCopy(t *testing.T) {
	vm := mustBuild(t, "(+ 1 2)")
	require.NoError(t, vm.Execute(context.Background()))

	clone := vm.Copy()
	clone.Reset()
	require.NoError(t, clone.Execute(context.Background()))
	require.EqualValues(t, 3, clone.Result())
	// Original is unaffected by clone's mutation.
	require.EqualValues(t, 3, vm.Result())
}

func TestEnv_AllTypedHelpers(t *testing.T) {
	t.Run("EnvInt", func(t *testing.T) {
		vm := newEnvVM(t, "(+ n 1)", "n")
		vm.EnvInt("n", 41)
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 42, vm.Result())
	})

	t.Run("EnvInt64", func(t *testing.T) {
		vm := newEnvVM(t, "(+ n 1)", "n")
		vm.EnvInt64("n", 41)
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 42, vm.Result())
	})

	t.Run("EnvFloat32", func(t *testing.T) {
		vm := newEnvVM(t, "(+ n 0.5)", "n")
		vm.EnvFloat32("n", 1.25)
		require.NoError(t, vm.Execute(context.Background()))
		require.InDelta(t, 1.75, vm.Result(), 1e-6)
	})

	t.Run("EnvFloat64", func(t *testing.T) {
		vm := newEnvVM(t, "(+ n 0.5)", "n")
		vm.EnvFloat64("n", 1.25)
		require.NoError(t, vm.Execute(context.Background()))
		require.InDelta(t, 1.75, vm.Result(), 1e-9)
	})

	t.Run("EnvString", func(t *testing.T) {
		vm := newEnvVM(t, `(eq foo "bar")`, "foo")
		vm.EnvString("foo", "bar")
		require.NoError(t, vm.Execute(context.Background()))
		require.Equal(t, true, vm.Result())
	})

	t.Run("Env_UnknownKey_NoOp", func(t *testing.T) {
		// Env silently ignores keys that were not registered as EnvVariables.
		vm := mustBuild(t, "(+ 1 2)")
		vm.Env("unknown", 123) // must not panic
		require.NoError(t, vm.Execute(context.Background()))
	})

	t.Run("EnvString_UnknownKey_NoOp", func(t *testing.T) {
		// Regression: EnvString used to silently overwrite stack[0] when the
		// key was unknown. Must now no-op like Env.
		vm := mustBuild(t, "(+ 1 2)")
		vm.EnvString("unknown", "x") // must not panic, must not corrupt state
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 3, vm.Result())
	})
}

// -----------------------------------------------------------------------------
// CodeString smoke test — ensures every opcode the compiler may emit has
// a String case in vm.CodeString().
// -----------------------------------------------------------------------------

func TestCodeStringCoversAllEmittedOpcodes(t *testing.T) {
	// Build a program that touches every keyword we know the compiler emits.
	code := stdMacroses + `
(defstruct point (X :type int) (Y :type int))
(setq s 0)
(defun add (a b) (+ a b))
(setq cl (lambda (x) (+ x 1)))
(setq h (make-hash-table))
(seth h "k" "v")
(geth h "k")
(setq v (make-vector))
(appendvs v 1)
(setv v 0 2)
(getv v 0)
(contains v 2)
(len v)
(setq i 0)
(while (lt i 1) (setq i (+ i 1)))
(dolist (k (list 1 2 3)) (setq s (+ s k)))
(if (eq 1 1) (progn (setq s (- s 1)) s) 0)
(append (list 1) 2)
(not (eq 1 2))
(and true (or false true))
(setq p (make point))
(setq p.X 1)
p.X
`
	bin, err := Compile(code)
	require.NoError(t, err)
	vm := NewVM(bin)
	dump := vm.CodeString()
	require.NotEmpty(t, dump)
	// CodeString must not leave any opcode unprinted (would panic on unknown).
	require.NotContains(t, dump, "unknown")
}

// -----------------------------------------------------------------------------
// External function validation
// -----------------------------------------------------------------------------

func TestExtFunctions_ValidationErrors(t *testing.T) {
	t.Run("NotAFunction", func(t *testing.T) {
		_, err := ExtFunctions(map[string]any{"x": 123})
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be function")
	})

	t.Run("MultiReturnValues", func(t *testing.T) {
		_, err := ExtFunctions(map[string]any{
			"x": func() (int, int) { return 1, 2 },
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "single value")
	})

	t.Run("ValidSingleReturn", func(t *testing.T) {
		opt, err := ExtFunctions(map[string]any{
			"double": func(x int64) int64 { return x * 2 },
		})
		require.NoError(t, err)
		vm, err := Build(`(double 21)`, opt)
		require.NoError(t, err)
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 42, vm.Result())
	})
}

func TestExtFunctionsOrPanic_Panics(t *testing.T) {
	require.Panics(t, func() {
		ExtFunctionsOrPanic(map[string]any{"x": 1})
	})
}

// -----------------------------------------------------------------------------
// Defstruct / make validation
// -----------------------------------------------------------------------------

func TestDefstruct_Validation(t *testing.T) {
	t.Run("LowercaseFieldName", func(t *testing.T) {
		_, err := Compile(`(defstruct p (x :type int))`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "uppercase")
	})

	t.Run("UnknownFieldAttr", func(t *testing.T) {
		_, err := Compile(`(defstruct p (X :foo bar))`)
		require.Error(t, err)
	})

	t.Run("UnknownTypeAttr", func(t *testing.T) {
		_, err := Compile(`(defstruct p (X :type blob))`)
		require.Error(t, err)
	})

	t.Run("MakeUnknownStruct", func(t *testing.T) {
		_, err := Compile(`(make unknown)`)
		require.Error(t, err)
	})

	t.Run("AllTypedFields", func(t *testing.T) {
		// int / float / string / untyped all in one definition.
		vm := mustBuild(t, `
(defstruct rec (I :type int) (F :type float) (S :type string) (A))
(setq r (make rec))
(setq r.I 7)
(setq r.F 1.5)
(setq r.S "hi")
(setq r.A 42)
r.I
`)
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 7, vm.Result())
	})
}

// -----------------------------------------------------------------------------
// Compile-time errors from emitCallFunction / emitLiteral
// -----------------------------------------------------------------------------

func TestCompile_UnknownFunction(t *testing.T) {
	_, err := Compile(`(no-such-fn 1 2)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown function")
}

func TestCompile_UnknownLiteral(t *testing.T) {
	_, err := Compile(`(+ unknown 1)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown literal")
}

// -----------------------------------------------------------------------------
// Runtime argument count checks for closures / macros
// -----------------------------------------------------------------------------

func TestArgCountErrors(t *testing.T) {
	t.Run("ClosureWrongArgs", func(t *testing.T) {
		vm := mustBuild(t, `(setq f (lambda (a b) (+ a b))) (f 1)`)
		err := vm.Execute(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "illegal arguments count")
	})

	t.Run("MacroTooFewArgs", func(t *testing.T) {
		// 'plus' macro requires 3 args; pass 1.
		src := `(defmacro plus (a b c) ` + "`" + `(+ ,a ,b ,c)) (plus 1)`
		_, err := Compile(src)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be equal")
	})

	t.Run("MacroRestNeedsMin", func(t *testing.T) {
		// forRange requires at least 3 fixed args (i, from, to) before &rest body.
		_, err := Compile(stdMacroses + `(forRange i 0)`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be greater than")
	})

	t.Run("DefunRestWithoutName", func(t *testing.T) {
		// `&rest` not followed by a parameter name used to crash with an
		// index-out-of-range panic; it must now produce a clear error.
		_, err := Compile(`(defun f (a &rest) a)`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "&rest")
	})

	t.Run("LambdaRestWithoutName", func(t *testing.T) {
		_, err := Compile(`(lambda (&rest) 1)`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "&rest")
	})

	t.Run("MacroRestWithoutName", func(t *testing.T) {
		_, err := Compile("(defmacro m (a &rest) `(+ ,a 1))")
		require.Error(t, err)
		require.Contains(t, err.Error(), "&rest")
	})
}

// -----------------------------------------------------------------------------
// List operations
// -----------------------------------------------------------------------------

func TestListOperations(t *testing.T) {
	t.Run("NestedList", func(t *testing.T) {
		require.Equal(t, "(1 (2 3) 4)", compileAndRun(t, "(list 1 (list 2 3) 4)"))
	})

	t.Run("AppendToList", func(t *testing.T) {
		// `append` adds the new element to the tail of the displayed list.
		require.Equal(t, "(1 2 3 0)", compileAndRun(t, "(append (list 1 2 3) 0)"))
	})

	t.Run("AppendToSingletonList", func(t *testing.T) {
		require.Equal(t, "(1 0)", compileAndRun(t, "(append (list 1) 0)"))
	})

	t.Run("EmptyList_Result", func(t *testing.T) {
		vm := mustBuild(t, "(list)")
		require.NoError(t, vm.Execute(context.Background()))
		require.Nil(t, vm.Result())
	})

	t.Run("EmptyList_Len", func(t *testing.T) {
		require.Equal(t, "0", compileAndRun(t, "(len (list))"))
	})

	t.Run("DolistOverEmptyList", func(t *testing.T) {
		vm := mustBuild(t, "(setq acc 7) (dolist (k (list)) (setq acc (+ acc 1))) acc")
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 7, vm.Result())
	})

	t.Run("CdrOfSingleton_IsNilSafe", func(t *testing.T) {
		// dolist exits cleanly after the single element — exercises the
		// previously crashing opCdr-then-opNil path.
		vm := mustBuild(t, "(setq acc 0) (dolist (k (list 42)) (setq acc k)) acc")
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 42, vm.Result())
	})
}

// -----------------------------------------------------------------------------
// Hash table operations — missing keys, overwrite
// -----------------------------------------------------------------------------

func TestHashTable_MissingKey(t *testing.T) {
	// `geth` on missing key returns a zero stackValue. The VM result API
	// dereferences it; for our purposes we just assert that execution
	// succeeds and we don't crash.
	vm := mustBuild(t, `
(setq h (make-hash-table))
(seth h "a" 1)
(geth h "missing")
`)
	require.NoError(t, vm.Execute(context.Background()))
}

func TestHashTable_Overwrite(t *testing.T) {
	require.EqualValues(t, "v2", compileAndRun(t, `
(setq h (make-hash-table))
(seth h "k" "v1")
(seth h "k" "v2")
(geth h "k")
`))
}

// -----------------------------------------------------------------------------
// String operations
// -----------------------------------------------------------------------------

func TestStringOps(t *testing.T) {
	t.Run("Concat3", func(t *testing.T) {
		require.Equal(t, "abc", compileAndRun(t, `(+ "a" "b" "c")`))
	})

	t.Run("LenEmpty", func(t *testing.T) {
		require.Equal(t, "0", compileAndRun(t, `(len "")`))
	})

	t.Run("GetvFirstChar", func(t *testing.T) {
		require.Equal(t, "a", compileAndRun(t, `(getv "abc" 0)`))
	})

	t.Run("GetvLastChar", func(t *testing.T) {
		require.Equal(t, "c", compileAndRun(t, `(getv "abc" 2)`))
	})

	t.Run("EqEmptyStrings", func(t *testing.T) {
		require.Equal(t, "true", compileAndRun(t, `(eq "" "")`))
	})

	t.Run("CmpStringsLte", func(t *testing.T) {
		require.Equal(t, "true", compileAndRun(t, `(lte "ab" "ab")`))
	})
}

// -----------------------------------------------------------------------------
// Tail-call optimization detail
// -----------------------------------------------------------------------------

func TestTCO_RecognisedOnDirectRecursion(t *testing.T) {
	src := `
(defun loop (n)
  (if (lt n 1)
      "done"
      (loop (- n 1))))
(loop 200)`
	require.Equal(t, "done", compileAndRun(t, src))
}

func TestDynamicStack_DeepNonTCORecursion(t *testing.T) {
	// Exercises stack growth far beyond the old fixed 256-slot limit.
	const n = 500
	src := `
(defun countdown (n)
  (if (lt n 1) 0 (+ 1 (countdown (- n 1)))))
`
	vm := mustBuild(t, src+fmt.Sprintf("(countdown %d)", n))
	require.NoError(t, vm.Execute(context.Background()))
	require.EqualValues(t, n, vm.Result())
	require.Greater(t, len(vm.stack), 256, "stack should grow for deep frames")
}

// -----------------------------------------------------------------------------
// `if` without else
// -----------------------------------------------------------------------------

func TestIfWithoutElse(t *testing.T) {
	// `if` with no else branch and a false condition still produces some
	// result (the branch instruction skips the then-body without pushing).
	// We just assert it compiles and runs without panicking.
	vm := mustBuild(t, "(setq x 1) (if (gt 1 2) (setq x 99)) x")
	require.NoError(t, vm.Execute(context.Background()))
	require.EqualValues(t, 1, vm.Result())
}

// -----------------------------------------------------------------------------
// Multi-arg + operator with mixed types
// -----------------------------------------------------------------------------

func TestSumChain_MixedNumeric(t *testing.T) {
	require.Equal(t, "10.5", compileAndRun(t, "(+ 1 2 3 4 0.5)"))
}

func TestQuoteAndBacktick(t *testing.T) {
	t.Run("Quote_Atom", func(t *testing.T) {
		// quoting an atom literal yields the literal itself.
		require.Equal(t, "foo", compileAndRun(t, `'foo`))
	})

	t.Run("Backtick_NoInterp", func(t *testing.T) {
		require.Equal(t, "(1 2 3)", compileAndRun(t, "`(1 2 3)"))
	})

	t.Run("Backtick_WithComma", func(t *testing.T) {
		// ,(+ 1 1) is evaluated and spliced in as `2`.
		require.Equal(t, "(1 2 3)", compileAndRun(t, "`(1 ,(+ 1 1) 3)"))
	})

	t.Run("Backtick_WithSplice", func(t *testing.T) {
		// ,@ splices another list inline.
		require.Equal(t, "(1 2 3 4)", compileAndRun(t, "`(1 ,@(list 2 3) 4)"))
	})
}

// -----------------------------------------------------------------------------
// Custom macro definition + usage end-to-end
// -----------------------------------------------------------------------------

func TestCustomMacro_Roundtrip(t *testing.T) {
	src := `
(defmacro inc (x) ` + "`" + `(+ ,x 1))
(setq a 4)
(setq a (inc a))
(setq a (inc a))
a`
	require.Equal(t, "6", compileAndRun(t, src))
}

// -----------------------------------------------------------------------------
// Negative numbers in source
// -----------------------------------------------------------------------------

func TestNegativeLiterals(t *testing.T) {
	require.Equal(t, "-5", compileAndRun(t, "(+ -3 -2)"))
	require.Equal(t, "true", compileAndRun(t, "(lt -10 -1)"))
}

// -----------------------------------------------------------------------------
// progn with single + multiple expressions
// -----------------------------------------------------------------------------

func TestProgn(t *testing.T) {
	t.Run("Single", func(t *testing.T) {
		require.Equal(t, "42", compileAndRun(t, "(progn 42)"))
	})
	t.Run("ScopeIsLexicalNotStackFrame", func(t *testing.T) {
		// variables defined in progn are visible after the progn block
		// (progn introduces a lexical scope, but setq writes to enclosing frame).
		vm := mustBuild(t, `
(setq x 0)
(progn (setq x 1) (setq x (+ x 1)))
x`)
		require.NoError(t, vm.Execute(context.Background()))
		require.EqualValues(t, 2, vm.Result())
	})
}

// -----------------------------------------------------------------------------
// While loop multiple body forms
// -----------------------------------------------------------------------------

func TestWhile_MultipleBodyForms(t *testing.T) {
	src := `
(setq i 0)
(setq acc 0)
(while (lt i 3)
  (setq acc (+ acc i))
  (setq acc (+ acc 10))
  (setq i (+ i 1)))
acc`
	// iters: i=0 -> acc=10, i=1 -> acc=21, i=2 -> acc=33
	require.Equal(t, "33", compileAndRun(t, src))
}

// -----------------------------------------------------------------------------
// Nested dolist (regression: previously crashed / produced wrong result
// because the loop kept a saved cursor on the stack outside the body, which
// made compile-time addresses drift from runtime sp).
// -----------------------------------------------------------------------------

func TestNestedDolist(t *testing.T) {
	src := `
(setq sum 0)
(dolist (a (list 1 2 3))
  (dolist (b (list 10 20))
    (setq sum (+ sum (* a b)))))
sum`
	// (1+2+3) * (10+20) = 6 * 30 = 180
	require.Equal(t, "180", compileAndRun(t, src))
}

func TestDolist_MultiStatementBody(t *testing.T) {
	// Each body statement leaves a value on the stack; dolist must pop all
	// of them per iteration, not just the last one.
	src := `
(setq sum 0)
(setq prod 1)
(dolist (k (list 1 2 3 4))
  (setq sum (+ sum k))
  (setq prod (* prod k)))
(+ sum prod)`
	// sum = 10, prod = 24 → 34
	require.Equal(t, "34", compileAndRun(t, src))
}

func TestDolist_ReturnsLastBoundValue(t *testing.T) {
	require.Equal(t, "3", compileAndRun(t, "(dolist (k (list 1 2 3)) k)"))
}

// -----------------------------------------------------------------------------
// Lambda captures (incl. transitive capture through an intermediate lambda
// that does NOT itself reference the captured variable — previously broken
// with "unexpected value type 0 in closure").
// -----------------------------------------------------------------------------

func TestLambda_CapturesAtEveryLevel(t *testing.T) {
	src := `
(defun mk (acc)
  (lambda (n)
    (setq acc (+ acc n))
    (lambda (v)
      (setq acc (+ acc n v))
      acc)))
(setq step ((mk 1) 2))   ; acc = 1 + 2 = 3
(step 4)                 ; acc = 3 + 2 + 4 = 9
`
	require.Equal(t, "9", compileAndRun(t, src))
}

func TestLambda_TransitiveCapture_Global(t *testing.T) {
	// Inner lambda captures `a` (a top-level/global variable) while the
	// middle lambda's body does not reference `a` at all.
	src := `
(setq a 10)
(setq mid (lambda () (lambda (x) (+ a x))))
(setq inner (mid))
(inner 5)`
	require.Equal(t, "15", compileAndRun(t, src))
}

func TestLambda_TransitiveCapture_DefunLocal(t *testing.T) {
	// Inner lambda captures `acc` from the enclosing defun frame; middle
	// lambda's body never mentions `acc`.
	src := `
(defun mk (acc)
  (lambda (n)
    (lambda (v) (+ acc n v))))
(setq inner ((mk 1) 2))
(inner 3)`
	require.Equal(t, "6", compileAndRun(t, src))
}

// -----------------------------------------------------------------------------
// Print: just smoke-test that programs containing `print` compile and run.
// We redirect to /dev/null in spirit by ignoring stdout — only correctness
// matters here, not the printed value.
// -----------------------------------------------------------------------------

func TestPrint_Compiles(t *testing.T) {
	vm := mustBuild(t, `(print "hello") (+ 1 2)`)
	require.NoError(t, vm.Execute(context.Background()))
	require.EqualValues(t, 3, vm.Result())
}

// -----------------------------------------------------------------------------
// EnableDebugSymbols: when enabled, the bytecode dump should contain
// human-readable annotations after `;`.
// -----------------------------------------------------------------------------

func TestEnableDebugSymbols(t *testing.T) {
	src := `(defun f (a) (+ a 1)) (f 1)`

	withDebug, err := Build(src, EnableDebugSymbols())
	require.NoError(t, err)
	dumpWithDebug := withDebug.CodeString()

	withoutDebug, err := Build(src)
	require.NoError(t, err)
	dumpWithoutDebug := withoutDebug.CodeString()

	require.NotEmpty(t, dumpWithDebug)
	require.NotEmpty(t, dumpWithoutDebug)
	require.Contains(t, dumpWithDebug, "define function f",
		"EnableDebugSymbols must include human-readable debug annotations")
	require.NotContains(t, dumpWithoutDebug, "define function f",
		"without EnableDebugSymbols the dump must not include debug annotations")

	require.NoError(t, withDebug.Execute(context.Background()))
	require.EqualValues(t, 2, withDebug.Result())
}

// -----------------------------------------------------------------------------
// car / cdr / splice nil safety (regression — used to panic in UnsafePointer
// or value-receiver dereference).
// -----------------------------------------------------------------------------

func TestConsOps_NilSafety(t *testing.T) {
	// dolist iterating a single-element list ends up calling (cdr) on a
	// single-element cons (returns nil), then on nil itself — must not panic.
	vm := mustBuild(t, `(dolist (k (list 1)) k)`)
	require.NoError(t, vm.Execute(context.Background()))
	require.EqualValues(t, 1, vm.Result())
}

// -----------------------------------------------------------------------------
// Error: TextPosition is propagated through Execute panics
// -----------------------------------------------------------------------------

func TestExecuteError_HasTextPosition(t *testing.T) {
	// (+ "a" 1) raises an "unexpected types" panic from opAdd; the deferred
	// recover should attach text position info.
	src := `(+ "a" 1)`
	vm, err := Build(src)
	require.NoError(t, err)
	err = vm.Execute(context.Background())
	require.Error(t, err)
	formatted := FormatErrorWithTextPosition(err, src)
	require.Contains(t, formatted, "^")
}

func TestInterrupt(t *testing.T) {
	endlessCode := `
(setq i 0)
(while (lt i 10) (setq i (- i 1)))
`
	vm, err := Build(endlessCode)
	require.NoError(t, err)

	t.Run("Timeout", func(t *testing.T) {
		local := vm.WithTimeout(100 * time.Millisecond)
		local.Reset()
		err = local.Execute(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "interrupted by timeout after")
	})

	t.Run("CancelContext", func(t *testing.T) {
		local := vm
		local.Reset()
		ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err = local.Execute(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "interrupted by context")
	})

	t.Run("ExternalStop", func(t *testing.T) {
		local := vm
		local.Reset()
		go func() {
			time.Sleep(100 * time.Millisecond)
			vm.Stop()
		}()
		err = local.Execute(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "interrupted by stop")
	})

	t.Run("StackOverflow", func(t *testing.T) {
		stackOverflowCode := `
(defun sum (n)
	(+ 1 (sum (+ n 1))))
(sum 0)
`
		local, err := Build(stackOverflowCode)
		require.NoError(t, err)
		local = local.WithMaxStackSize(200)
		err = local.Execute(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "interrupted by stack overflow")
	})

}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func mustBuild(t *testing.T, src string, opts ...CompileOption) *VM {
	t.Helper()
	vm, err := Build(src, opts...)
	require.NoError(t, err, "compile failed:\n%s", src)
	return vm
}

func newEnvVM(t *testing.T, src string, vars ...Label) *VM {
	t.Helper()
	bin, err := Compile(src, EnvVariables(vars...))
	require.NoError(t, err)
	return NewVM(bin)
}
