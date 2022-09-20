package tlvm

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommonOperators(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    string
		options []CompileOption
		result  any
	}{
		{
			name:   "setq",
			code:   "(setq n 1) n",
			result: 1,
		},
		{
			name:   "setqEvalParameter",
			code:   "(setq n (+ 1 1)) n",
			result: 2,
		},
		{
			name:   "sum",
			code:   "(+ 2 4)",
			result: 6,
		},
		{
			name:   "div",
			code:   "(/ 2.4 2)",
			result: 1.2,
		},
		{
			name:   "sub",
			code:   "(- 10 2)",
			result: 8,
		},
		{
			name:   "dolist",
			code:   "(setq acc 0) (dolist (k (list 1 2 3)) (setq acc (+ k acc))) acc",
			result: 6,
		},
		{
			name:   "defun1",
			code:   "(defun f (a) (+ a 1)) (f 10)",
			result: 11,
		},
		{
			name:   "defun2",
			code:   "(defun f (a b) (+ a b)) (f 10 20)",
			result: 30,
		},
		{
			name:   "if",
			code:   "(setq a 15) (if (gt a 10) 1 0)",
			result: 1,
		},
		{
			name:   "andTrue",
			code:   "(and (gt 10 1) (gt 10 9))",
			result: true,
		},
		{
			name:   "andFalse",
			code:   "(and (gt 10 1) (gt 1 10))",
			result: false,
		},
		{
			name:   "orTrue",
			code:   "(or (gt 1 1) (gt 10 1))",
			result: true,
		},
		{
			name:   "orFalse",
			code:   "(or (gt 1 10) (gt 1 10) (gt 1 20))",
			result: false,
		},
		{
			name:   "list",
			code:   "(list 10 20 30)",
			result: "(10 20 30)",
		},
		{
			name:   "lambda",
			code:   "(setq ll (lambda (x) (+ x 10))) (ll 10)",
			result: 20,
		},
		{
			name: "lambdaArgument",
			code: `
(defun fold (l f) (setq acc 0) (dolist (k l) (setq acc (f k acc))) acc)
(fold (list 10 20 30 40) (lambda (x y) (+ x y)))
`,
			result: 100,
		},
		{
			name: "closure",
			code: `
(defun inc (start) 
	(lambda () 
		(setq start (+ start 1))))
(setq plus (inc 10))
(plus)
(plus)
`,
			result: 12,
		},
		{
			name: "closureWithArgs",
			code: `
(defun inc (start) 
	(lambda (v) 
		(setq start (+ start v))))
(setq plus (inc 10))
(plus 10)
(plus 15)
`,
			result: 35,
		},
		{
			name: "closureMultipleArgs",
			code: `
(defun inc (n acc) 
	(lambda (v) 
		(setq acc (+ acc n v))))
(setq plus (inc 15 10))
(plus 10)
(plus 15)
`,
			result: 65,
		},
		{
			name: "closureInternalLambdas",
			code: `
(defun inc (acc)
  (lambda (n)
    (lambda (v)
      (setq acc (+ acc n v)))))
(setq base (inc 10))
(setq plus15 (base 15))
(setq plus10 (base 10))
(plus10 10) ; 0 + 10 + 10 = 20
(plus15 10) ; 20 + 15 + 10 = 45
(plus15 15) ; 45 + 15 + 15 = 75
`,
			result: 85,
		},
		{
			name: "externalFunction",
			code: `(match "[a-z]+" "aaa")`,
			options: []CompileOption{ExtFunctionsOrPanic(map[string]any{
				"match": func(r, s string) bool {
					return regexp.MustCompile(r).MatchString(s)
				},
			})},
			result: true,
		},
		{
			name:   "while",
			code:   `(setq i 0) (setq acc 0) (while (lt i 5) (setq acc (+ acc i)) (setq i (+ i 1))) acc`,
			result: 0 + 1 + 2 + 3 + 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vmCode, err := Compile(tc.code, tc.options...)
			require.NoError(t, err)

			vm := NewVM(vmCode)
			require.NoError(t, vm.Execute(), tc.code)
			switch vm.Result().(type) {
			case *cons:
				require.EqualValues(t, tc.result, fmt.Sprintf("%s", vm.Result()))
			default:
				require.EqualValues(t, tc.result, vm.Result())
			}
		})
	}
}

func TestTailCallOptimization(t *testing.T) {
	text := `
(defun factTCO (n acc) 
	(if (lt n 1) 
		acc
		(progn
			(setq acc (* n acc))
			(factTCO (- n 1) acc))))
(defun factNoTCO (n) 
	(if (lt n 1) 
		1
		(* n (factNoTCO (- n 1)))))
 `
	assert.Equal(t, fmt.Sprint(fact(50)), compileAndRun(t, text+`(factTCO 50 1)`))
	assert.Equal(t, fmt.Sprint(fact(15)), compileAndRun(t, text+`(factTCO 15 1)`))
	assert.Equal(t, fmt.Sprint(fact(15)), compileAndRun(t, text+`(factNoTCO 15)`))

	vmCode := compile(t, text+`(factNoTCO 50)`)
	vm := NewVM(vmCode)
	require.Error(t, vm.Execute())
}

func TestEnvVariables(t *testing.T) {
	t.Run("SimpleVariable", func(t *testing.T) {
		vm := NewVM(compile(t, "(+ n 1)", EnvVariables("n")))
		vm.EnvInt64("n", 1)
		require.NoError(t, vm.Execute())
		require.EqualValues(t, 2, vm.Result())
	})

	t.Run("Struct", func(t *testing.T) {
		type input struct {
			C int64
		}
		vm := NewVM(compile(t, "(+ n.C 1)", EnvVariables("n")))
		vm.Env("n", input{C: 3})
		require.NoError(t, vm.Execute())
		require.EqualValues(t, 4, vm.Result())
	})

	t.Run("NotExists", func(t *testing.T) {
		_, err := Compile("(+ n 1)")
		require.Error(t, err)
	})
}

func TestMacroExpand(t *testing.T) {
	require.EqualValues(t, "(list (+ 1 0) 1 1 1)", compileAndRun(t, `(defmacro spl ()
	    `+"`(list ,@(list (+ 1 0) 1) 1 1)"+`
	)
	(macroexpand '(spl))`))

	require.EqualValues(t, "(+ (+ 1 2) (+ 3 4) 5 11)", compileAndRun(t, `
(defmacro plus (a b c) 
`+"`(+ ,a ,b ,c ,(+ 5 6)))"+`
(macroexpand '(plus (+ 1 2) (+ 3 4) 5))`))
}

func TestSmoke(t *testing.T) {
	origText := `
(defun fact (n) 
	(setq next (- n 1))
	(if (gt n 0) 
		(* n (fact next)) 
		(progn 
			(+ 0 1)
			1
			)
	)
)

(defmacro spl ()
     ` + "`(list ,@(list (+ 1 0) 1) 1 1)" + `
)

(defun increment (x) 
	(setq ll (lambda (a c) (+ a (fact c))))
	(setq k 15)
	(setq q x)
	(dolist (k (spl)) ; 3+7+15+31
		(setq q (+ q (ll q k))))
	(+ q k))

(increment 1)
`

	res, err := Compile(origText)
	require.NoError(t, err)
	vm := NewVM(res)
	require.NoError(t, vm.Execute())

	require.EqualValues(t, 46, vm.Result())
}

func compileAndRun(t *testing.T, rawText string) string {
	return run(t, compile(t, rawText))
}

func compile(t *testing.T, text string, opts ...CompileOption) *VMByteCode {
	res, err := Compile(text, opts...)
	require.NoError(t, err)
	return res
}

func run(t *testing.T, code *VMByteCode) string {
	vm := NewVM(code)
	require.NoError(t, vm.Execute())
	return fmt.Sprintf("%v", vm.Result())
}

func fact(n int) int {
	if n <= 1 {
		return 1
	}
	return n * fact(n-1)
}
