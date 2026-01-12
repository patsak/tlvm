package tlvm

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommonOperators(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    string
		options []CompileOption
		result  any
		wantErr bool
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
			name:   "sumManyArgs",
			code:   "(+ 1 2 3 4 5)",
			result: 15,
		},
		{
			name:   "sumSingleArg",
			code:   "(+ 5)",
			result: 5,
		},
		{
			name:   "sumFloats",
			code:   "(+ 1.25 2.5)",
			result: 3.75,
		},
		{
			name:   "sumMixedIntFloat",
			code:   "(+ 1 2.5)",
			result: 3.5,
		},
		{
			name:    "sumInvalidTypes",
			code:    `(+ "a" 1)`,
			wantErr: true,
		},
		{
			name:   "div",
			code:   "(/ 2.4 2)",
			result: 1.2,
		},
		{
			name:   "divIntsToFloat",
			code:   "(/ 5 2)",
			result: 2.5,
		},
		{
			name:   "divMixedIntFloat",
			code:   "(/ 5 2.0)",
			result: 2.5,
		},
		{
			name:    "divInvalidTypes",
			code:    `(/ "a" 2)`,
			wantErr: true,
		},
		{
			name:   "sub",
			code:   "(- 10 2)",
			result: 8,
		},
		{
			name:   "subFloats",
			code:   "(- 10.5 2.25)",
			result: 8.25,
		},
		{
			name:   "subMixedIntFloat",
			code:   "(- 10 2.5)",
			result: 7.5,
		},
		{
			name:    "subInvalidTypes",
			code:    `(- "a" 2)`,
			wantErr: true,
		},
		{
			name:   "mulInts",
			code:   "(* 3 7)",
			result: 21,
		},
		{
			name:   "mulFloats",
			code:   "(* 2.5 4)",
			result: 10.0,
		},
		{
			name:    "mulInvalidTypes",
			code:    `(* "a" 2)`,
			wantErr: true,
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
(defun inc (acc in)
  (lambda (n)
    (setq acc (+ acc in n))
    (lambda (v)
      (setq acc (+ acc n v)))))
(setq base (inc 1 1)) 
(setq plus1 (base 1)) ; 1 + 1 + 1 = 3
(setq plus2 (base 2)) ; 3 + 1 + 2 = 6 
(plus1 1) ; 6 + 1 + 1 = 8
(plus2 1) ; 8 + 2 + 1 = 11
(plus2 2) ; 11 + 2 + 2 = 15
`,
			result: 15,
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
		{
			name:   "lambdaAsFirstElement",
			code:   `((lambda (a) (+ a 1)) 1)`,
			result: 2,
		},
		{
			name:   "rest",
			code:   `((lambda (a &rest b) b) 1 2 3)`,
			result: "(2 3)",
		},
		{
			name:   "restArg",
			code:   `((lambda (a &rest b) b) 1 2 3)`,
			result: "(2 3)",
		},
		{
			name: "hashTable",
			code: `
(setq t (make-hash-table))
(seth t "foo1" "bar1")
(seth t "foo2" "bar2")
(seth t "foo3" "bar3")
(seth t "foo4" "bar4")
(seth t "foo5" "bar5")
(geth t "foo1")
`,
			result: "bar1",
		},
		{
			name: "containsHashTable",
			code: `
(setq t (make-hash-table))
(seth t "foo1" "bar1")
(contains t "foo1")
`,
			result: true,
		},
		{
			name: "containsHashTableMissing",
			code: `
(setq t (make-hash-table))
(seth t "foo1" "bar1")
(contains t "foo2")
`,
			result: false,
		},
		{
			name: "vectorAppend",
			code: stdMacroses + `
(setq t (make-vector))
(appendvs t "bar1")
(appendvs t "bar2")
(appendvs t "bar3")
(appendvs t "bar4")
(getv t 1)
`,
			result: "bar2",
		},
		{
			name: "vectorUpdate",
			code: stdMacroses + `
(setq t (make-vector))
(appendvs t "bar1")
(appendvs t "bar2")
(setv t 1 "bar3")
(getv t 1)
`,
			result: "bar3",
		},
		{
			name: "containsVector",
			code: stdMacroses + `
(setq t (make-vector))
(appendvs t "foo1")
(contains t "foo1")
`,
			result: true,
		},
		{
			name: "containsVectorMapValue",
			code: stdMacroses + `
(setq t (make-vector))
(setq h (make-hash-table))
(seth h "foo" "bar")
(appendvs t h)
(contains t h)
`,
			result: true,
		},
		{
			name: "containsVectorMapValueMissing",
			code: stdMacroses + `
(setq t (make-vector))
(setq h1 (make-hash-table))
(setq h2 (make-hash-table))
(seth h1 "foo" "bar")
(appendvs t h1)
(contains t h2)
`,
			result: false,
		},
		{
			name: "lenVector",
			code: stdMacroses + `
(setq t (make-vector))
(appendvs t "bar1")
(len t)
`,
			result: 1,
		},
		{
			name: "lenList",
			code: stdMacroses + `
(len (list 1 2 3 4))
`,
			result: 4,
		},
		{
			name: "lenHashTable",
			code: `
(setq t (make-hash-table))
(seth t "foo1" "bar1")
(seth t "foo2" "bar2")
(len t)
`,
			result: 2,
		},
		{
			name: "globalVariable",
			code: `
(setq s 10)
(setq f (lambda (a) (setq s (+ s a))))
(defun inc () (setq s (+ s 1)))
(f 10)
(inc 0)
`,
			result: 21,
		},
		{
			name: "updateOriginalValueInClosure",
			code: `
(setq s 10)
(defun inc () (setq s (+ s 1)))
(inc)
(inc)
s
`,
			result: 12,
		},
		{
			name: "globalVariableManyClosures",
			code: `
(setq s 10)
(setq f (lambda (a) (setq s (+ s a))))
(defun x2 () (f s))
(f 10) ;; s == 20
(x2) ;; s = 20 + 20
(x2) ;; s = 40 + 40
s
`,
			result: 80,
		},
		{
			name: "appendList",
			code: `
(append (list 1 2 3) 4)
`,
			result: "(1 2 3 4)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vmCode, err := Compile(tc.code, tc.options...)
			require.NoError(t, err)

			vm := NewVM(vmCode)
			fmt.Printf("%s\n", vm.CodeString())
			err = vm.Execute()
			if tc.wantErr {
				require.Error(t, err, tc.code)
				return
			}
			require.NoError(t, err, tc.code)
			switch vv := vm.Result().(type) {
			case cons, *cons:
				require.EqualValues(t, tc.result, fmt.Sprintf("%s", vv))
			case *int64:
				require.EqualValues(t, tc.result, *vv)
			default:
				require.EqualValues(t, tc.result, vv)
			}
		})
	}
}

func TestTailCallOptimizationLargeN(t *testing.T) {
	text := `
(defun sumTCO (n acc)
	(if (lt n 1)
		acc
		(progn
			(setq acc (+ acc n))
			(sumTCO (- n 1) acc))))
(defun sumNoTCO (n)
	(if (lt n 1)
		0
		(+ n (sumNoTCO (- n 1)))))
`
	const n = 1000
	expected := fmt.Sprint(int64(n*(n+1)) / 2)

	require.Equal(t, expected, compileAndRun(t, text+fmt.Sprintf("(sumTCO %d 0)", n)))

	vmCode := compile(t, text+fmt.Sprintf("(sumNoTCO %d)", n))
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

	t.Run("String", func(t *testing.T) {
		vm := NewVM(compile(t, "(eq foo \"bar\")", EnvVariables("foo")))
		vm.Env("foo", "bar")
		require.NoError(t, vm.Execute())
		require.Equal(t, true, vm.Result())
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

	t.Run("AssignStructField", func(t *testing.T) {
		type input struct {
			C int64
		}
		v := input{C: 3}
		vm := NewVM(compile(t, "(setq n.C (+ n.C 1))", EnvVariables("n")))
		vm.Env("n", &v)
		require.NoError(t, vm.Execute())
		require.EqualValues(t, 4, vm.Result().(int64))
		require.EqualValues(t, 4, v.C)
	})

	t.Run("NotExists", func(t *testing.T) {
		_, err := Compile("(+ n 1)")
		require.Error(t, err)
	})
}

func TestMacroExpand(t *testing.T) {
	for _, tc := range []struct {
		name           string
		expectedExpand string
		macros         string
		input          string
	}{
		{
			name: "MacroSplice",
			macros: `(defmacro spl (l)
			` + "`(list ,@l 2 3)" + `
		)`,
			input:          `(spl ((+ 1 0) 1)))`,
			expectedExpand: "(list (+ 1 0) 1 2 3)",
		},
		{
			name:           "Comma",
			macros:         `(defmacro plus (a b c) ` + "`" + `(+ ,a ,b ,c ,(+ 5 6)))`,
			input:          "(plus (+ 1 2) (+ 3 4) 5))",
			expectedExpand: "(+ (+ 1 2) (+ 3 4) 5 11)",
		},
		{
			name:           "RestArgs",
			macros:         forRangeMacro,
			input:          "(forRange i 0 10 (setq new (+ acc i)) (setq acc new)))",
			expectedExpand: "(progn (setq i 0) (while (lt i 10) (setq new (+ acc i)) (setq acc new) (setq i (+ i 1))))",
		},
		{
			name:           "ForEach",
			macros:         forEachMacro,
			input:          "(forEach v (lambda (a) (+ c a))))",
			expectedExpand: "(progn (setq ff (lambda (a) (+ c a))) (forRange i 0 (len v) (ff (getv v i))))",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.EqualValues(t, tc.expectedExpand, compileAndRun(t, tc.macros+`
	(macroexpand '`+tc.input))
		})
	}
}

func TestStdMacro(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    string
		options []CompileOption
		result  any
	}{
		{
			name: "forEach",
			code: `
(setq v "abc")
(setq c "")

(forEach v (lambda (a) (setq c (+ c a))))
c
`,
			result: "abc",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vmCode, err := Compile(stdMacroses+tc.code, tc.options...)
			require.NoError(t, err)
			vm := NewVM(vmCode)
			require.NoError(t, vm.Execute(), tc.code)
			switch vm.Result().(type) {
			case *cons:
				require.EqualValues(t, tc.result, fmt.Sprintf("%s", vm.Result()), "unexpected result for code: \n%s", vm.CodeString())
			default:
				require.EqualValues(t, tc.result, vm.Result(), "unexpected result for code: \n%s", vm.CodeString())
			}
		})
	}
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

(defmacro spl (&rest a)
     ` + "`(list ,@a 1 1)" + `
)

(defun increment (x) 
	(setq ll (lambda (a c) (+ a (fact c))))
	(setq k 15)
	(setq q x)

	(dolist (k (spl (+ 1 0) 1)) ; 3+7+15+31
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

func TestReaderEdgeCases(t *testing.T) {
	t.Run("CommentAtEOF", func(t *testing.T) {
		_, err := Compile(`(+ 1 2) ; comment without trailing newline`)
		require.NoError(t, err)
	})
	t.Run("UnterminatedString", func(t *testing.T) {
		_, err := Compile(`"abc`)
		require.Error(t, err)
	})
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
	fmt.Printf("%s\n", vm.CodeString())
	require.NoError(t, vm.Execute())
	return fmt.Sprintf("%v", vm.Result())
}

func fact(n int) int {
	if n <= 1 {
		return 1
	}
	return n * fact(n-1)
}
