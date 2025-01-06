package tlvm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkVM(b *testing.B) {
	code := `
(defun fact (n) 
	(if (lt n 1) 
		1
		(* n (fact (- n 1))
		)
	)
)
(fact 15)
`

	bin, err := Compile(code)
	require.NoError(b, err)

	vm := NewVM(bin)
	require.NoError(b, vm.Execute())
	require.EqualValues(b, fact(15), vm.Result())

	b.Run("FactVM", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vm.Reset()
			vm.Execute()
		}
	})

	b.Run("BaseExprVM", func(b *testing.B) {
		code := `
(and 
	(or (eq Origin "MOW") (eq Country "RU")) 
	(or (eq Adults 1) (gte Value 100))
)
`
		bin, err := Compile(code, EnvVariables("Origin", "Country", "Value", "Adults"))
		require.NoError(b, err)

		vm := NewVM(bin)
		vm.EnvString("Origin", "MOW")
		vm.EnvString("Country", "RU")
		vm.EnvInt("Value", 100)
		vm.EnvInt("Adults", 1)
		require.NoError(b, vm.Execute())
		require.EqualValues(b, true, vm.Result())
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vm.Reset()
			vm.Execute()
		}
	})

	b.Run("BaseExprGO", func(b *testing.B) {
		f := func(Origin, Country string, Value, Adults int) bool {
			return (Origin == "MOW" || Country == "RU") && (Adults == 1 || Value >= 100)
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			f("MOW", "RU", 100, 1)
		}
	})

	b.Run("FactGo", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			fact(15)
		}
	})

	b.Run("ConcatVM", func(b *testing.B) {
		bin, err := Compile(`(+ "a" "b")`)
		require.NoError(b, err)
		vm := NewVM(bin)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vm.Reset()
			vm.Execute()
		}
	})

	b.Run("ConcatGO", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = "a" + "b"
		}
	})
}
