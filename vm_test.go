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

	b.Run("VM", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vm.Reset()
			vm.Execute()
		}
	})

	b.Run("Go", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			fact(15)
		}
	})
}

func fact(n int) int {
	if n <= 1 {
		return 1
	}
	return n * fact(n-1)
}
