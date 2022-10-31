package tlvm

var stdMacroses string

func registerMacros(m string) string {
	stdMacroses += "\n" + m
	return m
}

var appendAndSetMacro = registerMacros(`
(defmacro appendvs (v e)
	` + "`(setq ,v (appendv ,v ,e))" + `
	)
`)

var forRangeMacro = registerMacros(`
(defmacro forRange (i from to &rest body)
` + "`(progn " +
	"	(setq ,i ,from) " +
	"	(while (lt ,i ,to) " +
	"		,@body " +
	"		(setq ,i (+ ,i 1)))))" + `
`)

var forEachMacro = registerMacros(`
(defmacro forEach (vec f)
	` + "`(progn (setq ff ,f)" +
	"            (forRange i 0 (len ,vec) (ff (getv ,vec i))))" + `
	)
`)
