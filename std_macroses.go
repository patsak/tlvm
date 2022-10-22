package tlvm

var stdMacroses string

func registerMacros(m string) string {
	stdMacroses += "\n" + m
	return m
}

var forRangeMacro = registerMacros(`
(defmacro forRange (i from to &rest body) 
` + "`(progn (setq ,i ,from) (while (lt ,i ,to) ,@body)))" + `
`)

var appendAndSetMacro = registerMacros(`
(defmacro appendvs (v e)
	` + "`(setq ,v (appendv ,v ,e))" + `
	)
`)
