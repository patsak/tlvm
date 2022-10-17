package tlvm

var forRangeMacro = `
(defmacro forRange (i from to &rest body) 
` + "`(progn (setq ,i ,from) (while (lt ,i ,to) ,@body)))" + `
`
