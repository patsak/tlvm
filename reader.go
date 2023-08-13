package tlvm

import (
	"strconv"
	"unicode"

	"github.com/joomcode/errorx"
)

const (
	keywordComma         = "comma"
	keywordSplice        = "splice"
	keywordQuote         = "quote"
	keywordBacktick      = "backtick"
	keywordRest          = "&rest"
	keywordDefun         = "defun"
	keywordDefmacro      = "defmacro"
	keywordMacroexpand   = "macroexpand"
	keywordWhile         = "while"
	keywordLambda        = "lambda"
	keywordProgn         = "progn"
	keywordPrint         = "print"
	keywordSetq          = "setq"
	keywordIf            = "if"
	keywordOr            = "or"
	keywordNot           = "not"
	keywordAnd           = "and"
	keywordList          = "list"
	keywordLen           = "len"
	keywordDoList        = "dolist"
	keywordAppendv       = "append"
	keywordGetV          = "getv"
	keywordSetV          = "setv"
	keywordMakeVector    = "make-vector"
	keywordMakeHashTable = "make-hash-table"
	keywordGetH          = "geth"
	keywordSetH          = "seth"
	keywordLt            = "lt"
	keywordGt            = "gt"
	keywordEq            = "eq"
	keywordMul           = "*"
	keywordDiv           = "/"
	keywordPlus          = "+"
	keywordSub           = "-"
	keywordContains      = "contains"
)

func Read(sourceCode string) (SExpressions, error) {
	tokens, err := tokenize(sourceCode)
	if err != nil {
		return nil, err
	}
	reader := tokenReader{tokens: tokens}
	var res SExpressions
	for reader.hasNext() {
		r, err := reader.read()
		if err != nil {
			return nil, err
		}
		if r == nil {
			continue
		}
		res = append(res, r)
	}
	return res, nil
}

type token struct {
	value string
	pos   int
}

func tokenize(text string) ([]token, error) {
	if err := validateBrackets(text); err != nil {
		return nil, err
	}
	runes := []rune(text)
	var res []token

	for i := 0; i < len(runes); i++ {
		r := runes[i]
		var t token
		switch {
		case unicode.IsSpace(r):
			continue
		case r == ',' && i < len(runes) && runes[i+1] == '@':
			t = token{string(runes[i : i+2]), i}
			i++
		case r == ')', r == '(', r == '\'', r == '`', r == ',':
			t = token{string(r), i}
		case r == ';':
			j := i + 1
			for ; runes[j] != '\n' && j < len(runes); j++ {
			}
			t = token{string(runes[i:j]), i}
			i = j - 1
		case r == '"':
			j := i + 1
			for ; runes[j] != '"' && j < len(runes); j++ {
				if runes[j] == '\\' && j+1 >= len(runes) {
					j++
				}
			}
			t = token{string(runes[i : j+1]), i}
			i = j
		default:
			j := i + 1
			for ; j < len(runes) && isLiteral(runes[j]); j++ {
			}
			t = token{string(runes[i:j]), i}
			i = j - 1
		}
		res = append(res, t)
	}
	return res, nil
}

func isLiteral(rn rune) bool {
	return unicode.IsDigit(rn) || unicode.IsLetter(rn) || rn == '.' || rn == '+' || rn == '-'
}

func validateBrackets(text string) error {
	balance := 0
	lastBalancePosition := 0
	for i, r := range []rune(text) {
		if r == '(' {
			balance++
		}
		if r == ')' {
			balance--
		}
		if balance < 0 {
			return errorx.IllegalArgument.New("redundant bracket").WithProperty(errRawTextPositionProperty, i)
		}
		if balance == 0 {
			lastBalancePosition = i
		}
	}
	if balance > 0 {
		return errorx.IllegalArgument.New("can't find right bracket").WithProperty(errRawTextPositionProperty, lastBalancePosition)
	}
	return nil
}

type tokenReader struct {
	tokens []token
	pos    int
}

func (r *tokenReader) hasNext() bool {
	return r.pos < len(r.tokens)
}

func (r *tokenReader) peek() token {
	return r.tokens[r.pos]
}

func (r *tokenReader) next() token {
	res := r.tokens[r.pos]
	r.pos++
	return res
}

func (r *tokenReader) read() (_ any, err error) {
	for r.hasNext() {
		tok := r.next()

		switch {
		case tok.value == "(":
			firstCons := &cons{}
			if r.hasNext() && r.peek().value != ")" {
				firstCons.first, err = r.read()
				if err != nil {
					return nil, err
				}
			}
			currentCons := firstCons
			for r.hasNext() && r.peek().value != ")" {
				nextCons := &cons{}
				nextCons.first, err = r.read()
				if err != nil {
					return nil, err
				}
				currentCons.second = nextCons
				currentCons = nextCons
			}
			if r.hasNext() {
				r.next()
			}
			return firstCons, nil
		case tok.value == "'":
			return r.wrapInCons(keywordQuote, tok.pos)
		case tok.value == "`":
			return r.wrapInCons(keywordBacktick, tok.pos)
		case tok.value == ",@":
			return r.wrapInCons(keywordSplice, tok.pos)
		case tok.value == ",":
			return r.wrapInCons(keywordComma, tok.pos)
		case tok.value[0] == ';':
			continue
		case tok.value[0] == '"' && tok.value[len(tok.value)-1] == '"':
			s, err := strconv.Unquote(tok.value)
			if err != nil {
				panic(err)
			}
			return str{s, r.pos}, nil
		case tok.value == "true" || tok.value == "false":
			v, err := strconv.ParseBool(tok.value)
			if err != nil {
				panic(err)
			}
			return boolean{v, r.pos}, nil
		case unicode.IsDigit(rune(tok.value[0])) || ((tok.value[0] == '-' || tok.value[0] == '+') && len(tok.value) > 1):
			n, err := strconv.ParseInt(tok.value, 10, 64)
			if err != nil {
				f, err := strconv.ParseFloat(tok.value, 64)
				if err != nil {
					return nil, errorx.Decorate(err, "parse number error").WithProperty(errRawTextPositionProperty, r.pos)
				}
				return float{f, tok.pos}, nil
			} else {
				return number{n, tok.pos}, nil
			}
		default:
			return literal{tok.value, tok.pos}, nil
		}
	}

	return nil, nil
}

func (r *tokenReader) wrapInCons(name string, pos int) (any, error) {
	s, err := r.read()
	if err != nil {
		return nil, err
	}
	return &cons{first: literal{name, pos}, second: s}, nil
}
