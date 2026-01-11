package tlvm

type LazyString func() string

func (ls LazyString) String() string {
	return ls()
}
