package types

type Pair[L, R interface{}] struct {
	Left L
	Right R
}
