package bfsa

type Option interface {
	setup(*bucket) error
}

func WithResolver(scheme string, resolver Resolver) Option {
	return optionFunc(func(b *bucket) error {
		b.reg.Register(scheme, resolver)
		return nil
	})
}

// ----------------------------------------------------------------------------

type optionFunc func(*bucket) error

func (f optionFunc) setup(b *bucket) error {
	return f(b)
}
