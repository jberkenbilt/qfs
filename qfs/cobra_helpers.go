package qfs

type validator struct {
	typ   string
	value string
	fn    func(string) error
}

func (v *validator) String() string {
	return v.value
}

func (v *validator) Set(s string) error {
	if err := v.fn(s); err != nil {
		return err
	}
	v.value = s
	return nil
}

func (v *validator) Type() string {
	return v.typ
}

func newValidator(typ string, fn func(string) error) *validator {
	return &validator{
		typ: typ,
		fn:  fn,
	}
}
