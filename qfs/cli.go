package qfs

// Command-line Parsing -- our command-line syntax is complex and not well-suited
// to something like go-arg or flag, so parse arguments by hand.

type argParser struct {
}

func (q *Qfs) arg(p *argParser, arg string) error {
	q.dir = arg
	return nil
}

func WithCliArgs(args []string) Options {
	return func(q *Qfs) error {
		parser := &argParser{}
		for _, arg := range args {
			if err := q.arg(parser, arg); err != nil {
				return err
			}
		}
		return nil
	}
}
