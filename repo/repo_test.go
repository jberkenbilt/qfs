package repo_test

import (
	"github.com/jberkenbilt/qfs/repo"
	"testing"
)

func TestXXX(t *testing.T) {
	t.Skipf("XXX")
	r, err := repo.New("quack", "src", "", "http://localhost:9000")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = r.ReadDir("")
	if err != nil {
		t.Fatalf(err.Error())
	}
}
