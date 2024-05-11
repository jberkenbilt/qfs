package s3source

import "testing"

// This package is primarily tested through repo_test.

func TestPathRe(t *testing.T) {
	type testCase struct {
		path    string
		base    string
		fType   string
		modTime string
		rest    string
	}
	cases := []testCase{
		{
			path:    ".@d,123,0777",
			base:    ".",
			fType:   "d",
			modTime: "123",
			rest:    "0777",
		},
		{
			path:    "a@@@d,123,0777",
			base:    "a@@",
			fType:   "d",
			modTime: "123",
			rest:    "0777",
		},
		{
			path:    "a@@@d,123,0777",
			base:    "a@@",
			fType:   "d",
			modTime: "123",
			rest:    "0777",
		},
		{
			path:    "a@@@l,123,@@one@@two@@f,123,456",
			base:    "a@@",
			fType:   "l",
			modTime: "123",
			rest:    "@@one@@two@@f,123,456",
		},
		{
			path:    "a@.@d,123,0777",
			base:    "",
			fType:   "",
			modTime: "",
			rest:    "",
		},
		{
			path:    "a@.@l,123,0777@f,123,0666",
			base:    "",
			fType:   "",
			modTime: "",
			rest:    "",
		},
	}

	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			m := pathRe.FindStringSubmatch(c.path)
			if c.base == "" {
				if m != nil {
					t.Errorf("path unexpectedly matched: %#v", m)
				}
			} else {
				if m[1] != c.base {
					t.Errorf("wrong base: %s", m[1])
				}
				if m[2] != c.fType {
					t.Errorf("wrong type: %s", m[2])
				}
				if m[3] != c.modTime {
					t.Errorf("wrong time: %s", m[3])
				}
				if m[4] != c.rest {
					t.Errorf("wrong remainder: %s", m[4])
				}
			}
		})
	}
}
