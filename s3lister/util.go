package s3lister

import (
	"fmt"
	"log/slog"
	"time"
)

func nth(r []rune, n int) rune {
	if n >= len(r) {
		return 0
	}
	return r[n]
}

// firstDifference returns the first index at which the two rune arrays differ.
// If the runes are equal or one is a subset of the other, the return value is
// the length of the shorter array.
func firstDifference(r1, r2 []rune) int {
	minLen := len(r1)
	if t := len(r2); t < minLen {
		minLen = t
	}
	for i := 0; i < minLen; i++ {
		if r1[i] != r2[i] {
			return i
		}
	}
	return minLen
}

// stringDistance returns a number that can be used as a heuristic for how far
// two strings would be from each other in a uniformly distributed space of
// sorted strings with common prefixes. You can compare two pairs of strings to
// find which pair has the shorter gap between them. The difference is computed
// based on the first two differing characters, so the difference between "a" and
// "b" is the same as the distance between "wwwwa" and "wwwwb". In this way,
// clusters of distributed keys with common prefixes will be bisected with the
// same priority as top-level keys.
func stringDistance(s1, s2 string) uint64 {
	if s1 == s2 {
		return 0
	}
	if s1 > s2 {
		s1, s2 = s2, s1
	}
	r1 := []rune(s1)
	r2 := []rune(s2)
	firstDiff := firstDifference(r1, r2)
	d11 := nth(r1, firstDiff)
	d12 := nth(r1, firstDiff+1)
	d21 := nth(r2, firstDiff)
	d22 := nth(r2, firstDiff+1)
	return uint64((int64(d21-d11) << 32) + int64(d22-d12))
}

// stringMidpoint returns a new string that lies roughly halfway between two
// other strings. If the strings are off from each other by only a single
// position, the ~ character is appended to the common prefix + the lower
// character.
func stringMidpoint(s1, s2 string) string {
	if s1 == s2 {
		return s1
	}
	if s1 > s2 {
		s1, s2 = s2, s1
	}
	r1 := []rune(s1)
	r2 := []rune(s2)
	firstDiff := firstDifference(r1, r2)
	d1 := nth(r1, firstDiff)
	d2 := nth(r2, firstDiff)
	common := r1[:firstDiff]
	var mid uint32
	if d1 == 0 {
		// The first string is a prefix of the second string. Append half the next
		// character's offset from the beginning of printable characters, subject to
		// validation.
		mid = 0x10 + uint32(d2/2)
	} else if d2-d1 == 1 {
		// These are adjacent characters. Add something to the next rune. We use the
		// midpoint of the printable ASCII range (0x20 to 0x7E)
		common = append(common, d1)
		mid = 0x4F + uint32(nth(r1, firstDiff+1))
	} else {
		mid = uint32(d1+d2) / 2

	}
	// Filter out invalid codepoints. Ranges FDD0 to FDEF, D800 to
	// DFFF, and anything that ends with FFFE or FFFF are invalid
	// codepoints. Round up or down by splitting the difference.
	if mid >= 0xFDD0 && mid <= 0xFDDF {
		mid = 0xFDCF
	} else if mid >= 0xFDE0 && mid <= 0xFDEF {
		mid = 0xFDF0
	} else if mid >= 0xD800 && mid <= 0xDBFF {
		mid = 0xD7FF
	} else if mid >= 0xDC00 && mid <= 0xDFFF {
		mid = 0xE000
	} else if mid&0xFFFF == 0xFFFE {
		mid--
	} else if mid&0xFFFF == 0xFFFF {
		mid++
	} else if mid < 0x20 {
		mid = 0x20
	}
	if mid > 0x10FFFD {
		mid = 0x10FFFD
	}
	common = append(common, rune(mid))
	midpoint := string(common)
	if midpoint <= s1 || midpoint >= s2 {
		// If the characters are too close or we messed up, return the smaller string.
		midpoint = s1
	}
	return midpoint
}

// escapeUnicode formats a string with non-ASCII Unicode escaped.
func escapeUnicode(s string) string {
	r := fmt.Sprintf("%+q", s)
	return r[1 : len(r)-1]
}

// retryOnError retries fn if it returns an error with exponential fallback a
// maximum of maxTries times. The first failure results in a delay of
// initialDelay, and the delay is doubled for subsequent retries. The `what`
// parameter should work in "error from {what}; retrying".
func retryOnError(
	logger *slog.Logger,
	what string,
	maxTries int,
	initialDelay time.Duration,
	fn func() error,
) error {
	tries := 0
	delay := initialDelay
	for {
		tries++
		err := fn()
		if err == nil {
			return nil
		} else if tries < maxTries {
			// allow log suppression for test suite
			logger.Info("error from "+what+"; retrying", "error", err)
			time.Sleep(delay)
			delay *= 2
		} else {
			return fmt.Errorf("error from %s: %w", what, err)
		}
	}
}
