package parser

import "testing"

// FuzzParseInterval ensures ParseInterval never panics and handles
// a variety of inputs gracefully (returning either a duration or error).
func FuzzParseInterval(f *testing.F) {
	// Seeds with valid and edge syntaxes
	f.Add("interval '1 hour'")
	f.Add("'2 days'::interval")
	f.Add("make_interval(hours => 1, mins => 30)")
	f.Add("interval '0 seconds'")
	f.Add("INTERVAL '1 YEAR 2 MONTHS 3 DAYS'")
	f.Add("interval 1 hour") // relaxed, fallback parse
	f.Add("not an interval")

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ParseInterval(s)
	})
}
