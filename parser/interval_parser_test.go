package parser

import (
	"testing"
	"time"
)

// TestParseInterval tests the ParseInterval function with various SQL interval formats.
// Coverage includes all supported formats, units, and error cases as specified in ROADMAP.md.
func TestParseInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expr     string
		expected time.Duration
		wantErr  bool
	}{
		// Basic formats with common units
		{
			name:     "interval with single quotes - hour",
			expr:     "interval '1 hour'",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval with single quotes - hours plural",
			expr:     "interval '2 hours'",
			expected: 2 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval with double quotes - minute",
			expr:     `interval "1 minute"`,
			expected: time.Minute,
			wantErr:  false,
		},
		{
			name:     "interval with double quotes - minutes plural",
			expr:     `interval "15 minutes"`,
			expected: 15 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "interval - second",
			expr:     "interval '30 seconds'",
			expected: 30 * time.Second,
			wantErr:  false,
		},
		{
			name:     "interval - day",
			expr:     "interval '1 day'",
			expected: 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval - days plural",
			expr:     "interval '7 days'",
			expected: 7 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval - week",
			expr:     "interval '1 week'",
			expected: 7 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval - weeks plural",
			expr:     "interval '2 weeks'",
			expected: 14 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval - month (approximate 30 days)",
			expr:     "interval '1 month'",
			expected: 30 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval - months plural",
			expr:     "interval '3 months'",
			expected: 90 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval - year (approximate 365 days)",
			expr:     "interval '1 year'",
			expected: 365 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "interval - years plural",
			expr:     "interval '2 years'",
			expected: 730 * 24 * time.Hour,
			wantErr:  false,
		},

		// Cast format: 'N unit'::interval
		{
			name:     "cast format - hour",
			expr:     "'1 hour'::interval",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "cast format - days",
			expr:     "'7 days'::interval",
			expected: 7 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "cast format - minutes",
			expr:     "'30 minutes'::interval",
			expected: 30 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "cast format with double quotes",
			expr:     `"5 hours"::interval`,
			expected: 5 * time.Hour,
			wantErr:  false,
		},

		// make_interval function format
		{
			name:     "make_interval with hours",
			expr:     "make_interval(hours => 1)",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "make_interval with mins",
			expr:     "make_interval(mins => 30)",
			expected: 30 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "make_interval with secs",
			expr:     "make_interval(secs => 60)",
			expected: 60 * time.Second,
			wantErr:  false,
		},
		{
			name:     "make_interval with days",
			expr:     "make_interval(days => 7)",
			expected: 7 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "make_interval with weeks",
			expr:     "make_interval(weeks => 2)",
			expected: 14 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "make_interval with months",
			expr:     "make_interval(months => 1)",
			expected: 30 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "make_interval with years",
			expr:     "make_interval(years => 1)",
			expected: 365 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "make_interval with multiple parameters",
			expr:     "make_interval(hours => 1, mins => 30)",
			expected: time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "make_interval with complex combination",
			expr:     "make_interval(days => 1, hours => 2, mins => 30, secs => 45)",
			expected: 24*time.Hour + 2*time.Hour + 30*time.Minute + 45*time.Second,
			wantErr:  false,
		},

		// Compound intervals (PostgreSQL supports "1 day 2 hours" format)
		{
			name:     "compound interval - day and hours",
			expr:     "interval '1 day 2 hours'",
			expected: 24*time.Hour + 2*time.Hour,
			wantErr:  false,
		},
		{
			name:     "compound interval - hours and minutes",
			expr:     "interval '3 hours 30 minutes'",
			expected: 3*time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "compound interval - full specification",
			expr:     "interval '1 day 2 hours 30 minutes 45 seconds'",
			expected: 24*time.Hour + 2*time.Hour + 30*time.Minute + 45*time.Second,
			wantErr:  false,
		},

		// Bare interval strings (fallback parsing)
		{
			name:     "bare string - hour",
			expr:     "1 hour",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "bare string - days",
			expr:     "7 days",
			expected: 7 * 24 * time.Hour,
			wantErr:  false,
		},

		// Abbreviated units
		{
			name:     "abbreviated unit - hr",
			expr:     "interval '1 hr'",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "abbreviated unit - min",
			expr:     "interval '30 min'",
			expected: 30 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "abbreviated unit - sec",
			expr:     "interval '45 sec'",
			expected: 45 * time.Second,
			wantErr:  false,
		},

		// Decimal values (PostgreSQL supports fractional intervals)
		{
			name:     "decimal hours",
			expr:     "interval '1.5 hours'",
			expected: time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "decimal days",
			expr:     "interval '0.5 days'",
			expected: 12 * time.Hour,
			wantErr:  false,
		},

		// Case insensitivity
		{
			name:     "uppercase INTERVAL",
			expr:     "INTERVAL '1 HOUR'",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "mixed case",
			expr:     "InTeRvAl '2 HoUrS'",
			expected: 2 * time.Hour,
			wantErr:  false,
		},

		// Whitespace variations
		{
			name:     "extra whitespace",
			expr:     "  interval  '1  hour'  ",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "no whitespace in cast",
			expr:     "'1hour'::interval",
			expected: time.Hour,
			wantErr:  false,
		},

		// Error cases
		{
			name:    "empty string",
			expr:    "",
			wantErr: true,
		},
		{
			name:     "relaxed format - no quotes (fallback parsing)",
			expr:     "interval 1 hour",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "zero interval literal",
			expr:     "interval '0 hours'",
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "zero make_interval",
			expr:     "make_interval(hours => 0)",
			expected: 0,
			wantErr:  false,
		},
		{
			name:    "invalid unit",
			expr:    "interval '1 fortnight'",
			wantErr: true,
		},
		{
			name:    "invalid number",
			expr:    "interval 'abc hours'",
			wantErr: true,
		},
		{
			name:    "missing unit",
			expr:    "interval '5'",
			wantErr: true,
		},
		{
			name:    "malformed make_interval",
			expr:    "make_interval(invalid)",
			wantErr: true,
		},
		{
			name:    "make_interval with invalid parameter",
			expr:    "make_interval(fortnights => 1)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable for parallel execution
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseInterval(tt.expr)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseInterval(%q) expected error, got nil", tt.expr)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseInterval(%q) unexpected error: %v", tt.expr, err)
				return
			}

			if got != tt.expected {
				t.Errorf("ParseInterval(%q) = %v, want %v", tt.expr, got, tt.expected)
			}
		})
	}
}

// TestParseIntervalString tests the parseIntervalString helper function directly.
func TestParseIntervalString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "simple hour",
			input:    "1 hour",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "compound interval",
			input:    "2 days 3 hours",
			expected: 2*24*time.Hour + 3*time.Hour,
			wantErr:  false,
		},
		{
			name:     "all units combined",
			input:    "1 year 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds",
			expected: 365*24*time.Hour + 60*24*time.Hour + 21*24*time.Hour + 4*24*time.Hour + 5*time.Hour + 6*time.Minute + 7*time.Second,
			wantErr:  false,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid format",
			input:   "not an interval",
			wantErr: true,
		},
		{
			name:     "zero hour",
			input:    "0 hours",
			expected: 0,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseIntervalString(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("parseIntervalString(%q) expected error, got nil", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("parseIntervalString(%q) unexpected error: %v", tt.input, err)
				return
			}

			if got != tt.expected {
				t.Errorf("parseIntervalString(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// TestParseMakeInterval tests the parseMakeInterval helper function directly.
func TestParseMakeInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "single parameter - hours",
			input:    "make_interval(hours => 24)",
			expected: 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "multiple parameters",
			input:    "make_interval(days => 7, hours => 12)",
			expected: 7*24*time.Hour + 12*time.Hour,
			wantErr:  false,
		},
		{
			name:     "all parameters",
			input:    "make_interval(years => 1, months => 6, weeks => 2, days => 3, hours => 4, mins => 30, secs => 15)",
			expected: 365*24*time.Hour + 180*24*time.Hour + 14*24*time.Hour + 3*24*time.Hour + 4*time.Hour + 30*time.Minute + 15*time.Second,
			wantErr:  false,
		},
		{
			name:    "invalid format - no parentheses",
			input:   "make_interval",
			wantErr: true,
		},
		{
			name:    "empty parameters",
			input:   "make_interval()",
			wantErr: true,
		},
		{
			name:    "invalid parameter name",
			input:   "make_interval(invalid => 1)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseMakeInterval(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("parseMakeInterval(%q) expected error, got nil", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("parseMakeInterval(%q) unexpected error: %v", tt.input, err)
				return
			}

			if got != tt.expected {
				t.Errorf("parseMakeInterval(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// TestParseInterval_PostgreSQLFormats tests real-world PostgreSQL interval format variations
// as specified in ROADMAP.md line 167-174. This ensures production queries with various
// interval syntaxes are correctly parsed.
func TestParseInterval_PostgreSQLFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expr     string
		expected time.Duration
		wantErr  bool
	}{
		// Quoted intervals (basic single quotes)
		{
			name:     "quoted interval - single quotes",
			expr:     "'1 hour'",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "quoted interval - double quotes",
			expr:     `"2 hours"`,
			expected: 2 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "quoted interval with INTERVAL keyword",
			expr:     "interval '30 minutes'",
			expected: 30 * time.Minute,
			wantErr:  false,
		},

		// Compound intervals (multiple units in one string)
		{
			name:     "compound - day and hour",
			expr:     "'1 day 2 hours'",
			expected: 24*time.Hour + 2*time.Hour,
			wantErr:  false,
		},
		{
			name:     "compound - hours and minutes",
			expr:     "'3 hours 45 minutes'",
			expected: 3*time.Hour + 45*time.Minute,
			wantErr:  false,
		},
		{
			name:     "compound - full specification",
			expr:     "interval '7 days 6 hours 30 minutes 15 seconds'",
			expected: 7*24*time.Hour + 6*time.Hour + 30*time.Minute + 15*time.Second,
			wantErr:  false,
		},
		{
			name:     "compound - year month day",
			expr:     "'1 year 6 months 15 days'",
			expected: 365*24*time.Hour + 180*24*time.Hour + 15*24*time.Hour,
			wantErr:  false,
		},

		// Negative intervals (for future timestamps)
		{
			name:     "negative - hour",
			expr:     "'-1 hour'",
			expected: -time.Hour,
			wantErr:  false,
		},
		{
			name:     "negative - hours with interval keyword",
			expr:     "interval '-2 hours'",
			expected: -2 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "negative - day",
			expr:     "'-1 day'",
			expected: -24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "negative - compound interval",
			expr:     "'-1 day 12 hours'",
			expected: -(24*time.Hour + 12*time.Hour),
			wantErr:  false,
		},
		{
			name:     "negative - with cast syntax",
			expr:     "'-30 minutes'::interval",
			expected: -30 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "negative - week",
			expr:     "interval '-1 week'",
			expected: -7 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "negative - bare format",
			expr:     "-5 minutes",
			expected: -5 * time.Minute,
			wantErr:  false,
		},

		// ISO 8601 format (PostgreSQL supports this)
		{
			name:     "ISO 8601 - hour only",
			expr:     "'PT1H'",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - minutes only",
			expr:     "'PT30M'",
			expected: 30 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - seconds only",
			expr:     "'PT45S'",
			expected: 45 * time.Second,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - day only",
			expr:     "'P1D'",
			expected: 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - compound time",
			expr:     "'PT1H30M'",
			expected: time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - compound with day",
			expr:     "'P1DT2H30M'",
			expected: 24*time.Hour + 2*time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - full specification",
			expr:     "'P1Y2M3DT4H5M6S'",
			expected: 365*24*time.Hour + 60*24*time.Hour + 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - with interval keyword",
			expr:     "interval 'PT2H'",
			expected: 2 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - with cast syntax",
			expr:     "'PT45M'::interval",
			expected: 45 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - lowercase (PostgreSQL allows)",
			expr:     "'pt1h30m'",
			expected: time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - decimal seconds",
			expr:     "'PT1.5S'",
			expected: time.Duration(1.5 * float64(time.Second)),
			wantErr:  false,
		},
		{
			name:     "ISO 8601 - days (example)",
			expr:     "'P2D'",
			expected: 2 * 24 * time.Hour,
			wantErr:  false,
		},

		// Real-world production query patterns
		{
			name:     "production - sliding window query",
			expr:     "interval '1 hour'",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "production - metrics aggregation",
			expr:     "interval '5 minutes'",
			expected: 5 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "production - daily rollup",
			expr:     "interval '24 hours'",
			expected: 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "production - weekly report",
			expr:     "interval '7 days'",
			expected: 7 * 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "production - monthly approximation",
			expr:     "interval '30 days'",
			expected: 30 * 24 * time.Hour,
			wantErr:  false,
		},

		// Edge cases and error scenarios
		{
			name:     "edge - zero interval",
			expr:     "'0 hours'",
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "edge - fractional compound",
			expr:     "'1.5 hours 30 minutes'",
			expected: time.Hour + 30*time.Minute + 30*time.Minute, // 1.5h + 0.5h = 2h
			wantErr:  false,
		},
		{
			name:    "error - invalid interval string",
			expr:    "'foobar'",
			wantErr: true,
		},
		{
			name:    "error - invalid unit",
			expr:    "'1 fortnight'",
			wantErr: true,
		},
		{
			name:     "whitespace - extra spaces in compound",
			expr:     "'  1  day   2  hours  '",
			expected: 24*time.Hour + 2*time.Hour,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable for parallel execution
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseInterval(tt.expr)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseInterval(%q) expected error, got nil", tt.expr)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseInterval(%q) unexpected error: %v", tt.expr, err)
				return
			}

			if got != tt.expected {
				t.Errorf("ParseInterval(%q) = %v, want %v", tt.expr, got, tt.expected)
			}
		})
	}
}

// TestParseISO8601Interval tests the ISO 8601 interval parser directly.
func TestParseISO8601Interval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "hour only",
			input:    "PT1H",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "minute only",
			input:    "PT30M",
			expected: 30 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "second only",
			input:    "PT45S",
			expected: 45 * time.Second,
			wantErr:  false,
		},
		{
			name:     "day only",
			input:    "P1D",
			expected: 24 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "hour and minute",
			input:    "PT1H30M",
			expected: time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "day hour minute",
			input:    "P1DT2H30M",
			expected: 24*time.Hour + 2*time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "full format",
			input:    "P1Y2M3DT4H5M6S",
			expected: 365*24*time.Hour + 60*24*time.Hour + 3*24*time.Hour + 4*time.Hour + 5*time.Minute + 6*time.Second,
			wantErr:  false,
		},
		{
			name:     "decimal seconds",
			input:    "PT1.5S",
			expected: time.Duration(1.5 * float64(time.Second)),
			wantErr:  false,
		},
		{
			name:     "lowercase",
			input:    "pt2h",
			expected: 2 * time.Hour,
			wantErr:  false,
		},
		{
			name:    "missing P prefix",
			input:   "T1H",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid format",
			input:   "P",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseISO8601Interval(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("parseISO8601Interval(%q) expected error, got nil", tt.input)
				}
				return
			}

			if err != nil {
				t.Errorf("parseISO8601Interval(%q) unexpected error: %v", tt.input, err)
				return
			}

			if got != tt.expected {
				t.Errorf("parseISO8601Interval(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
