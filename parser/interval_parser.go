package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ParseInterval parses a PostgreSQL INTERVAL expression and returns a time.Duration.
//
// Supported formats:
//   - interval '1 hour'
//   - '1 hour'::interval
//   - make_interval(hours => 1)
//   - Negative intervals: '-1 hour', "interval '-1 hour'", or literal minus prefix
//   - ISO 8601 format: 'PT1H', 'P1D', 'P1DT2H30M'
//
// Supported units: second, minute, hour, day, week, month (30 days), year (365 days).
// Fractional values are supported (e.g., "1.5 hours"). Zero values are valid
// and return 0 without error.
//
// Notes and limitations:
//   - Month and year are approximated as 30 and 365 days respectively (no leap years).
//   - Only simple textual interval forms are supported; complex PostgreSQL interval
//     syntaxes like time-of-day tuples (e.g., "1 day 02:03:04") are not parsed.
//   - ISO 8601 supports common patterns: P[n]Y[n]M[n]DT[n]H[n]M[n]S
//
// Returns an error if the expression cannot be parsed or contains unsupported syntax.
func ParseInterval(expr string) (time.Duration, error) {
	if expr == "" {
		return 0, fmt.Errorf("empty interval expression")
	}

	// Normalize whitespace and case for easier parsing
	normalized := strings.TrimSpace(expr)

	// Pattern 1: interval '1 hour'
	// Match: interval 'N unit' or interval "N unit"
	intervalRegex := regexp.MustCompile(`(?i)^\s*interval\s+['"]([^'"]+)['"]\s*$`)
	if matches := intervalRegex.FindStringSubmatch(normalized); matches != nil {
		return parseIntervalString(matches[1])
	}

	// Pattern 2: '1 hour'::interval
	// Match: 'N unit'::interval or "N unit"::interval
	castRegex := regexp.MustCompile(`(?i)^\s*['"]([^'"]+)['"]\s*::\s*interval\s*$`)
	if matches := castRegex.FindStringSubmatch(normalized); matches != nil {
		return parseIntervalString(matches[1])
	}

	// Pattern 3: Bare quoted string '1 hour' or "1 hour" (without interval keyword or cast)
	// This handles cases like '1 hour', '-1 day', '1 day 2 hours', etc.
	bareQuotedRegex := regexp.MustCompile(`(?i)^\s*['"]([^'"]+)['"]\s*$`)
	if matches := bareQuotedRegex.FindStringSubmatch(normalized); matches != nil {
		// Check if it's an ISO 8601 format before treating as interval string
		inner := matches[1]
		if strings.HasPrefix(strings.ToUpper(inner), "P") {
			// Might be ISO 8601, let it fall through to that pattern
		} else {
			return parseIntervalString(inner)
		}
	}

	// Pattern 4: make_interval(hours => 1)
	// Supported parameters: years, months, weeks, days, hours, mins, secs
	if strings.HasPrefix(strings.ToLower(normalized), "make_interval") {
		return parseMakeInterval(normalized)
	}

	// Pattern 5: ISO 8601 format (e.g., 'PT1H', 'P1D', 'P1DT2H30M')
	// Check for quoted ISO 8601 within interval or cast syntax
	// Note: This pattern intentionally excludes ISO 'W' weeks designator.
	iso8601Regex := regexp.MustCompile(`(?i)^\s*(?:interval\s+)?['"]?(P(?:[0-9]+Y)?(?:[0-9]+M)?(?:[0-9]+D)?(?:T(?:[0-9]+H)?(?:[0-9]+M)?(?:[0-9]+(?:\.[0-9]+)?S)?)?)['"]?(?:\s*::\s*interval)?\s*$`)
	if matches := iso8601Regex.FindStringSubmatch(normalized); len(matches) > 1 {
		return parseISO8601Interval(matches[1])
	}

	// Fallback: try parsing as bare interval string (e.g., "1 hour")
	return parseIntervalString(normalized)
}

// parseIntervalString parses a bare interval string like "1 hour", "30 seconds", "7 days".
// Supports compound intervals like "1 day 2 hours" or "1 year 3 months".
// Supports negative intervals like "-1 hour" or "-2 days 3 hours".
func parseIntervalString(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty interval string")
	}

	// Normalize whitespace
	s = strings.TrimSpace(s)

	// Check for negative sign at the beginning
	// Negative intervals can be: "-1 hour", "- 1 hour", or "-1 hour 30 minutes"
	isNegative := false
	if strings.HasPrefix(s, "-") {
		isNegative = true
		// Remove the leading minus and any following whitespace
		s = strings.TrimSpace(s[1:])
	}

	// Regular expression to match one or more interval components
	// Format: "N unit" where N is an integer or decimal, unit is a time unit
	// Updated to support optional negative sign for each component
	componentRegex := regexp.MustCompile(`(?i)(-?\d+(?:\.\d+)?)\s*(years?|mons?|months?|weeks?|days?|hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)`)

	matches := componentRegex.FindAllStringSubmatch(s, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("invalid interval format: %q", s)
	}

	var totalDuration time.Duration

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}

		// Parse the numeric value (may include negative sign)
		valueStr := match[1]
		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid interval value %q: %w", valueStr, err)
		}

		// Parse the unit
		unit := strings.ToLower(match[2])
		duration, err := parseDurationForUnit(value, unit)
		if err != nil {
			return 0, err
		}

		totalDuration += duration
	}

	// Apply global negative sign if present at the start
	if isNegative {
		totalDuration = -totalDuration
	}

	return totalDuration, nil
}

// parseDurationForUnit converts a numeric value and time unit to a time.Duration.
// Approximate conversions: month = 30 days, year = 365 days (no leap year handling).
func parseDurationForUnit(value float64, unit string) (time.Duration, error) {
	switch unit {
	case "second", "seconds", "sec", "secs", "s":
		return time.Duration(value * float64(time.Second)), nil
	case "minute", "minutes", "min", "mins", "m":
		return time.Duration(value * float64(time.Minute)), nil
	case "hour", "hours", "hr", "hrs", "h":
		return time.Duration(value * float64(time.Hour)), nil
	case "day", "days":
		return time.Duration(value * float64(24*time.Hour)), nil
	case "week", "weeks":
		return time.Duration(value * float64(7*24*time.Hour)), nil
	case "month", "months", "mon", "mons":
		// Approximate: 30 days per month
		return time.Duration(value * float64(30*24*time.Hour)), nil
	case "year", "years":
		// Approximate: 365 days per year (no leap year)
		return time.Duration(value * float64(365*24*time.Hour)), nil
	default:
		return 0, fmt.Errorf("unsupported time unit: %q", unit)
	}
}

// parseMakeInterval parses a make_interval(...) function call.
// Example: make_interval(hours => 1, mins => 30)
// Supported parameters: years, months, weeks, days, hours, mins, secs.
func parseMakeInterval(expr string) (time.Duration, error) {
	// Extract the content inside make_interval(...)
	makeIntervalRegex := regexp.MustCompile(`(?i)^make_interval\s*\(\s*(.+?)\s*\)\s*$`)
	matches := makeIntervalRegex.FindStringSubmatch(expr)
	if len(matches) < 2 {
		return 0, fmt.Errorf("invalid make_interval format: %q", expr)
	}

	argsStr := matches[1]

	// Parse named parameters: param => value
	paramRegex := regexp.MustCompile(`(?i)(years?|months?|weeks?|days?|hours?|mins?|secs?)\s*=>\s*(\d+(?:\.\d+)?)`)
	paramMatches := paramRegex.FindAllStringSubmatch(argsStr, -1)

	if len(paramMatches) == 0 {
		return 0, fmt.Errorf("no valid parameters found in make_interval: %q", expr)
	}

	var totalDuration time.Duration

	for _, match := range paramMatches {
		if len(match) < 3 {
			continue
		}

		paramName := strings.ToLower(match[1])
		valueStr := match[2]

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid value %q in make_interval: %w", valueStr, err)
		}

		// Map parameter name to standard unit
		unit := mapMakeIntervalParam(paramName)
		duration, err := parseDurationForUnit(value, unit)
		if err != nil {
			return 0, err
		}

		totalDuration += duration
	}

	return totalDuration, nil
}

// mapMakeIntervalParam maps make_interval parameter names to standard time units.
func mapMakeIntervalParam(param string) string {
	switch param {
	case "years", "year":
		return "years"
	case "months", "month":
		return "months"
	case "weeks", "week":
		return "weeks"
	case "days", "day":
		return "days"
	case "hours", "hour":
		return "hours"
	case "mins", "min":
		return "minutes"
	case "secs", "sec":
		return "seconds"
	default:
		return param
	}
}

// parseISO8601Interval parses an ISO 8601 duration string to time.Duration.
// Supports the PostgreSQL ISO 8601 interval format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
//
// Examples:
//   - PT1H       -> 1 hour
//   - P1D        -> 1 day (24 hours)
//   - P1DT2H30M  -> 1 day + 2 hours + 30 minutes
//   - PT30M      -> 30 minutes
//   - P1Y2M3DT4H5M6S -> 1 year + 2 months + 3 days + 4 hours + 5 minutes + 6 seconds
//
// Notes:
//   - Year and month use approximate conversions (365 and 30 days respectively)
//   - The format must start with 'P' and use 'T' to separate date from time components
//   - Decimal seconds are supported (e.g., PT1.5S = 1.5 seconds)
func parseISO8601Interval(s string) (time.Duration, error) {
	if s == "" || !strings.HasPrefix(strings.ToUpper(s), "P") {
		return 0, fmt.Errorf("invalid ISO 8601 interval format: %q", s)
	}

	// Remove the leading 'P' and normalize to uppercase for parsing
	s = strings.ToUpper(s[1:])

	// Split on 'T' to separate date and time components
	// Format: P[date]T[time]
	parts := strings.SplitN(s, "T", 2)
	datePart := parts[0]
	timePart := ""
	if len(parts) > 1 {
		timePart = parts[1]
	}

	var totalDuration time.Duration

	// Parse date part: [n]Y[n]M[n]D
	// Note: In ISO 8601, M in date part means months, M in time part means minutes
	if datePart != "" {
		// Match years, months, days in order
		dateRegex := regexp.MustCompile(`(?:(\d+(?:\.\d+)?)Y)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)D)?`)
		matches := dateRegex.FindStringSubmatch(datePart)

		if matches != nil {
			// Years
			if matches[1] != "" {
				years, err := strconv.ParseFloat(matches[1], 64)
				if err != nil {
					return 0, fmt.Errorf("invalid year value in ISO 8601: %w", err)
				}
				totalDuration += time.Duration(years * float64(365*24*time.Hour))
			}

			// Months (in date context)
			if matches[2] != "" {
				months, err := strconv.ParseFloat(matches[2], 64)
				if err != nil {
					return 0, fmt.Errorf("invalid month value in ISO 8601: %w", err)
				}
				totalDuration += time.Duration(months * float64(30*24*time.Hour))
			}

			// Days
			if matches[3] != "" {
				days, err := strconv.ParseFloat(matches[3], 64)
				if err != nil {
					return 0, fmt.Errorf("invalid day value in ISO 8601: %w", err)
				}
				totalDuration += time.Duration(days * float64(24*time.Hour))
			}
		}
	}

	// Parse time part: [n]H[n]M[n]S
	if timePart != "" {
		// Match hours, minutes, seconds in order
		timeRegex := regexp.MustCompile(`(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?`)
		matches := timeRegex.FindStringSubmatch(timePart)

		if matches != nil {
			// Hours
			if matches[1] != "" {
				hours, err := strconv.ParseFloat(matches[1], 64)
				if err != nil {
					return 0, fmt.Errorf("invalid hour value in ISO 8601: %w", err)
				}
				totalDuration += time.Duration(hours * float64(time.Hour))
			}

			// Minutes (in time context)
			if matches[2] != "" {
				minutes, err := strconv.ParseFloat(matches[2], 64)
				if err != nil {
					return 0, fmt.Errorf("invalid minute value in ISO 8601: %w", err)
				}
				totalDuration += time.Duration(minutes * float64(time.Minute))
			}

			// Seconds
			if matches[3] != "" {
				seconds, err := strconv.ParseFloat(matches[3], 64)
				if err != nil {
					return 0, fmt.Errorf("invalid second value in ISO 8601: %w", err)
				}
				totalDuration += time.Duration(seconds * float64(time.Second))
			}
		}
	}

	// Validate that we parsed something
	if totalDuration == 0 && datePart == "" && timePart == "" {
		return 0, fmt.Errorf("invalid ISO 8601 interval format: %q (no components found)", s)
	}

	return totalDuration, nil
}
