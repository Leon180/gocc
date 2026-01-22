package scheduler

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// Cron expression format: "second minute hour day month weekday"
// Examples:
//   - "0 * * * * *"    → Every minute at second 0
//   - "*/5 * * * * *"  → Every 5 seconds
//   - "0 0 * * * *"    → Every hour at minute 0, second 0
//   - "0 30 9 * * 1-5" → 9:30 AM Monday-Friday
//
// Field ranges:
//   - second:  0-59
//   - minute:  0-59
//   - hour:    0-23
//   - day:     1-31
//   - month:   1-12
//   - weekday: 0-6 (0=Sunday)
//
// Special characters:
//   - *    → any value
//   - */n  → every n units
//   - n-m  → range from n to m
//   - n,m  → list of values

var (
	ErrInvalidCronExpr = errors.New("invalid cron expression")
	ErrInvalidField    = errors.New("invalid cron field")
)

// CronExpr represents a parsed cron expression.
type CronExpr struct {
	Second  []int // 0-59
	Minute  []int // 0-59
	Hour    []int // 0-23
	Day     []int // 1-31
	Month   []int // 1-12
	Weekday []int // 0-6 (Sunday=0)
	raw     string
}

// ParseCron parses a cron expression string.
func ParseCron(expr string) (*CronExpr, error) {
	fields := strings.Fields(expr)
	if len(fields) != 6 {
		return nil, ErrInvalidCronExpr
	}

	second, err := parseField(fields[0], 0, 59)
	if err != nil {
		return nil, err
	}

	minute, err := parseField(fields[1], 0, 59)
	if err != nil {
		return nil, err
	}

	hour, err := parseField(fields[2], 0, 23)
	if err != nil {
		return nil, err
	}

	day, err := parseField(fields[3], 1, 31)
	if err != nil {
		return nil, err
	}

	month, err := parseField(fields[4], 1, 12)
	if err != nil {
		return nil, err
	}

	weekday, err := parseField(fields[5], 0, 6)
	if err != nil {
		return nil, err
	}

	return &CronExpr{
		Second:  second,
		Minute:  minute,
		Hour:    hour,
		Day:     day,
		Month:   month,
		Weekday: weekday,
		raw:     expr,
	}, nil
}

// parseField parses a single cron field.
func parseField(field string, min, max int) ([]int, error) {
	// Handle "*" - all values
	if field == "*" {
		return rangeSlice(min, max), nil
	}

	// Handle "*/n" - every n
	if strings.HasPrefix(field, "*/") {
		step, err := strconv.Atoi(field[2:])
		if err != nil || step <= 0 {
			return nil, ErrInvalidField
		}
		var values []int
		for i := min; i <= max; i += step {
			values = append(values, i)
		}
		return values, nil
	}

	// Handle comma-separated values: "1,2,3"
	if strings.Contains(field, ",") {
		parts := strings.Split(field, ",")
		var values []int
		for _, p := range parts {
			v, err := strconv.Atoi(p)
			if err != nil || v < min || v > max {
				return nil, ErrInvalidField
			}
			values = append(values, v)
		}
		return values, nil
	}

	// Handle range: "1-5"
	if strings.Contains(field, "-") {
		parts := strings.Split(field, "-")
		if len(parts) != 2 {
			return nil, ErrInvalidField
		}
		start, err := strconv.Atoi(parts[0])
		if err != nil || start < min {
			return nil, ErrInvalidField
		}
		end, err := strconv.Atoi(parts[1])
		if err != nil || end > max || end < start {
			return nil, ErrInvalidField
		}
		return rangeSlice(start, end), nil
	}

	// Single value
	v, err := strconv.Atoi(field)
	if err != nil || v < min || v > max {
		return nil, ErrInvalidField
	}
	return []int{v}, nil
}

func rangeSlice(min, max int) []int {
	values := make([]int, max-min+1)
	for i := range values {
		values[i] = min + i
	}
	return values
}

// Next calculates the next execution time after the given time.
func (c *CronExpr) Next(from time.Time) time.Time {
	// Start from the next second
	t := from.Add(time.Second).Truncate(time.Second)

	// Maximum iterations to prevent infinite loop
	for i := 0; i < 366*24*60*60; i++ {
		if c.matches(t) {
			return t
		}
		t = t.Add(time.Second)
	}

	// Should never reach here for valid expressions
	return time.Time{}
}

// matches checks if a time matches the cron expression.
func (c *CronExpr) matches(t time.Time) bool {
	return contains(c.Second, t.Second()) &&
		contains(c.Minute, t.Minute()) &&
		contains(c.Hour, t.Hour()) &&
		contains(c.Day, t.Day()) &&
		contains(c.Month, int(t.Month())) &&
		contains(c.Weekday, int(t.Weekday()))
}

func contains(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// String returns the original cron expression.
func (c *CronExpr) String() string {
	return c.raw
}
