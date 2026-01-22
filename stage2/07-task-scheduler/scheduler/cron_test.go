package scheduler

import (
	"testing"
	"time"
)

func TestParseCron_Valid(t *testing.T) {
	tests := []struct {
		expr string
		want *CronExpr
	}{
		{
			expr: "0 * * * * *",
			want: &CronExpr{Second: []int{0}},
		},
		{
			expr: "*/5 * * * * *",
			want: &CronExpr{Second: []int{0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}},
		},
		{
			expr: "0 30 9 * * 1-5",
			want: &CronExpr{
				Second:  []int{0},
				Minute:  []int{30},
				Hour:    []int{9},
				Weekday: []int{1, 2, 3, 4, 5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			cron, err := ParseCron(tt.expr)
			if err != nil {
				t.Fatalf("ParseCron(%q) failed: %v", tt.expr, err)
			}

			if tt.want.Second != nil && !sliceEqual(cron.Second, tt.want.Second) {
				t.Errorf("Second mismatch: got %v, want %v", cron.Second, tt.want.Second)
			}
			if tt.want.Minute != nil && !sliceEqual(cron.Minute, tt.want.Minute) {
				t.Errorf("Minute mismatch: got %v, want %v", cron.Minute, tt.want.Minute)
			}
			if tt.want.Weekday != nil && !sliceEqual(cron.Weekday, tt.want.Weekday) {
				t.Errorf("Weekday mismatch: got %v, want %v", cron.Weekday, tt.want.Weekday)
			}
		})
	}
}

func TestParseCron_Invalid(t *testing.T) {
	tests := []string{
		"",
		"* * * * *",     // 5 fields instead of 6
		"60 * * * * *",  // second out of range
		"* 60 * * * *",  // minute out of range
		"* * 24 * * *",  // hour out of range
		"* * * 0 * *",   // day 0 is invalid
		"* * * * 13 *",  // month 13 is invalid
		"* * * * * 7",   // weekday 7 is invalid
		"abc * * * * *", // non-numeric
	}

	for _, expr := range tests {
		t.Run(expr, func(t *testing.T) {
			_, err := ParseCron(expr)
			if err == nil {
				t.Errorf("ParseCron(%q) should have failed", expr)
			}
		})
	}
}

func TestCronExpr_Next(t *testing.T) {
	// Test "0 * * * * *" - every minute at second 0
	cron, _ := ParseCron("0 * * * * *")
	from := time.Date(2024, 1, 1, 12, 30, 30, 0, time.UTC)
	next := cron.Next(from)

	if next.Second() != 0 || next.Minute() != 31 {
		t.Errorf("Expected 12:31:00, got %v", next.Format("15:04:05"))
	}
}

func TestCronExpr_NextEvery5Seconds(t *testing.T) {
	cron, _ := ParseCron("*/5 * * * * *")
	from := time.Date(2024, 1, 1, 12, 0, 2, 0, time.UTC)
	next := cron.Next(from)

	if next.Second() != 5 {
		t.Errorf("Expected second=5, got %d", next.Second())
	}
}

func TestCronExpr_NextSpecificTime(t *testing.T) {
	// 9:30:00 AM on Monday-Friday
	cron, _ := ParseCron("0 30 9 * * 1-5")

	// Friday 9:30:01 AM - next should be Monday 9:30:00 AM
	// Jan 5, 2024 is a Friday
	from := time.Date(2024, 1, 5, 9, 30, 1, 0, time.UTC)
	next := cron.Next(from)

	// Monday is Jan 8, 2024
	if next.Weekday() != time.Monday || next.Hour() != 9 || next.Minute() != 30 {
		t.Errorf("Expected Monday 9:30:00, got %v (%s)", next.Format("2006-01-02 15:04:05"), next.Weekday())
	}
}

func sliceEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
