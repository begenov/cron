package cron

import "time"

type Schedule interface {
	Next(t time.Time) time.Time
}

type SpecSchedule struct {
	Second, Minute, Hour, Dom, Month, Dow uint64
}

const (
	starBit = 1 << 63
)

func (s *SpecSchedule) Next(t time.Time) time.Time {
	t = t.Add(time.Second - time.Duration(t.Nanosecond())*time.Nanosecond)

	added := false

	yearLimit := t.Year() + 5

WRAP:
	if t.Year() > yearLimit {
		return time.Time{}
	}

	for 1<<uint(t.Month())&s.Month == 0 {
		if !added {
			added = true
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		}
		t = t.AddDate(0, 1, 0)
		if t.Month() == time.January {
			goto WRAP
		}
	}

	for !dayMatches(s, t) {
		if !added {
			added = true
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		}
		t = t.AddDate(0, 0, 1)
		if t.Hour() != 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		}
		if t.Day() == 1 {
			goto WRAP
		}
	}

	for 1<<uint(t.Hour())&s.Hour == 0 {
		if !added {
			added = true
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
		}
		t = t.Add(1 * time.Hour)
		if t.Minute() != 0 || t.Second() != 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
		}
		if t.Hour() == 0 {
			goto WRAP
		}
	}

	for 1<<uint(t.Minute())&s.Minute == 0 {
		if !added {
			added = true
			t = t.Truncate(time.Minute)
		}
		t = t.Add(1 * time.Minute)
		if t.Second() != 0 {
			t = t.Truncate(time.Minute)
		}
		if t.Minute() == 0 {
			goto WRAP
		}
	}

	for 1<<uint(t.Second())&s.Second == 0 {
		if !added {
			added = true
		}
		t = t.Add(1 * time.Second)
		if t.Second() == 0 {
			goto WRAP
		}
	}

	return t
}

func dayMatches(s *SpecSchedule, t time.Time) bool {
	domStar := s.Dom&starBit > 0
	dowStar := s.Dow&starBit > 0

	domMatch := 1<<uint(t.Day())&s.Dom > 0
	dowMatch := 1<<uint(t.Weekday())&s.Dow > 0

	if domStar || dowStar {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}

type EverySchedule struct {
	Delay time.Duration
}

func (s *EverySchedule) Next(t time.Time) time.Time {
	return t.Add(s.Delay)
}
