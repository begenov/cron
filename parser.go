package cron

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

func ParseStandard(spec string) (Schedule, error) {
	return newParser(false).parse(spec)
}

func ParseWithSeconds(spec string) (Schedule, error) {
	return newParser(true).parse(spec)
}

func Parse(spec string) (Schedule, error) {
	spec = strings.TrimSpace(spec)

	if strings.HasPrefix(spec, "@") {
		return parseAlias(spec)
	}

	fields := strings.Fields(spec)
	switch len(fields) {
	case 5:
		return ParseStandard(spec)
	case 6:
		return ParseWithSeconds(spec)
	default:
		return nil, fmt.Errorf("cron: expected 5 or 6 fields, got %d", len(fields))
	}
}

func parseAlias(spec string) (Schedule, error) {
	switch spec {
	case "@yearly", "@annually":
		return mustParseStandard("0 0 1 1 *"), nil
	case "@monthly":
		return mustParseStandard("0 0 1 * *"), nil
	case "@weekly":
		return mustParseStandard("0 0 * * 0"), nil
	case "@daily", "@midnight":
		return mustParseStandard("0 0 * * *"), nil
	case "@hourly":
		return mustParseStandard("0 * * * *"), nil
	}

	if strings.HasPrefix(spec, "@every ") {
		d, err := time.ParseDuration(spec[7:])
		if err != nil {
			return nil, fmt.Errorf("cron: invalid @every duration %q: %w", spec[7:], err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("cron: @every duration must be positive, got %s", d)
		}
		return &EverySchedule{Delay: d}, nil
	}

	return nil, fmt.Errorf("cron: unknown alias %q", spec)
}

func mustParseStandard(s string) Schedule {
	sc, err := ParseStandard(s)
	if err != nil {
		panic(err)
	}
	return sc
}

type parser struct {
	withSeconds bool
}

func newParser(withSeconds bool) *parser {
	return &parser{withSeconds: withSeconds}
}

var (
	months   = buildAliasMap([]string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}, 1)
	weekdays = buildAliasMap([]string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}, 0)
)

func buildAliasMap(names []string, offset int) map[string]int {
	m := make(map[string]int, len(names))
	for i, n := range names {
		m[n] = i + offset
	}
	return m
}

func (p *parser) parse(spec string) (*SpecSchedule, error) {
	fields := strings.Fields(spec)

	var (
		secField  = "0"
		minField  string
		hourField string
		domField  string
		monField  string
		dowField  string
	)

	if p.withSeconds {
		if len(fields) != 6 {
			return nil, fmt.Errorf("cron: expected 6 fields, got %d in %q", len(fields), spec)
		}
		secField, minField, hourField, domField, monField, dowField =
			fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]
	} else {
		if len(fields) != 5 {
			return nil, fmt.Errorf("cron: expected 5 fields, got %d in %q", len(fields), spec)
		}
		minField, hourField, domField, monField, dowField =
			fields[0], fields[1], fields[2], fields[3], fields[4]
	}

	var err error
	s := &SpecSchedule{}

	if s.Second, err = parseField(secField, 0, 59, nil); err != nil {
		return nil, fmt.Errorf("cron: second: %w", err)
	}
	if s.Minute, err = parseField(minField, 0, 59, nil); err != nil {
		return nil, fmt.Errorf("cron: minute: %w", err)
	}
	if s.Hour, err = parseField(hourField, 0, 23, nil); err != nil {
		return nil, fmt.Errorf("cron: hour: %w", err)
	}
	if s.Dom, err = parseField(domField, 1, 31, nil); err != nil {
		return nil, fmt.Errorf("cron: dom: %w", err)
	}
	if s.Month, err = parseField(monField, 1, 12, months); err != nil {
		return nil, fmt.Errorf("cron: month: %w", err)
	}
	if s.Dow, err = parseField(dowField, 0, 6, weekdays); err != nil {
		return nil, fmt.Errorf("cron: dow: %w", err)
	}

	if s.Dow&(1<<7) > 0 {
		s.Dow |= 1
	}

	return s, nil
}

func parseField(field string, min, max int, aliases map[string]int) (uint64, error) {
	var bits uint64
	isStar := false

	for _, part := range strings.Split(field, ",") {
		var (
			start, end, step int
			err              error
		)

		rangeAndStep := strings.SplitN(part, "/", 2)
		lowAndHigh := strings.SplitN(rangeAndStep[0], "-", 2)

		if lowAndHigh[0] == "*" || lowAndHigh[0] == "?" {
			isStar = true
			start = min
			end = max
		} else {
			if start, err = getVal(lowAndHigh[0], aliases); err != nil {
				return 0, fmt.Errorf("invalid value %q: %w", lowAndHigh[0], err)
			}
			if len(lowAndHigh) == 2 {
				if end, err = getVal(lowAndHigh[1], aliases); err != nil {
					return 0, fmt.Errorf("invalid range end %q: %w", lowAndHigh[1], err)
				}
			} else {
				end = start
			}
		}

		step = 1
		if len(rangeAndStep) == 2 {
			if step, err = strconv.Atoi(rangeAndStep[1]); err != nil || step <= 0 {
				return 0, fmt.Errorf("invalid step %q", rangeAndStep[1])
			}
		}

		if start < min || end > max {
			return 0, fmt.Errorf("value out of range [%d-%d]: %d-%d", min, max, start, end)
		}
		if start > end {
			return 0, fmt.Errorf("invalid range: %d > %d", start, end)
		}

		for i := start; i <= end; i += step {
			bits |= 1 << uint(i)
		}
	}

	if isStar && bits == (uint64(math.MaxUint64)>>(64-uint(max-min+1)))<<uint(min) {
		bits |= starBit
	}

	return bits, nil
}

func getVal(s string, aliases map[string]int) (int, error) {
	if aliases != nil {
		if v, ok := aliases[strings.ToLower(s)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("not a number: %q", s)
	}
	return v, nil
}
