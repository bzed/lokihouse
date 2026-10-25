package pushdown

import (
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/stretchr/testify/require"
)

func TestSplitLegs(t *testing.T) {
	at := func(s string) time.Time {
		v, err := time.Parse(time.TimeOnly, s)
		require.NoError(t, err)
		return time.Date(2026, 1, 1, v.Hour(), v.Minute(), v.Second(), 0, time.UTC)
	}
	cases := []struct {
		name       string
		start, end string
		step, iv   time.Duration
		want       [][2]string
	}{
		{"unaligned start", "10:07:30", "12:20:00", 5 * time.Minute, time.Hour,
			[][2]string{{"10:07:30", "10:57:30"}, {"11:02:30", "11:57:30"}, {"12:02:30", "12:17:30"}}},
		{"end off grid", "10:00:00", "10:59:59", 15 * time.Minute, 30 * time.Minute,
			[][2]string{{"10:00:00", "10:15:00"}, {"10:30:00", "10:45:00"}}},
		{"start on boundary", "10:00:00", "11:00:00", 15 * time.Minute, time.Hour,
			[][2]string{{"10:00:00", "10:45:00"}, {"11:00:00", "11:00:00"}}},
		{"step equals interval", "10:30:00", "13:30:00", time.Hour, time.Hour,
			[][2]string{{"10:30:00", "10:30:00"}, {"11:30:00", "11:30:00"}, {"12:30:00", "12:30:00"}, {"13:30:00", "13:30:00"}}},
		{"step above interval", "10:00:00", "14:30:00", 90 * time.Minute, time.Hour,
			[][2]string{{"10:00:00", "10:00:00"}, {"11:30:00", "11:30:00"}, {"13:00:00", "13:00:00"}, {"14:30:00", "14:30:00"}}},
		{"one bucket", "10:05:00", "10:55:00", time.Minute, time.Hour,
			[][2]string{{"10:05:00", "10:55:00"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var want []leg
			for _, w := range c.want {
				want = append(want, leg{at(w[0]), at(w[1])})
			}
			require.Equal(t, want, splitLegs(at(c.start), at(c.end), c.step, c.iv))
		})
	}
}

func TestSplitInterval(t *testing.T) {
	cases := []struct {
		q          string
		configured time.Duration
		want       time.Duration
	}{
		{`rate({a="b"}[5m])`, time.Hour, time.Hour},
		{`rate({a="b"}[2h])`, time.Hour, 2 * time.Hour},
		{`rate({a="b"}[5m] offset 3h)`, time.Hour, time.Hour},
		{`rate({a="b"}[5m]) / rate({a="b"}[90m])`, time.Hour, 90 * time.Minute},
		{`rate({a="b"}[2h])`, 0, 0},
	}
	for _, c := range cases {
		expr, err := syntax.ParseExpr(c.q)
		require.NoError(t, err)
		require.Equal(t, c.want, splitInterval(expr, c.configured), c.q)
	}
}

func TestConnNeed(t *testing.T) {
	cases := []struct {
		q    string
		want int64
	}{
		{`quantile_over_time(0.9, {a="b"} | unwrap x [5m])`, 2},
		{`count_over_time({a="1"}[1m]) / count_over_time({a="2"}[1m])`, 3},
		{`vector(1)`, 1},
		{`sum(rate({a="1"}[1m])) + sum(rate({a="2"}[1m])) * count_over_time({a="3"}[1m])`, 4},
	}
	for _, c := range cases {
		expr, err := syntax.ParseExpr(c.q)
		require.NoError(t, err)
		require.Equal(t, c.want, connNeed(expr), c.q)
	}
}

func TestLegParams(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	p, err := logql.NewLiteralParams(`rate({a="b"}[1m])`, t0, t0.Add(time.Hour), time.Minute, 0, 0, 100, nil, nil)
	require.NoError(t, err)
	lp := legParams{Params: p, start: t0.Add(10 * time.Minute), end: t0.Add(20 * time.Minute)}
	require.Equal(t, t0.Add(10*time.Minute), lp.Start())
	require.Equal(t, t0.Add(20*time.Minute), lp.End())
	require.Equal(t, p.QueryString(), lp.QueryString())
	require.Equal(t, p.Step(), lp.Step())
	require.Equal(t, p.GetExpression(), lp.GetExpression())
}
