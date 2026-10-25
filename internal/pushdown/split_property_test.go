package pushdown

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// TestProperty_SplitEqualsUnsplit draws random range queries and requires the
// splitter to return what the raw engine returns. It also requires that most
// draws really split, so the test cannot pass vacuously.
func TestProperty_SplitEqualsUnsplit(t *testing.T) {
	conn, base := splitFixtureConn(t)
	templates := []string{
		`quantile_over_time(0.9, {app="web"} | logfmt | unwrap dur [R] O) by (app)`,
		`sum by (app) (rate({app=~".+"} | logfmt | level="error" [R] O))`,
		`count_over_time({app="web"}[R] O) / count_over_time({app="api"} | logfmt [R] O)`,
		`topk(2, sum by (app) (count_over_time({app=~".+"} | logfmt [R] O)))`,
		`absent_over_time({app="nope"}[R] O)`,
		`max_over_time({app=~"web|late"} | logfmt | unwrap dur [R] O) by (app)`,
	}
	ivs := []time.Duration{10 * time.Minute, 30 * time.Minute, time.Hour}
	steps := []time.Duration{15 * time.Second, time.Minute, 7 * time.Minute, 30 * time.Minute, time.Hour, 90 * time.Minute}
	ranges := []time.Duration{time.Minute, 5 * time.Minute, 45 * time.Minute, 2 * time.Hour}
	offsets := []string{"", "offset 5m"}

	const draws = 150
	split, total := 0, 0
	for seed := int64(1); seed <= 3; seed++ {
		rng := rand.New(rand.NewSource(seed))
		for i := 0; i < draws; i++ {
			iv := ivs[rng.Intn(len(ivs))]
			step := steps[rng.Intn(len(steps))]
			rg := ranges[rng.Intn(len(ranges))]
			tpl := templates[rng.Intn(len(templates))]
			q := strings.NewReplacer("R", rg.String(), "O", offsets[rng.Intn(len(offsets))]).Replace(tpl)
			start := base.Add(time.Duration(rng.Int63n(int64(time.Hour))))
			end := start.Add(step + time.Duration(rng.Int63n(int64(5*time.Hour-step)+1)))

			s, raw := newSplitter(t, conn, semaphore.NewWeighted(16), nil)
			s.Limits = splitLimitsWithInterval{s.Limits, iv}
			p := rangeParams(t, q, start, end, step)
			desc := fmt.Sprintf("seed=%d draw=%d iv=%v step=%v q=%s start=%v end=%v", seed, i, iv, step, q, start, end)
			want, werr := raw.Query(p).Exec(tctx())
			got, gerr := s.Query(p).Exec(tctx())
			if werr != nil {
				require.EqualError(t, gerr, werr.Error(), desc)
				continue
			}
			require.NoError(t, gerr, desc)
			if strings.HasPrefix(q, "topk") {
				require.Equal(t, sortedValues(want.Data), sortedValues(got.Data), desc)
			} else {
				requireDataEqual(t, want.Data, got.Data, desc)
			}
			total++
			if got.Statistics.Summary.Splits > 1 {
				split++
			}
		}
	}
	require.GreaterOrEqual(t, split*100, total*60, "only %d of %d draws split", split, total)
}

type splitLimitsWithInterval struct {
	SplitLimits
	iv time.Duration
}

func (l splitLimitsWithInterval) QuerySplitDuration(string) time.Duration { return l.iv }
