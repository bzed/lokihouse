package pushdown

import (
	"context"
	"flag"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/stretchr/testify/require"

	"github.com/bzed/lokihouse/internal/chstore"
	"github.com/bzed/lokihouse/internal/chstore/chtest"
)

const (
	propDrawsPerSeed = 300
	propMinTierAShot = 80 // percent of eligible leaves that must run on Tier A
)

// TestProperty_TierAEqualsTierB is a seeded, reproducible property test: for a
// corpus of random eligible range-aggregation leaves (with offsets, non-dividing
// ranges/steps, filters and wrappers), the push-down engine in mode "on" must
// return the same Data as mode "off" (everything on the Phase 1 Tier B engine).
// It also asserts that Tier A actually ran for at least 80% of the eligible
// leaves, so the test cannot pass vacuously.
func TestProperty_TierAEqualsTierB(t *testing.T) {
	conn, _ := chtest.New(t)
	w := &chstore.Writer{Conn: conn}

	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("t", flag.PanicOnError))
	overrides, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)

	tierB := logql.NewEngine(logql.EngineOpts{}, &chstore.Querier{Conn: conn}, overrides, nopLogger{})
	tierA := &TierA{Conn: conn, Limits: overrides}
	engOn := &Engine{Mode: ModeOn, TierB: tierB, TierA: tierA, Limits: overrides, Logger: nopLogger{}}
	engOff := &Engine{Mode: ModeOff, TierB: tierB, TierA: tierA, Limits: overrides, Logger: nopLogger{}}

	// --- seed the data once ---
	start, words, err := seedPropData(t, w)
	require.NoError(t, err)
	datStart := start.UnixNano()

	eligible, tierAran := 0, 0
	for seed := int64(1); seed <= 3; seed++ {
		rng := rand.New(rand.NewSource(seed))
		for i := 0; i < propDrawsPerSeed; i++ {
			// --- draw the query parameters ---
			op := []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"}[rng.Intn(4)]
			rg := []time.Duration{10 * time.Second, 45 * time.Second, time.Minute, 5 * time.Minute}[rng.Intn(4)]
			st := []time.Duration{0, time.Second, 15 * time.Second, time.Minute, 7 * time.Minute}[rng.Intn(5)]
			offset := []time.Duration{0, 90 * time.Second, time.Minute}[rng.Intn(3)]

			// For instant queries (step=0), start must equal end (the engine
			// evaluates at a single point). For range queries, the span is
			// bounded by the step.
			startNS := datStart + rng.Int63n(int64(30*time.Minute))
			s := time.Unix(0, startNS).UTC()
			var e time.Time
			if st == 0 {
				e = s // instant: start == end
			} else {
				spanNS := rng.Int63n(int64(90*time.Minute) + 1)
				e = s.Add(time.Duration(spanNS))
			}

			filter := ""
			switch rng.Intn(6) {
			case 1:
				filter = `|= "` + words[rng.Intn(len(words))] + `"`
			case 2:
				filter = `!= "` + words[rng.Intn(len(words))] + `"`
			case 3:
				filter = `|= "` + words[rng.Intn(len(words))] + `" or "` + words[rng.Intn(len(words))] + `"`
			case 4:
				filter = `|~ "` + words[rng.Intn(len(words))] + `|` + words[rng.Intn(len(words))] + `"`
			case 5:
				filter = `!~ "^` + words[rng.Intn(len(words))] + `[0-9]"`
			}

			off := ""
			if offset > 0 {
				off = " offset " + offset.String()
			}
			inner := op + `({app=~".+"}` + filter + `[` + rg.String() + `]` + off + `)`
			// Wrappers. topk is omitted: its tie-break among equal values is
			// undefined (Phase 1 caveat), so it needs its own dedicated case rather
			// than this strict-equality property test.
			// Root vector aggs and root arithmetic run on Tier B (see Map); the
			// comparison and label_replace wrappers push their leaf down.
			switch rng.Intn(6) {
			case 1:
				inner = "sum by (app) (" + inner + ")"
			case 2:
				inner = "count(" + inner + ")"
			case 3:
				inner = inner + " / on() group_left sum(" + inner + ")"
			case 4:
				inner += " > 1"
			case 5:
				inner = `label_replace(` + inner + `, "dst", "$1", "app", "(.*)")`
			}

			p, err := logql.NewLiteralParams(inner, s, e, st, 0, logproto.FORWARD, 0, nil, nil)
			require.NoError(t, err)
			ctx := user.InjectOrgID(context.Background(), "t1")
			resOff, errOff := engOff.Query(ctx, p).Exec(ctx)
			if !anyEligibleLeaf(t, inner) {
				require.NoError(t, errOff, "seed=%d draw=%d q=%s", seed, i, inner)
				continue
			}
			eligible++
			before := TierALeaves.Load()
			resOn, errOn := engOn.Query(ctx, p).Exec(ctx)
			ran := TierALeaves.Load()-before > 0
			if ran {
				tierAran++
			}
			require.Equal(t, errOff == nil, errOn == nil,
				"seed=%d draw=%d q=%s start=%v end=%v step=%v: error mismatch on=%v off=%v",
				seed, i, inner, s, e, st, errOn, errOff)
			if errOff != nil {
				continue
			}
			requireDataEqual(t, resOff.Data, resOn.Data,
				"seed=%d draw=%d q=%s start=%v end=%v step=%v", seed, i, inner, s, e, st)
		}
	}

	if eligible == 0 {
		t.Fatal("property test ran no eligible leaves (vacuous)")
	}
	pct := 100 * tierAran / eligible
	if pct < propMinTierAShot {
		t.Fatalf("Tier A ran on only %d%% (%d/%d) of eligible leaves; want >= %d%%",
			pct, tierAran, eligible, propMinTierAShot)
	}
	t.Logf("Tier A ran on %d%% (%d/%d) of eligible leaves", pct, tierAran, eligible)
}

// anyEligibleLeaf reports whether any range-aggregation leaf inside q is
// eligible for Tier A. Used to count only the draws that should have run on
// Tier A in the coverage assertion.
func anyEligibleLeaf(t *testing.T, q string) bool {
	expr, err := syntax.ParseSampleExpr(q)
	require.NoError(t, err)
	_, leaves, ok := Map(expr)
	if !ok {
		return false
	}
	var a TierA
	for _, lf := range leaves {
		if a.Eligible(lf) {
			return true
		}
	}
	return false
}

type propEntry struct {
	ts time.Time
	ln string
	md []logproto.LabelAdapter
}

// seedPropData writes the property-test corpus: 6 streams, 3000 entries over
// 2h. Timestamps cluster on round seconds (many land exactly on window
// boundaries), 3% are exact duplicates pushed in a separate batch (a second
// part, no OPTIMIZE, to exercise FINAL dedup), and 20% carry 1-2
// structured-metadata keys. Returns the data start time and the word list
// used to build filters.
func seedPropData(t *testing.T, w *chstore.Writer) (time.Time, []string, error) {
	rng := rand.New(rand.NewSource(42))
	words := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
		"golf", "hotel", "india", "juliet", "kilo", "lima"}
	const total = 3000
	apps := []string{"web", "api", "worker", "cron", "batch", "edge"}

	// A fixed start keeps the test reproducible. Midnight makes leaves with an
	// offset reach into the previous day, where no streams exist (Tier A must
	// return empty Data, not nil).
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	byApp := make(map[string][]propEntry, len(apps))
	var dupes map[string][]logproto.Entry

	n := 0
	for n < total {
		app := apps[rng.Intn(len(apps))]
		// Cluster on round seconds: most land exactly on a second boundary.
		ts := start.Add(time.Duration(rng.Intn(7200)) * time.Second)
		if rng.Float64() < 0.1 {
			ts = ts.Add(time.Duration(rng.Intn(1000)) * time.Millisecond)
		}
		ln := words[rng.Intn(len(words))] + " " + words[rng.Intn(len(words))]
		var md []logproto.LabelAdapter
		if rng.Float64() < 0.2 {
			for j := 0; j < 1+rng.Intn(2); j++ {
				md = append(md,
					logproto.LabelAdapter{Name: "level", Value: words[rng.Intn(len(words))]},
					logproto.LabelAdapter{Name: "ns", Value: "n" + string(rune('0'+rng.Intn(10)))},
				)
			}
		}
		e := propEntry{ts: ts, ln: ln, md: md}
		byApp[app] = append(byApp[app], e)
		// 3% exact duplicates, kept aside for a second push batch.
		if rng.Float64() < 0.03 {
			if dupes == nil {
				dupes = make(map[string][]logproto.Entry)
			}
			dupes[app] = append(dupes[app], logproto.Entry{Timestamp: ts, Line: ln, StructuredMetadata: md})
		}
		n++
	}

	// Build the streams, per-app sorted by timestamp so the later duplicate
	// batch does not reorder entries within a stream.
	streams := make([]logproto.Stream, 0, len(apps))
	for _, app := range apps {
		ents := byApp[app]
		sort.Slice(ents, func(i, j int) bool { return ents[i].ts.Before(ents[j].ts) })
		es := make([]logproto.Entry, 0, len(ents))
		for _, e := range ents {
			es = append(es, logproto.Entry{Timestamp: e.ts, Line: e.ln, StructuredMetadata: e.md})
		}
		streams = append(streams, logproto.Stream{Labels: `{app="` + app + `"}`, Entries: es})
	}
	if err := w.Write(context.Background(), "t1", streams, nil); err != nil {
		return time.Time{}, nil, err
	}

	// Second batch: the exact duplicates, so the same (ts, line, md) lands in a
	// separate part and FINAL must dedup it.
	if len(dupes) > 0 {
		ds := make([]logproto.Stream, 0, len(dupes))
		for _, app := range apps {
			es := dupes[app]
			if len(es) == 0 {
				continue
			}
			ds = append(ds, logproto.Stream{Labels: `{app="` + app + `"}`, Entries: es})
		}
		if err := w.Write(context.Background(), "t1", ds, nil); err != nil {
			return time.Time{}, nil, err
		}
	}
	return start, words, nil
}
