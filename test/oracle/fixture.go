//go:build oracle

package oracle

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

// PushBatch is one push request: a tenant plus its Loki JSON body.
type PushBatch struct {
	Tenant string
	Body   []byte
}

const totalEntries = 2000

var (
	levels   = []string{"info", "warn", "error", "debug"}
	methods  = []string{"GET", "POST"}
	statuses = []int{200, 404, 500}
	apps     = []string{"api", "web", "legacy"}
	envs     = []string{"dev", "prod"}
	pods     = []string{"p1", "p2"}
	users    = []string{"alice", "bob", "carol", "dave", "erin"}
	words    = []string{"slow", "fast", "cache", "db", "auth", "timeout", "retry", "flush", "sync", "idle"}
	msgs     = []string{"handled request", "dropped connection", "cache miss", "flushed buffer", "auth ok", "db timeout", "gc pause", "metrics scraped"}
)

type fentry struct {
	ts   int64
	line string
	md   []map[string]string // per-entry structured metadata
}

type fstream struct {
	labels map[string]string
	entries []fentry
}

// Fixture returns a deterministic, seeded data set: 2 tenants, 12 streams over
// 3 apps × 2 envs × 2 pods, totalEntries entries per tenant over 90 minutes.
func Fixture(base time.Time) []PushBatch {
	var out []PushBatch
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		out = append(out, fixtureTenant(tenant, base))
	}
	return out
}

func fixtureTenant(tenant string, base time.Time) PushBatch {
	rnd := rand.New(rand.NewSource(42))
	streams := map[string]*fstream{}
	get := func(app, env, pod string) *fstream {
		key := app + "\x00" + env + "\x00" + pod
		s, ok := streams[key]
		if !ok {
			s = &fstream{labels: map[string]string{"app": app, "env": env, "pod": pod}}
			streams[key] = s
		}
		return s
	}
	add := func(app, env, pod string, ts time.Time, line string, md map[string]string) {
		e := fentry{ts: ts.UnixNano(), line: line}
		if md != nil {
			e.md = []map[string]string{md}
		}
		get(app, env, pod).entries = append(get(app, env, pod).entries, e)
	}

	step := (90 * time.Minute) / time.Duration(totalEntries)
	for i := 0; i < totalEntries; i++ {
		ts := base.Add(time.Duration(i) * step).Truncate(time.Microsecond)
		app := apps[rnd.Intn(len(apps))]
		env := envs[rnd.Intn(len(envs))]
		pod := pods[rnd.Intn(len(pods))]
		md := map[string]string{}
		if rnd.Float64() < 0.5 {
			md["trace_id"] = strconv.Itoa(rnd.Intn(1<<16))
		}
		var line string
		switch app {
		case "api":
			line = fmt.Sprintf(`{"level":"%s","status":%d,"latency_ms":%d,"user":{"id":%d,"name":"%s"},"tags":["alpha","beta"]}`,
				levels[rnd.Intn(len(levels))], statuses[rnd.Intn(len(statuses))], rnd.Intn(500)+1,
				rnd.Intn(100), users[rnd.Intn(len(users))])
		case "legacy":
			ip := fmt.Sprintf("10.%d.%d.%d", rnd.Intn(255)+1, rnd.Intn(255), rnd.Intn(255))
			line = fmt.Sprintf("%s from %s %s %s", words[rnd.Intn(len(words))], ip, words[rnd.Intn(len(words))], words[rnd.Intn(len(words))])
			if rnd.Float64() < 0.5 {
				line = "\x1b[31m" + line + "\x1b[0m"
			}
		default:
			line = fmt.Sprintf("level=%s method=%s status=%d duration=%dms bytes=%d msg=\"%s %s\"",
				levels[rnd.Intn(len(levels))], methods[rnd.Intn(len(methods))], statuses[rnd.Intn(len(statuses))],
				rnd.Intn(500), rnd.Intn(10000), words[rnd.Intn(len(words))], msgs[rnd.Intn(len(msgs))])
		}
		add(app, env, pod, ts, line, md)
	}

	// Three deliberate exact duplicates and two same-timestamp pairs in one stream.
	wdp := get("web", "dev", "p1")
	if len(wdp.entries) > 2 {
		wdp.entries = append(wdp.entries, wdp.entries[0], wdp.entries[1], wdp.entries[2])
	}
	base2 := base.Add(45 * time.Minute).Truncate(time.Microsecond)
	wdp.entries = append(wdp.entries,
		fentry{ts: base2.UnixNano(), line: "twin one"},
		fentry{ts: base2.UnixNano(), line: "twin two"})

	keys := make([]string, 0, len(streams))
	for k := range streams {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var ss []map[string]any
	for _, k := range keys {
		s := streams[k]
		vals := make([]any, 0, len(s.entries))
		for _, e := range s.entries {
			v := []any{strconv.FormatInt(e.ts, 10), e.line}
			if len(e.md) > 0 && len(e.md[0]) > 0 {
				v = append(v, e.md[0])
			}
			vals = append(vals, v)
		}
		ss = append(ss, map[string]any{"stream": s.labels, "values": vals})
	}
	body, _ := json.Marshal(map[string]any{"streams": ss})
	return PushBatch{Tenant: tenant, Body: body}
}
