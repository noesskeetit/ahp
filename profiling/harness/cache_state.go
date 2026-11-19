package harness

import "fmt"

// CacheProbe states, per step, whether the caches behind the measurement were
// cold or warm. Numbers from a cold PG buffer cache and numbers from a warm
// one are different experiments, and comparing them silently (repeat 1 against
// repeat 3 of another rung) is how "regressions" get invented.
type CacheProbe struct {
	Label              string   `json:"label"` // warm | cold | unknown
	PGHitRatio         *float64 `json:"pg_cache_hit_ratio"`
	PGBlksReadDelta    *int64   `json:"pg_blks_read_delta"`
	PGBlksHitDelta     *int64   `json:"pg_blks_hit_delta"`
	RedisUsedMemory    *int64   `json:"redis_used_memory_bytes"`
	RedisCommandsDelta *int64   `json:"redis_commands_delta"`
	PlatformRSSBytes   *uint64  `json:"platform_rss_bytes"`
	// PGObservations is how many samples of this step actually saw
	// pg_stat_database. Null deltas with 0 observations mean "cache state was
	// never measured" -- which is NOT the same statement as "no block
	// activity happened" (metrics_contract.go / DEF-PROF-3).
	PGObservations int    `json:"pg_observations"`
	Note           string `json:"note"`
}

// ProbeCache derives the state from the sampler stream of the step itself.
// The rule is declared, not tuned: hit ratio >= 0.99 counts as warm.
func ProbeCache(subs []Sample) *CacheProbe {
	if len(subs) < 2 {
		return &CacheProbe{Label: "unknown", PGObservations: countObserved(subs, pgHits),
			Note: "fewer than two samples in this step: cache state was not observed"}
	}
	p := &CacheProbe{
		PGBlksReadDelta:    deltaObserved(subs, pgReads),
		PGBlksHitDelta:     deltaObserved(subs, pgHits),
		RedisUsedMemory:    lastObserved(subs, func(x Sample) *int64 { return x.RedisUsedMemory }),
		RedisCommandsDelta: deltaObserved(subs, func(x Sample) *int64 { return x.RedisTotalCommands }),
		PlatformRSSBytes:   lastObserved(subs, func(x Sample) *uint64 { return x.ProcRSS }),
		PGObservations:     countObserved(subs, pgHits),
	}
	if p.PGBlksReadDelta == nil || p.PGBlksHitDelta == nil {
		p.Label = "unknown"
		p.Note = fmt.Sprintf("PG block statistics were observed in %d of %d samples: cache state was NOT measured for this step (unmeasured is not the same as idle)", p.PGObservations, len(subs))
		return p
	}
	hitDelta, readDelta := *p.PGBlksHitDelta, *p.PGBlksReadDelta
	denom := hitDelta + readDelta
	if denom <= 0 {
		p.Label = "unknown"
		p.Note = "no PG block activity observed in this window: hit ratio undefined"
		return p
	}
	ratio := float64(hitDelta) / float64(denom)
	p.PGHitRatio = &ratio
	if ratio >= 0.99 {
		p.Label = "warm"
		p.Note = "PG hit ratio >= 0.99 over the step window"
	} else {
		p.Label = "cold"
		p.Note = "PG hit ratio below 0.99: reads were still hitting disk; treat these numbers as cold-cache"
	}
	return p
}

func pgReads(x Sample) *int64 { return x.PGBlksRead }
func pgHits(x Sample) *int64  { return x.PGBlksHit }
