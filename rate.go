package throttled

import (
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

// A RateLimiter manages limiting the rate of actions by key.
type RateLimiter interface {
	// RateLimit checks whether a particular key has exceeded a rate
	// limit. It also returns a RateLimitResult to provide additional
	// information about the state of the RateLimiter.
	//
	// If the rate limit has not been exceeded, the underlying storage
	// is updated by the supplied quantity. For example, a quantity of
	// 1 might be used to rate limit a single request while a greater
	// quantity could rate limit based on the size of a file upload in
	// megabytes. If quantity is 0, no update is performed allowing
	// you to "peek" at the state of the RateLimiter for a given key.
	RateLimit(key string, quantity int) (bool, RateLimitResult, error)
}

// RateLimitResult represents the state of the RateLimiter for a
// given key at the time of the query. This state can be used, for
// example, to communicate information to the client via HTTP
// headers. Negative values indicate that the attribute is not
// relevant to the implementation or state.
type RateLimitResult struct {
	// Limit is the maximum number of requests that could be permitted
	// instantaneously for this key starting from an empty state. For
	// example, if a rate limiter allows 10 requests per second per
	// key, Limit would always be 10.
	Limit int

	// Remaining is the maximum number of requests that could be
	// permitted instantaneously for this key given the current
	// state. For example, if a rate limiter allows 10 requests per
	// second and has already received 6 requests for this key this
	// second, Remaining would be 4.
	Remaining int

	// ResetAfter is the time until the RateLimiter returns to its
	// initial state for a given key. For example, if a rate limiter
	// manages requests per second and received one request 200ms ago,
	// Reset would return 800ms. You can also think of this as the time
	// until Limit and Remaining will be equal.
	ResetAfter time.Duration

	// RetryAfter is the time until the next request will be permitted.
	// It should be -1 unless the rate limit has been exceeded.
	RetryAfter time.Duration
}

type limitResult struct {
	limited bool
}

func (r *limitResult) Limited() bool { return r.limited }

type rateLimitResult struct {
	limitResult

	limit, remaining  int
	reset, retryAfter time.Duration
}

func (r *rateLimitResult) Limit() int                { return r.limit }
func (r *rateLimitResult) Remaining() int            { return r.remaining }
func (r *rateLimitResult) Reset() time.Duration      { return r.reset }
func (r *rateLimitResult) RetryAfter() time.Duration { return r.retryAfter }

// Rate describes a frequency of an activity such as the number of requests
// allowed per minute.
type Rate struct {
	period time.Duration // Time between equally spaced requests at the rate
	count  int           // Used internally for deprecated `RateLimit` interface only
}

// RateQuota describes the number of requests allowed per time period.
// MaxRate specified the maximum sustained rate of requests and must
// be greater than zero. MaxBurst defines the number of requests that
// will be allowed to exceed the rate in a single burst and must be
// greater than or equal to zero.
//
// Rate{PerSec(1), 0} would mean that after each request, no more
// requests will be permitted for that client for one second.
// Rate{PerSec(2), 0} permits one request per 0.5 seconds rather than
// two requests in one second. In practice, you probably want to set
// MaxBurst >0 to provide some flexibility to clients that only need
// to make a handful of requests. In fact a MaxBurst of zero will
// *never* permit a request with a quantity greater than one because
// it will immediately exceed the limit.
type RateQuota struct {
	MaxRate  Rate
	MaxBurst int
}

// PerSec represents a number of requests per second.
func PerSec(n int) Rate { return Rate{time.Second / time.Duration(n), n} }

// PerMin represents a number of requests per minute.
func PerMin(n int) Rate { return Rate{time.Minute / time.Duration(n), n} }

// PerHour represents a number of requests per hour.
func PerHour(n int) Rate { return Rate{time.Hour / time.Duration(n), n} }

// PerDay represents a number of requests per day.
func PerDay(n int) Rate { return Rate{24 * time.Hour / time.Duration(n), n} }

// Clock provides the current time right now.
type Clock interface {
	Now() time.Time
}

// ClockFunc is a wrapper for functions that provide the current time right
// now.
type ClockFunc func() time.Time

// Now provides the current time right now as defined by the function wrapped
// in ClockFunc.
func (cf ClockFunc) Now() time.Time {
	return cf()
}

// GCRARateLimiter is a RateLimiter that users the generic cell-rate
// algorithm. The algorithm has been slightly modified from its usual
// form to support limiting with an additional quantity parameter, such
// as for limiting the number of bytes uploaded.
type GCRARateLimiter struct {
	Clock Clock

	limit int

	// Think of the DVT as our flexibility:
	// How far can you deviate from the nominal equally spaced schedule?
	// If you like leaky buckets, think about it as the size of your bucket.
	delayVariationTolerance time.Duration

	// Think of the emission interval as the time between events
	// in the nominal equally spaced schedule. If you like leaky buckets,
	// think of it as how frequently the bucket leaks one unit.
	emissionInterval time.Duration

	keys *lru.Cache

	sync.Mutex
}

// NewGCRARateLimiter creates a GCRARateLimiter. quota.Count defines
// the maximum number of requests permitted in an instantaneous burst
// and quota.Count / quota.Period defines the maximum sustained
// rate. For example, PerMin(60) permits 60 requests instantly per key
// followed by one request per second indefinitely whereas PerSec(1)
// only permits one request per second with no tolerance for bursts.
func NewGCRARateLimiter(maxKeys int, quota RateQuota) (*GCRARateLimiter, error) {
	if quota.MaxBurst < 0 {
		return nil, fmt.Errorf("Invalid RateQuota %#v. MaxBurst must be greater than zero.", quota)
	}
	if quota.MaxRate.period <= 0 {
		return nil, fmt.Errorf("Invalid RateQuota %#v. MaxRate must be greater than zero.", quota)
	}

	keys, err := lru.New(maxKeys)
	if err != nil {
		return nil, err
	}

	return &GCRARateLimiter{
		Clock:                   ClockFunc(time.Now),
		delayVariationTolerance: quota.MaxRate.period * (time.Duration(quota.MaxBurst) + 1),
		emissionInterval:        quota.MaxRate.period,
		limit:                   quota.MaxBurst + 1,
		keys:                    keys,
	}, nil
}

// RateLimit checks whether a particular key has exceeded a rate
// limit. It also returns a RateLimitResult to provide additional
// information about the state of the RateLimiter.
//
// If the rate limit has not been exceeded, the underlying storage is
// updated by the supplied quantity. For example, a quantity of 1
// might be used to rate limit a single request while a greater
// quantity could rate limit based on the size of a file upload in
// megabytes. If quantity is 0, no update is performed allowing you
// to "peek" at the state of the RateLimiter for a given key.
func (g *GCRARateLimiter) RateLimit(key string, quantity int) (bool, RateLimitResult, error) {
	var tat, newTat, now time.Time
	var ttl time.Duration
	rlc := RateLimitResult{Limit: g.limit, RetryAfter: -1}
	limited := false

	var tatVal int64

	g.Lock()

	// tat refers to the theoretical arrival time that would be expected
	// from equally spaced requests at exactly the rate limit.

	// START g.store.GetWithTime
	now = g.Clock.Now()
	storeVal, ok := g.keys.Get(key)
	if !ok {
		tatVal = -1
	} else {
		tatVal = *storeVal.(*int64)
	}
	// END g.store.GetWithTime

	if tatVal == -1 {
		tat = now
	} else {
		tat = time.Unix(0, tatVal)
	}

	increment := time.Duration(quantity) * g.emissionInterval
	if now.After(tat) {
		newTat = now.Add(increment)
	} else {
		newTat = tat.Add(increment)
	}

	// Block the request if the next permitted time is in the future
	allowAt := newTat.Add(-(g.delayVariationTolerance))
	if diff := now.Sub(allowAt); diff < 0 {
		if increment <= g.delayVariationTolerance {
			rlc.RetryAfter = -diff
		}
		ttl = tat.Sub(now)
		limited = true
	}

	if !limited {
		ttl = newTat.Sub(now)

		newVal := newTat.UnixNano()
		if tatVal == -1 {
			g.keys.Add(key, &newVal)
		} else {
			*storeVal.(*int64) = newVal
		}
	}

	g.Unlock()

	next := g.delayVariationTolerance - ttl
	if next > -g.emissionInterval {
		rlc.Remaining = int(next / g.emissionInterval)
	}
	rlc.ResetAfter = ttl

	return limited, rlc, nil
}
