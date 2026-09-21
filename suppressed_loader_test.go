package ttlcache

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// controlledStringLoader is a Loader whose calls can be observed and
// released by the test through channels. Every underlying call is
// recorded, announced on the entered channel, and then blocked until
// the release channel is closed. This lets tests replay concurrent
// loader/singleflight interleavings without sleeps.
type controlledStringLoader[K comparable] struct {
	mu        sync.Mutex
	keys      []K
	entered   chan K
	release   chan struct{}
	set       bool // insert the loaded item into the cache
	returnNil bool
}

func newControlledStringLoader[K comparable]() *controlledStringLoader[K] {
	return &controlledStringLoader[K]{
		entered: make(chan K, 16),
		release: make(chan struct{}),
	}
}

func (l *controlledStringLoader[K]) load(c *Cache[K, string], key K) *Item[K, string] {
	l.mu.Lock()
	l.keys = append(l.keys, key)
	seq := len(l.keys)
	l.mu.Unlock()

	l.entered <- key
	<-l.release

	if l.returnNil {
		return nil
	}

	value := fmt.Sprintf("value-%v#%d", key, seq)
	if l.set {
		return c.Set(key, value, time.Hour)
	}

	return NewItemWithOpts(key, value, time.Hour)
}

func (l *controlledStringLoader[K]) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.keys)
}

func (l *controlledStringLoader[K]) calledKeys() []K {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]K(nil), l.keys...)
}

// waitForSingleflightDuplicate blocks until at least two goroutines
// are inside singleflight.(*Group).Do, proving that a duplicate
// request has actually joined the in-flight flight instead of merely
// having been started. It relies on goroutine scheduling only, never
// on sleeps.
func waitForSingleflightDuplicate(t *testing.T) {
	t.Helper()

	const frame = "singleflight.(*Group).Do"
	buf := make([]byte, 1<<20)

	for {
		n := runtime.Stack(buf, true)
		if strings.Count(string(buf[:n]), frame) >= 2 {
			return
		}

		runtime.Gosched()
	}
}

// loadAsync starts a concurrent SuppressedLoader.Load call. The returned
// channel is closed once the goroutine has been spawned, and the result
// is written into the returned pointer holder once the call completes.
func loadAsync[K comparable](sl *SuppressedLoader[K, string], c *Cache[K, string], key K, started chan struct{}) <-chan *Item[K, string] {
	resCh := make(chan *Item[K, string], 1)

	go func() {
		if started != nil {
			close(started)
		}

		resCh <- sl.Load(c, key)
	}()

	return resCh
}

func Test_SuppressedLoader_Load_ConcurrentKeys(t *testing.T) {
	t.Run("same key is suppressed into a single load", func(t *testing.T) {
		loader := newControlledStringLoader[string]()
		loader.set = true
		sl := NewSuppressedLoader[string, string](LoaderFunc[string, string](loader.load), nil)
		cache := New[string, string]()

		first := loadAsync(sl, cache, "same", nil)
		require.Equal(t, "same", <-loader.entered, "first load should be in-flight")

		secondStarted := make(chan struct{})
		second := loadAsync(sl, cache, "same", secondStarted)
		<-secondStarted
		waitForSingleflightDuplicate(t)

		close(loader.release)

		item1, item2 := <-first, <-second
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		assert.Equal(t, 1, loader.callCount(), "overlapping loads of the same key must be merged")
		assert.Same(t, item1, item2, "merged loads must share the flight result")
		assert.Equal(t, "same", item1.Key())
		assert.Equal(t, "value-same#1", item1.Value())

		cached := cache.Get("same")
		require.NotNil(t, cached, "loader-inserted item should be cached after the race")
		assert.Equal(t, item1.Value(), cached.Value())
	})

	t.Run("distinct string keys load independently", func(t *testing.T) {
		loader := newControlledStringLoader[string]()
		loader.set = true
		sl := NewSuppressedLoader[string, string](LoaderFunc[string, string](loader.load), nil)
		cache := New[string, string]()

		first := loadAsync(sl, cache, "alpha", nil)
		require.Equal(t, "alpha", <-loader.entered, "first load should be in-flight")

		secondStarted := make(chan struct{})
		second := loadAsync(sl, cache, "beta", secondStarted)
		<-secondStarted
		require.Equal(t, "beta", <-loader.entered,
			"distinct key must reach the loader while another key's load is in-flight")

		close(loader.release)

		item1, item2 := <-first, <-second
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		assert.Equal(t, 2, loader.callCount())
		assert.Equal(t, "alpha", item1.Key())
		assert.Equal(t, "value-alpha#1", item1.Value())
		assert.Equal(t, "beta", item2.Key())
		assert.Equal(t, "value-beta#2", item2.Value())

		for key, value := range map[string]string{"alpha": item1.Value(), "beta": item2.Value()} {
			cached := cache.Get(key)
			require.NotNil(t, cached, "key %q should be cached after the race", key)
			assert.Equal(t, value, cached.Value())
		}
	})

	t.Run("distinct custom int keys load independently", func(t *testing.T) {
		type customIntKey int

		loader := newControlledStringLoader[customIntKey]()
		loader.set = true
		sl := NewSuppressedLoader[customIntKey, string](LoaderFunc[customIntKey, string](loader.load), nil)
		cache := New[customIntKey, string]()

		first := loadAsync(sl, cache, customIntKey(1), nil)
		require.Equal(t, customIntKey(1), <-loader.entered, "first load should be in-flight")

		secondStarted := make(chan struct{})
		second := loadAsync(sl, cache, customIntKey(2), secondStarted)
		<-secondStarted
		require.Equal(t, customIntKey(2), <-loader.entered,
			"same-type keys with different values must not share a flight")

		close(loader.release)

		item1, item2 := <-first, <-second
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		assert.Equal(t, 2, loader.callCount())
		assert.Equal(t, customIntKey(1), item1.Key())
		assert.Equal(t, "value-1#1", item1.Value())
		assert.Equal(t, customIntKey(2), item2.Key())
		assert.Equal(t, "value-2#2", item2.Value())

		for _, key := range []customIntKey{1, 2} {
			cached := cache.Get(key)
			require.NotNil(t, cached, "key %v should be cached after the race", key)
			assert.Equal(t, key, cached.Key())
		}
	})
}

// collidingStringer is a comparable key whose String() output does not
// depend on its value, so %v-based flight keys would wrongly merge
// distinct values.
type collidingStringer struct {
	id int
}

func (k collidingStringer) String() string {
	return "colliding-stringer"
}

func Test_SuppressedLoader_Load_KeysWithSameStringRepresentation(t *testing.T) {
	t.Run("struct keys with identical String output", func(t *testing.T) {
		loader := newControlledStringLoader[collidingStringer]()
		sl := NewSuppressedLoader[collidingStringer, string](
			LoaderFunc[collidingStringer, string](loader.load), nil,
		)
		cache := New[collidingStringer, string]()

		key1, key2 := collidingStringer{id: 1}, collidingStringer{id: 2}
		require.Equal(t, key1.String(), key2.String(), "test requires identical String output")
		require.NotEqual(t, key1, key2, "test requires distinct key values")

		first := loadAsync(sl, cache, key1, nil)
		require.Equal(t, key1, <-loader.entered, "first load should be in-flight")

		secondStarted := make(chan struct{})
		second := loadAsync(sl, cache, key2, secondStarted)
		<-secondStarted
		require.Equal(t, key2, <-loader.entered,
			"keys that only share a String representation must not share a flight")

		close(loader.release)

		item1, item2 := <-first, <-second
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		assert.Equal(t, 2, loader.callCount())
		assert.Equal(t, key1, item1.Key())
		assert.Equal(t, "value-colliding-stringer#1", item1.Value())
		assert.Equal(t, key2, item2.Key())
		assert.Equal(t, "value-colliding-stringer#2", item2.Value())
	})

	t.Run("any keys with different dynamic types", func(t *testing.T) {
		loader := newControlledStringLoader[any]()
		sl := NewSuppressedLoader[any, string](LoaderFunc[any, string](loader.load), nil)
		cache := New[any, string]()

		var key1, key2 any = int(7), int64(7)
		require.Equal(t, fmt.Sprint(key1), fmt.Sprint(key2), "test requires identical display text")
		require.NotEqual(t, key1, key2, "test requires distinct dynamic types")

		first := loadAsync(sl, cache, key1, nil)
		require.Equal(t, key1, <-loader.entered, "first load should be in-flight")

		secondStarted := make(chan struct{})
		second := loadAsync(sl, cache, key2, secondStarted)
		<-secondStarted
		require.Equal(t, key2, <-loader.entered,
			"any keys with different dynamic types must not share a flight")

		close(loader.release)

		item1, item2 := <-first, <-second
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		assert.Equal(t, 2, loader.callCount())
		assert.Equal(t, key1, item1.Key())
		assert.Equal(t, "value-7#1", item1.Value())
		assert.Equal(t, key2, item2.Key())
		assert.Equal(t, "value-7#2", item2.Value())
	})

	t.Run("overlapping loads of an equal key still merge", func(t *testing.T) {
		loader := newControlledStringLoader[collidingStringer]()
		sl := NewSuppressedLoader[collidingStringer, string](
			LoaderFunc[collidingStringer, string](loader.load), nil,
		)
		cache := New[collidingStringer, string]()

		key := collidingStringer{id: 1}

		first := loadAsync(sl, cache, key, nil)
		require.Equal(t, key, <-loader.entered, "first load should be in-flight")

		secondStarted := make(chan struct{})
		second := loadAsync(sl, cache, key, secondStarted)
		<-secondStarted
		waitForSingleflightDuplicate(t)

		close(loader.release)

		item1, item2 := <-first, <-second
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		assert.Equal(t, 1, loader.callCount(), "equal keys must still be deduplicated")
		assert.Same(t, item1, item2)
		assert.Equal(t, key, item1.Key())
	})
}

func Test_SuppressedLoader_Load_NaNKeys(t *testing.T) {
	loader := newControlledStringLoader[float64]()
	sl := NewSuppressedLoader[float64, string](LoaderFunc[float64, string](loader.load), nil)
	cache := New[float64, string]()

	first := loadAsync(sl, cache, math.NaN(), nil)
	<-loader.entered // first NaN load is in-flight

	secondStarted := make(chan struct{})
	second := loadAsync(sl, cache, math.NaN(), secondStarted)
	<-secondStarted
	<-loader.entered // a second NaN load must neither block on nor reuse the first flight

	close(loader.release)

	item1, item2 := <-first, <-second
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.Equal(t, 2, loader.callCount(), "NaN keys are never equal, so each load must execute")
	assert.NotSame(t, item1, item2)
	assert.True(t, math.IsNaN(item1.Key()))
	assert.True(t, math.IsNaN(item2.Key()))
	assert.Equal(t, "value-NaN#1", item1.Value())
	assert.Equal(t, "value-NaN#2", item2.Value())
}

func Test_SuppressedLoader_Load_LoaderResultLifecycle(t *testing.T) {
	t.Run("nil result is not retained and later misses reload", func(t *testing.T) {
		loader := newControlledStringLoader[string]()
		loader.returnNil = true
		sl := NewSuppressedLoader[string, string](LoaderFunc[string, string](loader.load), nil)
		cache := New[string, string]()

		first := loadAsync(sl, cache, "missing", nil)
		require.Equal(t, "missing", <-loader.entered, "first load should be in-flight")

		secondStarted := make(chan struct{})
		second := loadAsync(sl, cache, "missing", secondStarted)
		<-secondStarted
		waitForSingleflightDuplicate(t)

		close(loader.release)

		assert.Nil(t, <-first)
		assert.Nil(t, <-second)
		assert.Equal(t, 1, loader.callCount(), "overlapping nil-result loads should still merge")

		// A nil result must not be remembered: the next miss triggers
		// a fresh underlying load.
		loader.release = make(chan struct{})
		third := loadAsync(sl, cache, "missing", nil)
		require.Equal(t, "missing", <-loader.entered, "miss after a nil result must reload")
		close(loader.release)

		assert.Nil(t, <-third)
		assert.Equal(t, 2, loader.callCount())
		assert.Nil(t, cache.Get("missing"), "nil loader results must not create cache entries")
	})

	t.Run("returned item without Set does not create cache entries", func(t *testing.T) {
		loader := newControlledStringLoader[string]() // set == false
		sl := NewSuppressedLoader[string, string](LoaderFunc[string, string](loader.load), nil)
		cache := New[string, string]()

		resCh := loadAsync(sl, cache, "ghost", nil)
		require.Equal(t, "ghost", <-loader.entered, "load should be in-flight")
		close(loader.release)

		item := <-resCh
		require.NotNil(t, item)
		assert.Equal(t, "ghost", item.Key())

		assert.Nil(t, cache.Get("ghost"), "Get must not see an item the loader never Set")
		deleted, ok := cache.GetAndDelete("ghost")
		assert.Nil(t, deleted)
		assert.False(t, ok, "GetAndDelete must not fabricate an entry")

		metrics := cache.Metrics()
		assert.Equal(t, uint64(2), metrics.Misses, "Get and GetAndDelete both missed")
		assert.Equal(t, uint64(0), metrics.Hits)
		assert.Equal(t, uint64(0), metrics.Insertions, "no insertion may happen without Set")
	})

	t.Run("loader Set caches distinct keys separately", func(t *testing.T) {
		loader := newControlledStringLoader[string]()
		loader.set = true
		sl := NewSuppressedLoader[string, string](LoaderFunc[string, string](loader.load), nil)
		cache := New[string, string](WithLoader[string, string](sl))

		results := make([]<-chan *Item[string, string], 0, 2)

		getAsync := func(key string, started chan struct{}) <-chan *Item[string, string] {
			resCh := make(chan *Item[string, string], 1)

			go func() {
				if started != nil {
					close(started)
				}

				resCh <- cache.Get(key)
			}()

			return resCh
		}

		results = append(results, getAsync("k1", nil))
		require.Equal(t, "k1", <-loader.entered, "first Get should be loading")

		secondStarted := make(chan struct{})
		results = append(results, getAsync("k2", secondStarted))
		<-secondStarted
		require.Equal(t, "k2", <-loader.entered,
			"distinct key must reach the loader while another Get is loading")

		close(loader.release)

		item1, item2 := <-results[0], <-results[1]
		require.NotNil(t, item1)
		require.NotNil(t, item2)
		assert.Equal(t, "k1", item1.Key())
		assert.Equal(t, "value-k1#1", item1.Value())
		assert.Equal(t, "k2", item2.Key())
		assert.Equal(t, "value-k2#2", item2.Value())

		metrics := cache.Metrics()
		assert.Equal(t, uint64(2), metrics.Misses, "both initial Gets missed")
		assert.Equal(t, uint64(2), metrics.Insertions, "each loader Set inserted once")
		assert.Equal(t, uint64(0), metrics.Hits)

		// Subsequent Gets hit the cache and do not load again.
		hit1, hit2 := cache.Get("k1"), cache.Get("k2")
		require.NotNil(t, hit1)
		require.NotNil(t, hit2)
		assert.Equal(t, item1.Value(), hit1.Value())
		assert.Equal(t, item2.Value(), hit2.Value())
		assert.Equal(t, 2, loader.callCount(), "cache hits must not call the loader")

		metrics = cache.Metrics()
		assert.Equal(t, uint64(2), metrics.Misses)
		assert.Equal(t, uint64(2), metrics.Hits)
		assert.Equal(t, uint64(2), metrics.Insertions)
	})
}
