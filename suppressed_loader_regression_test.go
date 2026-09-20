package ttlcache

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingLoader is a channel-controlled Loader harness. Every entry
// into the wrapped Load is announced on the entered channel, and the
// load only completes once the release channel is closed. This makes
// the TTL cache/singleflight/loader state boundaries replayable without
// sleeps.
type recordingLoader[K comparable, V any] struct {
	entered chan K
	release chan struct{}

	mu     sync.Mutex
	keys   []K
	onLoad func(c *Cache[K, V], key K) *Item[K, V]
}

func newRecordingLoader[K comparable, V any](onLoad func(c *Cache[K, V], key K) *Item[K, V]) *recordingLoader[K, V] {
	return &recordingLoader[K, V]{
		entered: make(chan K, 32),
		release: make(chan struct{}),
		onLoad:  onLoad,
	}
}

func (l *recordingLoader[K, V]) Load(c *Cache[K, V], key K) *Item[K, V] {
	l.mu.Lock()
	l.keys = append(l.keys, key)
	l.mu.Unlock()

	l.entered <- key
	<-l.release

	return l.onLoad(c, key)
}

func (l *recordingLoader[K, V]) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.keys)
}

// reopen arms the loader gate for another round of loads.
func (l *recordingLoader[K, V]) reopen() {
	l.release = make(chan struct{})
}

// waitLoaderEntry asserts that the underlying loader was entered for
// the expected key. The timeout only guards against deadlocks; it is
// never used to infer ordering.
func waitLoaderEntry[K comparable, V any](t *testing.T, l *recordingLoader[K, V], want K) {
	t.Helper()

	select {
	case got := <-l.entered:
		require.True(t, loaderKeysMatch(want, got), "expected loader entry for key %v, got %v", want, got)
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for the underlying loader to be entered for key %v", want)
	}
}

// loaderKeysMatch compares loader keys, treating two NaN values as a
// match since NaN is never equal to itself.
func loaderKeysMatch[K comparable](a, b K) bool {
	if a == b {
		return true
	}

	fa, oka := any(a).(float64)
	fb, okb := any(b).(float64)

	return oka && okb && math.IsNaN(fa) && math.IsNaN(fb)
}

// yieldToScheduler lets already-started goroutines reach the
// singleflight boundary without sleeping.
func yieldToScheduler() {
	for i := 0; i < 1000; i++ {
		runtime.Gosched()
	}
}

// checkConcurrentMissIsolation proves that concurrent misses for two
// distinct keys of the same type are not merged: while the load for
// keyA is still blocked, the load for keyB must enter the underlying
// loader and complete independently.
func checkConcurrentMissIsolation[K comparable](t *testing.T, keyA, keyB K, valueFn func(K) string) {
	t.Helper()

	loader := newRecordingLoader[K, string](func(_ *Cache[K, string], key K) *Item[K, string] {
		return &Item[K, string]{key: key, value: valueFn(key)}
	})
	sl := NewSuppressedLoader[K, string](loader, nil)
	c := New[K, string]()

	results := make(chan *Item[K, string], 2)

	go func() { results <- sl.Load(c, keyA) }()
	waitLoaderEntry(t, loader, keyA)

	// keyA's load is now blocked inside the underlying loader; keyB's
	// load must still reach the underlying loader on its own.
	go func() { results <- sl.Load(c, keyB) }()
	waitLoaderEntry(t, loader, keyB)

	close(loader.release)

	want := map[K]string{keyA: valueFn(keyA), keyB: valueFn(keyB)}
	for i := 0; i < 2; i++ {
		item := <-results
		require.NotNil(t, item)
		expected, ok := want[item.Key()]
		require.True(t, ok, "returned item carries an unexpected key: %v", item.Key())
		assert.Equal(t, expected, item.Value())
		delete(want, item.Key())
	}
	assert.Empty(t, want, "each distinct key must produce its own item")

	assert.Equal(t, 2, loader.callCount())
	assert.Equal(t, 0, c.Len(), "the loader did not call Set, so the cache must stay empty")
}

// checkConcurrentSameKeyMerge proves that concurrent misses for the
// same key are still merged into a single underlying loader call.
func checkConcurrentSameKeyMerge[K comparable](t *testing.T, key K, valueFn func(K) string) {
	t.Helper()

	loader := newRecordingLoader[K, string](func(_ *Cache[K, string], key K) *Item[K, string] {
		return &Item[K, string]{key: key, value: valueFn(key)}
	})
	sl := NewSuppressedLoader[K, string](loader, nil)
	c := New[K, string]()

	results := make(chan *Item[K, string], 2)

	go func() { results <- sl.Load(c, key) }()
	waitLoaderEntry(t, loader, key)

	go func() { results <- sl.Load(c, key) }()
	yieldToScheduler()

	close(loader.release)

	item1, item2 := <-results, <-results
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.Same(t, item1, item2, "overlapping loads of the same key must share the suppressed result")
	assert.Equal(t, key, item1.Key())
	assert.Equal(t, valueFn(key), item1.Value())

	assert.Equal(t, 1, loader.callCount())
	assert.Equal(t, 0, c.Len(), "the loader did not call Set, so the cache must stay empty")
}

func Test_SuppressedLoader_Load_ConcurrentMissKeyIsolation(t *testing.T) {
	t.Run("same string key is merged", func(t *testing.T) {
		checkConcurrentSameKeyMerge[string](t, "key", func(k string) string { return "value-" + k })
	})

	t.Run("different string keys are not merged", func(t *testing.T) {
		checkConcurrentMissIsolation[string](t, "key-a", "key-b", func(k string) string { return "value-" + k })
	})

	t.Run("custom int keys are not merged", func(t *testing.T) {
		type customInt int
		checkConcurrentMissIsolation[customInt](t, 1, 2, func(k customInt) string {
			return fmt.Sprintf("value-%d", int(k))
		})
	})

	t.Run("same custom int key is merged", func(t *testing.T) {
		type customInt int
		checkConcurrentSameKeyMerge[customInt](t, 7, func(k customInt) string {
			return fmt.Sprintf("value-%d", int(k))
		})
	})
}

// stringerKey produces the same text via String() for every value, so
// only the actual field values distinguish two keys.
type stringerKey struct {
	id int
}

func (k stringerKey) String() string {
	return "identical-text"
}

func Test_SuppressedLoader_Load_SameStringRepresentationDistinctKeys(t *testing.T) {
	t.Run("struct keys with identical String() output", func(t *testing.T) {
		valueFn := func(k stringerKey) string { return fmt.Sprintf("value-%d", k.id) }

		checkConcurrentMissIsolation[stringerKey](t, stringerKey{id: 1}, stringerKey{id: 2}, valueFn)
		checkConcurrentSameKeyMerge[stringerKey](t, stringerKey{id: 1}, valueFn)
	})

	t.Run("any keys with identical display but different dynamic types", func(t *testing.T) {
		valueFn := func(k any) string { return fmt.Sprintf("%T:%v", k, k) }

		// fmt.Sprint renders both keys as "1".
		require.Equal(t, fmt.Sprint(any(int(1))), fmt.Sprint(any(int64(1))))

		checkConcurrentMissIsolation[any](t, any(int(1)), any(int64(1)), valueFn)
		checkConcurrentSameKeyMerge[any](t, any(int(1)), valueFn)
	})
}

func Test_SuppressedLoader_Load_NaNKeysNotMerged(t *testing.T) {
	var callSeq atomic.Int64

	loader := newRecordingLoader[float64, string](func(_ *Cache[float64, string], key float64) *Item[float64, string] {
		return &Item[float64, string]{
			key:   key,
			value: fmt.Sprintf("load-%d", callSeq.Add(1)),
		}
	})
	sl := NewSuppressedLoader[float64, string](loader, nil)
	c := New[float64, string]()

	nan1 := math.NaN()
	nan2 := math.NaN()

	results := make(chan *Item[float64, string], 2)

	go func() { results <- sl.Load(c, nan1) }()
	waitLoaderEntry(t, loader, nan1)

	// The first NaN load is still in flight; the second NaN load must
	// neither reuse its result nor block on it.
	go func() { results <- sl.Load(c, nan2) }()
	waitLoaderEntry(t, loader, nan2)

	close(loader.release)

	item1, item2 := <-results, <-results
	require.NotNil(t, item1)
	require.NotNil(t, item2)
	assert.NotSame(t, item1, item2)
	assert.True(t, math.IsNaN(item1.Key()))
	assert.True(t, math.IsNaN(item2.Key()))
	assert.NotEqual(t, item1.Value(), item2.Value(), "each NaN load must be served by its own underlying call")

	assert.Equal(t, 2, loader.callCount())
}

func Test_SuppressedLoader_LoaderResultLifecycle(t *testing.T) {
	t.Run("nil result is not cached and a later miss reloads", func(t *testing.T) {
		loader := newRecordingLoader[string, string](func(_ *Cache[string, string], _ string) *Item[string, string] {
			return nil
		})
		sl := NewSuppressedLoader[string, string](loader, nil)
		c := New[string, string](WithLoader[string, string](sl))

		results := make(chan *Item[string, string], 2)

		go func() { results <- c.Get("key") }()
		waitLoaderEntry(t, loader, "key")

		go func() { results <- c.Get("key") }()
		yieldToScheduler()

		close(loader.release)

		require.Nil(t, <-results)
		require.Nil(t, <-results)
		assert.Equal(t, 1, loader.callCount(), "the concurrent nil round must be suppressed into one load")
		assert.Equal(t, 0, c.Len())

		metrics := c.Metrics()
		assert.Equal(t, uint64(2), metrics.Misses)
		assert.Equal(t, uint64(0), metrics.Hits)
		assert.Equal(t, uint64(0), metrics.Insertions)

		// A nil load must not leave anything behind: the next miss has
		// to reach the underlying loader again.
		loader.reopen()

		go func() { results <- c.Get("key") }()
		waitLoaderEntry(t, loader, "key")
		close(loader.release)

		require.Nil(t, <-results)
		assert.Equal(t, 2, loader.callCount())
		assert.Equal(t, 0, c.Len())

		metrics = c.Metrics()
		assert.Equal(t, uint64(3), metrics.Misses)
		assert.Equal(t, uint64(0), metrics.Hits)
		assert.Equal(t, uint64(0), metrics.Insertions)
	})

	t.Run("returned item without Set does not create a cache entry", func(t *testing.T) {
		var mu sync.Mutex
		var calls int

		sl := NewSuppressedLoader[string, string](LoaderFunc[string, string](func(_ *Cache[string, string], key string) *Item[string, string] {
			mu.Lock()
			calls++
			mu.Unlock()
			return &Item[string, string]{key: key, value: "value-" + key}
		}), nil)
		c := New[string, string](WithLoader[string, string](sl))

		item := c.Get("key")
		require.NotNil(t, item)
		assert.Equal(t, "key", item.Key())
		assert.Equal(t, "value-key", item.Value())
		assert.Equal(t, 0, c.Len(), "a loader that does not call Set must not populate the cache")
		assert.False(t, c.Has("key"))

		got, ok := c.GetAndDelete("key")
		require.NotNil(t, got)
		assert.True(t, ok)
		assert.Equal(t, 0, c.Len(), "GetAndDelete must not make an entry appear out of thin air")
		assert.False(t, c.Has("key"))

		assert.Equal(t, 2, calls)

		metrics := c.Metrics()
		assert.Equal(t, uint64(2), metrics.Misses)
		assert.Equal(t, uint64(0), metrics.Hits)
		assert.Equal(t, uint64(0), metrics.Insertions)
	})

	t.Run("loader Set stores distinct keys separately", func(t *testing.T) {
		loader := newRecordingLoader[string, string](func(c *Cache[string, string], key string) *Item[string, string] {
			return c.Set(key, "value-"+key, time.Hour)
		})
		sl := NewSuppressedLoader[string, string](loader, nil)
		c := New[string, string](WithLoader[string, string](sl))

		results := make(chan *Item[string, string], 2)

		go func() { results <- c.Get("key-a") }()
		waitLoaderEntry(t, loader, "key-a")

		go func() { results <- c.Get("key-b") }()
		waitLoaderEntry(t, loader, "key-b")

		close(loader.release)

		want := map[string]string{"key-a": "value-key-a", "key-b": "value-key-b"}
		for i := 0; i < 2; i++ {
			item := <-results
			require.NotNil(t, item)
			assert.Equal(t, want[item.Key()], item.Value())
			delete(want, item.Key())
		}
		assert.Equal(t, 2, loader.callCount())
		assert.Equal(t, 2, c.Len(), "each distinct key must be cached under its own entry")

		// Subsequent Gets hit the cache and never re-enter the loader.
		itemA := c.Get("key-a")
		itemB := c.Get("key-b")
		require.NotNil(t, itemA)
		require.NotNil(t, itemB)
		assert.Equal(t, "value-key-a", itemA.Value())
		assert.Equal(t, "value-key-b", itemB.Value())
		assert.Equal(t, 2, loader.callCount())

		metrics := c.Metrics()
		assert.Equal(t, uint64(2), metrics.Misses)
		assert.Equal(t, uint64(2), metrics.Hits)
		assert.Equal(t, uint64(2), metrics.Insertions)
	})
}
