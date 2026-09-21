# Changelog

## Unreleased

### Fixed: `SuppressedLoader` merged concurrent misses of distinct keys

**Root cause.** `SuppressedLoader.Load` mapped every generic key to its
singleflight group key with `fmt.Sprintf("%T", key)` — the *type only*.
Any two values of the same type (e.g. user IDs `1` and `2`, or `"alpha"`
and `"beta"`) therefore shared one flight: a concurrent miss for key B
joined the in-flight load of key A and received A's `Item`, key and
value included.

**Implementation choice.** The wrapper still maps the generic
`comparable` key to a string for `singleflight.Group`, but now encodes
both type and value: `fmt.Sprintf("%T|%#v", key, key)`.

- `%T` keeps dynamic types apart when `K` is an interface (`int(7)` vs
  `int64(7)` display identically under `%v`).
- `%#v` (Go-syntax dump) is used instead of `%v` so that distinct values
  whose `String()` output collides still map to distinct flights.
- Keys that are not equal to themselves (`key != key`, i.e. NaN floats
  or anything containing one) bypass singleflight entirely: two NaN keys
  are always distinct, so each `Load` must execute the wrapped loader
  independently and must never block on, or reuse, another NaN flight.

**Coverage gap closed.** The previous tests only exercised a single
string key (`"test"`), so type-only flight keys could not be observed.
The new suite in `suppressed_loader_test.go` replays the concurrency
with channel-controlled entry/release (no sleeps): a controlled loader
announces every underlying call on a channel and blocks until the test
releases it, and same-key merge tests additionally wait until the
duplicate goroutine is observably parked inside `singleflight.(*Group).Do`
instead of inferring it from a start barrier.

**Adjacent semantics protected.**

- Same-key overlap still suppresses to exactly one underlying call and
  shares the flight's `Item` pointer (`..._ConcurrentKeys/same_key...`,
  `..._KeysWithSameStringRepresentation/overlapping...`).
- Nil loader results are not retained: a later miss reloads
  (`..._LoaderResultLifecycle/nil_result...`).
- A returned `Item` without `Set` creates no cache entry: `Get` and
  `GetAndDelete` stay misses and `Metrics.Insertions` stays 0
  (`..._LoaderResultLifecycle/returned_item_without_Set...`).
- A loader that calls `Set` caches distinct keys separately; later
  `Get`s hit without reloading, verified via `Metrics` misses/hits/
  insertions (`..._LoaderResultLifecycle/loader_Set...`).

**Most dangerous counterexample.** Two different keys of the same type
miss concurrently — e.g. custom int keys `1` and `2`: under the old
mapping the second request silently received the first key's `Item`
(wrong key *and* wrong value, i.e. cross-tenant data served from the
loader) while the cache recorded only the first key. This is locked by
`Test_SuppressedLoader_Load_ConcurrentKeys/distinct_custom_int_keys_load_independently`
(together with its string-key and same-`String()` variants), which
proves via loader-entry channels that the second key reaches the
underlying loader while the first key's load is still in flight, and
that each caller receives its own `Item.Key`/`Item.Value`.
