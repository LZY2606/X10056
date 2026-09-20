# Changelog

## Unreleased

### Fixed: `SuppressedLoader` merged concurrent misses of distinct keys

#### Root cause

`SuppressedLoader.Load` built its `singleflight.Group` key with
`fmt.Sprintf("%T", key)`, i.e. only the *type* of the cache key. Every
concurrent miss for keys of the same type collapsed into one underlying
`Loader.Load` call, and all waiters received the first key's `Item` —
its key/value did not correspond to their own request.

#### Implementation choices

The singleflight key is now derived from both the type and the value of
the cache key (`cache.go`):

- `%T|%#v` pins the (dynamic) type and the value. `%#v` renders Go
  syntax and ignores `Stringer` implementations, so two unequal struct
  values whose `String()` text is identical still map to different
  group keys. The `%T` prefix keeps `any` keys with equal display but
  different dynamic types (e.g. `int(1)` vs `int64(1)`, both rendered
  as `1` by `%#v`) apart.
- Pointer-like keys (`Ptr`, `Chan`, `UnsafePointer`) additionally mix
  in their address via reflection, because `%#v` renders a pointer's
  pointee: two distinct pointers to equal values are different cache
  keys and must not be merged.
- Keys that are not equal to themselves (`key != key`, i.e. NaN floats
  and values containing them) bypass suppression entirely and are
  passed straight to the wrapped loader. Such keys can never be
  retrieved from a map, so merging their loads is both incorrect (two
  NaN keys are always distinct) and useless.

The cache/`singleflight.Group` boundary itself is unchanged: same-key
concurrent misses still share exactly one underlying loader call and
one returned `*Item`.

#### Gaps in the original coverage

The pre-existing `Test_SuppressedLoader_Load` only exercised a single
key (`"test"`), so the type-only group key could never be observed
merging distinct keys. It also used `time.Sleep` to guess goroutine
scheduling instead of controlling the loader boundary, and it never
asserted anything about cache state or metrics after a load.

#### New regression tests (`suppressed_loader_regression_test.go`)

All new concurrency tests drive the loader through channels
(`recordingLoader` announces every underlying `Load` entry and blocks
until released); no sleeps are used, and a blocked first load is proof
— not an assumption — that a second request did or did not enter the
underlying loader. Everything runs under the race detector.

- `Test_SuppressedLoader_Load_ConcurrentMissKeyIsolation` — same string
  key (merged, one call, shared item), different string keys, and
  custom `int` keys (isolated, two calls, per-key items, cache left
  empty when the loader does not `Set`).
- `Test_SuppressedLoader_Load_SameStringRepresentationDistinctKeys` —
  struct values with identical `String()` output, and `any` keys with
  identical display but different dynamic types, proven to load
  independently while a same-typed key is still in flight; equal keys
  still merge.
- `Test_SuppressedLoader_Load_NaNKeysNotMerged` — a second NaN load
  issued while the first is still blocked must enter the underlying
  loader on its own and return its own item.
- `Test_SuppressedLoader_LoaderResultLifecycle` — a nil load is not
  cached and the next miss reloads; a returned item without `Set`
  leaves `Get`/`GetAndDelete` with an empty cache; a loader that calls
  `Set` stores distinct keys separately and later `Get`s hit without
  reloading. Miss/hit/insertion counts are asserted against the
  existing `Metrics` definitions.

#### Most dangerous counterexample and its regression

The most dangerous case at the TTL cache / singleflight / concurrent
loader boundary is: **two different keys of the same type miss at the
same time, and the second request is silently served the first key's
`Item`.** The cache stays internally consistent (nothing is inserted
unless the loader calls `Set`), so the corruption is invisible in
final-state checks — it only shows up as a wrong key/value pair handed
to a caller, and it disappears whenever the timing changes. This is
exactly what `Test_SuppressedLoader_Load_ConcurrentMissKeyIsolation`
("different string keys are not merged", "custom int keys are not
merged") replays deterministically: the first load is held inside the
underlying loader while the second key must demonstrably enter and
complete independently, and each returned `Item.Key`/`Item.Value` is
matched to its own request.

#### Adjacent-semantic regression protection

- Same-key suppression is asserted alongside every isolation case, so
  widening the group key cannot regress duplicate suppression.
- NaN bypass is checked for both non-reuse and non-blocking, guarding
  against "fixing" NaN by merging all NaNs together.
- Pointer-identity handling prevents a future simplification to
  value-only rendering from merging distinct pointers to equal values.
- Loader lifecycle semantics (nil result, no-`Set` result, `Set`
  result) are pinned against `Metrics`, so changes to the wrapper
  cannot silently alter miss/hit/insertion accounting.
