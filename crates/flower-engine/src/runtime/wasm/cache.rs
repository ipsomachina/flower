//! The bounded image cache: prepared images keyed by the SHA-256 of
//! their module's bytes, shared by every backend on the engine.
//!
//! The cache holds at most its budget in image weight (compiled code plus
//! the image module). Past it, the least recently used images leave; an
//! image larger than the whole budget stays alone. Backends never pin
//! images: they keep the module's bytes and a weak handle, so an evicted
//! image lives on only while calls still run in it, and the next call
//! after that prepares it again. Each key prepares once at a time:
//! concurrent callers wait for the first, and prepare themselves only if
//! it failed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

/// Cache key: SHA-256 of a module's bytes.
pub(super) type Key = [u8; 32];

/// An image the cache handed out.
pub(super) struct Cached<T> {
    value: T,
    weight: usize,
    /// Cache clock at the latest use.
    used: AtomicU64,
    /// Whether the cache still holds it.
    cached: AtomicBool,
}

impl<T> std::ops::Deref for Cached<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

/// What the cache holds and did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Images held.
    pub images: usize,
    /// Their summed weight, in bytes.
    pub bytes: usize,
    /// The weight the cache keeps at most, in bytes.
    pub budget: usize,
    /// Images prepared since the engine started.
    pub prepared: u64,
    /// Images evicted since the engine started.
    pub evicted: u64,
}

pub(super) struct Cache<T> {
    budget: usize,
    clock: AtomicU64,
    state: Mutex<State<T>>,
    /// Signalled whenever a preparation ends.
    settled: Condvar,
}

struct State<T> {
    entries: HashMap<Key, Slot<T>>,
    bytes: usize,
    prepared: u64,
    evicted: u64,
}

enum Slot<T> {
    Ready(Arc<Cached<T>>),
    Preparing,
}

impl<T> Cache<T> {
    pub(super) fn new(budget: usize) -> Self {
        Cache {
            budget,
            clock: AtomicU64::new(0),
            state: Mutex::new(State {
                entries: HashMap::new(),
                bytes: 0,
                prepared: 0,
                evicted: 0,
            }),
            settled: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    /// Mark a live image used. Returns false once the cache evicted it:
    /// the caller then goes through [`Cache::acquire`].
    pub(super) fn touch(&self, image: &Cached<T>) -> bool {
        if !image.cached.load(Ordering::Acquire) {
            return false;
        }
        image.used.store(self.tick(), Ordering::Relaxed);
        true
    }

    /// The image under `key`. An evicted image the caller still holds
    /// (`live`) goes back into the cache as it is; otherwise `prepare`
    /// builds it, unless another caller is already preparing it.
    pub(super) fn acquire<E>(
        &self,
        key: Key,
        live: Option<Arc<Cached<T>>>,
        prepare: impl FnOnce() -> Result<(T, usize), E>,
    ) -> Result<Arc<Cached<T>>, E> {
        let mut state = self.lock();
        loop {
            match state.entries.get(&key) {
                Some(Slot::Ready(image)) => {
                    image.used.store(self.tick(), Ordering::Relaxed);
                    return Ok(image.clone());
                }
                Some(Slot::Preparing) => {
                    state = self
                        .settled
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                None => break,
            }
        }
        if let Some(image) = live {
            self.insert(&mut state, key, image.clone());
            return Ok(image);
        }
        state.entries.insert(key, Slot::Preparing);
        drop(state);
        let pending = Pending {
            cache: self,
            key,
            armed: true,
        };
        let prepared = prepare();
        let mut state = pending.finish();
        let (value, weight) = prepared?;
        let image = Arc::new(Cached {
            value,
            weight,
            used: AtomicU64::new(0),
            cached: AtomicBool::new(false),
        });
        state.prepared += 1;
        self.insert(&mut state, key, image.clone());
        Ok(image)
    }

    /// Hold `image` under `key`, then evict down to the budget.
    fn insert(&self, state: &mut State<T>, key: Key, image: Arc<Cached<T>>) {
        image.used.store(self.tick(), Ordering::Relaxed);
        image.cached.store(true, Ordering::Release);
        state.bytes += image.weight;
        state.entries.insert(key, Slot::Ready(image));
        while state.bytes > self.budget {
            let victim = state
                .entries
                .iter()
                .filter_map(|(entry, slot)| match slot {
                    Slot::Ready(image) if *entry != key => {
                        Some((image.used.load(Ordering::Relaxed), *entry))
                    }
                    _ => None,
                })
                .min();
            let Some((_, victim)) = victim else {
                break;
            };
            if let Some(Slot::Ready(image)) = state.entries.remove(&victim) {
                image.cached.store(false, Ordering::Release);
                state.bytes -= image.weight;
                state.evicted += 1;
            }
        }
    }

    pub(super) fn stats(&self) -> CacheStats {
        let state = self.lock();
        CacheStats {
            images: state
                .entries
                .values()
                .filter(|slot| matches!(slot, Slot::Ready(_)))
                .count(),
            bytes: state.bytes,
            budget: self.budget,
            prepared: state.prepared,
            evicted: state.evicted,
        }
    }
}

/// A preparation in progress: clears its marker and wakes waiters when
/// it ends, even by unwinding.
struct Pending<'a, T> {
    cache: &'a Cache<T>,
    key: Key,
    armed: bool,
}

impl<'a, T> Pending<'a, T> {
    fn finish(mut self) -> MutexGuard<'a, State<T>> {
        self.armed = false;
        let mut state = self.cache.lock();
        state.entries.remove(&self.key);
        self.cache.settled.notify_all();
        state
    }
}

impl<T> Drop for Pending<'_, T> {
    fn drop(&mut self) {
        if self.armed {
            self.cache.lock().entries.remove(&self.key);
            self.cache.settled.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> Key {
        [byte; 32]
    }

    fn ready<T: Clone>(value: T, weight: usize) -> impl FnOnce() -> Result<(T, usize), String> {
        move || Ok((value, weight))
    }

    #[test]
    fn hits_share_one_preparation() {
        let cache = Cache::new(100);
        let first = cache.acquire(key(1), None, ready("a", 10)).unwrap();
        let second = cache
            .acquire(key(1), None, || -> Result<(&str, usize), String> {
                panic!("prepared twice")
            })
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.value, "a");
        let stats = cache.stats();
        assert_eq!((stats.images, stats.bytes, stats.prepared), (1, 10, 1));
    }

    #[test]
    fn least_recently_used_images_leave_past_the_budget() {
        let cache = Cache::new(25);
        let a = cache.acquire(key(1), None, ready(1, 10)).unwrap();
        let b = cache.acquire(key(2), None, ready(2, 10)).unwrap();
        // Using `a` makes `b` the oldest.
        assert!(cache.touch(&a));
        let c = cache.acquire(key(3), None, ready(3, 10)).unwrap();
        assert!(cache.touch(&a) && cache.touch(&c));
        assert!(!cache.touch(&b), "b was evicted");
        let stats = cache.stats();
        assert_eq!((stats.images, stats.bytes, stats.evicted), (2, 20, 1));
        // A caller still holding `b` puts it back without preparing.
        let back = cache
            .acquire(
                key(2),
                Some(b.clone()),
                || -> Result<(i32, usize), String> { panic!("prepared again") },
            )
            .unwrap();
        assert!(Arc::ptr_eq(&back, &b));
        assert_eq!(cache.stats().prepared, 3);
        // Without one, the key prepares anew.
        drop((a, back, b));
        let a = cache.acquire(key(1), None, ready(10, 10)).unwrap();
        assert_eq!(a.value, 10);
    }

    #[test]
    fn an_image_over_the_whole_budget_stays_alone() {
        let cache = Cache::new(25);
        cache.acquire(key(1), None, ready(1, 10)).unwrap();
        let big = cache.acquire(key(2), None, ready(2, 40)).unwrap();
        assert!(cache.touch(&big));
        let stats = cache.stats();
        assert_eq!((stats.images, stats.bytes), (1, 40));
    }

    #[test]
    fn failures_cache_nothing_and_let_the_next_caller_prepare() {
        let cache = Cache::<i32>::new(100);
        let failed = cache.acquire(key(1), None, || Err("boom".to_string()));
        assert_eq!(failed.err().as_deref(), Some("boom"));
        assert_eq!(cache.stats().images, 0);
        assert!(cache.acquire(key(1), None, ready(1, 1)).is_ok());
    }

    #[test]
    fn concurrent_callers_wait_for_one_preparation() {
        let cache = Arc::new(Cache::new(100));
        let preparations = Arc::new(AtomicU64::new(0));
        let threads = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let preparations = preparations.clone();
                std::thread::spawn(move || {
                    cache
                        .acquire(key(1), None, || -> Result<(u8, usize), String> {
                            preparations.fetch_add(1, Ordering::SeqCst);
                            std::thread::sleep(std::time::Duration::from_millis(50));
                            Ok((7, 1))
                        })
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let images = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(preparations.load(Ordering::SeqCst), 1);
        assert!(images.iter().all(|image| Arc::ptr_eq(image, &images[0])));
    }

    #[test]
    fn a_panicking_preparation_releases_its_waiters() {
        let cache = Arc::new(Cache::<u8>::new(100));
        let panicking = {
            let cache = cache.clone();
            std::thread::spawn(move || {
                let _ = cache.acquire(key(1), None, || -> Result<(u8, usize), String> {
                    panic!("preparation bug")
                });
            })
        };
        assert!(panicking.join().is_err());
        assert!(cache.acquire(key(1), None, ready(1, 1)).is_ok());
    }
}
