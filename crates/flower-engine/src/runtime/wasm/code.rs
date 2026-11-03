//! Compiled-function cache for Cranelift's incremental compilation.
//!
//! Preparing an image compiles a module twice — instrumented, then as
//! the image — with identical functions: only global initializers, data,
//! the start function and the `flower_init` export differ. A redeploy
//! that changes data alone does the same. Cranelift looks each function up here by a hash of its IR
//! and compiler settings and reuses the compiled bytes. The cache holds
//! at most its budget, dropping its oldest entries first.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};

#[derive(Debug)]
pub(super) struct CodeCache {
    budget: usize,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    bytes: usize,
    values: HashMap<Vec<u8>, Vec<u8>>,
    /// Keys, oldest first.
    order: VecDeque<Vec<u8>>,
}

impl CodeCache {
    pub(super) fn new(budget: usize) -> Self {
        CodeCache {
            budget,
            state: Mutex::default(),
        }
    }
}

impl wasmtime::CacheStore for CodeCache {
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.values.get(key).map(|value| Cow::Owned(value.clone()))
    }

    fn insert(&self, key: &[u8], value: Vec<u8>) -> bool {
        let size = key.len() + value.len();
        if size > self.budget {
            return false;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.values.contains_key(key) {
            return true;
        }
        while state.bytes + size > self.budget {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            if let Some(dropped) = state.values.remove(&oldest) {
                state.bytes -= oldest.len() + dropped.len();
            }
        }
        state.bytes += size;
        state.values.insert(key.to_vec(), value);
        state.order.push_back(key.to_vec());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::CacheStore;

    #[test]
    fn holds_its_budget_dropping_the_oldest() {
        let cache = CodeCache::new(10);
        assert!(cache.insert(b"a", vec![0; 4]));
        assert!(cache.insert(b"b", vec![1; 4]));
        assert!(cache.get(b"a").is_some());
        // Five more bytes push out the oldest entry.
        assert!(cache.insert(b"c", vec![2; 4]));
        assert!(cache.get(b"a").is_none());
        assert_eq!(cache.get(b"b").as_deref(), Some(&[1u8; 4][..]));
        assert_eq!(cache.get(b"c").as_deref(), Some(&[2u8; 4][..]));
        // An entry larger than the whole budget is refused.
        assert!(!cache.insert(b"d", vec![0; 10]));
        assert!(cache.get(b"b").is_some());
    }
}
