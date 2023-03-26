use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TypeId(u64);

static ID_GEN: AtomicU64 = AtomicU64::new(0);

impl TypeId {
    pub fn new() -> Self {
        Self(ID_GEN.fetch_add(1, SeqCst))
    }

    pub fn preload(i: u64) {
        ID_GEN.fetch_add(1000 * i, SeqCst);
    }
}

impl Default for TypeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:0>4x}", self.0)
    }
}
