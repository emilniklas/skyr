use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::RwLock;

pub struct Flyweight<T> {
    values: RwLock<Option<HashMap<u64, T>>>,
}

impl<T> Default for Flyweight<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Flyweight<T> {
    pub const fn new() -> Self {
        Self {
            values: RwLock::new(None),
        }
    }
}

impl<T> Flyweight<T>
where
    T: Eq + Hash,
{
    pub fn make<'a>(&'a self, value: T) -> &'a T {
        let guard = self.values.read().unwrap();

        let mut hasher = RandomState::new().build_hasher();
        value.hash(&mut hasher);
        let key = hasher.finish();

        unsafe {
            if let Some(r) = guard.as_ref().and_then(|hm| hm.get(&key)) {
                return &*(r as *const _) as &'a _;
            }
        }

        drop(guard);

        let mut guard = self.values.write().unwrap();

        let hm = guard.get_or_insert(Default::default());

        let r = hm.entry(key).or_insert(value);

        unsafe { &*(r as *const _) as &'a _ }
    }
}
