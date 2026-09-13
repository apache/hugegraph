//! Independent Rust store boundary. Java remains selectable by the caller.

pub trait StoreBackend {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>>;
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn rollback(&mut self, key: &[u8]);
}

#[derive(Default)]
pub struct InMemoryStore(std::collections::BTreeMap<Vec<u8>, Vec<u8>>);

impl StoreBackend for InMemoryStore {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> { self.0.insert(key, value) }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> { self.0.get(key).cloned() }
    fn rollback(&mut self, key: &[u8]) { self.0.remove(key); }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn commit_and_rollback_visibility() {
        let mut s = InMemoryStore::default();
        assert_eq!(s.put(b"k".to_vec(), b"v".to_vec()), None);
        assert_eq!(s.get(b"k"), Some(b"v".to_vec()));
        s.rollback(b"k");
        assert_eq!(s.get(b"k"), None);
    }
}
