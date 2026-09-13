//! Independent Rust store boundary. Java remains selectable by the caller.

pub trait StoreBackend {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>>;
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn rollback(&mut self, key: &[u8]);
}

#[derive(Default)]
pub struct InMemoryStore(std::collections::BTreeMap<Vec<u8>, Vec<u8>>);

impl InMemoryStore {
    pub fn snapshot(&self) -> Vec<u8> {
        self.0.iter().flat_map(|(k,v)| [k.len() as u32].into_iter().flat_map(|n| n.to_le_bytes()).chain(k.iter().copied()).chain([v.len() as u32].into_iter().flat_map(|n| n.to_le_bytes())).chain(v.iter().copied())).collect()
    }
    pub fn restore(bytes: &[u8]) -> Option<Self> {
        let mut p=0; let mut m=std::collections::BTreeMap::new();
        while p<bytes.len() { if p+4>bytes.len(){return None}; let kl=u32::from_le_bytes(bytes[p..p+4].try_into().ok()?) as usize;p+=4; if p+kl+4>bytes.len(){return None}; let k=bytes[p..p+kl].to_vec();p+=kl; let vl=u32::from_le_bytes(bytes[p..p+4].try_into().ok()?) as usize;p+=4;if p+vl>bytes.len(){return None};m.insert(k,bytes[p..p+vl].to_vec());p+=vl; } Some(Self(m))
    }
}

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
    #[test] fn snapshot_round_trip() { let mut s=InMemoryStore::default(); s.put(b"k".to_vec(),b"v".to_vec()); let r=InMemoryStore::restore(&s.snapshot()).unwrap(); assert_eq!(r.get(b"k"),Some(b"v".to_vec())); }
}
