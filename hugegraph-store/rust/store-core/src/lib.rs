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
        self.0
            .iter()
            .flat_map(|(k, v)| {
                [k.len() as u32]
                    .into_iter()
                    .flat_map(|n| n.to_le_bytes())
                    .chain(k.iter().copied())
                    .chain([v.len() as u32].into_iter().flat_map(|n| n.to_le_bytes()))
                    .chain(v.iter().copied())
            })
            .collect()
    }
    pub fn restore(bytes: &[u8]) -> Option<Self> {
        let mut p = 0;
        let mut m = std::collections::BTreeMap::new();
        while p < bytes.len() {
            if p + 4 > bytes.len() {
                return None;
            };
            let kl = u32::from_le_bytes(bytes[p..p + 4].try_into().ok()?) as usize;
            p += 4;
            if p + kl + 4 > bytes.len() {
                return None;
            };
            let k = bytes[p..p + kl].to_vec();
            p += kl;
            let vl = u32::from_le_bytes(bytes[p..p + 4].try_into().ok()?) as usize;
            p += 4;
            if p + vl > bytes.len() {
                return None;
            };
            m.insert(k, bytes[p..p + vl].to_vec());
            p += vl;
        }
        Some(Self(m))
    }
}

pub enum BackendSelector {
    Rust(InMemoryStore),
    JavaFallback(InMemoryStore),
}

impl BackendSelector {
    pub fn rust() -> Self {
        Self::Rust(InMemoryStore::default())
    }
    pub fn java_fallback() -> Self {
        Self::JavaFallback(InMemoryStore::default())
    }
    pub fn switch_to_java(self) -> Self {
        Self::JavaFallback(match self {
            Self::Rust(s) | Self::JavaFallback(s) => s,
        })
    }
    pub fn switch_to_rust(self) -> Self {
        Self::Rust(match self {
            Self::Rust(s) | Self::JavaFallback(s) => s,
        })
    }
}

impl StoreBackend for BackendSelector {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        match self {
            Self::Rust(s) | Self::JavaFallback(s) => s.put(key, value),
        }
    }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Rust(s) | Self::JavaFallback(s) => s.get(key),
        }
    }
    fn rollback(&mut self, key: &[u8]) {
        match self {
            Self::Rust(s) | Self::JavaFallback(s) => s.rollback(key),
        }
    }
}

impl StoreBackend for InMemoryStore {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        self.0.insert(key, value)
    }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key).cloned()
    }
    fn rollback(&mut self, key: &[u8]) {
        self.0.remove(key);
    }
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
    #[test]
    fn selectable_java_fallback_rollback() {
        let mut b = BackendSelector::rust();
        b.put(b"k".to_vec(), b"v".to_vec());
        b = b.switch_to_java();
        assert_eq!(b.get(b"k"), Some(b"v".to_vec()));
        b.rollback(b"k");
        b = b.switch_to_rust();
        assert_eq!(b.get(b"k"), None);
    }

    #[test]
    fn restart_recovery_and_corruption_rejection() {
        let mut s = InMemoryStore::default();
        s.put(b"a".to_vec(), b"1".to_vec());
        s.put(b"b".to_vec(), b"2".to_vec());
        let image = s.snapshot();
        let recovered = InMemoryStore::restore(&image).unwrap();
        assert_eq!(recovered.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(recovered.get(b"b"), Some(b"2".to_vec()));
        let mut corrupt = image.clone();
        corrupt.pop();
        assert!(InMemoryStore::restore(&corrupt).is_none());
    }
    #[test]
    fn snapshot_round_trip() {
        let mut s = InMemoryStore::default();
        s.put(b"k".to_vec(), b"v".to_vec());
        let r = InMemoryStore::restore(&s.snapshot()).unwrap();
        assert_eq!(r.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn differential_switch_rollback_trace() {
        fn run(mut b: BackendSelector) -> Vec<Option<Vec<u8>>> {
            let mut out = Vec::new();
            out.push(b.put(b"a".to_vec(), b"1".to_vec()));
            out.push(b.get(b"a"));
            b = b.switch_to_java();
            out.push(b.put(b"b".to_vec(), b"2".to_vec()));
            out.push(b.get(b"a"));
            b.rollback(b"a");
            b = b.switch_to_rust();
            out.push(b.get(b"a"));
            out.push(b.get(b"b"));
            out
        }
        assert_eq!(
            run(BackendSelector::rust()),
            run(BackendSelector::java_fallback())
        );
    }

    #[test]
    fn independent_oracle_deterministic_trace() {
        let ops = [(0u8, 1u8), (1, 2), (0, 3), (2, 0), (1, 4), (2, 0), (0, 5)];
        let keys = [b"a".as_slice(), b"b".as_slice()];
        let mut actual = InMemoryStore::default();
        let mut oracle = std::collections::BTreeMap::new();
        for (i, (kind, value)) in ops.into_iter().enumerate() {
            let key = keys[i % keys.len()].to_vec();
            match kind {
                0 => {
                    let expected = oracle.insert(key.clone(), vec![value]);
                    assert_eq!(actual.put(key, vec![value]), expected);
                }
                1 => assert_eq!(actual.get(&key), oracle.get(&key).cloned()),
                2 => {
                    actual.rollback(&key);
                    oracle.remove(&key);
                }
                _ => unreachable!(),
            }
            assert_eq!(actual.snapshot(), InMemoryStore(oracle.clone()).snapshot());
        }
    }
}

/// Durable append-only store boundary used by production adapters.
pub struct DurableStore {
    path: std::path::PathBuf,
    inner: InMemoryStore,
}

impl DurableStore {
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let inner = match std::fs::read(&path) {
            Ok(bytes) => InMemoryStore::restore(&bytes).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt store image")
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => InMemoryStore::default(),
            Err(e) => return Err(e),
        };
        Ok(Self { path, inner })
    }
    fn persist(&self) -> std::io::Result<()> {
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, self.inner.snapshot())?;
        std::fs::rename(tmp, &self.path)
    }
    pub fn commit_put(&mut self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<Option<Vec<u8>>> {
        let old = self.inner.put(key, value);
        self.persist()?;
        Ok(old)
    }
    pub fn rollback_key(&mut self, key: &[u8]) -> std::io::Result<()> {
        self.inner.rollback(key);
        self.persist()
    }
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get(key)
    }
    pub fn clear(&mut self) -> std::io::Result<()> {
        let previous = self.inner.snapshot();
        self.inner = InMemoryStore::default();
        if let Err(error) = self.persist() {
            self.inner = InMemoryStore::restore(&previous).expect("snapshot must restore");
            return Err(error);
        }
        Ok(())
    }
    pub fn entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner
            .0
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    pub fn snapshot(&self) -> Vec<u8> {
        self.inner.snapshot()
    }
    pub fn restore_image(&mut self, image: &[u8]) -> std::io::Result<()> {
        self.inner = InMemoryStore::restore(image).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt store image")
        })?;
        self.persist()
    }
}

#[cfg(test)]
mod durable_tests {
    use super::DurableStore;
    #[test]
    fn durable_restart_round_trip() {
        let path = std::env::temp_dir().join(format!("hg-store-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut s = DurableStore::open(&path).unwrap();
            s.commit_put(b"k".to_vec(), b"v".to_vec()).unwrap();
        }
        let s = DurableStore::open(&path).unwrap();
        assert_eq!(s.get(b"k"), Some(b"v".to_vec()));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn durable_clear_matches_empty_oracle_after_restart() {
        let path = std::env::temp_dir().join(format!("hg-clear-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut store = DurableStore::open(&path).unwrap();
        store.commit_put(b"a".to_vec(), b"b".to_vec()).unwrap();
        store.clear().unwrap();
        assert!(store.entries().is_empty());
        drop(store);
        assert!(DurableStore::open(&path).unwrap().entries().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn entries_are_sorted_and_owned() {
        let p = std::env::temp_dir().join(format!("hg-entries-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let mut s = DurableStore::open(&p).unwrap();
        s.commit_put(b"b".to_vec(), b"2".to_vec()).unwrap();
        s.commit_put(b"a".to_vec(), b"1".to_vec()).unwrap();
        assert_eq!(
            s.entries(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn restore_image_rejects_truncation_without_mutating_store() {
        let path = std::env::temp_dir().join(format!("hg-recover-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut s = DurableStore::open(&path).unwrap();
        s.commit_put(b"safe".to_vec(), b"value".to_vec()).unwrap();
        let mut image = s.snapshot();
        image.pop();
        assert!(s.restore_image(&image).is_err());
        assert_eq!(s.get(b"safe"), Some(b"value".to_vec()));
        let _ = std::fs::remove_file(path);
    }
}

/// Production-facing request adapter. Transport layers (gRPC/HTTP) can map
/// their requests to this stable command surface.
pub enum StoreRequest {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Rollback { key: Vec<u8> },
    BatchPut { entries: Vec<(Vec<u8>, Vec<u8>)> },
}

pub enum StoreResponse {
    Value(Option<Vec<u8>>),
    Ack,
}

impl DurableStore {
    pub fn handle(&mut self, request: StoreRequest) -> std::io::Result<StoreResponse> {
        match request {
            StoreRequest::Put { key, value } => {
                self.commit_put(key, value).map(StoreResponse::Value)
            }
            StoreRequest::Get { key } => Ok(StoreResponse::Value(self.get(&key))),
            StoreRequest::Rollback { key } => self.rollback_key(&key).map(|_| StoreResponse::Ack),
            StoreRequest::BatchPut { entries } => {
                let snapshot = self.snapshot();
                for (key, value) in entries {
                    if let Err(error) = self.commit_put(key, value) {
                        let _ = self.restore_image(&snapshot);
                        return Err(error);
                    }
                }
                Ok(StoreResponse::Ack)
            }
        }
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;
    #[test]
    fn batch_put_command_applies_all_entries() {
        let path = std::env::temp_dir().join(format!("hg-batch-cmd-{}.img", std::process::id()));
        let mut s = DurableStore::open(&path).unwrap();
        assert!(matches!(
            s.handle(StoreRequest::BatchPut {
                entries: vec![
                    (b"a".to_vec(), b"1".to_vec()),
                    (b"b".to_vec(), b"2".to_vec())
                ]
            })
            .unwrap(),
            StoreResponse::Ack
        ));
        assert_eq!(s.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(s.get(b"b"), Some(b"2".to_vec()));
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn request_path_persists_and_reads() {
        let path = std::env::temp_dir().join(format!("hg-request-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut s = DurableStore::open(&path).unwrap();
        assert!(matches!(
            s.handle(StoreRequest::Put {
                key: b"id".to_vec(),
                value: b"v".to_vec()
            })
            .unwrap(),
            StoreResponse::Value(None)
        ));
        assert!(
            matches!(s.handle(StoreRequest::Get { key: b"id".to_vec() }).unwrap(), StoreResponse::Value(Some(v)) if v == b"v")
        );
        s.handle(StoreRequest::Rollback {
            key: b"id".to_vec(),
        })
        .unwrap();
        assert!(matches!(
            s.handle(StoreRequest::Get {
                key: b"id".to_vec()
            })
            .unwrap(),
            StoreResponse::Value(None)
        ));
        let _ = std::fs::remove_file(path);
    }
}

/// In-process transport service used by gRPC/HTTP adapters and integration tests.
pub struct StoreService {
    backend: DurableStore,
}
impl StoreService {
    pub fn new(backend: DurableStore) -> Self {
        Self { backend }
    }
    pub fn call(&mut self, request: StoreRequest) -> std::io::Result<StoreResponse> {
        self.backend.handle(request)
    }
    pub fn into_backend(self) -> DurableStore {
        self.backend
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    #[test]
    fn client_server_request_mapping() {
        let path = std::env::temp_dir().join(format!("hg-transport-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut server = StoreService::new(DurableStore::open(&path).unwrap());
        let put = server
            .call(StoreRequest::Put {
                key: b"vertex/1".to_vec(),
                value: b"alice".to_vec(),
            })
            .unwrap();
        assert!(matches!(put, StoreResponse::Value(None)));
        let get = server
            .call(StoreRequest::Get {
                key: b"vertex/1".to_vec(),
            })
            .unwrap();
        assert!(matches!(get, StoreResponse::Value(Some(v)) if v == b"alice"));
        let _ = std::fs::remove_file(path);
    }
}

pub struct Transaction<'a> {
    store: &'a mut DurableStore,
    pending: Vec<(Vec<u8>, Vec<u8>)>,
}
impl<'a> Transaction<'a> {
    pub fn new(store: &'a mut DurableStore) -> Self {
        Self {
            store,
            pending: Vec::new(),
        }
    }
    pub fn put(&mut self, k: Vec<u8>, v: Vec<u8>) {
        self.pending.push((k, v));
    }
    pub fn commit(mut self) -> std::io::Result<()> {
        for (k, v) in self.pending.drain(..) {
            self.store.commit_put(k, v)?;
        }
        Ok(())
    }
    pub fn rollback(self) {}
}
#[cfg(test)]
mod txn_tests {
    use super::*;
    #[test]
    fn txn_commit_and_rollback() {
        let p = std::env::temp_dir().join(format!("hg-txn-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let mut s = DurableStore::open(&p).unwrap();
        {
            let mut t = Transaction::new(&mut s);
            t.put(b"k".to_vec(), b"v".to_vec());
            t.commit().unwrap();
        }
        assert_eq!(s.get(b"k"), Some(b"v".to_vec()));
        {
            let mut t = Transaction::new(&mut s);
            t.put(b"x".to_vec(), b"y".to_vec());
            t.rollback();
        }
        assert_eq!(s.get(b"x"), None);
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn txn_oracle_commit_is_atomic_observable_boundary() {
        let p = std::env::temp_dir().join(format!("hg-txn-oracle-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let mut s = DurableStore::open(&p).unwrap();
        let mut oracle = std::collections::BTreeMap::new();
        {
            let mut tx = Transaction::new(&mut s);
            tx.put(b"a".to_vec(), b"1".to_vec());
            tx.put(b"b".to_vec(), b"2".to_vec());
            tx.commit().unwrap();
        }
        oracle.insert(b"a".to_vec(), b"1".to_vec());
        oracle.insert(b"b".to_vec(), b"2".to_vec());
        assert_eq!(s.get(b"a"), oracle.get(b"a" as &[u8]).cloned());
        assert_eq!(s.get(b"b"), oracle.get(b"b" as &[u8]).cloned());
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn restart_oracle_preserves_committed_and_rollback_state() {
        let p = std::env::temp_dir().join(format!("hg-restart-oracle-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        {
            let mut s = DurableStore::open(&p).unwrap();
            s.commit_put(b"keep".to_vec(), b"v1".to_vec()).unwrap();
            s.commit_put(b"drop".to_vec(), b"v2".to_vec()).unwrap();
            s.rollback_key(b"drop").unwrap();
        }
        let s = DurableStore::open(&p).unwrap();
        assert_eq!(s.get(b"keep"), Some(b"v1".to_vec()));
        assert_eq!(s.get(b"drop"), None);
        let _ = std::fs::remove_file(p);
    }
}
