//! Transport adapter boundary for hg-store-grpc.
use hugegraph_store_core::{StoreRequest, StoreResponse, StoreService};
pub struct RpcServer {
    service: StoreService,
}
impl RpcServer {
    pub fn new(service: StoreService) -> Self {
        Self { service }
    }
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<Option<Vec<u8>>> {
        match self.service.call(StoreRequest::Put { key, value })? {
            StoreResponse::Value(v) => Ok(v),
            StoreResponse::Ack => unreachable!(),
        }
    }
    pub fn get(&mut self, key: Vec<u8>) -> std::io::Result<Option<Vec<u8>>> {
        match self.service.call(StoreRequest::Get { key })? {
            StoreResponse::Value(v) => Ok(v),
            StoreResponse::Ack => unreachable!(),
        }
    }
    pub fn rollback(&mut self, key: Vec<u8>) -> std::io::Result<()> {
        self.service
            .call(StoreRequest::Rollback { key })
            .map(|_| ())
    }
}

/// RPC names currently exposed by hg-store-grpc; generated bindings can use
/// this registry to guarantee the adapter does not silently omit a method.
pub const HG_STORE_GRPC_RPCS: &[&str] = &[
    "Healthy.IsOk",
    "QueryService.query",
    "QueryService.query0",
    "QueryService.count",
    "GraphStore.ScanPartition",
    "HgStoreSession.Get2",
    "HgStoreSession.BatchGet2",
    "HgStoreSession.Batch",
    "HgStoreSession.Table",
    "HgStoreSession.Graph",
    "HgStoreSession.Clean",
    "HgStoreState.SubState",
    "HgStoreState.UnsubState",
    "HgStoreState.getScanState",
    "HgStoreState.getPeers",
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn grpc_contract_registry_is_complete() {
        assert_eq!(HG_STORE_GRPC_RPCS.len(), 15);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcEnvelope {
    pub method: String,
    pub payload: Vec<u8>,
}
impl RpcEnvelope {
    pub fn new(method: impl Into<String>, payload: Vec<u8>) -> Result<Self, String> {
        let method = method.into();
        if !HG_STORE_GRPC_RPCS.contains(&method.as_str()) {
            return Err(format!("unsupported RPC: {method}"));
        }
        Ok(Self { method, payload })
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut out = (self.method.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(self.method.as_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 {
            return Err("truncated envelope".into());
        }
        let ml = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        if bytes.len() < 4 + ml + 4 {
            return Err("truncated method".into());
        }
        let method = String::from_utf8(bytes[4..4 + ml].to_vec()).map_err(|_| "invalid method")?;
        let p = 4 + ml;
        let pl = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
        if bytes.len() != p + 4 + pl {
            return Err("invalid payload length".into());
        }
        Self::new(method, bytes[p + 4..].to_vec())
    }
}
#[cfg(test)]
mod envelope_tests {
    use super::*;
    #[test]
    fn envelope_round_trip_and_rejects_unknown() {
        let e = RpcEnvelope::new("HgStoreSession.Get2", b"x".to_vec()).unwrap();
        assert_eq!(RpcEnvelope::decode(&e.encode()).unwrap(), e);
        assert!(RpcEnvelope::new("bad", vec![]).is_err());
    }
}

use std::io::{Read, Write};
pub fn serve_once(
    mut stream: std::net::TcpStream,
    mut service: StoreService,
) -> std::io::Result<()> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let n = u32::from_le_bytes(len) as usize;
    let mut buf = vec![0; n];
    stream.read_exact(&mut buf)?;
    let env = RpcEnvelope::decode(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    validate_dispatch(&env.method, &env.payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let response = match env.method.as_str() {
        "HgStoreSession.Get2" => service.call(StoreRequest::Get { key: env.payload })?,
        "HgStoreSession.Batch" => service.call(StoreRequest::Put {
            key: b"batch".to_vec(),
            value: env.payload,
        })?,
        _ => StoreResponse::Ack,
    };
    let payload = match response {
        StoreResponse::Value(Some(v)) => v,
        StoreResponse::Value(None) | StoreResponse::Ack => Vec::new(),
    };
    stream.write_all(&(payload.len() as u32).to_le_bytes())?;
    stream.write_all(&payload)
}

pub fn call_once(addr: std::net::SocketAddr, envelope: RpcEnvelope) -> std::io::Result<Vec<u8>> {
    let mut s = std::net::TcpStream::connect(addr)?;
    let data = envelope.encode();
    s.write_all(&(data.len() as u32).to_le_bytes())?;
    s.write_all(&data)?;
    let mut l = [0u8; 4];
    s.read_exact(&mut l)?;
    let n = u32::from_le_bytes(l) as usize;
    let mut out = vec![0; n];
    s.read_exact(&mut out)?;
    Ok(out)
}

#[cfg(test)]
mod socket_tests {
    use super::*;
    use hugegraph_store_core::DurableStore;
    #[test]
    fn loopback_get_round_trip() {
        let path = std::env::temp_dir().join(format!("hg-net-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut seed = DurableStore::open(&path).unwrap();
        seed.commit_put(b"k".to_vec(), b"v".to_vec()).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let t = std::thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();
            serve_once(s, StoreService::new(seed)).unwrap();
        });
        let out = call_once(
            addr,
            RpcEnvelope::new("HgStoreSession.Get2", b"k".to_vec()).unwrap(),
        )
        .unwrap();
        t.join().unwrap();
        assert_eq!(out, b"v");
        let _ = std::fs::remove_file(path);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Get2Request {
    pub key: Vec<u8>,
}
impl Get2Request {
    pub fn encode(&self) -> Vec<u8> {
        self.key.clone()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("empty Get2 key".into());
        }
        Ok(Self {
            key: bytes.to_vec(),
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Get2Response {
    pub value: Option<Vec<u8>>,
}
impl Get2Response {
    pub fn encode(&self) -> Vec<u8> {
        self.value.clone().unwrap_or_default()
    }
    pub fn decode(bytes: &[u8]) -> Self {
        Self {
            value: (!bytes.is_empty()).then(|| bytes.to_vec()),
        }
    }
}
#[cfg(test)]
mod codec_tests {
    use super::*;
    #[test]
    fn get2_codec_round_trip() {
        let r = Get2Request {
            key: b"vertex/1".to_vec(),
        };
        assert_eq!(Get2Request::decode(&r.encode()).unwrap(), r);
        assert!(Get2Request::decode(&[]).is_err());
        let x = Get2Response {
            value: Some(b"alice".to_vec()),
        };
        assert_eq!(Get2Response::decode(&x.encode()), x);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcMessage {
    pub method: &'static str,
    pub payload: Vec<u8>,
}
impl RpcMessage {
    pub fn encode(&self) -> Vec<u8> {
        RpcEnvelope::new(self.method, self.payload.clone())
            .unwrap()
            .encode()
    }
    pub fn decode(bytes: &[u8]) -> Result<(String, Vec<u8>), String> {
        let e = RpcEnvelope::decode(bytes)?;
        Ok((e.method, e.payload))
    }
}
#[cfg(test)]
mod message_tests {
    use super::*;
    #[test]
    fn all_registered_methods_encode() {
        for &m in HG_STORE_GRPC_RPCS {
            let x = RpcMessage {
                method: m,
                payload: vec![1, 2],
            };
            let (name, p) = RpcMessage::decode(&x.encode()).unwrap();
            assert_eq!(name, m);
            assert_eq!(p, vec![1, 2]);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRequest {
    pub query: String,
}
impl QueryRequest {
    pub fn encode(&self) -> Vec<u8> {
        self.query.as_bytes().to_vec()
    }
    pub fn decode(b: &[u8]) -> Result<Self, String> {
        let q = String::from_utf8(b.to_vec()).map_err(|_| "invalid utf8")?;
        if q.trim().is_empty() {
            Err("empty query".into())
        } else {
            Ok(Self { query: q })
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRequest {
    pub operations: Vec<Vec<u8>>,
}
impl BatchRequest {
    pub fn encode(&self) -> Vec<u8> {
        self.operations
            .iter()
            .flat_map(|o| {
                let mut x = (o.len() as u32).to_le_bytes().to_vec();
                x.extend(o);
                x
            })
            .collect()
    }
    pub fn decode(b: &[u8]) -> Result<Self, String> {
        let mut p = 0;
        let mut out = Vec::new();
        while p < b.len() {
            if p + 4 > b.len() {
                return Err("truncated batch".into());
            }
            let n = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            if n == 0 || p + n > b.len() {
                return Err("invalid batch item".into());
            }
            out.push(b[p..p + n].to_vec());
            p += n
        }
        if out.is_empty() {
            Err("empty batch".into())
        } else {
            Ok(Self { operations: out })
        }
    }
}
#[cfg(test)]
mod schema_tests {
    use super::*;
    #[test]
    fn query_batch_codecs() {
        let q = QueryRequest {
            query: "g.V()".into(),
        };
        assert_eq!(QueryRequest::decode(&q.encode()).unwrap(), q);
        assert!(QueryRequest::decode(b" ").is_err());
        let b = BatchRequest {
            operations: vec![b"put".to_vec(), b"del".to_vec()],
        };
        assert_eq!(BatchRequest::decode(&b.encode()).unwrap(), b);
        assert!(BatchRequest::decode(&[]).is_err());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRequest {
    pub partition: u64,
}
impl StateRequest {
    pub fn encode(&self) -> Vec<u8> {
        self.partition.to_le_bytes().to_vec()
    }
    pub fn decode(b: &[u8]) -> Result<Self, String> {
        if b.len() != 8 {
            Err("invalid state request".into())
        } else {
            Ok(Self {
                partition: u64::from_le_bytes(b.try_into().unwrap()),
            })
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRequest {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}
impl ScanRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = (self.start.len() as u32).to_le_bytes().to_vec();
        o.extend(&self.start);
        o.extend(&(self.end.len() as u32).to_le_bytes());
        o.extend(&self.end);
        o
    }
    pub fn decode(b: &[u8]) -> Result<Self, String> {
        if b.len() < 8 {
            return Err("truncated scan".into());
        }
        let n = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
        if 4 + n + 4 > b.len() {
            return Err("invalid scan".into());
        }
        let p = 4 + n;
        let m = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
        if p + 4 + m != b.len() {
            return Err("invalid scan end".into());
        }
        Ok(Self {
            start: b[4..4 + n].to_vec(),
            end: b[p + 4..].to_vec(),
        })
    }
}
#[cfg(test)]
mod state_graph_tests {
    use super::*;
    #[test]
    fn codecs() {
        let s = StateRequest { partition: 7 };
        assert_eq!(StateRequest::decode(&s.encode()).unwrap(), s);
        let x = ScanRequest {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
        };
        assert_eq!(ScanRequest::decode(&x.encode()).unwrap(), x);
        assert!(StateRequest::decode(&[]).is_err());
    }
}

pub fn validate_dispatch(method: &str, payload: &[u8]) -> Result<(), String> {
    match method {
        "QueryService.query0" | "QueryService.count" => QueryRequest::decode(payload).map(|_| ()),
        "HgStoreSession.Batch" => BatchRequest::decode(payload).map(|_| ()),
        "HgStoreState.SubState"
        | "HgStoreState.UnsubState"
        | "HgStoreState.getScanState"
        | "HgStoreState.getPeers" => StateRequest::decode(payload).map(|_| ()),
        "GraphStore.ScanPartition" => ScanRequest::decode(payload).map(|_| ()),
        "HgStoreSession.Get2" => Get2Request::decode(payload).map(|_| ()),
        _ => Ok(()),
    }
}
#[cfg(test)]
mod dispatch_tests {
    use super::*;
    #[test]
    fn typed_dispatch_validates_routes() {
        assert!(validate_dispatch("QueryService.count", b"g.V()").is_ok());
        assert!(validate_dispatch("HgStoreState.SubState", &7u64.to_le_bytes()).is_ok());
        assert!(validate_dispatch(
            "GraphStore.ScanPartition",
            &ScanRequest {
                start: b"a".to_vec(),
                end: b"z".to_vec()
            }
            .encode()
        )
        .is_ok());
        assert!(validate_dispatch("QueryService.count", b"").is_err());
    }
}
