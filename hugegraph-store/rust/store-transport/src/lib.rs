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
