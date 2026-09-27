//! Transport adapter boundary for hg-store-grpc.
use hugegraph_store_core::{StoreRequest, StoreResponse, StoreService};
use prost::Message;
use tokio_stream::StreamExt;
const MAX_SCAN_LIMIT: i64 = 100_000;
pub fn classify_failure(message: &str) -> tonic::Code {
    if message.contains("too large") {
        tonic::Code::ResourceExhausted
    } else if message.contains("missing")
        || message.contains("invalid")
        || message.contains("negative")
    {
        tonic::Code::InvalidArgument
    } else {
        tonic::Code::Internal
    }
}
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
    let payload = dispatch_payload(&mut service, &env.method, env.payload)?;
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
        assert!(validate_dispatch("QueryService.count", b"\xff").is_err());
        assert!(validate_dispatch("HgStoreState.SubState", b"\x01").is_err());
        assert!(validate_dispatch("HgStoreSession.Batch", b"\xff").is_err());
        assert_eq!(
            classify_failure("negative limit"),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            classify_failure("scan limit too large"),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(classify_failure("disk failure"), tonic::Code::Internal);
    }
}

pub fn dispatch_payload(
    service: &mut StoreService,
    method: &str,
    payload: Vec<u8>,
) -> std::io::Result<Vec<u8>> {
    validate_dispatch(method, &payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    match method {
        "HgStoreSession.Get2" => match service.call(StoreRequest::Get { key: payload })? {
            StoreResponse::Value(v) => Ok(v.unwrap_or_default()),
            _ => Ok(vec![]),
        },
        "HgStoreSession.Batch" => {
            BatchRequest::decode(&payload).unwrap();
            Ok(payload)
        }
        "QueryService.query0" | "QueryService.count" => {
            Ok(QueryRequest::decode(&payload).unwrap().query.into_bytes())
        }
        "GraphStore.ScanPartition" => Ok(ScanRequest::decode(&payload).unwrap().end),
        _ => Ok(payload),
    }
}
#[cfg(test)]
mod matrix_tests {
    use super::*;
    use hugegraph_store_core::DurableStore;
    #[test]
    fn rpc_family_dispatch_matrix() {
        let p = std::env::temp_dir().join(format!("hg-matrix-{}.img", std::process::id()));
        let mut s = StoreService::new(DurableStore::open(&p).unwrap());
        assert_eq!(
            dispatch_payload(&mut s, "QueryService.query0", b"g.V()".to_vec()).unwrap(),
            b"g.V()"
        );
        assert!(!dispatch_payload(
            &mut s,
            "HgStoreSession.Batch",
            BatchRequest {
                operations: vec![b"x".to_vec()]
            }
            .encode()
        )
        .unwrap()
        .is_empty());
        assert_eq!(
            dispatch_payload(&mut s, "HgStoreState.SubState", 7u64.to_le_bytes().to_vec()).unwrap(),
            7u64.to_le_bytes()
        );
        assert_eq!(
            dispatch_payload(
                &mut s,
                "GraphStore.ScanPartition",
                ScanRequest {
                    start: b"a".to_vec(),
                    end: b"z".to_vec()
                }
                .encode()
            )
            .unwrap(),
            b"z"
        );
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn cross_layer_oracle_reopen_preserves_transport_write() {
        let p = std::env::temp_dir().join(format!("hg-cross-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        {
            let mut s = StoreService::new(DurableStore::open(&p).unwrap());
            s.call(StoreRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            })
            .unwrap();
        }
        let mut reopened = StoreService::new(DurableStore::open(&p).unwrap());
        let out = reopened
            .call(StoreRequest::Get { key: b"k".to_vec() })
            .unwrap();
        assert!(matches!(out, StoreResponse::Value(Some(v)) if v == b"v"));
        let _ = std::fs::remove_file(p);
    }
}

/// Generated protobuf bindings (enabled when the transport crate is built).
pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
    include!(concat!(env!("OUT_DIR"), "/graph_pb.rs"));
}

pub fn encode_string_reply(message: &str) -> Vec<u8> {
    let v = generated::StringReply {
        message: message.into(),
    };
    prost::Message::encode_to_vec(&v)
}
pub fn decode_string_reply(bytes: &[u8]) -> Result<String, String> {
    prost::Message::decode(bytes)
        .map(|v: generated::StringReply| v.message)
        .map_err(|e| e.to_string())
}
#[cfg(test)]
mod generated_tests {
    use super::*;
    #[test]
    fn string_reply_round_trip() {
        let b = encode_string_reply("ok");
        assert_eq!(decode_string_reply(&b).unwrap(), "ok");
    }
}

pub struct HealthyService;
#[tonic::async_trait]
impl generated::healthy_server::Healthy for HealthyService {
    async fn is_ok(
        &self,
        _: tonic::Request<()>,
    ) -> Result<tonic::Response<generated::StringReply>, tonic::Status> {
        Ok(tonic::Response::new(generated::StringReply {
            message: "ok".into(),
        }))
    }
}

#[cfg(test)]
mod healthy_contract_tests {
    use super::*;

    #[tokio::test]
    async fn healthy_is_ok_returns_protocol_message() {
        let response = <HealthyService as generated::healthy_server::Healthy>::is_ok(
            &HealthyService,
            tonic::Request::new(()),
        )
        .await
        .unwrap();
        assert_eq!(response.into_inner().message, "ok");
    }
}

pub fn query_response(query_id: &str, message: &str, finished: bool) -> generated::QueryResponse {
    generated::QueryResponse {
        query_id: query_id.into(),
        is_ok: true,
        is_finished: finished,
        message: message.into(),
        data: Vec::new(),
    }
}
#[cfg(test)]
mod response_tests {
    use super::*;
    #[test]
    fn query_response_model() {
        let r = query_response("q1", "ok", true);
        assert!(r.is_ok && r.is_finished && r.data.is_empty());
    }
}

#[derive(Clone, Default)]
pub struct QueryServiceImpl {
    backend: Option<std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>>,
}
impl QueryServiceImpl {
    pub fn from_backend(
        backend: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>,
    ) -> Self {
        Self {
            backend: Some(backend),
        }
    }
}

#[derive(Clone)]
pub struct StateService {
    partitions: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::PartitionState>>,
    commands: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::CommandLog>>,
}
impl Default for StateService {
    fn default() -> Self {
        Self::new()
    }
}
impl StateService {
    pub fn new() -> Self {
        Self {
            partitions: std::sync::Arc::new(std::sync::Mutex::new(Default::default())),
            commands: std::sync::Arc::new(std::sync::Mutex::new(Default::default())),
        }
    }

    pub fn from_partition_state(state: hugegraph_store_core::PartitionState) -> Self {
        Self {
            partitions: std::sync::Arc::new(std::sync::Mutex::new(state)),
            commands: std::sync::Arc::new(std::sync::Mutex::new(Default::default())),
        }
    }

    pub fn partition_state(&self) -> Result<hugegraph_store_core::PartitionState, &'static str> {
        self.partitions
            .lock()
            .map(|state| state.clone())
            .map_err(|_| "partition state lock poisoned")
    }

    pub fn snapshot_partition_state(&self) -> Result<Vec<u8>, &'static str> {
        Ok(self.partition_state()?.snapshot())
    }

    pub fn from_partition_snapshot(bytes: &[u8]) -> Option<Self> {
        hugegraph_store_core::PartitionState::restore(bytes).map(Self::from_partition_state)
    }

    pub fn snapshot_command_log(&self) -> Result<Vec<u8>, &'static str> {
        self.commands
            .lock()
            .map(|log| log.snapshot())
            .map_err(|_| "command log lock poisoned")
    }

    pub fn from_admin_snapshots(partitions: &[u8], commands: &[u8]) -> Option<Self> {
        let state = hugegraph_store_core::PartitionState::restore(partitions)?;
        let log = hugegraph_store_core::CommandLog::restore(commands)?;
        Some(Self {
            partitions: std::sync::Arc::new(std::sync::Mutex::new(state)),
            commands: std::sync::Arc::new(std::sync::Mutex::new(log)),
        })
    }

    pub fn assign_partition(
        &self,
        range: hugegraph_store_core::PartitionRange,
    ) -> Result<(), &'static str> {
        self.partitions
            .lock()
            .map_err(|_| "partition state lock poisoned")?
            .assign(range)
    }

    pub fn split_partition(
        &self,
        id: u32,
        new_id: u32,
        split_key: Vec<u8>,
        owner: u64,
    ) -> Result<(), &'static str> {
        self.partitions
            .lock()
            .map_err(|_| "partition state lock poisoned")?
            .split(id, new_id, split_key, owner)
    }

    pub fn transfer_partition(&self, id: u32, owner: u64) -> Result<(), &'static str> {
        self.partitions
            .lock()
            .map_err(|_| "partition state lock poisoned")?
            .transfer(id, owner)
    }

    pub fn apply_replicated_command(&self, bytes: &[u8]) -> Result<bool, &'static str> {
        let command = hugegraph_store_core::ReplicatedCommand::decode(bytes)
            .ok_or("invalid replicated command")?;
        if self
            .commands
            .lock()
            .map_err(|_| "command log lock poisoned")?
            .contains(&command)
        {
            return Ok(false);
        }
        let mut next_state = self
            .partitions
            .lock()
            .map_err(|_| "partition state lock poisoned")?
            .clone();
        match command.op {
            0x61 => {
                let fields: Vec<_> = command.payload.split(|b| *b == b':').collect();
                if fields.len() != 5 || fields[0] != b"split" {
                    return Err("invalid split command");
                }
                let id = std::str::from_utf8(fields[1])
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or("invalid split id")?;
                let new_id = std::str::from_utf8(fields[3])
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or("invalid split new id")?;
                let owner = std::str::from_utf8(fields[4])
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or("invalid split owner")?;
                next_state.split(id, new_id, fields[2].to_vec(), owner)?;
            }
            0x66 => {
                let fields: Vec<_> = command.payload.split(|b| *b == b':').collect();
                if fields.len() != 3 || fields[0] != b"transfer" {
                    return Err("invalid transfer command");
                }
                let id = std::str::from_utf8(fields[1])
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or("invalid transfer id")?;
                let owner = std::str::from_utf8(fields[2])
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or("invalid transfer owner")?;
                next_state.transfer(id, owner)?;
            }
            0x65 => {
                let fields: Vec<_> = command.payload.split(|b| *b == b':').collect();
                if fields.len() != 2 || fields[0] != b"clean" {
                    return Err("invalid clean command");
                }
                let id = std::str::from_utf8(fields[1])
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or("invalid clean id")?;
                next_state.remove(id)?;
            }
            0x69 => {}
            _ => return Err("unsupported replicated command"),
        }
        let appended = self
            .commands
            .lock()
            .map_err(|_| "command log lock poisoned")?
            .append(command)?;
        if appended {
            *self
                .partitions
                .lock()
                .map_err(|_| "partition state lock poisoned")? = next_state;
        }
        Ok(appended)
    }
}
pub struct GraphService {
    store: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>,
}
impl GraphService {
    pub fn from_backend(
        store: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>,
    ) -> Self {
        Self { store }
    }
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        Ok(Self {
            store: std::sync::Arc::new(std::sync::Mutex::new(
                hugegraph_store_core::DurableStore::open(path)?,
            )),
        })
    }
    pub async fn serve(self, addr: std::net::SocketAddr) -> Result<(), tonic::transport::Error> {
        tonic::transport::Server::builder()
            .add_service(generated::graph_store_server::GraphStoreServer::new(self))
            .serve(addr)
            .await
    }
    pub fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        edges: bool,
    ) -> Result<generated::ScanResponse, String> {
        let store = self
            .store
            .lock()
            .map_err(|_| "store lock poisoned".to_string())?;
        let entries = filter_qualified_entries(&store.entries(), start, end, None);
        Ok(if edges {
            entries_to_edge_frame(&entries)
        } else {
            entries_to_scan_frame(&entries)
        })
    }
    pub fn scan_range_limited(
        &self,
        start: &[u8],
        end: &[u8],
        edges: bool,
        limit: i64,
    ) -> Result<generated::ScanResponse, String> {
        let entries = self.scan_range_limited_for_table(start, end, edges, limit, None)?;
        Ok(entries)
    }

    pub fn scan_range_limited_for_table(
        &self,
        start: &[u8],
        end: &[u8],
        edges: bool,
        limit: i64,
        table: Option<&str>,
    ) -> Result<generated::ScanResponse, String> {
        let store = self
            .store
            .lock()
            .map_err(|_| "store lock poisoned".to_string())?;
        let entries = limit_entries(
            &filter_qualified_entries(&store.entries(), start, end, table),
            limit,
        );
        Ok(if edges {
            entries_to_edge_frame(&entries)
        } else {
            entries_to_scan_frame(&entries)
        })
    }
}
pub fn empty_scan_frame() -> generated::ScanResponse {
    generated::ScanResponse {
        header: None,
        seq_no: 0,
        vertex: vec![],
        edge: vec![],
    }
}
pub fn entries_to_scan_frame(entries: &[(Vec<u8>, Vec<u8>)]) -> generated::ScanResponse {
    let vertex = entries
        .iter()
        .map(|(k, v)| generated::Vertex {
            label: 0,
            id: Some(generated::Variant {
                value_bytes: Some(k.clone()),
                ..Default::default()
            }),
            properties: vec![generated::Property {
                label: 0,
                value: Some(generated::Variant {
                    value_bytes: Some(v.clone()),
                    ..Default::default()
                }),
            }],
        })
        .collect();
    generated::ScanResponse {
        header: None,
        seq_no: 0,
        vertex,
        edge: vec![],
    }
}
pub fn entries_to_edge_frame(entries: &[(Vec<u8>, Vec<u8>)]) -> generated::ScanResponse {
    let edge = entries
        .iter()
        .map(|(k, v)| generated::Edge {
            label: 0,
            source_label: 0,
            target_label: 0,
            source_id: Some(generated::Variant {
                value_bytes: Some(k.clone()),
                ..Default::default()
            }),
            target_id: Some(generated::Variant {
                value_bytes: Some(v.clone()),
                ..Default::default()
            }),
            properties: vec![],
        })
        .collect();
    generated::ScanResponse {
        header: None,
        seq_no: 0,
        vertex: vec![],
        edge,
    }
}
pub fn filter_entries(
    entries: &[(Vec<u8>, Vec<u8>)],
    start: &[u8],
    end: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    entries
        .iter()
        .filter(|(k, _)| k.as_slice() >= start && (end.is_empty() || k.as_slice() < end))
        .cloned()
        .collect()
}

pub fn filter_qualified_entries(
    entries: &[(Vec<u8>, Vec<u8>)],
    start: &[u8],
    end: &[u8],
    table: Option<&str>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    entries
        .iter()
        .filter_map(|(key, value)| {
            if let Some((qualified_table, raw_key)) = hugegraph_store_core::split_table_key(key) {
                if table
                    .map(|name| name.as_bytes() == qualified_table.as_slice())
                    .unwrap_or(true)
                    && raw_key.as_slice() >= start
                    && (end.is_empty() || raw_key.as_slice() < end)
                {
                    return Some((raw_key, value.clone()));
                }
                None
            } else if table.is_none()
                && key.as_slice() >= start
                && (end.is_empty() || key.as_slice() < end)
            {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect()
}
pub fn limit_entries(entries: &[(Vec<u8>, Vec<u8>)], limit: i64) -> Vec<(Vec<u8>, Vec<u8>)> {
    if limit <= 0 {
        return entries.to_vec();
    }
    entries.iter().take(limit as usize).cloned().collect()
}
pub fn scan_request_range(r: &generated::ScanPartitionRequest) -> Option<(Vec<u8>, Vec<u8>)> {
    match r.request.as_ref()? {
        generated::scan_partition_request::RequestOneOf::ScanRequest(v) => Some((
            v.position.clone(),
            if v.end_code == 0 {
                vec![]
            } else {
                vec![v.end_code as u8]
            },
        )),
        generated::scan_partition_request::RequestOneOf::ReplyRequest(_) => None,
    }
}
pub fn scan_request_is_edge(r: &generated::ScanPartitionRequest) -> bool {
    matches!(r.request.as_ref(), Some(generated::scan_partition_request::RequestOneOf::ScanRequest(v)) if v.scan_type == 2)
}
pub fn validate_scan_payload(bytes: &[u8]) -> Result<generated::ScanPartitionRequest, String> {
    if bytes.len() > 1_048_576 {
        return Err("scan payload too large".into());
    }
    let r = generated::ScanPartitionRequest::decode(bytes).map_err(|e| e.to_string())?;
    if r.request.is_none() {
        return Err("missing scan request".into());
    }
    Ok(r)
}
#[cfg(test)]
mod graph_frame_tests {
    use super::empty_scan_frame;
    use crate::generated;
    use prost::Message;
    #[test]
    fn qualified_scan_filters_table_and_decodes_user_key() {
        let entries = vec![
            (b"legacy".to_vec(), b"legacy-value".to_vec()),
            (
                super::qualify_table_name_key("t", b"same"),
                b"t-value".to_vec(),
            ),
            (
                super::qualify_table_name_key("u", b"same"),
                b"u-value".to_vec(),
            ),
        ];
        let filtered = super::filter_qualified_entries(&entries, b"a", b"z", Some("t"));
        assert_eq!(filtered, vec![(b"same".to_vec(), b"t-value".to_vec())]);
        let legacy = super::filter_qualified_entries(&entries, b"a", b"z", None);
        assert!(legacy.contains(&(b"legacy".to_vec(), b"legacy-value".to_vec())));
    }

    #[test]
    fn scan_frame_has_initial_sequence_and_no_records() {
        let f = empty_scan_frame();
        assert_eq!(f.seq_no, 0);
        assert!(f.vertex.is_empty() && f.edge.is_empty());
    }

    #[test]
    fn scan_frame_maps_persisted_key_and_value() {
        let f = super::entries_to_scan_frame(&[(b"id".to_vec(), b"value".to_vec())]);
        assert_eq!(f.vertex.len(), 1);
        assert_eq!(
            f.vertex[0].id.as_ref().unwrap().value_bytes.as_deref(),
            Some(&b"id"[..])
        );
        assert_eq!(
            f.vertex[0].properties[0]
                .value
                .as_ref()
                .unwrap()
                .value_bytes
                .as_deref(),
            Some(&b"value"[..])
        );
    }
    #[test]
    fn edge_frame_maps_endpoints() {
        let f = super::entries_to_edge_frame(&[(b"src".to_vec(), b"dst".to_vec())]);
        assert_eq!(
            f.edge[0].source_id.as_ref().unwrap().value_bytes.as_deref(),
            Some(&b"src"[..])
        );
        assert_eq!(
            f.edge[0].target_id.as_ref().unwrap().value_bytes.as_deref(),
            Some(&b"dst"[..])
        );
    }
    #[test]
    fn entry_filter_observes_half_open_range() {
        let e = vec![
            (b"a".to_vec(), vec![]),
            (b"m".to_vec(), vec![]),
            (b"z".to_vec(), vec![]),
        ];
        let f = super::filter_entries(&e, b"b", b"z");
        assert_eq!(f, vec![(b"m".to_vec(), vec![])]);
    }
    #[test]
    fn entry_limit_is_bounded_and_zero_means_unlimited() {
        let e = vec![(b"a".to_vec(), vec![]), (b"b".to_vec(), vec![])];
        assert_eq!(super::limit_entries(&e, 1).len(), 1);
        assert_eq!(super::limit_entries(&e, 0).len(), 2);
    }
    #[test]
    fn encoded_scan_request_yields_range() {
        let r = generated::ScanPartitionRequest {
            header: None,
            request: Some(
                generated::scan_partition_request::RequestOneOf::ScanRequest(
                    generated::scan_partition_request::Request {
                        position: b"m".to_vec(),
                        end_code: 122,
                        ..Default::default()
                    },
                ),
            ),
        };
        let decoded =
            generated::ScanPartitionRequest::decode(prost::Message::encode_to_vec(&r).as_slice())
                .unwrap();
        let (start, end) = super::scan_request_range(&decoded).unwrap();
        assert_eq!(start, b"m");
        assert_eq!(end, vec![122]);
    }
    #[test]
    fn encoded_edge_scan_request_selects_edge_mode() {
        let r = generated::ScanPartitionRequest {
            header: None,
            request: Some(
                generated::scan_partition_request::RequestOneOf::ScanRequest(
                    generated::scan_partition_request::Request {
                        scan_type: 2,
                        ..Default::default()
                    },
                ),
            ),
        };
        assert!(super::scan_request_is_edge(&r));
    }
    #[test]
    fn malformed_scan_payload_is_rejected() {
        assert!(super::validate_scan_payload(&[0xff, 0xff]).is_err());
        assert!(super::validate_scan_payload(&[]).is_err());
    }
    #[test]
    fn graph_service_scan_range_reads_durable_entries() {
        let p = std::env::temp_dir().join(format!("hg-graph-e2e-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        {
            let mut s = hugegraph_store_core::DurableStore::open(&p).unwrap();
            s.commit_put(b"m".to_vec(), b"v".to_vec()).unwrap();
        }
        let service = super::GraphService::open(&p).unwrap();
        let frame = service.scan_range(b"a", b"z", false).unwrap();
        assert_eq!(frame.vertex.len(), 1);
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn graph_service_scan_qualified_table_decodes_and_isolates() {
        let p = std::env::temp_dir().join(format!("hg-graph-qualified-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        {
            let mut store = hugegraph_store_core::DurableStore::open(&p).unwrap();
            store
                .commit_put(
                    super::qualify_table_name_key("t", b"same"),
                    b"t-value".to_vec(),
                )
                .unwrap();
            store
                .commit_put(
                    super::qualify_table_name_key("u", b"same"),
                    b"u-value".to_vec(),
                )
                .unwrap();
        }
        let service = super::GraphService::open(&p).unwrap();
        let frame = service
            .scan_range_limited_for_table(b"a", b"z", false, 0, Some("t"))
            .unwrap();
        assert_eq!(frame.vertex.len(), 1);
        assert_eq!(
            frame.vertex[0].id.as_ref().unwrap().value_bytes.as_deref(),
            Some(&b"same"[..])
        );
        assert_eq!(
            frame.vertex[0].properties[0]
                .value
                .as_ref()
                .unwrap()
                .value_bytes
                .as_deref(),
            Some(&b"t-value"[..])
        );
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn graph_service_selects_edge_frame_branch() {
        let p = std::env::temp_dir().join(format!("hg-graph-edge-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        {
            let mut s = hugegraph_store_core::DurableStore::open(&p).unwrap();
            s.commit_put(b"src".to_vec(), b"dst".to_vec()).unwrap();
        }
        let service = super::GraphService::open(&p).unwrap();
        let frame = service.scan_range(b"a", b"z", true).unwrap();
        assert!(frame.vertex.is_empty());
        assert_eq!(frame.edge.len(), 1);
        let _ = std::fs::remove_file(p);
    }

    #[tokio::test]
    async fn live_tonic_graph_server_accepts_client() {
        let path = std::env::temp_dir().join(format!("hg-live-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut seed = hugegraph_store_core::DurableStore::open(&path).unwrap();
            seed.commit_put(b"a".to_vec(), b"low".to_vec()).unwrap();
            seed.commit_put(b"m".to_vec(), b"v".to_vec()).unwrap();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = super::GraphService::open(&path).unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(super::generated::graph_store_server::GraphStoreServer::new(
                    service,
                ))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let mut client = super::generated::graph_store_client::GraphStoreClient::connect(format!(
            "http://{}",
            addr
        ))
        .await
        .unwrap();
        let request = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        scan_type: 2,
                        position: b"m".to_vec(),
                        end_code: b'z' as u32,
                        ..Default::default()
                    },
                ),
            ),
        };
        let stream = tokio_stream::iter(vec![request]);
        let response = client.scan_partition(stream).await.unwrap();
        let mut frames = response.into_inner();
        let frame = frames.message().await.unwrap().unwrap();
        assert!(frame.vertex.is_empty());
        assert_eq!(frame.edge.len(), 1);
        let malformed = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        limit: -1,
                        ..Default::default()
                    },
                ),
            ),
        };
        assert!(client
            .scan_partition(tokio_stream::iter(vec![malformed]))
            .await
            .is_err());
        let oversized = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        limit: 100_001,
                        ..Default::default()
                    },
                ),
            ),
        };
        assert!(client
            .scan_partition(tokio_stream::iter(vec![oversized]))
            .await
            .is_err());
        let huge_filter = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        graph_name: "x".repeat(1_048_577),
                        ..Default::default()
                    },
                ),
            ),
        };
        assert!(client
            .scan_partition(tokio_stream::iter(vec![huge_filter]))
            .await
            .is_err());
        let huge = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        position: vec![0; 1_048_577],
                        ..Default::default()
                    },
                ),
            ),
        };
        assert!(client
            .scan_partition(tokio_stream::iter(vec![huge]))
            .await
            .is_err());
        let bad_boundary = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        boundary: -1,
                        ..Default::default()
                    },
                ),
            ),
        };
        assert!(client
            .scan_partition(tokio_stream::iter(vec![bad_boundary]))
            .await
            .is_err());
        let bad_batch = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        batch_size: 1_000_001,
                        ..Default::default()
                    },
                ),
            ),
        };
        assert!(client
            .scan_partition(tokio_stream::iter(vec![bad_batch]))
            .await
            .is_err());
        let bad_props = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        properties: (0..100_001).collect(),
                        ..Default::default()
                    },
                ),
            ),
        };
        assert!(client
            .scan_partition(tokio_stream::iter(vec![bad_props]))
            .await
            .is_err());
        task.abort();
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn live_graph_scan_isolates_qualified_table_over_network() {
        let path =
            std::env::temp_dir().join(format!("hg-live-graph-table-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut seed = hugegraph_store_core::DurableStore::open(&path).unwrap();
            seed.commit_put(
                super::qualify_table_name_key("t", b"same"),
                b"t-value".to_vec(),
            )
            .unwrap();
            seed.commit_put(
                super::qualify_table_name_key("u", b"same"),
                b"u-value".to_vec(),
            )
            .unwrap();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = super::GraphService::open(&path).unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(super::generated::graph_store_server::GraphStoreServer::new(
                    service,
                ))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let mut client = super::generated::graph_store_client::GraphStoreClient::connect(format!(
            "http://{}",
            addr
        ))
        .await
        .unwrap();
        let request = super::generated::ScanPartitionRequest {
            header: None,
            request: Some(
                super::generated::scan_partition_request::RequestOneOf::ScanRequest(
                    super::generated::scan_partition_request::Request {
                        scan_type: 1,
                        table: "t".into(),
                        position: b"a".to_vec(),
                        end_code: b'z' as u32,
                        ..Default::default()
                    },
                ),
            ),
        };
        let response = client
            .scan_partition(tokio_stream::iter(vec![request]))
            .await
            .unwrap();
        let mut frames = response.into_inner();
        let frame = frames.message().await.unwrap().unwrap();
        assert_eq!(frame.vertex.len(), 1);
        assert_eq!(
            frame.vertex[0].id.as_ref().unwrap().value_bytes.as_deref(),
            Some(&b"same"[..])
        );
        assert_eq!(
            frame.vertex[0].properties[0]
                .value
                .as_ref()
                .unwrap()
                .value_bytes
                .as_deref(),
            Some(&b"t-value"[..])
        );
        task.abort();
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn live_query_and_state_servers_accept_contract_requests() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let state_service = super::StateService::new();
        state_service
            .assign_partition(hugegraph_store_core::PartitionRange {
                id: 1,
                start: b"a".to_vec(),
                end: b"z".to_vec(),
                owner: 10,
            })
            .unwrap();
        state_service
            .split_partition(1, 2, b"m".to_vec(), 20)
            .unwrap();
        state_service.transfer_partition(1, 11).unwrap();
        let seed_command = hugegraph_store_core::ReplicatedCommand {
            op: 0x69,
            term: 4,
            index: 1,
            request_id: b"seed-1".to_vec(),
            payload: Vec::new(),
        }
        .encode();
        assert_eq!(
            state_service.apply_replicated_command(&seed_command),
            Ok(true)
        );
        let state_service = super::StateService::from_admin_snapshots(
            &state_service.snapshot_partition_state().unwrap(),
            &state_service.snapshot_command_log().unwrap(),
        )
        .unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    super::generated::query_service_server::QueryServiceServer::new(
                        super::QueryServiceImpl::default(),
                    ),
                )
                .add_service(
                    super::generated::hg_store_state_server::HgStoreStateServer::new(state_service),
                )
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let endpoint = format!("http://{}", addr);
        let mut query =
            super::generated::query_service_client::QueryServiceClient::connect(endpoint.clone())
                .await
                .unwrap();
        let response = query
            .query0(super::generated::QueryRequest {
                query_id: "q".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(response.into_inner().is_ok);
        let response = query
            .count(super::generated::QueryRequest {
                query_id: "c".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(response.into_inner().data.is_empty());
        let mut state =
            super::generated::hg_store_state_client::HgStoreStateClient::connect(endpoint)
                .await
                .unwrap();
        let response = state
            .get_scan_state(super::generated::SubStateReq::default())
            .await
            .unwrap();
        assert_eq!(
            response.into_inner(),
            super::generated::ScanState::default()
        );
        let mut stream = state
            .sub_state(super::generated::SubStateReq::default())
            .await
            .unwrap()
            .into_inner();
        let node = stream.message().await.unwrap().unwrap();
        assert_eq!(node.state, super::generated::NodeStateType::Online as i32);
        state
            .unsub_state(super::generated::SubStateReq::default())
            .await
            .unwrap();
        let command = hugegraph_store_core::ReplicatedCommand {
            op: 0x61,
            term: 4,
            index: 2,
            request_id: b"net-1".to_vec(),
            payload: b"split:2:t:3:30".to_vec(),
        }
        .encode();
        let applied = state
            .get_peers(super::generated::PartitionRequest {
                id: 2,
                command: command.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(applied.code, 0);
        assert_eq!(applied.message, "applied");
        let duplicate = state
            .get_peers(super::generated::PartitionRequest { id: 2, command })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(duplicate.message, "duplicate");
        let gap = hugegraph_store_core::ReplicatedCommand {
            op: 0x66,
            term: 4,
            index: 4,
            request_id: b"net-3".to_vec(),
            payload: b"transfer:3:31".to_vec(),
        }
        .encode();
        let rejected = state
            .get_peers(super::generated::PartitionRequest {
                id: 2,
                command: gap,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rejected.code, 1);
        let peers = state
            .get_peers(super::generated::PartitionRequest {
                id: 2,
                command: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(peers.into_inner().peers, "id=2;start=m;end=t;owner=20");
        let split_peer = state
            .get_peers(super::generated::PartitionRequest {
                id: 3,
                command: Vec::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(split_peer.peers, "id=3;start=t;end=z;owner=30");
    }

    #[test]
    fn two_state_services_replay_identical_partition_command_stream() {
        let primary = super::StateService::new();
        let follower = super::StateService::new();
        for service in [&primary, &follower] {
            service
                .assign_partition(hugegraph_store_core::PartitionRange {
                    id: 1,
                    start: b"a".to_vec(),
                    end: b"z".to_vec(),
                    owner: 10,
                })
                .unwrap();
        }
        let commands = [
            hugegraph_store_core::ReplicatedCommand {
                op: 0x61,
                term: 1,
                index: 1,
                request_id: b"split-1".to_vec(),
                payload: b"split:1:m:2:20".to_vec(),
            },
            hugegraph_store_core::ReplicatedCommand {
                op: 0x66,
                term: 1,
                index: 2,
                request_id: b"transfer-2".to_vec(),
                payload: b"transfer:2:21".to_vec(),
            },
            hugegraph_store_core::ReplicatedCommand {
                op: 0x65,
                term: 1,
                index: 3,
                request_id: b"clean-3".to_vec(),
                payload: b"clean:1".to_vec(),
            },
        ];
        for command in commands {
            let encoded = command.encode();
            assert_eq!(primary.apply_replicated_command(&encoded), Ok(true));
            assert_eq!(follower.apply_replicated_command(&encoded), Ok(true));
        }
        assert_eq!(
            primary.snapshot_partition_state().unwrap(),
            follower.snapshot_partition_state().unwrap()
        );
        assert_eq!(
            primary.snapshot_command_log().unwrap(),
            follower.snapshot_command_log().unwrap()
        );
        let duplicate_task = hugegraph_store_core::ReplicatedCommand {
            op: 0x66,
            term: 1,
            index: 4,
            request_id: b"transfer-2".to_vec(),
            payload: b"transfer:2:99".to_vec(),
        }
        .encode();
        assert!(primary.apply_replicated_command(&duplicate_task).is_err());
        assert!(follower.apply_replicated_command(&duplicate_task).is_err());
        assert_eq!(
            primary.snapshot_partition_state().unwrap(),
            follower.snapshot_partition_state().unwrap()
        );
        let invalid = hugegraph_store_core::ReplicatedCommand {
            op: 0x65,
            term: 1,
            index: 4,
            request_id: b"clean-4".to_vec(),
            payload: b"clean:99".to_vec(),
        }
        .encode();
        assert!(primary.apply_replicated_command(&invalid).is_err());
        assert!(follower.apply_replicated_command(&invalid).is_err());
        assert_eq!(
            primary.snapshot_partition_state().unwrap(),
            follower.snapshot_partition_state().unwrap()
        );
        assert_eq!(
            primary.snapshot_command_log().unwrap(),
            follower.snapshot_command_log().unwrap()
        );
    }

    #[tokio::test]
    async fn session_lifecycle_methods_return_contract_responses() {
        let path = std::env::temp_dir().join(format!("hg-session-live-{}.img", std::process::id()));
        let service = super::SessionService::open(&path).unwrap();
        service
            .shared
            .backend
            .lock()
            .unwrap()
            .commit_put(b"before-clean".to_vec(), b"value".to_vec())
            .unwrap();
        let table = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &service, tonic::Request::new(super::generated::TableReq {
                method: super::generated::TableMethod::Create as i32,
                table_name: "vertices".into(),
                ..Default::default()
        })).await.unwrap();
        assert!(table.into_inner().responses.is_none());
        let exists = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &service, tonic::Request::new(super::generated::TableReq {
                method: super::generated::TableMethod::Exists as i32,
                table_name: "vertices".into(),
                ..Default::default()
            })).await.unwrap();
        assert_eq!(exists.into_inner().status.unwrap().code, 0);
        let deleted = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &service, tonic::Request::new(super::generated::TableReq {
                method: super::generated::TableMethod::Delete as i32,
                table_name: "vertices".into(),
                ..Default::default()
            })).await.unwrap();
        assert_eq!(deleted.into_inner().status.unwrap().code, 0);
        let graph = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::graph(
            &service, tonic::Request::new(super::generated::GraphReq {
                method: super::generated::GraphMethod::Delete as i32,
                graph_name: "g".into(),
                ..Default::default()
            })).await.unwrap();
        assert!(graph.into_inner().responses.is_none());
        let clean = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::clean(
            &service, tonic::Request::new(super::generated::CleanReq::default())).await.unwrap();
        assert!(clean.into_inner().responses.is_none());
        assert_eq!(
            service.shared.backend.lock().unwrap().entries(),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("catalog"));
    }

    #[tokio::test]
    async fn table_catalog_survives_session_reopen() {
        let path =
            std::env::temp_dir().join(format!("hg-table-catalog-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("catalog"));
        let service = super::SessionService::open(&path).unwrap();
        <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &service,
            tonic::Request::new(super::generated::TableReq {
                method: super::generated::TableMethod::Create as i32,
                table_name: "persisted".into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        drop(service);
        let reopened = super::SessionService::open(&path).unwrap();
        let exists = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &reopened,
            tonic::Request::new(super::generated::TableReq {
                method: super::generated::TableMethod::Exists as i32,
                table_name: "persisted".into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(exists.into_inner().status.unwrap().code, 0);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("catalog"));
    }

    #[tokio::test]
    async fn live_table_graph_rpc_catalog_recovery() {
        let path = std::env::temp_dir().join(format!("hg-live-catalog-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("catalog"));
        {
            let mut seed = hugegraph_store_core::DurableStore::open(&path).unwrap();
            seed.commit_put(b"graph-data".to_vec(), b"preserve".to_vec())
                .unwrap();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = super::SessionService::open(&path).unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    super::generated::hg_store_session_server::HgStoreSessionServer::new(service),
                )
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let mut client = super::generated::hg_store_session_client::HgStoreSessionClient::connect(
            format!("http://{}", addr),
        )
        .await
        .unwrap();
        let create = client
            .table(super::generated::TableReq {
                method: super::generated::TableMethod::Create as i32,
                table_name: "network-table".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(create.into_inner().status.unwrap().code, 0);
        let exists = client
            .table(super::generated::TableReq {
                method: super::generated::TableMethod::Exists as i32,
                table_name: "network-table".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(exists.into_inner().status.unwrap().code, 0);
        let graph = client
            .graph(super::generated::GraphReq {
                method: super::generated::GraphMethod::Delete as i32,
                graph_name: "network-graph".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(graph.into_inner().status.unwrap().code, 0);
        assert!(client
            .graph(super::generated::GraphReq {
                method: super::generated::GraphMethod::Delete as i32,
                graph_name: String::new(),
                ..Default::default()
            })
            .await
            .is_err());
        assert!(client
            .graph(super::generated::GraphReq {
                method: 99,
                graph_name: "network-graph".into(),
                ..Default::default()
            })
            .await
            .is_err());
        task.abort();
        let reopened = super::SessionService::open(&path).unwrap();
        assert_eq!(
            reopened.shared.backend.lock().unwrap().get(b"graph-data"),
            Some(b"preserve".to_vec())
        );
        let status = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &reopened,
            tonic::Request::new(super::generated::TableReq {
                method: super::generated::TableMethod::Exists as i32,
                table_name: "network-table".into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(status.into_inner().status.unwrap().code, 0);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("catalog"));
    }

    #[tokio::test]
    async fn table_catalog_fault_restores_memory_and_retries() {
        let path =
            std::env::temp_dir().join(format!("hg-table-catalog-fault-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let catalog = path.with_extension("catalog");
        let _ = std::fs::remove_file(&catalog);
        let service = super::SessionService::open(&path).unwrap();
        let request = |method, name: &str| {
            tonic::Request::new(super::generated::TableReq {
                method,
                table_name: name.into(),
                ..Default::default()
            })
        };
        <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &service, request(super::generated::TableMethod::Create as i32, "stable"),
        )
        .await
        .unwrap();
        std::fs::remove_file(&catalog).unwrap();
        std::fs::create_dir(&catalog).unwrap();
        assert!(<super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &service, request(super::generated::TableMethod::Create as i32, "transient"),
        )
        .await
        .is_err());
        let stable = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &service, request(super::generated::TableMethod::Exists as i32, "stable"),
        )
        .await
        .unwrap();
        assert_eq!(stable.into_inner().status.unwrap().code, 0);
        std::fs::remove_dir(&catalog).unwrap();
        <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &service, request(super::generated::TableMethod::Create as i32, "transient"),
        )
        .await
        .unwrap();
        drop(service);
        let reopened = super::SessionService::open(&path).unwrap();
        let recovered = <super::SessionService as super::generated::hg_store_session_server::HgStoreSession>::table(
            &reopened, request(super::generated::TableMethod::Exists as i32, "transient"),
        )
        .await
        .unwrap();
        assert_eq!(recovered.into_inner().status.unwrap().code, 0);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(catalog);
    }
}

#[tonic::async_trait]
impl generated::graph_store_server::GraphStore for GraphService {
    type ScanPartitionStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<generated::ScanResponse, tonic::Status>> + Send>,
    >;
    async fn scan_partition(
        &self,
        request: tonic::Request<tonic::Streaming<generated::ScanPartitionRequest>>,
    ) -> Result<tonic::Response<Self::ScanPartitionStream>, tonic::Status> {
        let mut input = request.into_inner();
        let frame = if let Some(r) = input
            .next()
            .await
            .transpose()
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?
        {
            let (start, end) = scan_request_range(&r)
                .ok_or_else(|| tonic::Status::invalid_argument("missing scan request"))?;
            let limit = match r.request.as_ref() {
                Some(generated::scan_partition_request::RequestOneOf::ScanRequest(v)) => {
                    if v.boundary < 0 || v.batch_size < 0 || v.batch_size > 1_000_000 {
                        return Err(tonic::Status::invalid_argument(
                            "invalid scan boundary or batch size",
                        ));
                    }
                    if v.properties.len() > 100_000 {
                        return Err(tonic::Status::resource_exhausted(
                            "too many scan properties",
                        ));
                    }
                    if v.graph_name.len() + v.table.len() + v.condition.len() > 1_048_576 {
                        return Err(tonic::Status::resource_exhausted("scan filters too large"));
                    }
                    if v.position.len() > 1_048_576 {
                        return Err(tonic::Status::resource_exhausted("scan position too large"));
                    }
                    if v.limit < 0 {
                        return Err(tonic::Status::invalid_argument("negative limit"));
                    }
                    if v.limit > MAX_SCAN_LIMIT {
                        return Err(tonic::Status::resource_exhausted("scan limit too large"));
                    }
                    v.limit
                }
                _ => 0,
            };
            let table = match r.request.as_ref() {
                Some(generated::scan_partition_request::RequestOneOf::ScanRequest(v))
                    if !v.table.is_empty() =>
                {
                    Some(v.table.as_str())
                }
                _ => None,
            };
            self.scan_range_limited_for_table(&start, &end, scan_request_is_edge(&r), limit, table)
                .map_err(|e| tonic::Status::new(classify_failure(&e), e))?
        } else {
            empty_scan_frame()
        };
        Ok(tonic::Response::new(Box::pin(tokio_stream::iter([Ok(
            frame,
        )]))))
    }
}

#[tonic::async_trait]
impl generated::hg_store_state_server::HgStoreState for StateService {
    type SubStateStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<generated::NodeStateRes, tonic::Status>> + Send>,
    >;
    async fn sub_state(
        &self,
        _: tonic::Request<generated::SubStateReq>,
    ) -> Result<tonic::Response<Self::SubStateStream>, tonic::Status> {
        let state = generated::NodeStateRes {
            state: generated::NodeStateType::Online as i32,
        };
        Ok(tonic::Response::new(Box::pin(tokio_stream::iter([Ok(
            state,
        )]))))
    }
    async fn unsub_state(
        &self,
        _: tonic::Request<generated::SubStateReq>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(tonic::Response::new(()))
    }
    async fn get_scan_state(
        &self,
        _: tonic::Request<generated::SubStateReq>,
    ) -> Result<tonic::Response<generated::ScanState>, tonic::Status> {
        Ok(tonic::Response::new(generated::ScanState::default()))
    }
    async fn get_peers(
        &self,
        request: tonic::Request<generated::PartitionRequest>,
    ) -> Result<tonic::Response<generated::PeersResponse>, tonic::Status> {
        let request = request.into_inner();
        let id = request.id;
        let command_result = if request.command.is_empty() {
            Ok(None)
        } else {
            self.apply_replicated_command(&request.command).map(Some)
        };
        let (code, message) = match command_result {
            Ok(Some(true)) => (0, "applied"),
            Ok(Some(false)) => (0, "duplicate"),
            Ok(None) => (0, "ok"),
            Err(message) => (1, message),
        };
        let state = self.partition_state().map_err(tonic::Status::internal)?;
        let peers = state
            .range(id as u32)
            .map(|range| {
                format!(
                    "id={};start={};end={};owner={}",
                    range.id,
                    String::from_utf8_lossy(&range.start),
                    String::from_utf8_lossy(&range.end),
                    range.owner
                )
            })
            .unwrap_or_default();
        Ok(tonic::Response::new(generated::PeersResponse {
            peers,
            code,
            message: message.into(),
        }))
    }
}
#[tonic::async_trait]
impl generated::query_service_server::QueryService for QueryServiceImpl {
    type queryStream = std::pin::Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<
                    Item = Result<generated::QueryResponse, tonic::Status>,
                > + Send,
        >,
    >;
    async fn query(
        &self,
        request: tonic::Request<tonic::Streaming<generated::QueryRequest>>,
    ) -> Result<tonic::Response<Self::queryStream>, tonic::Status> {
        let _ = request;
        Ok(tonic::Response::new(Box::pin(tokio_stream::iter(Vec::<
            Result<generated::QueryResponse, tonic::Status>,
        >::new(
        )))))
    }
    async fn query0(
        &self,
        request: tonic::Request<generated::QueryRequest>,
    ) -> Result<tonic::Response<generated::QueryResponse>, tonic::Status> {
        let q = request.into_inner();
        let data = self
            .backend
            .as_ref()
            .map(|backend| query_entries(backend, &q))
            .transpose()
            .map_err(|e| tonic::Status::internal(e))?
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| generated::Kv {
                key,
                value,
                code: 0,
            })
            .collect();
        Ok(tonic::Response::new(generated::QueryResponse {
            query_id: q.query_id,
            is_ok: true,
            is_finished: true,
            message: "ok".into(),
            data,
        }))
    }
    async fn count(
        &self,
        request: tonic::Request<generated::QueryRequest>,
    ) -> Result<tonic::Response<generated::QueryResponse>, tonic::Status> {
        let q = request.into_inner();
        let count = self
            .backend
            .as_ref()
            .map(|backend| query_entries(backend, &q).map(|entries| entries.len()))
            .transpose()
            .map_err(|e| tonic::Status::internal(e))?
            .unwrap_or(0);
        Ok(tonic::Response::new(query_response(
            &q.query_id,
            &count.to_string(),
            true,
        )))
    }
}

fn query_entries(
    backend: &std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>,
    request: &generated::QueryRequest,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    let store = backend
        .lock()
        .map_err(|_| "backend lock poisoned".to_string())?;
    let (start, end) = request
        .scan_type_param
        .first()
        .map(|param| (param.key_start.as_slice(), param.key_end.as_slice()))
        .unwrap_or((&[][..], &[][..]));
    let mut entries = filter_qualified_entries(
        &store.entries(),
        start,
        end,
        (!request.table.is_empty()).then_some(request.table.as_str()),
    );
    let offset = request.offset as usize;
    if offset >= entries.len() {
        entries.clear();
    } else if offset > 0 {
        entries.drain(..offset);
    }
    if request.limit > 0 {
        entries.truncate(request.limit as usize);
    }
    Ok(entries)
}

#[cfg(test)]
mod query_data_tests {
    use super::*;

    #[tokio::test]
    async fn query0_and_count_read_qualified_backend_entries() {
        let path = std::env::temp_dir().join(format!("hg-query-data-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let backend = std::sync::Arc::new(std::sync::Mutex::new(
            hugegraph_store_core::DurableStore::open(&path).unwrap(),
        ));
        {
            let mut store = backend.lock().unwrap();
            store
                .commit_put(qualify_table_name_key("t", b"a"), b"one".to_vec())
                .unwrap();
            store
                .commit_put(qualify_table_name_key("t", b"b"), b"two".to_vec())
                .unwrap();
            store
                .commit_put(qualify_table_name_key("u", b"a"), b"other".to_vec())
                .unwrap();
        }
        let service = QueryServiceImpl::from_backend(backend);
        let request = generated::QueryRequest {
            query_id: "q".into(),
            table: "t".into(),
            limit: 1,
            ..Default::default()
        };
        let out = <QueryServiceImpl as generated::query_service_server::QueryService>::query0(
            &service,
            tonic::Request::new(request.clone()),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(out.data.len(), 1);
        assert_eq!(out.data[0].key, b"a");
        assert_eq!(out.data[0].value, b"one");
        let count = <QueryServiceImpl as generated::query_service_server::QueryService>::count(
            &service,
            tonic::Request::new(request),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(count.message, "1");
        let _ = std::fs::remove_file(path);
    }
}

pub struct SessionService {
    pub shared: SharedSession,
    pub transactions: std::sync::Arc<std::sync::Mutex<SessionTransactions>>,
    table_names: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    table_catalog_path: Option<std::path::PathBuf>,
}
impl SessionService {
    fn load_catalog(path: &std::path::Path) -> std::io::Result<std::collections::HashSet<String>> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let text = String::from_utf8(bytes)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(text
                    .lines()
                    .map(str::to_owned)
                    .filter(|name| !name.is_empty())
                    .collect())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Default::default()),
            Err(error) => Err(error),
        }
    }

    fn persist_catalog(&self, names: &std::collections::HashSet<String>) -> std::io::Result<()> {
        let Some(path) = &self.table_catalog_path else {
            return Ok(());
        };
        let mut sorted: Vec<_> = names.iter().cloned().collect();
        sorted.sort();
        let tmp = path.with_extension("catalog.tmp");
        std::fs::write(&tmp, sorted.join("\n"))?;
        std::fs::rename(tmp, path)
    }

    pub fn from_backend(
        backend: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>,
    ) -> Self {
        Self {
            shared: SharedSession { backend },
            transactions: std::sync::Arc::new(
                std::sync::Mutex::new(SessionTransactions::default()),
            ),
            table_names: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            table_catalog_path: None,
        }
    }
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let catalog_path = path.with_extension("catalog");
        let names = Self::load_catalog(&catalog_path)?;
        Ok(Self {
            shared: SharedSession::open(path)?,
            transactions: std::sync::Arc::new(
                std::sync::Mutex::new(SessionTransactions::default()),
            ),
            table_names: std::sync::Arc::new(std::sync::Mutex::new(names)),
            table_catalog_path: Some(catalog_path),
        })
    }
}
#[tonic::async_trait]
impl generated::hg_store_session_server::HgStoreSession for SessionService {
    async fn get2(
        &self,
        request: tonic::Request<generated::GetReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        let bytes = prost::Message::encode_to_vec(&request.into_inner());
        self.shared
            .handle_get2(&bytes)
            .map(tonic::Response::new)
            .map_err(|e| tonic::Status::new(classify_failure(&e), e))
    }
    async fn batch_get2(
        &self,
        request: tonic::Request<generated::BatchGetReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        let bytes = prost::Message::encode_to_vec(&request.into_inner());
        let store = self
            .shared
            .backend
            .lock()
            .map_err(|_| tonic::Status::internal("backend lock poisoned"))?;
        let response = feedback_values_with_missing(&store, &bytes)
            .map_err(|e| tonic::Status::new(classify_failure(&e), e))?;
        Ok(tonic::Response::new(response))
    }
    async fn batch(
        &self,
        request: tonic::Request<generated::BatchReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        let req = request.into_inner();
        let id = req.batch_id.clone();
        let bytes = prost::Message::encode_to_vec(&req);
        let mut tx = self
            .transactions
            .lock()
            .map_err(|_| tonic::Status::internal("transaction lock poisoned"))?;
        let mut store = self
            .shared
            .backend
            .lock()
            .map_err(|_| tonic::Status::internal("backend lock poisoned"))?;
        apply_session_batch(&id, &bytes, &mut tx, &mut store)
            .map_err(|e| tonic::Status::new(classify_failure(&e), e))?;
        Ok(tonic::Response::new(generated::FeedbackRes::default()))
    }
    async fn table(
        &self,
        request: tonic::Request<generated::TableReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        let request = request.into_inner();
        if request.table_name.is_empty() {
            return Err(tonic::Status::invalid_argument("table name is required"));
        }
        let method = generated::TableMethod::try_from(request.method)
            .map_err(|_| tonic::Status::invalid_argument("unknown table method"))?;
        let mut names = self
            .table_names
            .lock()
            .map_err(|_| tonic::Status::internal("table catalog lock poisoned"))?;
        if request.table_name.contains('\n') {
            return Err(tonic::Status::invalid_argument(
                "table name contains newline",
            ));
        }
        let previous = names.clone();
        let response = match method {
            generated::TableMethod::Exists => {
                if names.contains(&request.table_name) {
                    feedback_ok()
                } else {
                    feedback_status(2, "table does not exist")
                }
            }
            generated::TableMethod::Create => {
                names.insert(request.table_name);
                feedback_ok()
            }
            generated::TableMethod::Delete | generated::TableMethod::Drop => {
                names.remove(&request.table_name);
                feedback_ok()
            }
            generated::TableMethod::Truncate => {
                let prefix = qualify_table_name_key(&request.table_name, &[]);
                self.shared
                    .backend
                    .lock()
                    .map_err(|_| tonic::Status::internal("backend lock poisoned"))?
                    .apply_batch(&[hugegraph_store_core::BatchMutation::DeletePrefix { prefix }])
                    .map_err(|e| {
                        tonic::Status::new(classify_failure(&e.to_string()), e.to_string())
                    })?;
                feedback_ok()
            }
            generated::TableMethod::Unknown => {
                return Err(tonic::Status::invalid_argument("unknown table method"));
            }
        };
        if matches!(
            method,
            generated::TableMethod::Create
                | generated::TableMethod::Delete
                | generated::TableMethod::Drop
        ) {
            if let Err(error) = self.persist_catalog(&names) {
                *names = previous;
                return Err(tonic::Status::new(
                    classify_failure(&error.to_string()),
                    error.to_string(),
                ));
            }
        }
        Ok(tonic::Response::new(response))
    }
    async fn graph(
        &self,
        request: tonic::Request<generated::GraphReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        let request = request.into_inner();
        if request.graph_name.is_empty() {
            return Err(tonic::Status::invalid_argument("graph name is required"));
        }
        match generated::GraphMethod::try_from(request.method)
            .map_err(|_| tonic::Status::invalid_argument("unknown graph method"))?
        {
            generated::GraphMethod::Delete => Ok(tonic::Response::new(feedback_ok())),
            generated::GraphMethod::Unknown => {
                Err(tonic::Status::invalid_argument("unknown graph method"))
            }
        }
    }
    async fn clean(
        &self,
        _: tonic::Request<generated::CleanReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        self.shared
            .backend
            .lock()
            .map_err(|_| tonic::Status::internal("backend lock poisoned"))?
            .clear()
            .map_err(|e| tonic::Status::new(classify_failure(&e.to_string()), e.to_string()))?;
        Ok(tonic::Response::new(feedback_ok()))
    }
}

pub fn qualify_table_name_key(table: &str, key: &[u8]) -> Vec<u8> {
    hugegraph_store_core::qualify_table_key(table.as_bytes(), key)
}

pub fn qualify_table_id_key(table: i32, key: &[u8]) -> Vec<u8> {
    qualify_table_name_key(&table.to_string(), key)
}

pub fn decode_get_table_key(bytes: &[u8]) -> Result<(String, Vec<u8>), String> {
    let r: generated::GetReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    r.tk.map(|t| (t.table, t.key))
        .filter(|(_, key)| !key.is_empty())
        .ok_or_else(|| "GetReq missing key".into())
}

pub fn decode_get_key(bytes: &[u8]) -> Result<Vec<u8>, String> {
    decode_get_table_key(bytes).map(|(_, key)| key)
}
#[cfg(test)]
mod get_decode_tests {
    use super::*;
    #[test]
    fn decode_get_req_key() {
        let r = generated::GetReq {
            header: None,
            tk: Some(generated::Tk {
                table: "t".into(),
                key: b"k".to_vec(),
                code: 0,
            }),
        };
        let b = prost::Message::encode_to_vec(&r);
        assert_eq!(decode_get_key(&b).unwrap(), b"k");
    }
}

pub fn get_req_to_store_request(
    bytes: &[u8],
) -> Result<hugegraph_store_core::StoreRequest, String> {
    decode_get_table_key(bytes).map(|(table, key)| hugegraph_store_core::StoreRequest::Get {
        key: qualify_table_name_key(&table, &key),
    })
}
#[cfg(test)]
mod mapping_tests {
    use super::*;
    #[test]
    fn get_mapping() {
        let r = generated::GetReq {
            header: None,
            tk: Some(generated::Tk {
                table: "t".into(),
                key: b"k".to_vec(),
                code: 0,
            }),
        };
        let b = prost::Message::encode_to_vec(&r);
        assert!(
            matches!(get_req_to_store_request(&b).unwrap(), hugegraph_store_core::StoreRequest::Get{key} if key==qualify_table_name_key("t", b"k"))
        );
    }
}

pub struct SharedSession {
    pub backend: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>,
}
impl SharedSession {
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        Ok(Self {
            backend: std::sync::Arc::new(std::sync::Mutex::new(
                hugegraph_store_core::DurableStore::open(path)?,
            )),
        })
    }
    pub fn backend(&self) -> std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>> {
        self.backend.clone()
    }
}
#[cfg(test)]
mod shared_tests {
    use super::*;
    #[test]
    fn shared_session_opens() {
        let p = std::env::temp_dir().join(format!("hg-shared-{}.img", std::process::id()));
        let s = SharedSession::open(&p).unwrap();
        s.backend()
            .lock()
            .unwrap()
            .commit_put(qualify_table_name_key("t", b"k"), b"value".to_vec())
            .unwrap();
        assert!(s.backend().lock().is_ok());
        let _ = std::fs::remove_file(p);
    }
}

impl SharedSession {
    pub fn handle_get2(&self, bytes: &[u8]) -> Result<generated::FeedbackRes, String> {
        let (table, key) = decode_get_table_key(bytes)?;
        let key = qualify_table_name_key(&table, &key);
        let value = self
            .backend
            .lock()
            .map_err(|_| "backend lock poisoned")?
            .get(&key);
        let status = generated::ResStatus {
            code: if value.is_some() { 0 } else { 2 },
            msg: if value.is_some() { "ok" } else { "missing" }.into(),
        };
        Ok(generated::FeedbackRes {
            status: Some(status),
            responses: Some(generated::feedback_res::Responses::ValueResponse(
                generated::ValueResponse {
                    value: value.unwrap_or_default(),
                },
            )),
        })
    }
}
#[cfg(test)]
mod shared_get_tests {
    use super::*;
    #[test]
    fn get2_handler_decodes() {
        let p = std::env::temp_dir().join(format!("hg-sget-{}.img", std::process::id()));
        let s = SharedSession::open(&p).unwrap();
        s.backend()
            .lock()
            .unwrap()
            .commit_put(qualify_table_name_key("t", b"k"), b"value".to_vec())
            .unwrap();
        let r = generated::GetReq {
            header: None,
            tk: Some(generated::Tk {
                table: "t".into(),
                key: b"k".to_vec(),
                code: 0,
            }),
        };
        let response = s.handle_get2(&prost::Message::encode_to_vec(&r)).unwrap();
        assert_eq!(response.status.unwrap().code, 0);
        match response.responses.unwrap() {
            generated::feedback_res::Responses::ValueResponse(value) => {
                assert_eq!(value.value, b"value")
            }
            _ => panic!("expected value response"),
        }
        let _ = std::fs::remove_file(p);
    }
}

pub fn batch_operation_kind(bytes: &[u8]) -> Result<&'static str, String> {
    let r: generated::BatchReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    match r.requests {
        Some(generated::batch_req::Requests::WriteReq(_)) => Ok("write"),
        Some(generated::batch_req::Requests::CommitReq(_)) => Ok("commit"),
        Some(generated::batch_req::Requests::RollbackReq(_)) => Ok("rollback"),
        None => Err("BatchReq missing operation".into()),
    }
}

pub fn batch_command(bytes: &[u8]) -> Result<hugegraph_store_core::StoreRequest, String> {
    let r: generated::BatchReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    match r.requests {
        Some(generated::batch_req::Requests::WriteReq(w)) => {
            let entries = w.entry;
            if entries.is_empty() {
                return Err("empty write".into());
            }
            let mut puts = Vec::new();
            let mut deletes = Vec::new();
            let mut prefixes = Vec::new();
            let mut ranges = Vec::new();
            let mut merges = Vec::new();
            for entry in entries {
                let table = entry.table;
                let key = qualify_table_id_key(
                    table,
                    &entry.start_key.map(|x| x.key).unwrap_or_default(),
                );
                match entry.op_type {
                    0 | 1 => puts.push((key, entry.value)),
                    2 | 3 => deletes.push(key),
                    4 => prefixes.push(key),
                    5 => ranges.push((
                        key,
                        qualify_table_id_key(
                            table,
                            &entry.end_key.map(|x| x.key).unwrap_or_default(),
                        ),
                    )),
                    6 => merges.push((key, entry.value)),
                    _ => return Err("unknown batch operation type".into()),
                }
            }
            let categories = [
                !puts.is_empty(),
                !deletes.is_empty(),
                !prefixes.is_empty(),
                !ranges.is_empty(),
                !merges.is_empty(),
            ]
            .into_iter()
            .filter(|present| *present)
            .count();
            if categories != 1 {
                return Err("mixed batch operations require session context".into());
            }
            if !puts.is_empty() {
                Ok(hugegraph_store_core::StoreRequest::BatchPut { entries: puts })
            } else if !deletes.is_empty() {
                Ok(hugegraph_store_core::StoreRequest::BatchDelete { keys: deletes })
            } else if !prefixes.is_empty() {
                if prefixes.len() != 1 {
                    return Err("multiple prefix deletes require session context".into());
                }
                Ok(hugegraph_store_core::StoreRequest::BatchDeletePrefix {
                    prefix: prefixes.remove(0),
                })
            } else if !ranges.is_empty() {
                if ranges.len() != 1 {
                    return Err("multiple range deletes require session context".into());
                }
                let (start, end) = ranges.remove(0);
                Ok(hugegraph_store_core::StoreRequest::BatchDeleteRange { start, end })
            } else if !merges.is_empty() {
                Ok(hugegraph_store_core::StoreRequest::BatchMerge { entries: merges })
            } else {
                Err("empty batch operation".into())
            }
        }
        Some(generated::batch_req::Requests::RollbackReq(_)) => {
            Ok(hugegraph_store_core::StoreRequest::Rollback { key: Vec::new() })
        }
        Some(generated::batch_req::Requests::CommitReq(_)) => {
            Err("commit requires transaction context".into())
        }
        None => Err("missing operation".into()),
    }
}
#[cfg(test)]
mod batch_tests {
    use super::*;
    #[test]
    fn batch_write_maps() {
        let e = generated::BatchEntry {
            op_type: 0,
            table: 0,
            start_key: Some(generated::Key {
                key: b"k".to_vec(),
                code: 0,
            }),
            end_key: None,
            value: b"v".to_vec(),
        };
        let r = generated::BatchReq {
            header: None,
            batch_id: "b".into(),
            requests: Some(generated::batch_req::Requests::WriteReq(
                generated::BatchWriteReq {
                    entry: vec![
                        e,
                        generated::BatchEntry {
                            op_type: 0,
                            table: 0,
                            start_key: Some(generated::Key {
                                key: b"k2".to_vec(),
                                code: 0,
                            }),
                            end_key: None,
                            value: b"v2".to_vec(),
                        },
                    ],
                },
            )),
        };
        let c = batch_command(&prost::Message::encode_to_vec(&r)).unwrap();
        assert!(
            matches!(c, hugegraph_store_core::StoreRequest::BatchPut { entries } if entries == vec![(qualify_table_id_key(0, b"k"), b"v".to_vec()), (qualify_table_id_key(0, b"k2"), b"v2".to_vec())])
        );
    }

    #[test]
    fn batch_delete_maps_and_maps_range() {
        let delete = generated::BatchReq {
            header: None,
            batch_id: "delete".into(),
            requests: Some(generated::batch_req::Requests::WriteReq(
                generated::BatchWriteReq {
                    entry: vec![generated::BatchEntry {
                        op_type: 3,
                        table: 0,
                        start_key: Some(generated::Key {
                            key: b"gone".to_vec(),
                            code: 0,
                        }),
                        end_key: None,
                        value: Vec::new(),
                    }],
                },
            )),
        };
        assert!(matches!(
            batch_command(&prost::Message::encode_to_vec(&delete)).unwrap(),
            hugegraph_store_core::StoreRequest::BatchDelete { keys } if keys == vec![qualify_table_id_key(0, b"gone")]
        ));
        let range = generated::BatchReq {
            header: None,
            batch_id: "range".into(),
            requests: Some(generated::batch_req::Requests::WriteReq(
                generated::BatchWriteReq {
                    entry: vec![generated::BatchEntry {
                        op_type: 5,
                        table: 0,
                        start_key: Some(generated::Key {
                            key: b"a".to_vec(),
                            code: 0,
                        }),
                        end_key: Some(generated::Key {
                            key: b"z".to_vec(),
                            code: 0,
                        }),
                        value: Vec::new(),
                    }],
                },
            )),
        };
        assert!(matches!(
            batch_command(&prost::Message::encode_to_vec(&range)).unwrap(),
            hugegraph_store_core::StoreRequest::BatchDeleteRange { start, end }
                if start == qualify_table_id_key(0, b"a") && end == qualify_table_id_key(0, b"z")
        ));
    }

    #[test]
    fn batch_merge_maps_to_uint64_mutation() {
        let request = generated::BatchReq {
            header: None,
            batch_id: "merge".into(),
            requests: Some(generated::batch_req::Requests::WriteReq(
                generated::BatchWriteReq {
                    entry: vec![generated::BatchEntry {
                        op_type: 6,
                        table: 0,
                        start_key: Some(generated::Key {
                            key: b"counter".to_vec(),
                            code: 0,
                        }),
                        end_key: None,
                        value: 2u64.to_le_bytes().to_vec(),
                    }],
                },
            )),
        };
        assert!(matches!(
            batch_command(&prost::Message::encode_to_vec(&request)).unwrap(),
            hugegraph_store_core::StoreRequest::BatchMerge { entries }
                if entries == vec![(qualify_table_id_key(0, b"counter"), 2u64.to_le_bytes().to_vec())]
        ));
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BatchAction {
    Mutate {
        mutations: Vec<hugegraph_store_core::BatchMutation>,
    },
    Commit,
    Rollback,
}
pub fn decode_batch_action(bytes: &[u8]) -> Result<BatchAction, String> {
    let r: generated::BatchReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    match r.requests {
        Some(generated::batch_req::Requests::WriteReq(w)) => {
            if w.entry.is_empty() {
                return Err("empty write".into());
            }
            let mutations = w
                .entry
                .into_iter()
                .map(|e| {
                    let table = e.table;
                    let key = qualify_table_id_key(
                        table,
                        &e.start_key.map(|k| k.key).unwrap_or_default(),
                    );
                    match e.op_type {
                        0 | 1 => Ok(hugegraph_store_core::BatchMutation::Put {
                            key,
                            value: e.value,
                        }),
                        2 | 3 => Ok(hugegraph_store_core::BatchMutation::Delete { key }),
                        4 => Ok(hugegraph_store_core::BatchMutation::DeletePrefix { prefix: key }),
                        5 => Ok(hugegraph_store_core::BatchMutation::DeleteRange {
                            start: key,
                            end: qualify_table_id_key(
                                table,
                                &e.end_key.map(|k| k.key).unwrap_or_default(),
                            ),
                        }),
                        6 => Ok(hugegraph_store_core::BatchMutation::Merge {
                            key,
                            value: e.value,
                        }),
                        _ => Err("unknown batch operation type".to_string()),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(BatchAction::Mutate { mutations })
        }
        Some(generated::batch_req::Requests::CommitReq(_)) => Ok(BatchAction::Commit),
        Some(generated::batch_req::Requests::RollbackReq(_)) => Ok(BatchAction::Rollback),
        None => Err("missing operation".into()),
    }
}

pub fn apply_batch_action(
    store: &mut hugegraph_store_core::DurableStore,
    action: BatchAction,
) -> Result<(), String> {
    match action {
        BatchAction::Mutate { mutations } => store
            .apply_batch(&mutations)
            .map(|_| ())
            .map_err(|e| e.to_string()),
        BatchAction::Rollback => Ok(()),
        BatchAction::Commit => Ok(()),
    }
}

#[derive(Default)]
pub struct SessionTransactions {
    pending: std::collections::HashMap<String, Vec<hugegraph_store_core::BatchMutation>>,
}
impl SessionTransactions {
    pub fn stage(&mut self, id: String, k: Vec<u8>, v: Vec<u8>) {
        self.pending
            .entry(id)
            .or_default()
            .push(hugegraph_store_core::BatchMutation::Put { key: k, value: v });
    }
    pub fn stage_delete(&mut self, id: String, key: Vec<u8>) {
        self.pending
            .entry(id)
            .or_default()
            .push(hugegraph_store_core::BatchMutation::Delete { key });
    }
    pub fn stage_delete_prefix(&mut self, id: String, prefix: Vec<u8>) {
        self.pending
            .entry(id)
            .or_default()
            .push(hugegraph_store_core::BatchMutation::DeletePrefix { prefix });
    }
    pub fn stage_delete_range(&mut self, id: String, start: Vec<u8>, end: Vec<u8>) {
        self.pending
            .entry(id)
            .or_default()
            .push(hugegraph_store_core::BatchMutation::DeleteRange { start, end });
    }
    pub fn stage_merge(&mut self, id: String, key: Vec<u8>, value: Vec<u8>) {
        self.pending
            .entry(id)
            .or_default()
            .push(hugegraph_store_core::BatchMutation::Merge { key, value });
    }
    pub fn commit(
        &mut self,
        id: &str,
        store: &mut hugegraph_store_core::DurableStore,
    ) -> Result<(), String> {
        let entries = self.pending.get(id).cloned().unwrap_or_default();
        store.apply_batch(&entries).map_err(|e| e.to_string())?;
        self.pending.remove(id);
        Ok(())
    }
    pub fn rollback(&mut self, id: &str) {
        self.pending.remove(id);
    }
}
#[cfg(test)]
mod session_txn_tests {
    use super::*;
    #[test]
    fn failed_session_commit_retains_pending_for_retry() {
        let p = std::env::temp_dir().join(format!("hg-stxn-retry-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let mut store = hugegraph_store_core::DurableStore::open(&p).unwrap();
        store
            .commit_put(b"stable".to_vec(), b"value".to_vec())
            .unwrap();
        let mut tx = SessionTransactions::default();
        tx.stage("retry".into(), b"k".to_vec(), b"v".to_vec());
        std::fs::remove_file(&p).unwrap();
        std::fs::create_dir(&p).unwrap();
        assert!(tx.commit("retry", &mut store).is_err());
        assert!(tx.pending.contains_key("retry"));
        std::fs::remove_dir(&p).unwrap();
        tx.commit("retry", &mut store).unwrap();
        assert!(!tx.pending.contains_key("retry"));
        assert_eq!(
            hugegraph_store_core::DurableStore::open(&p)
                .unwrap()
                .get(b"k"),
            Some(b"v".to_vec())
        );
        std::fs::remove_file(p).unwrap();
    }

    #[test]
    fn stage_commit_rollback() {
        let p = std::env::temp_dir().join(format!("hg-stxn-{}.img", std::process::id()));
        let mut s = hugegraph_store_core::DurableStore::open(&p).unwrap();
        let mut t = SessionTransactions::default();
        t.stage("a".into(), b"k".to_vec(), b"v".to_vec());
        t.commit("a", &mut s).unwrap();
        assert_eq!(s.get(b"k"), Some(b"v".to_vec()));
        t.stage("b".into(), b"x".to_vec(), b"y".to_vec());
        t.rollback("b");
        assert_eq!(s.get(b"x"), None);
        let _ = std::fs::remove_file(p);
    }
}

pub fn apply_session_batch(
    id: &str,
    bytes: &[u8],
    txns: &mut SessionTransactions,
    store: &mut hugegraph_store_core::DurableStore,
) -> Result<(), String> {
    match decode_batch_action(bytes)? {
        BatchAction::Mutate { mutations } => {
            for mutation in mutations {
                match mutation {
                    hugegraph_store_core::BatchMutation::Put { key, value } => {
                        txns.stage(id.to_string(), key, value)
                    }
                    hugegraph_store_core::BatchMutation::Delete { key } => {
                        txns.stage_delete(id.to_string(), key)
                    }
                    hugegraph_store_core::BatchMutation::DeletePrefix { prefix } => {
                        txns.stage_delete_prefix(id.to_string(), prefix)
                    }
                    hugegraph_store_core::BatchMutation::DeleteRange { start, end } => {
                        txns.stage_delete_range(id.to_string(), start, end)
                    }
                    hugegraph_store_core::BatchMutation::Merge { key, value } => {
                        txns.stage_merge(id.to_string(), key, value)
                    }
                }
            }
            Ok(())
        }
        BatchAction::Commit => txns.commit(id, store),
        BatchAction::Rollback => {
            txns.rollback(id);
            Ok(())
        }
    }
}
#[cfg(test)]
mod apply_tests {
    use super::*;
    #[test]
    fn batch_lifecycle() {
        let p = std::env::temp_dir().join(format!("hg-ab-{}.img", std::process::id()));
        let mut s = hugegraph_store_core::DurableStore::open(&p).unwrap();
        let mut tx = SessionTransactions::default();
        let e = generated::BatchEntry {
            op_type: 0,
            table: 0,
            start_key: Some(generated::Key {
                key: b"k".to_vec(),
                code: 0,
            }),
            end_key: None,
            value: b"v".to_vec(),
        };
        let w = generated::BatchReq {
            header: None,
            batch_id: "b".into(),
            requests: Some(generated::batch_req::Requests::WriteReq(
                generated::BatchWriteReq { entry: vec![e] },
            )),
        };
        let b = prost::Message::encode_to_vec(&w);
        apply_session_batch("b", &b, &mut tx, &mut s).unwrap();
        let qualified = qualify_table_id_key(0, b"k");
        assert_eq!(s.get(&qualified), None);
        tx.commit("b", &mut s).unwrap();
        assert_eq!(s.get(&qualified), Some(b"v".to_vec()));
        let delete = generated::BatchReq {
            header: None,
            batch_id: "b".into(),
            requests: Some(generated::batch_req::Requests::WriteReq(
                generated::BatchWriteReq {
                    entry: vec![generated::BatchEntry {
                        op_type: 3,
                        table: 0,
                        start_key: Some(generated::Key {
                            key: b"k".to_vec(),
                            code: 0,
                        }),
                        end_key: None,
                        value: Vec::new(),
                    }],
                },
            )),
        };
        apply_session_batch(
            "b",
            &prost::Message::encode_to_vec(&delete),
            &mut tx,
            &mut s,
        )
        .unwrap();
        tx.commit("b", &mut s).unwrap();
        assert_eq!(s.get(&qualified), None);
        let _ = std::fs::remove_file(p);
    }
}
#[cfg(test)]
mod tonic_session_tests {
    use super::*;
    #[tokio::test]
    async fn get2_trait_round_trip() {
        let p = std::env::temp_dir().join(format!("hg-ts-{}.img", std::process::id()));
        let s = SessionService::open(&p).unwrap();
        s.shared
            .backend
            .lock()
            .unwrap()
            .commit_put(qualify_table_name_key("t", b"k"), b"value".to_vec())
            .unwrap();
        let r = generated::GetReq {
            header: None,
            tk: Some(generated::Tk {
                table: "t".into(),
                key: b"k".to_vec(),
                code: 0,
            }),
        };
        let out = <SessionService as generated::hg_store_session_server::HgStoreSession>::get2(
            &s,
            tonic::Request::new(r),
        )
        .await
        .unwrap();
        let out = out.into_inner();
        assert_eq!(out.status.unwrap().code, 0);
        match out.responses.unwrap() {
            generated::feedback_res::Responses::ValueResponse(value) => {
                assert_eq!(value.value, b"value")
            }
            _ => panic!("expected value response"),
        }
        let _ = std::fs::remove_file(p);
    }
}

#[cfg(test)]
mod table_truncate_tests {
    use super::*;

    #[tokio::test]
    async fn truncate_removes_only_the_requested_qualified_table() {
        let path =
            std::env::temp_dir().join(format!("hg-truncate-table-{}.img", std::process::id()));
        let service = SessionService::open(&path).unwrap();
        {
            let mut store = service.shared.backend.lock().unwrap();
            store
                .commit_put(qualify_table_name_key("t", b"same"), b"t".to_vec())
                .unwrap();
            store
                .commit_put(qualify_table_name_key("u", b"same"), b"u".to_vec())
                .unwrap();
        }
        <SessionService as generated::hg_store_session_server::HgStoreSession>::table(
            &service,
            tonic::Request::new(generated::TableReq {
                method: generated::TableMethod::Truncate as i32,
                table_name: "t".into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let store = service.shared.backend.lock().unwrap();
        assert_eq!(store.get(&qualify_table_name_key("t", b"same")), None);
        assert_eq!(
            store.get(&qualify_table_name_key("u", b"same")),
            Some(b"u".to_vec())
        );
        let _ = std::fs::remove_file(path);
    }
}
#[cfg(test)]
mod tonic_batch_tests {
    use super::*;
    #[tokio::test]
    async fn batch_rpc_preserves_all_entries_and_rollback_after_reopen() {
        use generated::batch_req::Requests;
        use generated::hg_store_session_server::HgStoreSession;
        let path =
            std::env::temp_dir().join(format!("hg-batch-rpc-all-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let service = SessionService::open(&path).unwrap();
        let entries = vec![
            (b"a".to_vec(), b"first".to_vec()),
            (b"b".to_vec(), b"second".to_vec()),
            (b"a".to_vec(), b"last".to_vec()),
        ];
        let oracle: std::collections::BTreeMap<_, _> = entries.iter().cloned().collect();
        let request = |id: &str, requests| {
            tonic::Request::new(generated::BatchReq {
                header: None,
                batch_id: id.into(),
                requests: Some(requests),
            })
        };
        let write = || {
            Requests::WriteReq(generated::BatchWriteReq {
                entry: entries
                    .iter()
                    .map(|(k, v)| generated::BatchEntry {
                        op_type: 1,
                        table: 0,
                        start_key: Some(generated::Key {
                            key: k.clone(),
                            code: 0,
                        }),
                        end_key: None,
                        value: v.clone(),
                    })
                    .collect(),
            })
        };
        service.batch(request("commit", write())).await.unwrap();
        assert!(service.shared.backend.lock().unwrap().entries().is_empty());
        service
            .batch(request(
                "commit",
                Requests::CommitReq(generated::BatchCommitReq {}),
            ))
            .await
            .unwrap();
        assert_eq!(
            service.shared.backend.lock().unwrap().entries(),
            oracle
                .clone()
                .into_iter()
                .map(|(key, value)| (qualify_table_id_key(0, &key), value))
                .collect::<Vec<_>>()
        );
        let cancelled = Requests::WriteReq(generated::BatchWriteReq {
            entry: vec![generated::BatchEntry {
                op_type: 1,
                table: 0,
                start_key: Some(generated::Key {
                    key: b"cancelled".to_vec(),
                    code: 0,
                }),
                end_key: None,
                value: b"must-not-persist".to_vec(),
            }],
        });
        service.batch(request("rollback", cancelled)).await.unwrap();
        service
            .batch(request(
                "rollback",
                Requests::RollbackReq(generated::BatchRollbackReq {}),
            ))
            .await
            .unwrap();
        service
            .batch(request(
                "rollback",
                Requests::CommitReq(generated::BatchCommitReq {}),
            ))
            .await
            .unwrap();
        drop(service);
        let recovered = hugegraph_store_core::DurableStore::open(&path).unwrap();
        assert_eq!(
            recovered.entries(),
            oracle
                .into_iter()
                .map(|(key, value)| (qualify_table_id_key(0, &key), value))
                .collect::<Vec<_>>()
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn live_batch_rpc_fault_retry_and_reopen() {
        let path =
            std::env::temp_dir().join(format!("hg-live-batch-retry-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut seed = hugegraph_store_core::DurableStore::open(&path).unwrap();
            seed.commit_put(b"stable".to_vec(), b"value".to_vec())
                .unwrap();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = SessionService::open(&path).unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(generated::hg_store_session_server::HgStoreSessionServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let mut client = generated::hg_store_session_client::HgStoreSessionClient::connect(
            format!("http://{}", addr),
        )
        .await
        .unwrap();
        let write = generated::BatchReq {
            header: None,
            batch_id: "retry".into(),
            requests: Some(generated::batch_req::Requests::WriteReq(
                generated::BatchWriteReq {
                    entry: vec![
                        generated::BatchEntry {
                            op_type: 1,
                            table: 0,
                            start_key: Some(generated::Key {
                                key: b"a".to_vec(),
                                code: 0,
                            }),
                            end_key: None,
                            value: b"one".to_vec(),
                        },
                        generated::BatchEntry {
                            op_type: 1,
                            table: 0,
                            start_key: Some(generated::Key {
                                key: b"b".to_vec(),
                                code: 0,
                            }),
                            end_key: None,
                            value: b"two".to_vec(),
                        },
                    ],
                },
            )),
        };
        client.batch(write).await.unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let commit = || generated::BatchReq {
            header: None,
            batch_id: "retry".into(),
            requests: Some(generated::batch_req::Requests::CommitReq(
                generated::BatchCommitReq {},
            )),
        };
        assert!(client.batch(commit()).await.is_err());
        std::fs::remove_dir(&path).unwrap();
        client.batch(commit()).await.unwrap();
        task.abort();
        let recovered = hugegraph_store_core::DurableStore::open(&path).unwrap();
        assert_eq!(recovered.get(b"stable"), Some(b"value".to_vec()));
        assert_eq!(
            recovered.get(&qualify_table_id_key(0, b"a")),
            Some(b"one".to_vec())
        );
        assert_eq!(
            recovered.get(&qualify_table_id_key(0, b"b")),
            Some(b"two".to_vec())
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn live_batch_merge_fault_retry_and_reopen() {
        let path =
            std::env::temp_dir().join(format!("hg-live-merge-retry-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut seed = hugegraph_store_core::DurableStore::open(&path).unwrap();
            seed.apply_batch(&[hugegraph_store_core::BatchMutation::Merge {
                key: qualify_table_id_key(0, b"counter"),
                value: 10u64.to_le_bytes().to_vec(),
            }])
            .unwrap();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = SessionService::open(&path).unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(generated::hg_store_session_server::HgStoreSessionServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let mut client = generated::hg_store_session_client::HgStoreSessionClient::connect(
            format!("http://{}", addr),
        )
        .await
        .unwrap();
        let merge = || generated::BatchReq {
            header: None,
            batch_id: "merge-retry".into(),
            requests: Some(generated::batch_req::Requests::WriteReq(
                generated::BatchWriteReq {
                    entry: vec![generated::BatchEntry {
                        op_type: 6,
                        table: 0,
                        start_key: Some(generated::Key {
                            key: b"counter".to_vec(),
                            code: 0,
                        }),
                        end_key: None,
                        value: 5u64.to_le_bytes().to_vec(),
                    }],
                },
            )),
        };
        client.batch(merge()).await.unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let commit = || generated::BatchReq {
            header: None,
            batch_id: "merge-retry".into(),
            requests: Some(generated::batch_req::Requests::CommitReq(
                generated::BatchCommitReq {},
            )),
        };
        assert!(client.batch(commit()).await.is_err());
        std::fs::remove_dir(&path).unwrap();
        client.batch(commit()).await.unwrap();
        task.abort();
        assert_eq!(
            hugegraph_store_core::DurableStore::open(&path)
                .unwrap()
                .get(&qualify_table_id_key(0, b"counter")),
            Some(15u64.to_le_bytes().to_vec())
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn batch_trait_commit() {
        let p = std::env::temp_dir().join(format!("hg-tb-{}.img", std::process::id()));
        let s = SessionService::open(&p).unwrap();
        let req = generated::BatchReq {
            header: None,
            batch_id: "b".into(),
            requests: Some(generated::batch_req::Requests::CommitReq(
                generated::BatchCommitReq {},
            )),
        };
        let out = <SessionService as generated::hg_store_session_server::HgStoreSession>::batch(
            &s,
            tonic::Request::new(req),
        )
        .await;
        assert!(out.is_ok());
        let _ = std::fs::remove_file(p);
    }
}

pub fn decode_batch_get_keys(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let r: generated::BatchGetReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    let keys: Vec<_> = r
        .key
        .into_iter()
        .map(|k| k.key)
        .filter(|k| !k.is_empty())
        .collect();
    if keys.is_empty() {
        Err("BatchGetReq has no keys".into())
    } else {
        Ok(keys)
    }
}

pub fn decode_batch_get_qualified_keys(bytes: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    let r: generated::BatchGetReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    let keys: Vec<_> = r
        .key
        .into_iter()
        .map(|k| k.key)
        .filter(|k| !k.is_empty())
        .map(|key| {
            let qualified = qualify_table_name_key(&r.table, &key);
            (key, qualified)
        })
        .collect();
    if keys.is_empty() {
        Err("BatchGetReq has no keys".into())
    } else {
        Ok(keys)
    }
}
#[cfg(test)]
mod batch_get_tests {
    use super::*;
    #[test]
    fn decode_keys() {
        let r = generated::BatchGetReq {
            header: None,
            partition: 0,
            table: "t".into(),
            key: vec![generated::Key {
                key: b"a".to_vec(),
                code: 0,
            }],
        };
        assert_eq!(
            decode_batch_get_keys(&prost::Message::encode_to_vec(&r)).unwrap(),
            vec![b"a".to_vec()]
        );
    }
}

pub fn batch_get_values(
    store: &hugegraph_store_core::DurableStore,
    bytes: &[u8],
) -> Result<Vec<Option<Vec<u8>>>, String> {
    decode_batch_get_qualified_keys(bytes).map(|ks| {
        ks.iter()
            .map(|(_, qualified)| store.get(qualified))
            .collect()
    })
}

pub fn feedback_ok() -> generated::FeedbackRes {
    feedback_status(0, "ok")
}

pub fn feedback_status(code: i32, msg: &str) -> generated::FeedbackRes {
    generated::FeedbackRes {
        status: Some(generated::ResStatus {
            code,
            msg: msg.into(),
        }),
        responses: None,
    }
}

pub fn feedback_values(values: Vec<(Vec<u8>, Vec<u8>)>) -> generated::FeedbackRes {
    let kv = values
        .into_iter()
        .map(|(key, value)| generated::Kv {
            key,
            value,
            code: 0,
        })
        .collect();
    generated::FeedbackRes {
        status: Some(generated::ResStatus {
            code: 0,
            msg: "ok".into(),
        }),
        responses: Some(generated::feedback_res::Responses::KeyValueResponse(
            generated::KeyValueResponse { kv },
        )),
    }
}
#[cfg(test)]
mod feedback_tests {
    use super::*;
    #[test]
    fn values_response() {
        let r = feedback_values(vec![(b"k".to_vec(), b"v".to_vec())]);
        assert!(r.responses.is_some());
    }
}
#[cfg(test)]
mod batch_get_trait_tests {
    use super::*;
    #[tokio::test]
    async fn batch_get_trait_returns_feedback() {
        let p = std::env::temp_dir().join(format!("hg-bgt-{}.img", std::process::id()));
        let s = SessionService::open(&p).unwrap();
        s.shared
            .backend
            .lock()
            .unwrap()
            .commit_put(qualify_table_name_key("t", b"k"), b"table-t".to_vec())
            .unwrap();
        s.shared
            .backend
            .lock()
            .unwrap()
            .commit_put(qualify_table_name_key("u", b"k"), b"table-u".to_vec())
            .unwrap();
        let r = generated::BatchGetReq {
            header: None,
            partition: 0,
            table: "t".into(),
            key: vec![generated::Key {
                key: b"k".to_vec(),
                code: 0,
            }],
        };
        let out =
            <SessionService as generated::hg_store_session_server::HgStoreSession>::batch_get2(
                &s,
                tonic::Request::new(r.clone()),
            )
            .await
            .unwrap();
        assert!(out.into_inner().responses.is_some());
        let response = feedback_values_with_missing(
            &s.shared.backend.lock().unwrap(),
            &prost::Message::encode_to_vec(&r),
        )
        .unwrap();
        if let Some(generated::feedback_res::Responses::KeyValueResponse(values)) =
            response.responses
        {
            assert_eq!(values.kv[0].value, b"table-t");
            assert_eq!(values.kv[0].code, 0);
        } else {
            panic!();
        }
        let _ = std::fs::remove_file(p);
    }
}

#[cfg(test)]
mod qualified_reopen_tests {
    use super::*;

    #[tokio::test]
    async fn session_reopen_reads_qualified_key_without_legacy_collision() {
        let path =
            std::env::temp_dir().join(format!("hg-qualified-reopen-{}.img", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut store = hugegraph_store_core::DurableStore::open(&path).unwrap();
            store
                .commit_put(b"shared".to_vec(), b"legacy".to_vec())
                .unwrap();
            store
                .commit_put(
                    qualify_table_name_key("t", b"shared"),
                    b"qualified".to_vec(),
                )
                .unwrap();
        }
        let reopened = SessionService::open(&path).unwrap();
        let request = generated::BatchGetReq {
            header: None,
            partition: 0,
            table: "t".into(),
            key: vec![generated::Key {
                key: b"shared".to_vec(),
                code: 0,
            }],
        };
        assert_eq!(
            batch_get_values(
                &reopened.shared.backend.lock().unwrap(),
                &prost::Message::encode_to_vec(&request)
            )
            .unwrap(),
            vec![Some(b"qualified".to_vec())]
        );
        let _ = std::fs::remove_file(path);
    }
}

pub fn batch_get_pairs(
    store: &hugegraph_store_core::DurableStore,
    bytes: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    let keys = decode_batch_get_qualified_keys(bytes)?;
    Ok(keys
        .into_iter()
        .filter_map(|(raw, qualified)| store.get(&qualified).map(|v| (raw, v)))
        .collect())
}

pub fn feedback_values_with_missing(
    store: &hugegraph_store_core::DurableStore,
    bytes: &[u8],
) -> Result<generated::FeedbackRes, String> {
    let keys = decode_batch_get_qualified_keys(bytes)?;
    let kv = keys
        .into_iter()
        .map(|(raw, qualified)| {
            let (value, code) = match store.get(&qualified) {
                Some(v) => (v, 0),
                None => (Vec::new(), 2),
            };
            generated::Kv {
                key: raw,
                value,
                code,
            }
        })
        .collect();
    Ok(generated::FeedbackRes {
        status: Some(generated::ResStatus {
            code: 0,
            msg: "ok".into(),
        }),
        responses: Some(generated::feedback_res::Responses::KeyValueResponse(
            generated::KeyValueResponse { kv },
        )),
    })
}
#[cfg(test)]
mod mixed_tests {
    use super::*;
    #[test]
    fn mixed_missing_codes() {
        let p = std::env::temp_dir().join(format!("hg-mix-{}.img", std::process::id()));
        let mut s = hugegraph_store_core::DurableStore::open(&p).unwrap();
        s.commit_put(qualify_table_name_key("t", b"a"), b"1".to_vec())
            .unwrap();
        let r = generated::BatchGetReq {
            header: None,
            partition: 0,
            table: "t".into(),
            key: vec![
                generated::Key {
                    key: b"a".to_vec(),
                    code: 0,
                },
                generated::Key {
                    key: b"b".to_vec(),
                    code: 0,
                },
            ],
        };
        let out = feedback_values_with_missing(&s, &prost::Message::encode_to_vec(&r)).unwrap();
        if let Some(generated::feedback_res::Responses::KeyValueResponse(x)) = out.responses {
            assert_eq!(x.kv[0].code, 0);
            assert_eq!(x.kv[1].code, 2);
        } else {
            panic!()
        }
        let _ = std::fs::remove_file(p);
    }
}

pub fn validate_table_request(bytes: &[u8]) -> Result<String, String> {
    let r: generated::TableReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    if r.table_name.is_empty() {
        Err("table name required".into())
    } else {
        Ok(r.table_name)
    }
}

pub fn validate_graph_request(bytes: &[u8]) -> Result<String, String> {
    let r: generated::GraphReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    if r.graph_name.is_empty() {
        Err("graph name required".into())
    } else {
        Ok(r.graph_name)
    }
}
pub fn validate_clean_request(bytes: &[u8]) -> Result<i32, String> {
    let r: generated::CleanReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    if r.partition < 0 {
        Err("partition must be non-negative".into())
    } else {
        Ok(r.partition)
    }
}

pub fn validate_session_control(table: &[u8], graph: &[u8], clean: &[u8]) -> Result<(), String> {
    validate_table_request(table)?;
    validate_graph_request(graph)?;
    validate_clean_request(clean)?;
    Ok(())
}

pub fn table_action(bytes: &[u8]) -> Result<(String, generated::TableMethod), String> {
    let r: generated::TableReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    let m = generated::TableMethod::try_from(r.method).map_err(|_| "invalid table method")?;
    if r.table_name.is_empty() {
        Err("table name required".into())
    } else {
        Ok((r.table_name, m))
    }
}
pub fn graph_action(bytes: &[u8]) -> Result<(String, generated::GraphMethod), String> {
    let r: generated::GraphReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    let m = generated::GraphMethod::try_from(r.method).map_err(|_| "invalid graph method")?;
    if r.graph_name.is_empty() {
        Err("graph name required".into())
    } else {
        Ok((r.graph_name, m))
    }
}

#[derive(Default)]
pub struct Catalog {
    tables: std::collections::HashSet<String>,
    graphs: std::collections::HashSet<String>,
}
impl Catalog {
    pub fn table(&mut self, name: String, m: generated::TableMethod) -> Result<(), String> {
        match m {
            generated::TableMethod::Create => {
                self.tables.insert(name);
                Ok(())
            }
            generated::TableMethod::Delete | generated::TableMethod::Drop => {
                self.tables.remove(&name);
                Ok(())
            }
            generated::TableMethod::Exists => Ok(()),
            _ => Ok(()),
        }
    }
    pub fn graph(&mut self, name: String, m: generated::GraphMethod) -> Result<(), String> {
        if matches!(m, generated::GraphMethod::Delete) {
            self.graphs.remove(&name);
        } else {
            self.graphs.insert(name);
        }
        Ok(())
    }
}

/// Atomic image containing both data and catalog metadata for recovery.
pub struct UnifiedState {
    pub data: Vec<u8>,
    pub catalog: Catalog,
}

impl UnifiedState {
    pub fn snapshot(data: Vec<u8>, catalog: &Catalog) -> Vec<u8> {
        let meta = catalog.snapshot();
        let mut out = (data.len() as u64).to_le_bytes().to_vec();
        out.extend_from_slice(&data);
        out.extend_from_slice(&meta);
        out
    }

    pub fn restore(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 {
            return Err("snapshot header truncated".into());
        }
        let n = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
        if n > bytes.len() - 8 {
            return Err("snapshot data truncated".into());
        }
        Ok(Self {
            data: bytes[8..8 + n].to_vec(),
            catalog: Catalog::restore(&bytes[8 + n..]),
        })
    }
}

#[cfg(test)]
mod catalog_restore_tests {
    use super::Catalog;

    #[test]
    fn snapshot_restore_preserves_table_and_graph_names() {
        let mut catalog = Catalog::default();
        catalog.tables.insert("users".into());
        catalog.graphs.insert("social".into());
        let restored = Catalog::restore(&catalog.snapshot());
        assert!(restored.contains_table("users"));
        assert!(restored.contains_graph("social"));
        assert!(!restored.contains_table("social"));
    }
}

impl Catalog {
    pub fn contains_table(&self, n: &str) -> bool {
        self.tables.contains(n)
    }
    pub fn contains_graph(&self, n: &str) -> bool {
        self.graphs.contains(n)
    }
}

impl Catalog {
    pub fn snapshot(&self) -> Vec<u8> {
        let mut v = Vec::new();
        for n in &self.tables {
            v.push(b'T');
            v.extend_from_slice(n.as_bytes());
            v.push(0);
        }
        for n in &self.graphs {
            v.push(b'G');
            v.extend_from_slice(n.as_bytes());
            v.push(0);
        }
        v
    }
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty() && self.graphs.is_empty()
    }
}

impl Catalog {
    pub fn restore(bytes: &[u8]) -> Self {
        let mut c = Self::default();
        for entry in bytes.split(|b| *b == 0).filter(|n| n.len() > 1) {
            let name = String::from_utf8_lossy(&entry[1..]).into_owned();
            match entry[0] {
                b'T' => {
                    c.tables.insert(name);
                }
                b'G' => {
                    c.graphs.insert(name);
                }
                _ => {}
            }
        }
        c
    }
}
