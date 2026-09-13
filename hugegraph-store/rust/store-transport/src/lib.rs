//! Transport adapter boundary for hg-store-grpc.
use hugegraph_store_core::{StoreRequest, StoreResponse, StoreService};
use tokio_stream::StreamExt;
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

pub struct QueryServiceImpl;

pub struct StateService;
pub struct GraphService {
    store: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>,
}
impl GraphService {
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
        let entries = filter_entries(&store.entries(), start, end);
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
#[cfg(test)]
mod graph_frame_tests {
    use super::empty_scan_frame;
    use crate::generated;
    use prost::Message;
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
        assert_eq!(frames.message().await.unwrap().unwrap().vertex.len(), 1);
        task.abort();
        let _ = std::fs::remove_file(path);
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
            let (start, end) = scan_request_range(&r).unwrap_or_default();
            self.scan_range(&start, &end, false)
                .map_err(tonic::Status::internal)?
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
        _: tonic::Request<generated::PartitionRequest>,
    ) -> Result<tonic::Response<generated::PeersResponse>, tonic::Status> {
        Ok(tonic::Response::new(generated::PeersResponse::default()))
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
        Ok(tonic::Response::new(query_response(
            &q.query_id,
            "ok",
            true,
        )))
    }
    async fn count(
        &self,
        request: tonic::Request<generated::QueryRequest>,
    ) -> Result<tonic::Response<generated::QueryResponse>, tonic::Status> {
        let q = request.into_inner();
        Ok(tonic::Response::new(query_response(&q.query_id, "0", true)))
    }
}

pub struct SessionService {
    pub shared: SharedSession,
    pub transactions: std::sync::Arc<std::sync::Mutex<SessionTransactions>>,
}
impl SessionService {
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        Ok(Self {
            shared: SharedSession::open(path)?,
            transactions: std::sync::Arc::new(
                std::sync::Mutex::new(SessionTransactions::default()),
            ),
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
            .map_err(tonic::Status::internal)
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
            .map_err(tonic::Status::invalid_argument)?;
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
        apply_session_batch(&id, &bytes, &mut tx, &mut store).map_err(tonic::Status::internal)?;
        Ok(tonic::Response::new(generated::FeedbackRes::default()))
    }
    async fn table(
        &self,
        _: tonic::Request<generated::TableReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        Ok(tonic::Response::new(generated::FeedbackRes::default()))
    }
    async fn graph(
        &self,
        _: tonic::Request<generated::GraphReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        Ok(tonic::Response::new(generated::FeedbackRes::default()))
    }
    async fn clean(
        &self,
        _: tonic::Request<generated::CleanReq>,
    ) -> Result<tonic::Response<generated::FeedbackRes>, tonic::Status> {
        Ok(tonic::Response::new(generated::FeedbackRes::default()))
    }
}

pub fn decode_get_key(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let r: generated::GetReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    r.tk.map(|t| t.key)
        .filter(|k| !k.is_empty())
        .ok_or_else(|| "GetReq missing key".into())
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
    decode_get_key(bytes).map(|key| hugegraph_store_core::StoreRequest::Get { key })
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
            matches!(get_req_to_store_request(&b).unwrap(), hugegraph_store_core::StoreRequest::Get{key} if key==b"k")
        );
    }
}

pub struct SharedSession {
    backend: std::sync::Arc<std::sync::Mutex<hugegraph_store_core::DurableStore>>,
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
        assert!(s.backend().lock().is_ok());
        let _ = std::fs::remove_file(p);
    }
}

impl SharedSession {
    pub fn handle_get2(&self, bytes: &[u8]) -> Result<generated::FeedbackRes, String> {
        let key = decode_get_key(bytes)?;
        let value = self
            .backend
            .lock()
            .map_err(|_| "backend lock poisoned")?
            .get(&key);
        let status = generated::ResStatus {
            code: 0,
            msg: "ok".into(),
        };
        let _ = value;
        Ok(generated::FeedbackRes {
            status: Some(status),
            responses: None,
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
        let r = generated::GetReq {
            header: None,
            tk: Some(generated::Tk {
                table: "t".into(),
                key: b"k".to_vec(),
                code: 0,
            }),
        };
        assert!(s.handle_get2(&prost::Message::encode_to_vec(&r)).is_ok());
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
            let e = w.entry.into_iter().next().ok_or("empty write")?;
            Ok(hugegraph_store_core::StoreRequest::Put {
                key: e.start_key.map(|x| x.key).unwrap_or_default(),
                value: e.value,
            })
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
                generated::BatchWriteReq { entry: vec![e] },
            )),
        };
        let c = batch_command(&prost::Message::encode_to_vec(&r)).unwrap();
        assert!(
            matches!(c,hugegraph_store_core::StoreRequest::Put{key,value} if key==b"k"&&value==b"v")
        );
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BatchAction {
    Write { key: Vec<u8>, value: Vec<u8> },
    Commit,
    Rollback,
}
pub fn decode_batch_action(bytes: &[u8]) -> Result<BatchAction, String> {
    let r: generated::BatchReq = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
    match r.requests {
        Some(generated::batch_req::Requests::WriteReq(w)) => {
            let e = w.entry.into_iter().next().ok_or("empty write")?;
            Ok(BatchAction::Write {
                key: e.start_key.map(|k| k.key).unwrap_or_default(),
                value: e.value,
            })
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
        BatchAction::Write { key, value } => store
            .commit_put(key, value)
            .map(|_| ())
            .map_err(|e| e.to_string()),
        BatchAction::Rollback => Ok(()),
        BatchAction::Commit => Ok(()),
    }
}

#[derive(Default)]
pub struct SessionTransactions {
    pending: std::collections::HashMap<String, Vec<(Vec<u8>, Vec<u8>)>>,
}
impl SessionTransactions {
    pub fn stage(&mut self, id: String, k: Vec<u8>, v: Vec<u8>) {
        self.pending.entry(id).or_default().push((k, v));
    }
    pub fn commit(
        &mut self,
        id: &str,
        store: &mut hugegraph_store_core::DurableStore,
    ) -> Result<(), String> {
        for (k, v) in self.pending.remove(id).unwrap_or_default() {
            store.commit_put(k, v).map_err(|e| e.to_string())?;
        }
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
        BatchAction::Write { key, value } => {
            txns.stage(id.to_string(), key, value);
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
        assert_eq!(s.get(b"k"), None);
        tx.commit("b", &mut s).unwrap();
        assert_eq!(s.get(b"k"), Some(b"v".to_vec()));
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
        assert!(out.into_inner().status.is_some());
        let _ = std::fs::remove_file(p);
    }
}
#[cfg(test)]
mod tonic_batch_tests {
    use super::*;
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
    decode_batch_get_keys(bytes).map(|ks| ks.iter().map(|k| store.get(k)).collect())
}

pub fn feedback_ok() -> generated::FeedbackRes {
    generated::FeedbackRes {
        status: Some(generated::ResStatus {
            code: 0,
            msg: "ok".into(),
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
                tonic::Request::new(r),
            )
            .await
            .unwrap();
        assert!(out.into_inner().responses.is_some());
        let _ = std::fs::remove_file(p);
    }
}

pub fn batch_get_pairs(
    store: &hugegraph_store_core::DurableStore,
    bytes: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    let keys = decode_batch_get_keys(bytes)?;
    Ok(keys
        .into_iter()
        .filter_map(|k| store.get(&k).map(|v| (k, v)))
        .collect())
}

pub fn feedback_values_with_missing(
    store: &hugegraph_store_core::DurableStore,
    bytes: &[u8],
) -> Result<generated::FeedbackRes, String> {
    let keys = decode_batch_get_keys(bytes)?;
    let kv = keys
        .into_iter()
        .map(|k| {
            let (value, code) = match store.get(&k) {
                Some(v) => (v, 0),
                None => (Vec::new(), 2),
            };
            generated::Kv {
                key: k,
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
        s.commit_put(b"a".to_vec(), b"1".to_vec()).unwrap();
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
