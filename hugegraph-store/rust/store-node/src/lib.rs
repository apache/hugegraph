//! Rust store node composition boundary.
use hugegraph_store_transport::{
    GraphService, HealthyService, QueryServiceImpl, SessionService, StateService,
};

pub struct StoreNode {
    pub graph: GraphService,
    pub session: SessionService,
    state: std::sync::Arc<std::sync::atomic::AtomicU8>,
    state_path: std::path::PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeState {
    Starting = 0,
    Online = 1,
    Stopped = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeCommand {
    Start,
    Stop,
    CleanData,
}

impl StoreNode {
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let state_path = path.with_extension("node-state");
        let persisted = match std::fs::read(&state_path) {
            Ok(v) if v.len() == 1 && v[0] <= NodeState::Stopped as u8 => v[0],
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid node state",
                ))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => NodeState::Starting as u8,
            Err(e) => return Err(e),
        };
        let session = SessionService::open(path)?;
        let graph = GraphService::from_backend(session.shared.backend());
        Ok(Self {
            graph,
            session,
            state: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(persisted)),
            state_path,
        })
    }

    pub fn set_state(&self, state: NodeState) -> std::io::Result<()> {
        let tmp = self.state_path.with_extension("node-state.tmp");
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&[state as u8])?;
        file.sync_all()?;
        std::fs::rename(tmp, &self.state_path)?;
        self.state
            .store(state as u8, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub fn state(&self) -> NodeState {
        match self.state.load(std::sync::atomic::Ordering::Acquire) {
            1 => NodeState::Online,
            2 => NodeState::Stopped,
            _ => NodeState::Starting,
        }
    }

    pub fn can_accept_requests(&self) -> bool {
        self.state() == NodeState::Online
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        if !self.can_accept_requests() {
            return Err("node is not online".into());
        }
        let mut store = self
            .session
            .shared
            .backend
            .lock()
            .map_err(|_| "backend lock poisoned".to_string())?;
        store
            .commit_put(key, value)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let store = self
            .session
            .shared
            .backend
            .lock()
            .map_err(|_| "backend lock poisoned".to_string())?;
        Ok(store.get(key))
    }

    pub fn execute(&self, command: NodeCommand) -> Result<NodeState, &'static str> {
        if command == NodeCommand::CleanData {
            return Err("use clean_data for data command");
        }
        let next = match (self.state(), command) {
            (NodeState::Starting, NodeCommand::Start)
            | (NodeState::Stopped, NodeCommand::Start) => NodeState::Online,
            (NodeState::Online, NodeCommand::Stop) => NodeState::Stopped,
            _ => return Err("invalid node command for current state"),
        };
        self.set_state(next)
            .map_err(|_| "failed to persist node state")?;
        Ok(next)
    }

    pub fn start(&self) -> Result<(), &'static str> {
        self.execute(NodeCommand::Start).map(|_| ())
    }

    pub fn stop(&self) -> Result<(), &'static str> {
        self.execute(NodeCommand::Stop).map(|_| ())
    }

    pub fn clean_data(&self) -> Result<(), String> {
        if !self.can_accept_requests() {
            return Err("node is not online".into());
        }
        let mut store = self
            .session
            .shared
            .backend
            .lock()
            .map_err(|_| "backend lock poisoned".to_string())?;
        store.clear().map(|_| ()).map_err(|e| e.to_string())
    }

    pub fn health_service(&self) -> HealthyService {
        HealthyService
    }
    pub fn query_service(&self) -> QueryServiceImpl {
        QueryServiceImpl
    }
    pub fn state_service(&self) -> StateService {
        StateService
    }

    pub fn service_names() -> [&'static str; 5] {
        [
            "Healthy",
            "QueryService",
            "HgStoreState",
            "GraphStore",
            "HgStoreSession",
        ]
    }

    pub async fn serve(self, addr: std::net::SocketAddr) -> Result<(), tonic::transport::Error> {
        let healthy = self.health_service();
        let query = self.query_service();
        let state = self.state_service();
        tonic::transport::Server::builder()
            .add_service(hugegraph_store_transport::generated::healthy_server::HealthyServer::new(healthy))
            .add_service(hugegraph_store_transport::generated::query_service_server::QueryServiceServer::new(query))
            .add_service(hugegraph_store_transport::generated::hg_store_state_server::HgStoreStateServer::new(state))
            .add_service(hugegraph_store_transport::generated::graph_store_server::GraphStoreServer::new(self.graph))
            .add_service(hugegraph_store_transport::generated::hg_store_session_server::HgStoreSessionServer::new(self.session))
            .serve(addr)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn node_state_lifecycle_is_explicit() {
        let path = std::env::temp_dir().join(format!("hg-node-state-{}.img", std::process::id()));
        let node = StoreNode::open(&path).unwrap();
        assert_eq!(node.state(), NodeState::Starting);
        assert!(node.put(b"k".to_vec(), b"v".to_vec()).is_err());
        assert!(!node.can_accept_requests());
        std::fs::create_dir(path.with_extension("node-state.tmp")).unwrap();
        assert!(node.execute(NodeCommand::Start).is_err());
        assert_eq!(node.state(), NodeState::Starting);
        let _ = std::fs::remove_dir(path.with_extension("node-state.tmp"));
        assert_eq!(node.execute(NodeCommand::Start).unwrap(), NodeState::Online);
        assert_eq!(node.state(), NodeState::Online);
        assert!(node.can_accept_requests());
        node.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(node.execute(NodeCommand::Stop).unwrap(), NodeState::Stopped);
        assert_eq!(node.state(), NodeState::Stopped);
        drop(node);
        let reopened = StoreNode::open(&path).unwrap();
        assert_eq!(reopened.state(), NodeState::Stopped);
        assert_eq!(reopened.get(b"k").unwrap(), Some(b"v".to_vec()));
        std::fs::write(path.with_extension("node-state"), [9]).unwrap();
        assert!(StoreNode::open(&path).is_err());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("node-state"));
    }

    #[test]
    fn concurrent_puts_preserve_data_and_online_state() {
        let path =
            std::env::temp_dir().join(format!("hg-node-concurrent-{}.img", std::process::id()));
        let node = std::sync::Arc::new(StoreNode::open(&path).unwrap());
        node.execute(NodeCommand::Start).unwrap();
        let mut workers = Vec::new();
        for i in 0..8u8 {
            let n = node.clone();
            workers.push(std::thread::spawn(move || {
                n.put(vec![i], vec![i + 1]).unwrap();
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(node.can_accept_requests());
        for i in 0..8u8 {
            assert_eq!(node.get(&[i]).unwrap(), Some(vec![i + 1]));
        }
        assert_eq!(
            node.graph
                .scan_range(&[0], &[8], false)
                .unwrap()
                .vertex
                .len(),
            8
        );
        node.execute(NodeCommand::Stop).unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("node-state"));
    }

    #[test]
    fn unified_server_lists_all_production_services() {
        assert_eq!(StoreNode::service_names().len(), 5);
        assert!(StoreNode::service_names().contains(&"GraphStore"));
        assert!(StoreNode::service_names().contains(&"HgStoreSession"));
    }

    #[test]
    fn node_put_get_matches_independent_oracle() {
        let path = std::env::temp_dir().join(format!("hg-node-oracle-{}.img", std::process::id()));
        let node = StoreNode::open(&path).unwrap();
        node.start().unwrap();
        let mut oracle = std::collections::BTreeMap::new();
        for i in 0..16u8 {
            node.put(vec![i], vec![i ^ 0x5a]).unwrap();
            oracle.insert(vec![i], vec![i ^ 0x5a]);
        }
        for (k, v) in oracle {
            assert_eq!(node.get(&k).unwrap(), Some(v));
        }
        let frame = node.graph.scan_range(&[0], &[16], false).unwrap();
        assert_eq!(frame.vertex.len(), 16);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("node-state"));
    }

    #[tokio::test]
    async fn live_unified_server_answers_health() {
        let path = std::env::temp_dir().join(format!("hg-node-live-{}.img", std::process::id()));
        {
            let mut seed = hugegraph_store_core::DurableStore::open(&path).unwrap();
            seed.commit_put(b"live-key".to_vec(), b"live-value".to_vec())
                .unwrap();
        }
        let node = StoreNode::open(&path).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let task = tokio::spawn(node.serve(addr));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut client =
            hugegraph_store_transport::generated::healthy_client::HealthyClient::connect(format!(
                "http://{}",
                addr
            ))
            .await
            .unwrap();
        assert_eq!(
            client
                .is_ok(tonic::Request::new(()))
                .await
                .unwrap()
                .into_inner()
                .message,
            "ok"
        );
        let mut query = hugegraph_store_transport::generated::query_service_client::QueryServiceClient::connect(format!("http://{}", addr)).await.unwrap();
        assert!(
            query
                .query0(tonic::Request::new(
                    hugegraph_store_transport::generated::QueryRequest {
                        query_id: "live".into(),
                        ..Default::default()
                    }
                ))
                .await
                .unwrap()
                .into_inner()
                .is_ok
        );
        let mut state = hugegraph_store_transport::generated::hg_store_state_client::HgStoreStateClient::connect(format!("http://{}", addr)).await.unwrap();
        assert_eq!(
            state
                .get_scan_state(tonic::Request::new(
                    hugegraph_store_transport::generated::SubStateReq::default()
                ))
                .await
                .unwrap()
                .into_inner(),
            hugegraph_store_transport::generated::ScanState::default()
        );
        let mut graph =
            hugegraph_store_transport::generated::graph_store_client::GraphStoreClient::connect(
                format!("http://{}", addr),
            )
            .await
            .unwrap();
        let request = hugegraph_store_transport::generated::ScanPartitionRequest { header: None, request: Some(hugegraph_store_transport::generated::scan_partition_request::RequestOneOf::ScanRequest(hugegraph_store_transport::generated::scan_partition_request::Request { position: vec![], end_code: b'z' as u32, ..Default::default() })) };
        let mut frames = graph
            .scan_partition(tonic::Request::new(tokio_stream::iter([request])))
            .await
            .unwrap()
            .into_inner();
        let frame = frames.message().await.unwrap().unwrap();
        assert_eq!(frame.seq_no, 0);
        assert_eq!(frame.vertex.len(), 1);
        assert_eq!(
            frame.vertex[0].id.as_ref().unwrap().value_bytes.as_deref(),
            Some(&b"live-key"[..])
        );
        assert_eq!(
            frame.vertex[0].properties[0]
                .value
                .as_ref()
                .unwrap()
                .value_bytes
                .as_deref(),
            Some(&b"live-value"[..])
        );
        let mut session = hugegraph_store_transport::generated::hg_store_session_client::HgStoreSessionClient::connect(format!("http://{}", addr)).await.unwrap();
        let feedback = session
            .get2(tonic::Request::new(
                hugegraph_store_transport::generated::GetReq {
                    tk: Some(hugegraph_store_transport::generated::Tk {
                        key: b"missing".to_vec(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(feedback.status.is_some() || feedback.responses.is_some());
        task.abort();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("node-state"));
    }
}
