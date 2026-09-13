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
        Ok(Self {
            graph: GraphService::open(path.clone())?,
            session: SessionService::open(path)?,
            state: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(persisted)),
            state_path,
        })
    }

    pub fn set_state(&self, state: NodeState) -> std::io::Result<()> {
        let tmp = self.state_path.with_extension("node-state.tmp");
        std::fs::write(&tmp, [state as u8])?;
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

    pub fn health_service(&self) -> HealthyService {
        HealthyService
    }
    pub fn query_service(&self) -> QueryServiceImpl {
        QueryServiceImpl
    }
    pub fn state_service(&self) -> StateService {
        StateService
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
}
