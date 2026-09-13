//! Rust store node composition boundary.
use hugegraph_store_transport::{
    GraphService, HealthyService, QueryServiceImpl, SessionService, StateService,
};

pub struct StoreNode {
    pub graph: GraphService,
    pub session: SessionService,
    state: std::sync::Arc<std::sync::atomic::AtomicU8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeState {
    Starting = 0,
    Online = 1,
    Stopped = 2,
}

impl StoreNode {
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        Ok(Self {
            graph: GraphService::open(path.clone())?,
            session: SessionService::open(path)?,
            state: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(NodeState::Starting as u8)),
        })
    }

    pub fn set_state(&self, state: NodeState) {
        self.state
            .store(state as u8, std::sync::atomic::Ordering::Release);
    }

    pub fn state(&self) -> NodeState {
        match self.state.load(std::sync::atomic::Ordering::Acquire) {
            1 => NodeState::Online,
            2 => NodeState::Stopped,
            _ => NodeState::Starting,
        }
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
        node.set_state(NodeState::Online);
        assert_eq!(node.state(), NodeState::Online);
        node.set_state(NodeState::Stopped);
        assert_eq!(node.state(), NodeState::Stopped);
        let _ = std::fs::remove_file(path);
    }
}
