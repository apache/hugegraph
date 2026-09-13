//! Rust store node composition boundary.
use hugegraph_store_transport::{
    GraphService, HealthyService, QueryServiceImpl, SessionService, StateService,
};

pub struct StoreNode {
    pub graph: GraphService,
    pub session: SessionService,
}

impl StoreNode {
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        Ok(Self {
            graph: GraphService::open(path.clone())?,
            session: SessionService::open(path)?,
        })
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
