//! Transport adapter boundary for hg-store-grpc.
use hugegraph_store_core::{StoreRequest, StoreResponse, StoreService};
pub struct RpcServer { service: StoreService }
impl RpcServer {
    pub fn new(service: StoreService) -> Self { Self { service } }
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> std::io::Result<Option<Vec<u8>>> {
        match self.service.call(StoreRequest::Put { key, value })? { StoreResponse::Value(v) => Ok(v), StoreResponse::Ack => unreachable!() }
    }
    pub fn get(&mut self, key: Vec<u8>) -> std::io::Result<Option<Vec<u8>>> {
        match self.service.call(StoreRequest::Get { key })? { StoreResponse::Value(v) => Ok(v), StoreResponse::Ack => unreachable!() }
    }
    pub fn rollback(&mut self, key: Vec<u8>) -> std::io::Result<()> {
        self.service.call(StoreRequest::Rollback { key }).map(|_| ())
    }
}
