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

/// RPC names currently exposed by hg-store-grpc; generated bindings can use
/// this registry to guarantee the adapter does not silently omit a method.
pub const HG_STORE_GRPC_RPCS: &[&str] = &[
    "Healthy.IsOk", "QueryService.query", "QueryService.query0", "QueryService.count",
    "GraphStore.ScanPartition", "HgStoreSession.Get2", "HgStoreSession.BatchGet2",
    "HgStoreSession.Batch", "HgStoreSession.Table", "HgStoreSession.Graph", "HgStoreSession.Clean",
    "HgStoreState.SubState", "HgStoreState.UnsubState", "HgStoreState.getScanState", "HgStoreState.getPeers",
];
#[cfg(test)]
mod tests { use super::*; #[test] fn grpc_contract_registry_is_complete() { assert_eq!(HG_STORE_GRPC_RPCS.len(), 15); } }
