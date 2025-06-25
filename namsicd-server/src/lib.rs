mod raft;
mod storage;

use std::collections::HashMap;

pub use raft::Config as RaftConfig;
use tokio::sync::oneshot;

#[derive(Default)]
pub struct Config {
    raft_config: RaftConfig,
}

pub fn init(
    member: HashMap<u64, String>,
    id: u64,
    config: Config,
) -> tonic::transport::server::Router {
    let (event_tx, operation_rx) = raft::init(member, id, config.raft_config);

    tokio::spawn(storage::process(operation_rx));

    tonic::transport::Server::builder()
        .add_service(raft::GrpcServer::service(event_tx.clone()))
        .add_service(storage::GrpcServer::service(event_tx.clone()))
}

struct Operation {
    data: Vec<u8>,
    result_tx: oneshot::Sender<Vec<u8>>,
}

impl Operation {
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn result(self, data: Vec<u8>) {
        if self.result_tx.send(data).is_err() {
            todo!()
        }
    }
}
