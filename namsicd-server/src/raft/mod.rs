mod config;
mod grpc;
mod log;
mod pb {
    tonic::include_proto!("namsicdraftpb");
}
mod state;

use std::collections::HashMap;

pub use config::Config;
pub use grpc::GrpcServer;
use pb::RaftMessage;
use state::StateMachine;
use tokio::sync::{mpsc, oneshot};

use crate::Operation;

const CHAN_BUF_TODO: usize = 100;

pub enum RaftError {
    NotLeader,
}

pub enum Event {
    Message(RaftMessage),
    Apply((Vec<u8>, oneshot::Sender<Result<Vec<u8>, RaftError>>)),
}

pub fn init(
    member: HashMap<u64, String>,
    id: u64,
    config: Config,
) -> (mpsc::Sender<Event>, mpsc::Receiver<Operation>) {
    let (event_tx, event_rx) = mpsc::channel(CHAN_BUF_TODO);
    let (state_machine, operation_rx) =
        StateMachine::new(id, &member, config.election_timeout_range);
    tokio::spawn(state_machine.serve(event_rx, config.tick_duration));
    (event_tx, operation_rx)
}
