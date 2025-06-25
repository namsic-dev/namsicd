use tokio::sync::mpsc;
use tonic::{Request, Response, Status, transport};

use super::{
    Event,
    pb::{RaftMessage, Void, raft_client, raft_server},
};

pub struct GrpcServer {
    event_tx: mpsc::Sender<Event>,
}

impl GrpcServer {
    pub fn service(event_tx: mpsc::Sender<Event>) -> raft_server::RaftServer<Self> {
        raft_server::RaftServer::new(Self { event_tx })
    }
}

#[tonic::async_trait]
impl raft_server::Raft for GrpcServer {
    async fn dispatch(&self, request: Request<RaftMessage>) -> Result<Response<Void>, Status> {
        self.event_tx
            .send(Event::Message(request.into_inner()))
            .await
            .unwrap();
        Ok(Response::new(Void {}))
    }
}

pub async fn connect(addr: String, mut rx: mpsc::Receiver<RaftMessage>) {
    let Ok(channel) = transport::Channel::from_shared(format!("http://{addr}")) else {
        todo!();
    };
    let mut client = raft_client::RaftClient::new(channel.connect_lazy());
    while let Some(message) = rx.recv().await {
        if let Err(e) = client.dispatch(message).await {
            tracing::debug!(addr, "{e}");
        }
    }
    tracing::warn!(addr, "close raft grpc client")
}
