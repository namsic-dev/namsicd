use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use crate::raft;

use super::{
    UpdateRequest,
    pb::{DeleteRequest, DeleteResponse, PutRequest, PutResponse, kv_server},
};

pub struct GrpcServer {
    event_tx: mpsc::Sender<raft::Event>,
}

impl GrpcServer {
    pub fn service(event_tx: mpsc::Sender<raft::Event>) -> kv_server::KvServer<Self> {
        kv_server::KvServer::new(Self { event_tx })
    }
}

#[tonic::async_trait]
impl kv_server::Kv for GrpcServer {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .send(raft::Event::Apply((
                UpdateRequest::Put(request.into_inner()).encode_to_vec(),
                tx,
            )))
            .await
            .unwrap();

        match rx.await.unwrap() {
            Ok(result) => Ok(Response::new(
                PutResponse::decode(result.as_slice()).unwrap(),
            )),
            Err(_e) => Err(Status::unimplemented("rafterror")),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .send(raft::Event::Apply((
                UpdateRequest::Delete(request.into_inner()).encode_to_vec(),
                tx,
            )))
            .await
            .unwrap();

        match rx.await.unwrap() {
            Ok(result) => Ok(Response::new(
                DeleteResponse::decode(result.as_slice()).unwrap(),
            )),
            Err(_e) => Err(Status::unimplemented("rafterror")),
        }
    }
}
