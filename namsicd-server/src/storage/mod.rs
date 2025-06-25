mod grpc;
mod pb {
    tonic::include_proto!("namsicdkvpb");
}

use std::collections::BTreeMap;

pub use grpc::GrpcServer;
use pb::{DeleteRequest, DeleteResponse, PutRequest, PutResponse};
use prost::{
    Message,
    bytes::{Buf, BufMut},
};
use tokio::sync::mpsc;

use crate::Operation;

trait StorageInterface {
    fn put(&mut self, request: PutRequest) -> PutResponse;
    fn delete(&mut self, request: DeleteRequest) -> DeleteResponse;
}

#[derive(Default)]
struct Storage {
    inner: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl StorageInterface for Storage {
    fn put(&mut self, request: PutRequest) -> PutResponse {
        self.inner.insert(request.key, request.value);
        PutResponse {}
    }
    fn delete(&mut self, request: DeleteRequest) -> DeleteResponse {
        self.inner.remove(&request.key);
        DeleteResponse {}
    }
}

pub async fn process(mut operation_rx: mpsc::Receiver<Operation>) {
    let mut storage = Storage::default();

    while let Some(operation) = operation_rx.recv().await {
        let Ok(request) = UpdateRequest::decode(operation.data()) else {
            todo!()
        };

        match request {
            UpdateRequest::Put(put_request) => {
                operation.result(storage.put(put_request).encode_to_vec())
            }
            UpdateRequest::Delete(delete_request) => {
                operation.result(storage.delete(delete_request).encode_to_vec())
            }
        }
    }
}

enum UpdateRequest {
    Put(PutRequest),
    Delete(DeleteRequest),
}

impl UpdateRequest {
    fn code(&self) -> u32 {
        match self {
            Self::Put(_) => 1,
            Self::Delete(_) => 2,
        }
    }

    fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u32(self.code());

        match self {
            Self::Put(req) => buf.extend(req.encode_to_vec()),
            Self::Delete(req) => buf.extend(req.encode_to_vec()),
        }
        buf
    }

    fn decode(v: &[u8]) -> Result<Self, ()> {
        let mut buf = prost::bytes::Bytes::from(v.to_vec());
        let code = buf.get_u32();
        match code {
            1 => Ok(Self::Put(PutRequest::decode(buf).unwrap())),
            2 => Ok(Self::Delete(DeleteRequest::decode(buf).unwrap())),
            _ => todo!(),
        }
    }
}
