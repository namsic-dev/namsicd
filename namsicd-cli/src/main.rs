use pb::{PutRequest, kv_client};
use tonic::transport;

mod pb {
    tonic::include_proto!("namsicdkvpb");
}

#[tokio::main]
async fn main() {
    let addr = String::from("http://[::1]:50052");
    let mut client = kv_client::KvClient::new(
        transport::Channel::from_shared(addr)
            .unwrap()
            .connect_lazy(),
    );

    loop {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        let line = line.trim();
        if line == "quit" {
            break;
        }

        let res = client
            .put(PutRequest {
                key: b"a".to_vec(),
                value: line.as_bytes().to_vec(),
            })
            .await;

        match res {
            Ok(a) => {
                println!("{:?}", a.get_ref());
            }
            Err(e) => {
                println!("{e}");
            }
        }
    }
}
