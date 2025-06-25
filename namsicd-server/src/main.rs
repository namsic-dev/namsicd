use std::collections::HashMap;

use namsicd_server::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // RAFTID=1 cargo run
    let mut member = HashMap::new();
    member.insert(1, "[::1]:50051".to_string());
    member.insert(2, "[::1]:50052".to_string());
    member.insert(3, "[::1]:50053".to_string());
    let id: u64 = std::env::var("RAFTID").unwrap().parse().unwrap();
    let addr = member.get(&id).unwrap().parse().unwrap();
    tracing::info!("addr: {}", &addr);

    let router = namsicd_server::init(member, id, Config::default());

    router.serve(addr).await?;
    Ok(())
}
