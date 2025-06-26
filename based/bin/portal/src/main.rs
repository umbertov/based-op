use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use alloy_primitives::{hex};
use bop_common::utils::init_tracing;
use clap::Parser;
use cli::PortalArgs;
use reth_rpc_layer::JwtSecret;
use server::PortalServer;
use tracing::info;
use std::sync::Arc;
use reqwest::Url;

mod cli;
mod middleware;
mod server;
mod proxy;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = PortalArgs::parse();
    let _guard = init_tracing((&args).into());

    let addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), args.portal_port);
    let portal_server = PortalServer::new(args.clone()).await?;

    let jwt_secret = JwtSecret::from_hex("0x19e75be29bb925ed29a97eb510209afaf5762f9e4978bb520a82c8d33d52c785").unwrap();
    let proxy1 = proxy::NodeGethPair::new(proxy::NodeGethPairArgs {
        op_node_url: Url::parse("http://localhost:9545").unwrap(),
        op_geth_url: Url::parse("http://localhost:8545").unwrap(),
        op_geth_engine_url: Url::parse("http://localhost:8551").unwrap(),
        op_geth_engine_jwt: jwt_secret.clone(),
        portal: portal_server.clone(),
        timeout_ms: 1000,
        ingress_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8550)
    }).await?;

    let proxy2 = proxy::NodeGethPair::new(proxy::NodeGethPairArgs {
        op_node_url: Url::parse("http://localhost:19545").unwrap(),
        op_geth_url: Url::parse("http://localhost:18545").unwrap(),
        op_geth_engine_url: Url::parse("http://localhost:18551").unwrap(),
        op_geth_engine_jwt: jwt_secret.clone(),
        portal: portal_server.clone(),
        timeout_ms: 1000,
        ingress_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 18550)
    }).await?;

    info!(%addr, registry_url = %args.registry_url, fallback_url = %args.fallback_url, fallback_eth_url = %args.fallback_eth_url, "starting Based Portal");

    // server.run(addr)

    // let t1 = portal_server.run(addr).await?;
    let t2 = proxy1.run().await?;
    let t3 = proxy2.run().await?;

    tokio::select! {
        // _ = t1.stopped() => {},
        _ = t2.stopped() => {},
        _ = t3.stopped() => {},
    }

    Ok(())
}
