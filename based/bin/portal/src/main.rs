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
    let server: PortalServer = portal_server;

    let portal_clone = server.clone();
    let jwt_secret = JwtSecret::from_hex("0x2940a604f20f4389f4cdfa57f754b6080624058f1474579a970bb26354c7dd6e").unwrap();
    let proxy1 = proxy::NodeGethPair::new(proxy::NodeGethPairArgs {
        op_node_url: Url::parse("http://localhost:9545").unwrap(),
        op_geth_url: Url::parse("http://localhost:8545").unwrap(),
        op_geth_engine_url: Url::parse("http://localhost:8551").unwrap(),
        op_geth_engine_jwt: jwt_secret.clone(),
        portal: portal_clone,
        timeout_ms: 1000,
        ingress_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8550)
    }).await?;

    info!(%addr, registry_url = %args.registry_url, fallback_url = %args.fallback_url, fallback_eth_url = %args.fallback_eth_url, "starting Based Portal");

    // server.run(addr)

    let server_task = server.run(addr);
    let proxy1_task = proxy1.run();

    let _ = tokio::join!(
        server_task,
        proxy1_task,
    );

    Ok(())
}
