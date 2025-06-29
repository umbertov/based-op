use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use alloy_primitives::hex;
use bop_common::utils::init_tracing;
use clap::Parser;
use cli::PortalArgs;
use reqwest::Url;
use reth_rpc_layer::JwtSecret;
use server::{PortalServer, PortalServerInner};
use tracing::{error, info};

mod cli;
mod middleware;
mod proxy;
mod server;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = PortalArgs::parse();
    let _guard = init_tracing((&args).into());

    let addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), args.portal_port);
    let portal_server = PortalServer::new(args.clone()).await?;

    let jwt_secret = JwtSecret::from_hex("0x9956a38d431e988ea082d1e44719cc456081264b68fcec1497f54f35a4056cb2").unwrap();
    let proxy1 = proxy::NodeGethPair::new(proxy::NodeGethPairArgs {
        op_node_url: Url::parse("http://localhost:9545").unwrap(),
        op_geth_url: Url::parse("http://localhost:8545").unwrap(),
        op_geth_engine_url: Url::parse("http://localhost:8551").unwrap(),
        op_geth_engine_jwt: jwt_secret.clone(),
        portal: portal_server.clone(),
        timeout_ms: 1000,
        ingress_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8550),
    })
    .await;

    let proxy2 = proxy::NodeGethPair::new(proxy::NodeGethPairArgs {
        op_node_url: Url::parse("http://localhost:19545").unwrap(),
        op_geth_url: Url::parse("http://localhost:18545").unwrap(),
        op_geth_engine_url: Url::parse("http://localhost:18551").unwrap(),
        op_geth_engine_jwt: jwt_secret.clone(),
        portal: portal_server.clone(),
        timeout_ms: 1000,
        ingress_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 18550),
    })
    .await;

    info!(%addr, registry_url = %args.registry_url, fallback_url = %args.fallback_url, fallback_eth_url = %args.fallback_eth_url, "starting Based Portal");

    // proxy1.activate().await;
    // let _ = proxy2.stop_sequencer().await;
    // let _ = proxy2.start_sequencer(proxy1.get_current_unsafe_l2().await).await;

    let t1 = portal_server.run(addr).await?;
    let t2 = proxy1.run().await?;
    let t3 = proxy2.run().await?;

    let t4 = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
        proxy1.pair_node_p2p(&proxy2).await.unwrap_or({
            error!("Failed to pair nodes");
        });
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            info!("");
            info!("Current Safe L2 state:");
            info!("Node1: {}", proxy1.get_current_safe_l2().await);
            info!("Node2: {}", proxy2.get_current_safe_l2().await);
            info!("Current Unsafe L2 state:");
            info!("Node1: {}", proxy1.get_current_unsafe_l2().await);
            info!("Node2: {}", proxy2.get_current_unsafe_l2().await);
        }
    });

    tokio::select! {
        _ = t1.stopped() => {},
        _ = t2.stopped() => {},
        _ = t3.stopped() => {},
        _ = t4 => {},
    }

    Ok(())
}
