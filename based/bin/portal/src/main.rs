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

use crate::proxy::{NodeGethPair, ProxyManager};

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

    info!(%addr, registry_url = %args.registry_url, fallback_url = %args.fallback_url, fallback_eth_url = %args.fallback_eth_url, "starting Based Portal");

    let jwt_secret = JwtSecret::from_hex("0x34156704bca7c6396348ffec4b3a8097b089066f8b996f29a2c7d70164fb94df").unwrap();

    let proxies_args = vec![
        proxy::NodeGethPairArgs {
            op_node_url: Url::parse("http://localhost:9545").unwrap(),
            op_geth_url: Url::parse("http://localhost:8545").unwrap(),
            op_geth_engine_url: Url::parse("http://localhost:8551").unwrap(),
            op_geth_engine_jwt: jwt_secret.clone(),
            portal: portal_server.clone(),
            timeout_ms: 1000,
            ingress_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8550),
        },
        proxy::NodeGethPairArgs {
            op_node_url: Url::parse("http://localhost:19545").unwrap(),
            op_geth_url: Url::parse("http://localhost:18545").unwrap(),
            op_geth_engine_url: Url::parse("http://localhost:18551").unwrap(),
            op_geth_engine_jwt: jwt_secret.clone(),
            portal: portal_server.clone(),
            timeout_ms: 1000,
            ingress_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 18550),
        },
    ];
    let mut manager = ProxyManager::new(portal_server.clone());
    manager.setup(proxies_args).await?;
    let proxies = manager.get_pairs().clone();
    tokio::spawn(async move {
        let _ = manager.run().await;
    });

    util_head_monitor(proxies);

    let _ = tokio::join!(portal_server.run(addr).await?.stopped());
    Ok(())
}

fn util_head_monitor(proxies: Vec<NodeGethPair>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
            info!("Current Unsafe L2 state:");
            for (i, proxy) in proxies.iter().enumerate() {
                info!(
                    "Node{}: {}, Seq: {}, Alive: {}",
                    i + 1,
                    proxy.get_current_unsafe_l2().await,
                    proxy.sequencer_active().await,
                    proxy.is_alive().await
                );
            }
        }
    });
}
