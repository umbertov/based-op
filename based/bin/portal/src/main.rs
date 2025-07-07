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

use crate::proxy::{NodeGethPair, NodeGethPairConfig, ProxyManager};

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


    let mut manager = ProxyManager::new(portal_server.clone());
    manager.setup_from_config_file("test_config.json").await?;
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
