use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bop_common::utils::init_tracing;
use clap::Parser;
use cli::PortalArgs;
use server::PortalServer;
use tracing::info;

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

    info!(%addr, registry_url = %args.registry_url, "starting Based Portal");

    let mut manager = ProxyManager::new(portal_server.clone());
    manager
        .setup_from_config_file(&args.proxy_config_file)
        .await
        .expect("Failed to setup proxy manager from config file");
    util_head_monitor(manager.get_pairs().clone());
    manager.wait_all_initialized().await.expect("Failed to wait for all proxies to initialize");
    manager.ensure_single_sequencer(true).await.expect("Failed to ensure single sequencer");

    tokio::spawn(async move {
        let _ = manager.run().await;
    });

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
