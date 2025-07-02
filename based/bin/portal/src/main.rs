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

use crate::proxy::ProxyManager;

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

    let t1 = portal_server.run(addr).await?;
    let t2 = proxy1.run().await?;
    let t3 = proxy2.run().await?;

    let mut manager = ProxyManager::new(
        portal_server.clone()
    );
    manager.add_pair(proxy1.clone()).await;
    manager.add_pair(proxy2.clone()).await;

    let t4 = tokio::spawn(async move {
        manager.run().await;
    });

    let p1 = proxy1.clone();
    let p2 = proxy2.clone();
    let t5 = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            info!("");
            info!("Current Safe L2 state:");
            info!("Node1: {}", p1.get_current_safe_l2().await);
            info!("Node2: {}", p2.get_current_safe_l2().await);
            info!("Current Unsafe L2 state:");
            info!("Node1: {}, Seq: {}", p1.get_current_unsafe_l2().await, p1.sequencer_active().await);
            info!("Node2: {}, Seq: {}", p2.get_current_unsafe_l2().await, p2.sequencer_active().await);
        }
    });

    // let p1 = proxy1.clone();
    // let p2 = proxy2.clone();
    // let t5 = tokio::spawn(async move {
    //     loop {
    //         p1.pair_node_p2p(&p2).await.unwrap_or({
    //             error!("Failed to pair nodes");
    //         });
    //         tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
    //     }
    // });

    // let p1 = proxy1.clone();
    // let p2 = proxy2.clone();
    // let t6 = tokio::spawn(async move {
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    //     let _ = p1.stop_sequencer().await;
    //     let _ = p2.stop_sequencer().await;
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    //     let _ = p1.start_sequencer(p1.get_current_unsafe_l2().await).await;
    //     loop {
    //         let p1_head = p1.get_current_unsafe_l2().await;
    //         let p2_head = p2.get_current_unsafe_l2().await;
    //         if p1_head == p2_head {
    //             break;
    //         } else {
    //             info!("Waiting for proxies to sync: Node1: {}, Node2: {}", p1_head, p2_head);
    //             tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    //         }
    //     }
    //     let _ = p1.start_sequencer(p2.get_current_unsafe_l2().await).await;
    //     p1.activate().await;

    //     loop {
    //         tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    //         while !p1.get_current_unsafe_l2().await.is_zero() {
    //             tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    //             let _ = p2.stop_sequencer().await;
    //         }
    //         let _ = p2.start_sequencer(p2.get_current_unsafe_l2().await).await;
    //         p1.deactivate().await;
    //         p2.activate().await;

    //         tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    //         while !p2.get_current_unsafe_l2().await.is_zero() {
    //             tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    //             let _ = p1.stop_sequencer().await;
    //         }
    //         let _ = p1.start_sequencer(p1.get_current_unsafe_l2().await).await;
    //         p2.deactivate().await;
    //         p1.activate().await;
    //     }
    // });

    let _ = tokio::join!(t1.stopped(), t2.stopped(), t3.stopped(), t4, t5);

    Ok(())
}
