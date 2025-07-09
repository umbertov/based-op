use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, sync::{atomic::AtomicU64, Arc}};

use bop_common::utils::init_tracing;
use clap::Parser;
use cli::PortalArgs;
use proxy::{NodeGethPairArgs, NodeGethPairConfig};
use reqwest::Url;
use reth_rpc_layer::JwtSecret;
use server::PortalServer;
use tracing::info;

use crate::proxy::NodeGethPair;

mod cli;
mod middleware;
mod proxy;
mod server;

pub struct Portal {
    pub pairs: Vec<NodeGethPair>,
    pub active_pair: Arc<AtomicU64>,
    pub portal: PortalServer,
}

impl Portal {
    pub async fn new(args: PortalArgs) -> eyre::Result<Self> {
        let portal = PortalServer::new(&args).await?;
        let file = std::fs::File::open(args.proxy_config_file).expect("Failed to open NodeGethPairConfig config json file");
        let node_geth_pair_config: NodeGethPairConfig =
            serde_json::from_reader(file).expect("Failed to parse NodeGethPairConfig config json file");

        let pairs = 
            .collect();
        self.pairs.push(pair);

        Ok(Self {pairs: Vec::new(), active_pair: Arc::new(AtomicU64::new(0)), portal })
    }

    pub async fn setup(&mut self, proxies_args: Vec<NodeGethPairArgs>) -> eyre::Result<()> {
        for args in proxies_args {
            let proxy = NodeGethPair::new(args).await;
            let p = proxy.clone();
            tokio::spawn(async move {
                let _ = p.run().await.unwrap().stopped().await;
            });
            self.add_pair(proxy.clone());
        }

        self.portal.inner.proxies.write().extend(self.pairs.iter().cloned());

        Ok(())
    }

    pub async fn setup_from_config_file(&mut self, config_file: &str) -> eyre::Result<()> {
        info!("Loaded NodeGethPairConfig from file: {:?}", node_geth_pair_config);
        let proxies_args = node_geth_pair_config.into_args(&self.portal);
        self.setup(proxies_args).await?;
        info!("Setup NodeGethPairs from config file completed.");
        Ok(())
    }

    pub fn add_pair(&mut self, pair: NodeGethPair) {
    }

    pub fn get_pairs(&self) -> &Vec<NodeGethPair> {
        &self.pairs
    }

    pub async fn ensure_single_sequencer(&self, bridge_portal: bool) -> eyre::Result<()> {
        info!("Ensuring single sequencer across all pairs...");
        let mut sequencer_count = 0;
        for (idx, pair) in self.pairs.iter().enumerate() {
            if pair.sequencer_active().await {
                sequencer_count += 1;
                if sequencer_count > 1 {
                    let _ = pair.deactivate().await;
                    let _ = pair.stop_sequencer().await;
                } else {
                    self.active_pair.store(idx as u64, Ordering::Relaxed);
                }
            }
        }
        if sequencer_count == 0 {
            for (i, pair) in self.pairs.iter().enumerate() {
                if pair.is_alive().await {
                    sequencer_count += 1;
                    let _ = pair.start_sequencer(pair.get_current_unsafe_l2().await).await;
                    self.active_pair.store(i as u64, Ordering::Relaxed);
                    info!("Started sequencer on pair index: {}", i);
                    break;
                }
            }
        }
        if sequencer_count == 0 {
            error!("No active sequencer found across all pairs!");
        }
        if bridge_portal {
            let _ = self.pairs[self.active_pair.load(Ordering::Relaxed) as usize].activate().await;
        }
        info!("Single sequencer ensured, active pair index: {}", self.active_pair.load(Ordering::Relaxed));
        Ok(())
    }

    pub async fn wait_all_initialized(&self) -> eyre::Result<()> {
        let mut all_initialized = false;
        while !all_initialized {
            all_initialized = true;
            for pair in &self.pairs {
                if !pair.is_alive().await {
                    all_initialized = false;
                    break;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            info!("Waiting for all pairs to be initialized...");
        }
        info!("All pairs are initialized...");
        Ok(())
    }

    pub async fn wait_head_sync(&self) -> eyre::Result<()> {
        let mut all_head_sync = false;
        let mut head = B256::ZERO;
        while !all_head_sync {
            all_head_sync = true;
            head = B256::ZERO;
            for pair in &self.pairs {
                let current_pair_head = pair.get_current_unsafe_l2().await;
                if head.is_zero() {
                    head = current_pair_head;
                } else if head != current_pair_head {
                    all_head_sync = false;
                    break;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            info!("Waiting for all pairs to sync to head: {}", hex::encode(head));
        }
        info!("All pairs are synced to head: {}", hex::encode(head));
        Ok(())
    }

    pub async fn wait_ready(&self) -> eyre::Result<()> {
        if self.pairs.is_empty() {
            return Err(eyre::eyre!("No pairs available for head sync"));
        }
        self.wait_all_initialized().await?;
        info!("All nodes are paired, proceeding to head sync...");
        self.wait_head_sync().await?;
        info!("All nodes are synced to head, proceeding to ensure single sequencer...");
        Ok(())
    }

    pub async fn pair_all_nodes(&self) -> eyre::Result<()> {
        for (i, pair) in self.pairs.iter().enumerate() {
            for (j, other_pair) in self.pairs.iter().enumerate() {
                if i != j {
                    let _ = pair.pair_node_p2p(other_pair).await;
                    info!("Paired Node {} with Node {}", i, j);
                }
            }
        }
        Ok(())
    }

    pub async fn run(&self) -> eyre::Result<()> {
        self.wait_all_initialized().await?;
        self.pair_all_nodes().await?;
        self.ensure_single_sequencer(true).await?;
        self.wait_ready().await?;
        self.ensure_single_sequencer(true).await?;

        info!("Starting sequencer rotation loop...");
        loop {
            let current_index = self.active_pair.load(Ordering::Relaxed) as usize;
            let next_index = (current_index + 1) % self.pairs.len();
            let p1 = &self.pairs[current_index];
            let p2 = &self.pairs[next_index];
            while p1.is_alive().await && p1.sequencer_active().await {
                for (i, pair) in self.pairs.iter().enumerate() {
                    if i == current_index {
                        continue;
                    }
                    let _ = pair.stop_sequencer().await;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
            let _ = p1.stop_sequencer().await;
            p1.deactivate().await;
            let _ = p2.start_sequencer(p2.get_current_unsafe_l2().await).await;
            p2.activate().await;
            info!("Switched active sequencer from Node {} to Node {}", current_index, next_index);
            self.active_pair.store(next_index as u64, Ordering::Relaxed);
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        }
        // Ok(())
    }
}
#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = PortalArgs::parse();
    let _guard = init_tracing((&args).into());

    let addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), args.portal_port);
    let portal_server = PortalServer::new(args.clone()).await?;

    info!(%addr, registry_url = %args.registry_url, "starting Based Portal");

    let mut manager = Portal::new(portal_server.clone());
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
