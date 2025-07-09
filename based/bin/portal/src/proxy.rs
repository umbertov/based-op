use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr}, ops::Deref, sync::{
        atomic::{AtomicBool, Ordering}, Arc
    }
};

use alloy_eips::eip7685::RequestsOrHash;
use alloy_primitives::{B256};
use alloy_rpc_types::engine::{ExecutionPayloadV3, ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus};
use bop_common::{
    api::{
        EngineApiClient, EngineApiServer, EthApiClient, OpNodeAdminApiClient, OpNodeApiClient, OpNodeP2PApiClient,
        PROXY_CAPABILITIES,
    },
    communication::messages::{RpcError, RpcResult},
    time::Duration,
    utils::uuid,
};
use jsonrpsee::{
    core::async_trait,
    http_client::{HttpClientBuilder, transport::HttpBackend},
    server::{RpcServiceBuilder, ServerBuilder, ServerHandle},
};
use op_alloy_rpc_types_engine::{OpExecutionPayloadEnvelopeV4, OpExecutionPayloadV4, OpPayloadAttributes};
use reqwest::Url;
use reth_rpc_layer::{AuthClientLayer, AuthClientService, JwtSecret};
use serde::Deserialize;
use tokio::sync::Mutex;
use tower::{ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::{Level, warn};
use either::Either;

use crate::{middleware::ProxyService};

pub type RpcClient = jsonrpsee::http_client::HttpClient;
pub type AuthRpcClient = jsonrpsee::http_client::HttpClient<AuthClientService<HttpBackend>>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NodeGethPairArgs {
    pub op_geth_url: String,
    pub op_geth_engine_url: String,
    pub op_geth_engine_jwt: String,
    pub port: u16,
    pub port_engine: u16,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NodeGethPairConfig {
    pub proxies: Vec<NodeGethPairArgs>,
    pub timeout_ms: u64,
}

impl From<NodeGethPairConfig> for Vec<NodeGethPair> {
    fn from(value: NodeGethPairConfig) -> Self {
        value.proxies
            .into_iter()
            .filter_map(|proxy| {
                 let timeout = Duration::from_millis(value.timeout_ms);
                let op_node_url = Url::parse(&proxy.op_node_url).expect("Invalid op_node_url");
                let op_geth_url = Url::parse(&proxy.op_geth_url).expect("Invalid op_geth_url");
                let op_geth_engine_url = Url::parse(&proxy.op_geth_engine_url).expect("Invalid op_geth_engine_url");
                let op_geth_engine_jwt = JwtSecret::from_hex(&proxy.op_geth_engine_jwt).expect("Invalid JWT secret");
                let ingress_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), proxy.portal_ingress_port);
                let op_node_client = create_client(op_node_url, timeout).ok()?;
                let op_geth_client = create_client(op_geth_url, timeout).ok()?;
                
                let op_geth_engine_client = create_auth_client(op_geth_engine_url, op_geth_engine_jwt, timeout).ok()?;
                let inner = NodeGethPairInner {
                        op_node_client,
                        op_geth_client,
                        op_geth_engine_client,
                        active: Arc::new(AtomicBool::new(false)),
                        ingress_addr,
                    };


                    Some(NodeGethPair(Arc::new(inner)))
                }).collect()
    }
}


impl NodeGethPair {
    pub fn new(args: NodeGethPairArgs, timeout: Duration) -> eyre::Result<Self> {
        let op_geth_url = Url::parse(&args.op_geth_url).expect("Invalid op_geth_url");
        let op_geth_engine_url = Url::parse(&args.op_geth_engine_url).expect("Invalid op_geth_engine_url");
        let op_geth_engine_jwt = JwtSecret::from_hex(&args.op_geth_engine_jwt).expect("Invalid JWT secret");
        let ingress_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), args.portal_ingress_port);
        let op_geth_client = Arc::new(Mutex::new(create_client(op_geth_url, timeout)?));
        
        let op_geth_engine_client = Arc::new(Mutex::new(create_auth_client(op_geth_engine_url, op_geth_engine_jwt)?)));
        let inner = NodeGethPairInner {
                op_geth_client,
                op_geth_engine_client,
                ingress_addr,
            };
        Ok(Self(Arc::new(inner)))
    }

    pub async fn run(&self, op_geth_client: Arc<Mutex<RpcClient>>, op_geth_engine_client: Arc<Mutex<AuthRpcClient>>, ingress_addr: SocketAddr ) -> eyre::Result<ServerHandle> {
        // Clone the necessary fields before moving into the closure

        let rpc_middleware = RpcServiceBuilder::new().layer_fn(move |s| {

        async move {
            if &req.method_name()[0..=5] == "portal" || supported_methods.contains(&req.method_name()) {
                debug!(method = %req.method_name(), "handling request");
                inner.call(req).await
            } else {
                let params = WrapParams(req.params());
                let r: Result<serde_json::Value, jsonrpsee::core::ClientError> = match req.method_name().split_once('_')
                {
                    Some(("engine", _)) => {
                        debug!(method = %req.method_name(), "forwarding request to eth engine fallback");
                        op_geth_engine_client.request(req.method_name(), params).await
                    }
                    Some(("eth", _)) => {
                        debug!(method = %req.method_name(), "forwarding request to eth fallback");
                        op_geth_client.request(req.method_name(), params).await
                    }
                }
            }
        });

        let server = ServerBuilder::default()
            .max_request_body_size(u32::MAX)
            .max_response_body_size(u32::MAX)
            .set_rpc_middleware(rpc_middleware)
            .build(ingress_addr)
            .await?;

        let module = EngineApiServer::into_rpc((*self.inner).clone());
        // module.merge(EthApiServer::into_rpc((*self.inner).clone())).expect("failed to merge modules");

        let server_handle = server.start(module);
        Ok(server_handle)
    }

    pub async fn get_current_unsafe_l2(&self) -> B256 {
        match self.inner.op_node_client.sync_status().await {
            Ok(status) => status.unsafe_l2.hash,
            Err(_err) => B256::ZERO,
        }
    }

    pub async fn get_chain_id(&self) -> eyre::Result<String> {
        let info = self.inner.op_geth_client.chain_id().await;
        match info {
            Ok(info) => Ok(info),
            Err(err) => Err(eyre::eyre!("Failed to get chain ID: {}", err)),
        }
    }

    pub async fn pair_node_p2p(&self, other: &NodeGethPair) -> eyre::Result<()> {
        let multi_address_self = self.inner.op_node_client.peer_info().await?.addresses[0].clone();
        let multi_address_other = other.inner.op_node_client.peer_info().await?.addresses[0].clone();
        self.inner.op_node_client.connect_peer(multi_address_other.clone()).await?;
        other.inner.op_node_client.connect_peer(multi_address_self.clone()).await?;

        Ok(())
    }

    pub async fn start_sequencer(&self, head: B256) -> eyre::Result<()> {
        match self.inner.op_node_client.start_sequencer(head).await {
            Ok(_) => Ok(()),
            Err(err) => Err(eyre::eyre!("Failed to start sequencer: {}", err)),
        }
    }

    pub async fn stop_sequencer(&self) -> eyre::Result<()> {
        match self.inner.op_node_client.stop_sequencer().await {
            Ok(_) => Ok(()),
            Err(err) => Err(eyre::eyre!("Failed to stop sequencer: {}", err)),
        }
    }

    pub async fn sequencer_active(&self) -> bool {
        match self.inner.op_node_client.sequencer_active().await {
            Ok(active) => active,
            Err(err) => {
                warn!("Failed to check sequencer status: {}", err);
                false
            }
        }
    }

    pub async fn is_alive(&self) -> bool {
        for _ in 0..3 {
            if (!self.get_current_unsafe_l2().await.is_zero()) && self.get_chain_id().await.is_ok() {
                return true;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
        false
    }
}


#[async_trait]
impl EngineApiServer for NodeGethPairInner {
    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn fork_choice_updated_v3(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<OpPayloadAttributes>,
    ) -> RpcResult<ForkchoiceUpdated> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.fork_choice_updated_v3(fork_choice_state, payload_attributes).await;
        } else {
            match self.op_geth_engine_client.fork_choice_updated_v3(fork_choice_state, payload_attributes).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn new_payload_v4(
        &self,
        payload: OpExecutionPayloadV4,
        versioned_hashes: Vec<B256>,
        parent_beacon_block_root: B256,
        requests: RequestsOrHash,
    ) -> RpcResult<PayloadStatus> {
        if self.active.load(Ordering::Relaxed) {
            return self
                .portal
                .inner
                .new_payload_v4(payload, versioned_hashes, parent_beacon_block_root, requests)
                .await;
        } else {
            match self
                .op_geth_engine_client
                .new_payload_v4(payload, versioned_hashes, parent_beacon_block_root, requests)
                .await
            {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn new_payload_v3(
        &self,
        payload: ExecutionPayloadV3,
        versioned_hashes: Vec<B256>,
        parent_beacon_block_root: B256,
    ) -> RpcResult<PayloadStatus> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.new_payload_v3(payload, versioned_hashes, parent_beacon_block_root).await;
        } else {
            match self.op_geth_engine_client.new_payload_v3(payload, versioned_hashes, parent_beacon_block_root).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn get_payload_v4(&self, payload_id: PayloadId) -> RpcResult<OpExecutionPayloadEnvelopeV4> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.get_payload_v4(payload_id).await;
        } else {
            match self.op_geth_engine_client.get_payload_v4(payload_id).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }
}

fn create_client(url: Url, timeout: Duration) -> eyre::Result<RpcClient> {
    let client = HttpClientBuilder::default()
        .max_request_size(u32::MAX)
        .max_response_size(u32::MAX)
        .request_timeout(timeout.into())
        .build(url)?;
    Ok(client)
}

fn create_auth_client(url: Url, jwt: JwtSecret, timeout: Duration) -> eyre::Result<AuthRpcClient> {
    let secret_layer = AuthClientLayer::new(jwt);
    let middleware = tower::ServiceBuilder::default().layer(secret_layer);

    let client = HttpClientBuilder::default()
        .max_request_size(u32::MAX)
        .max_response_size(u32::MAX)
        .set_http_middleware(middleware)
        .request_timeout(timeout.into())
        .build(url)?;

    Ok(client)
}

