use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use alloy_eips::eip7685::RequestsOrHash;
use alloy_primitives::{Address, B256, Bytes, U256, hex};
use alloy_rpc_types::{
    engine::{payload, ExecutionPayloadV3, ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus}, BlockId, BlockNumberOrTag
};
use bop_common::{
    api::{
        ControlApiClient, EngineApiClient, EngineApiServer, EthApiClient, EthApiServer, OpGethAdminApiClient,
        OpNodeApiClient, OpNodeP2PApiClient, OpRpcBlock, PORTAL_CAPABILITIES, PROXY_CAPABILITIES, PortalApiServer, RegistryApiClient,
        RegistryApiServer,
    },
    communication::messages::{RpcError, RpcResult},
    time::{Duration, Instant},
    utils::{uuid, wait_for_signal},
};
use jsonrpsee::{
    core::{async_trait, ClientError},
    http_client::{transport::HttpBackend, HttpClientBuilder},
    server::{RpcServiceBuilder, ServerBuilder, ServerHandle},
};
use op_alloy_rpc_types::OpTransactionReceipt;
use op_alloy_rpc_types_engine::{OpExecutionPayloadEnvelopeV4, OpExecutionPayloadV4, OpPayloadAttributes};
use parking_lot::RwLock;
use reqwest::Url;
use reth_rpc_layer::{AuthClientLayer, AuthClientService, JwtSecret};
use tokio::sync::Mutex;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tracing::{Instrument, Level, debug, error, info, trace};

use crate::{cli::PortalArgs, middleware::ProxyService, server::PortalServer};
use jsonrpsee::server::HttpBody;

pub type RpcClient = jsonrpsee::http_client::HttpClient;
pub type AuthRpcClient = jsonrpsee::http_client::HttpClient<AuthClientService<HttpBackend>>;

pub struct NodeGethPairArgs {
    pub op_node_url: Url,
    pub op_geth_url: Url,
    pub op_geth_engine_url: Url,
    pub op_geth_engine_jwt: JwtSecret,
    pub portal: PortalServer,
    pub timeout_ms: u64,
    pub ingress_addr: SocketAddr,
}

#[derive(Clone)]
pub struct NodeGethPair {
    pub op_node_client: RpcClient,
    pub op_geth_client: RpcClient,
    pub op_geth_engine_client: AuthRpcClient,
    pub portal: PortalServer,
    pub head_hash: B256,
    pub active: Arc<RwLock<bool>>,
    pub ingress_addr: SocketAddr,
}

impl NodeGethPair {
    pub async fn new(args: NodeGethPairArgs) -> eyre::Result<Self> {
        let timeout = Duration::from_millis(args.timeout_ms);
        let op_node_client = create_client(args.op_node_url.clone(), timeout)?;
        let op_geth_client = create_client(args.op_geth_url.clone(), timeout)?;
        let op_geth_engine_client = create_auth_client(
            args.op_geth_engine_url,
            args.op_geth_engine_jwt,
            timeout,
        )?;

        Ok(NodeGethPair {
            op_node_client,
            op_geth_client,
            op_geth_engine_client,
            portal: args.portal,
            head_hash: B256::ZERO,
            active: Arc::new(RwLock::new(false)),
            ingress_addr: args.ingress_addr,
        })
    }

    pub async fn run(&self) -> eyre::Result<(ServerHandle)> {
        // Clone the necessary fields before moving into the closure
        let op_geth_client = self.op_geth_client.clone();
        let op_geth_engine_client = self.op_geth_engine_client.clone();
        let op_node_client = self.op_node_client.clone();
        let registry_client = self.portal.registry_client.clone();
        let ingress_addr = self.ingress_addr;

        let rpc_middleware = RpcServiceBuilder::new().layer_fn(move |s| {
            ProxyService::new(
                PROXY_CAPABILITIES,
                s,
                op_geth_client.clone(),
                op_geth_engine_client.clone(),
                op_node_client.clone(),
                registry_client.clone(),
            )
        });

        let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);
        let cors_middleware = ServiceBuilder::new().layer(cors);
        let server = ServerBuilder::default()
            .max_request_body_size(u32::MAX)
            .max_response_body_size(u32::MAX)
            .set_http_middleware(cors_middleware)
            .set_rpc_middleware(rpc_middleware)
            .build(ingress_addr)
            .await?;

        let mut module = EngineApiServer::into_rpc(self.clone());
        module.merge(EthApiServer::into_rpc(self.clone())).expect("failed to merge modules");

        let server_handle = server.start(module);
        Ok(server_handle)
    }
}

/// This is a temporary API to broacast transactions to both gateway and fallback. In practice this should not be
/// receiving user facing calls so we need to find another way to do this
#[async_trait]
impl EthApiServer for NodeGethPair {
    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        if self.active.read().clone() {
            return self.portal.send_raw_transaction(bytes.clone()).await;
        } else {
            match self.op_geth_client.send_raw_transaction(bytes.clone()).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<OpTransactionReceipt>> {
        if !self.active.read().clone() {
            return self.portal.transaction_receipt(hash).await;
        } else {
            match self.op_geth_client.transaction_receipt(hash).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_by_number(&self, number: BlockNumberOrTag, full: bool) -> RpcResult<Option<OpRpcBlock>> {
        if !self.active.read().clone() {
            return self.portal.block_by_number(number, full).await;
        } else {
            match self.op_geth_client.block_by_number(number, full).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_by_hash(&self, hash: B256, full: bool) -> RpcResult<Option<OpRpcBlock>> {
        if !self.active.read().clone() {
            return self.portal.block_by_hash(hash, full).await;
        } else {
            match self.op_geth_client.block_by_hash(hash, full).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_number(&self) -> RpcResult<U256> {
        if !self.active.read().clone() {
            return self.portal.block_number().await;
        } else {
            match self.op_geth_client.block_number().await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn transaction_count(&self, address: Address, block_number: Option<BlockId>) -> RpcResult<U256> {
        if !self.active.read().clone() {
            return self.portal.transaction_count(address, block_number).await;
        } else {
            match self.op_geth_client.transaction_count(address, block_number).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn balance(&self, address: Address, block_number: Option<BlockId>) -> RpcResult<U256> {
        if !self.active.read().clone() {
            return self.portal.balance(address, block_number).await;
        } else {
            match self.op_geth_client.balance(address, block_number).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }
}

#[async_trait]
impl EngineApiServer for NodeGethPair {
    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn fork_choice_updated_v3(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<OpPayloadAttributes>,
    ) -> RpcResult<ForkchoiceUpdated> {
        if self.active.read().clone() {
            return self.portal.fork_choice_updated_v3(fork_choice_state, payload_attributes).await;
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
        if self.active.read().clone() {
            return self.portal.new_payload_v4(payload, versioned_hashes, parent_beacon_block_root, requests).await;
        } else {
            match self.op_geth_engine_client.new_payload_v4(payload, versioned_hashes, parent_beacon_block_root, requests).await {
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
        if self.active.read().clone() {
            return self.portal.new_payload_v3(payload, versioned_hashes, parent_beacon_block_root).await;
        } else {
            match self.op_geth_engine_client.new_payload_v3(payload, versioned_hashes, parent_beacon_block_root).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn get_payload_v4(&self, payload_id: PayloadId) -> RpcResult<OpExecutionPayloadEnvelopeV4> {
        if self.active.read().clone() {
            return self.portal.get_payload_v4(payload_id).await;
        } else {
            match self.op_geth_engine_client.get_payload_v4(payload_id).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }
}

impl NodeGethPair {
    pub async fn get_current_unsafe_l2(&self) -> B256 {
        match self.op_node_client.sync_status().await {
            Ok(status) => status.unsafe_l2.hash,
            Err(err) => B256::ZERO
        }
    }

    pub async fn get_current_safe_l2(&self) -> B256 {
        match self.op_node_client.sync_status().await {
            Ok(status) => status.safe_l2.hash,
            Err(err) => B256::ZERO
        }
    }

    pub async fn pair_node_p2p(&self, other: &NodeGethPair) -> eyre::Result<()> {
        let multi_address_self = self.op_node_client.peer_info().await?.addresses[0].clone();
        let multi_address_other = other.op_node_client.peer_info().await?.addresses[0].clone();
        self.op_node_client.connect_peer(multi_address_other.clone()).await?;
        other.op_node_client.connect_peer(multi_address_self.clone()).await?;

        Ok(())
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