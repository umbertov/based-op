use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use alloy_eips::eip7685::RequestsOrHash;
use alloy_primitives::{Address, B256, Bytes, U256, hex};
use alloy_rpc_types::{
    BlockId, BlockNumberOrTag,
    engine::{ExecutionPayloadV3, ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus, payload},
};
use bop_common::{
    api::{
        ControlApiClient, EngineApiClient, EngineApiServer, EthApiClient, EthApiServer, OpGethAdminApiClient,
        OpNodeAdminApiClient, OpNodeApiClient, OpNodeP2PApiClient, OpRpcBlock, PORTAL_CAPABILITIES, PROXY_CAPABILITIES,
        PortalApiServer, RegistryApiClient, RegistryApiServer,
    },
    communication::messages::{RpcError, RpcResult},
    time::{Duration, Instant},
    utils::{uuid, wait_for_signal},
};
use jsonrpsee::{
    core::{ClientError, async_trait},
    http_client::{HttpClientBuilder, transport::HttpBackend},
    server::{HttpBody, RpcServiceBuilder, ServerBuilder, ServerHandle},
};
use op_alloy_rpc_types::OpTransactionReceipt;
use op_alloy_rpc_types_engine::{OpExecutionPayloadEnvelopeV4, OpExecutionPayloadV4, OpPayloadAttributes};
use parking_lot::RwLock;
use reqwest::Url;
use reth_rpc_layer::{AuthClientLayer, AuthClientService, JwtSecret};
use tokio::sync::Mutex;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tracing::{Instrument, Level, debug, error, info, trace, warn};

use crate::{cli::PortalArgs, middleware::ProxyService, server::PortalServer};

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
pub struct NodeGethPairInner {
    pub op_node_client: RpcClient,
    pub op_geth_client: RpcClient,
    pub op_geth_engine_client: AuthRpcClient,
    pub portal: PortalServer,
    pub head_hash: B256,
    pub active: Arc<AtomicBool>,
    pub ingress_addr: SocketAddr,
}

#[derive(Clone)]
pub struct NodeGethPair {
    pub inner: Arc<NodeGethPairInner>,
}

impl NodeGethPair {
    pub async fn new(args: NodeGethPairArgs) -> Self {
        let inner = NodeGethPairInner::new(args).await.expect("Failed to create NodeGethPairInner");
        NodeGethPair { inner: Arc::new(inner) }
    }

    pub async fn activate(&self) {
        self.inner.active.store(true, Ordering::Relaxed);
    }

    pub async fn deactivate(&self) {
        self.inner.active.store(false, Ordering::Relaxed);
    }

    pub async fn run(&self) -> eyre::Result<(ServerHandle)> {
        // Clone the necessary fields before moving into the closure
        let op_geth_client = self.inner.op_geth_client.clone();
        let op_geth_engine_client = self.inner.op_geth_engine_client.clone();
        let op_node_client = self.inner.op_node_client.clone();
        let registry_client = self.inner.portal.inner.registry_client.clone();
        let ingress_addr = self.inner.ingress_addr;

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

        let mut module = EngineApiServer::into_rpc((*self.inner).clone());
        module.merge(EthApiServer::into_rpc((*self.inner).clone())).expect("failed to merge modules");

        let server_handle = server.start(module);
        Ok(server_handle)
    }

    pub async fn get_current_unsafe_l2(&self) -> B256 {
        match self.inner.op_node_client.sync_status().await {
            Ok(status) => status.unsafe_l2.hash,
            Err(err) => B256::ZERO,
        }
    }

    pub async fn get_current_safe_l2(&self) -> B256 {
        match self.inner.op_node_client.sync_status().await {
            Ok(status) => status.safe_l2.hash,
            Err(err) => B256::ZERO,
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
            Err(err) => {
                error!(%err, "Failed to start sequencer");
                Err(eyre::eyre!("Failed to start sequencer: {}", err))
            }
        }
    }

    pub async fn stop_sequencer(&self) -> eyre::Result<()> {
        match self.inner.op_node_client.stop_sequencer().await {
            Ok(_) => Ok(()),
            Err(err) => {
                error!(%err, "Failed to stop sequencer");
                Err(eyre::eyre!("Failed to stop sequencer: {}", err))
            }
        }
    }
}

impl NodeGethPairInner {
    pub async fn new(args: NodeGethPairArgs) -> eyre::Result<Self> {
        let timeout = Duration::from_millis(args.timeout_ms);
        let op_node_client = create_client(args.op_node_url.clone(), timeout)?;
        let op_geth_client = create_client(args.op_geth_url.clone(), timeout)?;
        let op_geth_engine_client = create_auth_client(args.op_geth_engine_url, args.op_geth_engine_jwt, timeout)?;

        Ok(NodeGethPairInner {
            op_node_client,
            op_geth_client,
            op_geth_engine_client,
            portal: args.portal,
            head_hash: B256::ZERO,
            active: Arc::new(AtomicBool::new(false)),
            ingress_addr: args.ingress_addr,
        })
    }
}

/// This is a temporary API to broacast transactions to both gateway and fallback. In practice this should not be
/// receiving user facing calls so we need to find another way to do this
#[async_trait]
impl EthApiServer for NodeGethPairInner {
    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.send_raw_transaction(bytes.clone()).await;
        } else {
            match self.op_geth_client.send_raw_transaction(bytes.clone()).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<OpTransactionReceipt>> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.transaction_receipt(hash).await;
        } else {
            match self.op_geth_client.transaction_receipt(hash).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_by_number(&self, number: BlockNumberOrTag, full: bool) -> RpcResult<Option<OpRpcBlock>> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.block_by_number(number, full).await;
        } else {
            match self.op_geth_client.block_by_number(number, full).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_by_hash(&self, hash: B256, full: bool) -> RpcResult<Option<OpRpcBlock>> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.block_by_hash(hash, full).await;
        } else {
            match self.op_geth_client.block_by_hash(hash, full).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_number(&self) -> RpcResult<U256> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.block_number().await;
        } else {
            match self.op_geth_client.block_number().await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn transaction_count(&self, address: Address, block_number: Option<BlockId>) -> RpcResult<U256> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.transaction_count(address, block_number).await;
        } else {
            match self.op_geth_client.transaction_count(address, block_number).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn balance(&self, address: Address, block_number: Option<BlockId>) -> RpcResult<U256> {
        if self.active.load(Ordering::Relaxed) {
            return self.portal.inner.balance(address, block_number).await;
        } else {
            match self.op_geth_client.balance(address, block_number).await {
                Ok(payload) => Ok(payload),
                Err(_err) => Err(RpcError::Internal),
            }
        }
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
        warn!("Forkchoice updated v3. {:?}", self.active.load(Ordering::Relaxed));

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
        warn!("New payload v4. {:?}", self.active.load(Ordering::Relaxed));

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
        warn!("New payload v3. {:?}", self.active.load(Ordering::Relaxed));

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
        warn!("Get payload v4. {:?}", self.active.load(Ordering::Relaxed));

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
