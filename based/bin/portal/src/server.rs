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
    BlockId, BlockNumberOrTag,
    engine::{ExecutionPayloadV3, ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus},
};
use bop_common::{
    api::{
        ControlApiClient, EngineApiClient, EngineApiServer, EthApiClient, EthApiServer, OpGethAdminApiClient,
        OpNodeApiClient, OpNodeP2PApiClient, OpRpcBlock, PORTAL_CAPABILITIES, PortalApiServer, RegistryApiClient,
        RegistryApiServer,
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
use tracing::{Instrument, Level, debug, error, info, trace};

use crate::{cli::PortalArgs, middleware::ProxyService, proxy::NodeGethPair};

pub type RpcClient = jsonrpsee::http_client::HttpClient;
pub type AuthRpcClient = jsonrpsee::http_client::HttpClient<AuthClientService<HttpBackend>>;

#[derive(Clone)]
struct Gateway {
    id: Url,
    jwt: String,
    address: Address,
    client: AuthRpcClient,
    ping: Arc<Duration>,
    last_seen: Arc<Option<Instant>>,
}

impl Gateway {
    pub fn is_active(&self, gateway_inactivity_timeout_ms: u64) -> bool {
        match self.last_seen.as_ref() {
            Some(last_seen) => {
                let elapsed = last_seen.elapsed();
                elapsed < Duration::from_millis(gateway_inactivity_timeout_ms)
            }
            None => false,
        }
    }
}

impl fmt::Debug for Gateway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl Gateway {
    fn new(id: Url, client: AuthRpcClient, jwt: String, address: Address) -> Self {
        Self { id, jwt, client, ping: Arc::new(Duration::from_millis(0)), last_seen: Arc::new(None), address }
    }
}

#[derive(Clone)]
pub struct PortalServerInner {
    pub registry_client: RpcClient,
    pub current_gateway_candidate: Arc<Mutex<Option<Gateway>>>,
    pub current_gateway: Arc<Mutex<Option<Gateway>>>,
    pub gateway_timeout: Duration,
    pub gateways: Arc<RwLock<Vec<Gateway>>>,
    pub new_payload_block_number: Arc<AtomicU64>,
    pub new_payload_block_hash: Arc<Mutex<B256>>,
    pub current_block_number: Arc<AtomicU64>,
    pub args: Arc<PortalArgs>,
    pub current_proxy: Arc<Mutex<Option<NodeGethPair>>>,
}

#[derive(Clone)]
pub struct PortalServer {
    pub inner: Arc<PortalServerInner>,
}

impl PortalServer {
    pub async fn new(args: PortalArgs) -> eyre::Result<Self> {
        let inner = PortalServerInner::new(args).await?;
        Ok(Self { inner: Arc::new(inner) })
    }
}

impl PortalServer {
    pub async fn run(&self, addr: SocketAddr) -> eyre::Result<(ServerHandle)> {
        let registry_client = self.inner.registry_client.clone();

        let self_clone = self.clone();
        let rpc_middleware = RpcServiceBuilder::new().layer_fn(move |s| {
            // let rt = tokio::runtime::Handle::current();
            // let guard = rt.block_on(self_clone.inner.current_proxy.lock());
            // let current_proxy =
            //     <std::option::Option<NodeGethPair> as Clone>::clone(&(*guard)).expect("No current proxy set");
            let current_proxy = tokio::task::block_in_place(|| {
                let rt = tokio::runtime::Handle::current();
                // Now we can block on the future inside block_in_place
                let guard = rt.block_on(self_clone.inner.current_proxy.lock());
                <std::option::Option<NodeGethPair> as Clone>::clone(&(*guard)).expect("No current proxy set")
            });
            let op_geth_client = current_proxy.inner.op_geth_client.clone();
            let op_geth_engine_client = current_proxy.inner.op_geth_engine_client.clone();
            let op_node_client = current_proxy.inner.op_node_client.clone();

            ProxyService::new(
                PORTAL_CAPABILITIES,
                s,
                op_geth_client,
                op_geth_engine_client,
                op_node_client,
                registry_client.clone(),
            )
        });

        // temp: remove when factoring out the portal
        let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);
        let cors_middleware = ServiceBuilder::new().layer(cors);
        let server = ServerBuilder::default()
            .max_request_body_size(u32::MAX)
            .max_response_body_size(u32::MAX)
            .set_rpc_middleware(rpc_middleware)
            .set_http_middleware(cors_middleware)
            .build(addr)
            .await?;

        let mut module = EngineApiServer::into_rpc((*self.inner).clone());
        module.merge(EthApiServer::into_rpc((*self.inner).clone())).expect("failed to merge modules");
        module.merge(PortalApiServer::into_rpc((*self.inner).clone())).expect("failed to merge modules");

        let self_clone = self.clone();
        tokio::spawn(async move {
            loop {
                match self_clone.inner.refresh_gateway_list().await {
                    Ok(_) => {}
                    Err(err) => {
                        error!(%err, "Failed to fetch registered gateways");
                    }
                }
                tokio::time::sleep(Duration::from_secs(1).into()).await;
            }
        });
        let server_handle = server.start(module);
        Ok(server_handle)
    }
}

impl PortalServerInner {
    pub async fn new(args: PortalArgs) -> eyre::Result<Self> {
        let registry_client =
            create_client(args.registry_url.clone(), Duration::from_millis(args.registry_timeout_ms))?;

        let gateway_timeout = Duration::from_millis(args.gateway_timeout_ms);

        let gateways = vec![];
        let gateways = Arc::new(RwLock::new(gateways));

        let temp = Self {
            registry_client,
            current_gateway_candidate: Arc::new(Mutex::new(None)),
            current_gateway: Arc::new(Mutex::new(None)),
            gateway_timeout,
            gateways,
            new_payload_block_number: Arc::new(AtomicU64::new(0)),
            new_payload_block_hash: Arc::new(Mutex::new(B256::ZERO)),
            current_block_number: Arc::new(AtomicU64::new(0)),
            args: Arc::new(args),
            current_proxy: Arc::new(Mutex::new(None)),
        };

        match temp.refresh_gateway_list().await {
            Ok(_) => {
                temp.update_current_gateway().await?;
                info!("Successfully fetched registered gateways");
            }
            Err(err) => {
                error!(%err, "Failed to fetch registered gateways");
            }
        }

        match temp.registry_client.current_gateway().await {
            Ok((block_number, _, _, _)) => {
                temp.current_block_number.store(block_number, Ordering::Relaxed);
            }
            Err(err) => {
                error!(%err, "Failed to get future gateway from registry");
            }
        };

        Ok(temp)
    }

    fn gateways(&self) -> Vec<Gateway> {
        self.gateways.read().clone()
    }

    async fn fetch_registered_gateways(&self) -> eyre::Result<()> {
        let mut gateways = vec![];
        let registered_gateways = self.registry_client.registered_gateways().await?;
        for (gateway_url, address, jwt_as_b256) in registered_gateways {
            let jwt_str = hex::encode(jwt_as_b256);
            let client = create_gateway_client(gateway_url, jwt_str.clone(), address, self.gateway_timeout);
            if let Ok(client) = client {
                gateways.push(client);
            }
        }

        for gateway in gateways.iter_mut() {
            let ping_start = Instant::now();
            match ControlApiClient::heartbeat(&gateway.client).await {
                Ok(_) => {
                    let ping_duration = ping_start.elapsed();
                    gateway.ping = Arc::new(ping_duration);
                    gateway.last_seen = Arc::new(Some(Instant::now()));
                    info!("successfully pinged gateway={} ping={:>9}", gateway.id, ping_duration.to_string());
                }
                Err(err) => {
                    error!(%err, ?gateway, "failed to ping gateway");
                }
            }
        }

        *self.gateways.write() = gateways;
        Ok(())
    }

    async fn update_current_gateway_candidate(
        &self,
        n_blocks_into_future: u64,
        expected_block_number: Option<u64>,
    ) -> eyre::Result<()> {
        let (block_number, gateway_url, _, _) = self.registry_client.get_future_gateway(n_blocks_into_future).await?;
        if let Some(expected_block_number) = expected_block_number {
            if block_number != expected_block_number {
                error!(
                    "CRITICAL: The block number we got from the registry ({}) does not match the expected block number ({})",
                    block_number, expected_block_number
                );
                panic!(
                    "CRITICAL: The block number we got from the registry ({}) does not match the expected block number ({})",
                    block_number, expected_block_number
                );
                // return Ok(());
            }
        }
        let current_gateway_index = self.gateways().iter().position(|g| g.id == gateway_url);
        match current_gateway_index {
            Some(index) => {
                let gateway = self.gateways().get(index).cloned().unwrap();
                *self.current_gateway_candidate.lock().await = Some(gateway);
                let mut i = index;
                while !self
                    .current_gateway_candidate
                    .lock()
                    .await
                    .as_ref()
                    .unwrap()
                    .is_active(self.args.gateway_inactivity_timeout_ms)
                {
                    i = (i + 1) % self.gateways().len();
                    if i == index {
                        error!("CRITICAL: No gateway is available, all gateways are stale");
                        return Ok(());
                    }
                    *self.current_gateway_candidate.lock().await = Some(self.gateways().get(i).cloned().unwrap());
                }
            }
            None => {
                error!(
                    "CRITICAL: Couldn't find the current gateway in the list we got from the registry. This means the registry is inconsistent"
                );
            }
        }

        Ok(())
    }

    async fn update_current_gateway(&self) -> eyre::Result<()> {
        match self.current_gateway_candidate.lock().await.clone() {
            Some(new_gateway) => {
                self.current_gateway.lock().await.replace(new_gateway);
            }
            None => {
                error!("CRITICAL: Couldn't find the current gateway");
            }
        }
        // match self.current_gateway.lock().await.replace() {
        //     Some(old_gateway) => {
        //         info!(?old_gateway, "updated current gateway");
        //     }
        //     None => {
        //         error!("CRITICAL: Couldn't find the current gateway");
        //     }
        // }
        Ok(())
    }

    pub async fn refresh_gateway_list(&self) -> eyre::Result<()> {
        self.fetch_registered_gateways().await?;
        // self.update_current_gateway_candidate(0, None).await?;
        Ok(())
    }

    pub async fn on_fork(&self, expected_block_number: Option<u64>) -> eyre::Result<()> {
        self.update_current_gateway_candidate(0, expected_block_number).await?;
        self.update_current_gateway().await?;
        self.current_block_number.store(0, Ordering::Relaxed);
        Ok(())
    }

    async fn send_fcu(
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<OpPayloadAttributes>,
        gateway: Gateway,
    ) {
        match gateway.client.fork_choice_updated_v3(fork_choice_state, payload_attributes).await {
            Ok(res) => {
                if res.is_valid() {
                    trace!(?gateway, ?res, "gateway response");
                } else {
                    trace!(?gateway, ?res, "Error: gateway response");
                }
            }
            Err(err) => trace!(%err, "Error: failed gateway"),
        }
        debug!(?gateway, "served fcu")
    }

    pub async fn set_current_proxy(&self, proxy: NodeGethPair) {
        let mut guard = self.current_proxy.lock().await;
        (*guard) = Some(proxy);
    }
}

/// This is a temporary API to broacast transactions to both gateway and fallback. In practice this should not be
/// receiving user facing calls so we need to find another way to do this
#[async_trait]
impl EthApiServer for PortalServerInner {
    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        // send to gateways and fallback
        for gateway in self.gateways() {
            let bytes = bytes.clone();
            tokio::spawn(async move {
                if let Err(err) = gateway.client.send_raw_transaction(bytes).await {
                    error!(%err, ?gateway, "eth_sendRawTransaction: failed to send to gateway");
                }
            });
        }

        let op_geth_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_client.clone()
        };
        let response = op_geth_client.send_raw_transaction(bytes).await?;
        Ok(response)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<OpTransactionReceipt>> {
        debug!(%hash, "new request");

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let fallback_fut = tokio::spawn(
            {
                let client = op_geth_engine_client.clone();
                async move { client.transaction_receipt(hash).await }
            }
            .in_current_span(),
        );

        let Some(current_gateway) = self.current_gateway.lock().await.clone() else { return Ok(fallback_fut.await??) };

        let gateway_fut =
            tokio::spawn({ async move { current_gateway.client.transaction_receipt(hash).await } }.in_current_span());

        let (fallback, gateway) = tokio::join!(fallback_fut, gateway_fut);
        // ignore join errors
        let fallback = fallback?;
        let gateway = gateway?;

        let payload = gateway.or(fallback)?;

        Ok(payload)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_by_number(&self, number: BlockNumberOrTag, full: bool) -> RpcResult<Option<OpRpcBlock>> {
        debug!(%number, full, "new request");

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let fallback_fut = tokio::spawn(
            {
                let client = op_geth_engine_client.clone();
                async move { client.block_by_number(number, full).await }
            }
            .in_current_span(),
        );

        let Some(current_gateway) = self.current_gateway.lock().await.clone() else { return Ok(fallback_fut.await??) };

        let gateway_fut = tokio::spawn(
            { async move { current_gateway.client.block_by_number(number, full).await } }.in_current_span(),
        );

        let (fallback, gateway) = tokio::join!(fallback_fut, gateway_fut);
        // ignore join errors
        let fallback = fallback?;
        let gateway = gateway?;

        let payload = gateway.or(fallback)?;

        Ok(payload)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_by_hash(&self, hash: B256, full: bool) -> RpcResult<Option<OpRpcBlock>> {
        debug!(%hash, full, "new request");

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let fallback_fut = tokio::spawn(
            {
                let client = op_geth_engine_client.clone();
                async move { client.block_by_hash(hash, full).await }
            }
            .in_current_span(),
        );
        let Some(current_gateway) = self.current_gateway.lock().await.clone() else { return Ok(fallback_fut.await??) };
        let gateway_fut =
            tokio::spawn({ async move { current_gateway.client.block_by_hash(hash, full).await } }.in_current_span());

        let (fallback, gateway) = tokio::join!(fallback_fut, gateway_fut);
        // ignore join errors
        let fallback = fallback?;
        let gateway = gateway?;

        let payload = gateway.or(fallback)?;

        Ok(payload)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn block_number(&self) -> RpcResult<U256> {
        debug!("block number request");

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let fallback_fut = tokio::spawn(
            {
                let client = op_geth_engine_client.clone();
                async move { client.block_number().await }
            }
            .in_current_span(),
        );
        let Some(current_gateway) = self.current_gateway.lock().await.clone() else { return Ok(fallback_fut.await??) };
        let gateway_fut =
            tokio::spawn({ async move { current_gateway.client.block_number().await } }.in_current_span());

        let (fallback, gateway) = tokio::join!(fallback_fut, gateway_fut);
        // ignore join errors
        let fallback = fallback?;
        let gateway = gateway?;

        let payload = gateway.or(fallback)?;

        Ok(payload)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn transaction_count(&self, address: Address, block_number: Option<BlockId>) -> RpcResult<U256> {
        debug!(%address, ?block_number, "new request");

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let fallback_fut = tokio::spawn(
            {
                let client = op_geth_engine_client.clone();
                async move { client.transaction_count(address, block_number).await }
            }
            .in_current_span(),
        );
        let Some(current_gateway) = self.current_gateway.lock().await.clone() else { return Ok(fallback_fut.await??) };
        let gateway_fut = tokio::spawn(
            { async move { current_gateway.client.transaction_count(address, block_number).await } }.in_current_span(),
        );

        let (fallback, gateway) = tokio::join!(fallback_fut, gateway_fut);
        // ignore join errors
        let fallback = fallback?;
        let gateway = gateway?;

        let payload = gateway.or(fallback)?;

        Ok(payload)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::TRACE))]
    async fn balance(&self, address: Address, block_number: Option<BlockId>) -> RpcResult<U256> {
        debug!(%address, ?block_number, "new request");

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let fallback_fut = tokio::spawn(
            {
                let client = op_geth_engine_client.clone();
                async move { client.balance(address, block_number).await }
            }
            .in_current_span(),
        );
        let Some(current_gateway) = self.current_gateway.lock().await.clone() else { return Ok(fallback_fut.await??) };
        let gateway_fut = tokio::spawn(
            { async move { current_gateway.client.balance(address, block_number).await } }.in_current_span(),
        );

        let (fallback, gateway) = tokio::join!(fallback_fut, gateway_fut);
        // ignore join errors
        let fallback = fallback?;
        let gateway = gateway?;

        let payload = gateway.or(fallback)?;

        Ok(payload)
    }
}

#[async_trait]
impl EngineApiServer for PortalServerInner {
    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn fork_choice_updated_v3(
        &self,
        fork_choice_state: ForkchoiceState,
        payload_attributes: Option<OpPayloadAttributes>,
    ) -> RpcResult<ForkchoiceUpdated> {
        let parent_block_hash = fork_choice_state.head_block_hash;

        if let Some(payload_attributes) = payload_attributes.as_ref() {
            let no_tx_pool = payload_attributes.no_tx_pool.unwrap_or(false);
            let gas_limit = payload_attributes.gas_limit.unwrap_or(0);
            debug!(parent_block_hash = %parent_block_hash, no_tx_pool = %no_tx_pool, gas_limit = %gas_limit, "new request (with attributes)");
        } else {
            debug!(%parent_block_hash, "new request (no attributes)");
        }

        if payload_attributes.is_some() && *self.new_payload_block_hash.lock().await == parent_block_hash {
            let new_block_number = self.new_payload_block_number.load(Ordering::Relaxed) + 1;
            self.current_block_number.store(new_block_number, Ordering::Relaxed);
            match self.on_fork(Some(new_block_number)).await {
                Ok(_) => {}
                Err(err) => {
                    error!(%err, "failed to process new payload");
                }
            };
        }

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let response =
            op_geth_engine_client.fork_choice_updated_v3(fork_choice_state, payload_attributes.clone()).await?;

        if let Some(current_gateway) = self.current_gateway.as_ref().lock().await.clone() {
            if payload_attributes.is_some() {
                // pick only one gateway for this block
                tokio::spawn(Self::send_fcu(fork_choice_state, payload_attributes, current_gateway).in_current_span());
            } else {
                // send to all gateways
                for gateway in self.gateways() {
                    let payload_attributes = payload_attributes.clone();
                    tokio::spawn(Self::send_fcu(fork_choice_state, payload_attributes, gateway).in_current_span());
                }
            }
        }

        Ok(response)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn new_payload_v4(
        &self,
        payload: OpExecutionPayloadV4,
        versioned_hashes: Vec<B256>,
        parent_beacon_block_root: B256,
        requests: RequestsOrHash,
    ) -> RpcResult<PayloadStatus> {
        let block_number = payload.payload_inner.payload_inner.payload_inner.block_number;
        let block_hash = payload.payload_inner.payload_inner.payload_inner.block_hash;
        let gas_limit = payload.payload_inner.payload_inner.payload_inner.gas_limit;
        let gas_used = payload.payload_inner.payload_inner.payload_inner.gas_used;
        let n_txs = payload.payload_inner.payload_inner.payload_inner.transactions.len();
        let n_withdrawals = payload.payload_inner.payload_inner.withdrawals.len();
        let blob_gas_used = payload.payload_inner.blob_gas_used;
        let excess_blob_gas = payload.payload_inner.excess_blob_gas;

        debug!(block_number, %block_hash, gas_limit, gas_used, n_txs, n_withdrawals, blob_gas_used, excess_blob_gas, "new request");
        self.new_payload_block_number.store(block_number, Ordering::Relaxed);
        *self.new_payload_block_hash.lock().await = block_hash;

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let response = op_geth_engine_client
            .new_payload_v4(payload.clone(), versioned_hashes.clone(), parent_beacon_block_root, requests.clone())
            .await
            .inspect_err(|e| tracing::error!("issue sending new_payload_v4 to el {e}"))?;

        // send to all gateways
        for gateway in self.gateways() {
            let payload = payload.clone();
            let requests = requests.clone();
            let versioned_hashes = versioned_hashes.clone();

            tokio::spawn(
                async move {
                    match gateway
                        .client
                        .new_payload_v4(payload, versioned_hashes, parent_beacon_block_root, requests)
                        .await
                    {
                        Ok(res) => {
                            if res.is_valid() {
                                debug!(?gateway, ?res, "gateway response");
                            } else {
                                error!(?gateway, ?res, "gateway response");
                            }
                        }
                        Err(ClientError::Call(_)) => {}
                        Err(err) => error!(?gateway, %err, "failed gateway"),
                    }
                }
                .in_current_span(),
            );
        }

        Ok(response)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn new_payload_v3(
        &self,
        payload: ExecutionPayloadV3,
        versioned_hashes: Vec<B256>,
        parent_beacon_block_root: B256,
    ) -> RpcResult<PayloadStatus> {
        let block_number = payload.payload_inner.payload_inner.block_number;
        let block_hash = payload.payload_inner.payload_inner.block_hash;
        let gas_limit = payload.payload_inner.payload_inner.gas_limit;
        let gas_used = payload.payload_inner.payload_inner.gas_used;
        let n_txs = payload.payload_inner.payload_inner.transactions.len();
        let n_withdrawals = payload.payload_inner.withdrawals.len();
        let blob_gas_used = payload.blob_gas_used;
        let excess_blob_gas = payload.excess_blob_gas;

        debug!(block_number, %block_hash, gas_limit, gas_used, n_txs, n_withdrawals, blob_gas_used, excess_blob_gas, "new request");
        self.new_payload_block_number.store(block_number, Ordering::Relaxed);
        *self.new_payload_block_hash.lock().await = block_hash;

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let response = op_geth_engine_client
            .new_payload_v3(payload.clone(), versioned_hashes.clone(), parent_beacon_block_root)
            .await?;

        // send to all gateways
        for gateway in self.gateways() {
            let payload = payload.clone();
            let versioned_hashes = versioned_hashes.clone();

            tokio::spawn(
                async move {
                    match gateway.client.new_payload_v3(payload, versioned_hashes, parent_beacon_block_root).await {
                        Ok(res) => {
                            if res.is_valid() {
                                debug!(?gateway, ?res, "gateway response");
                            } else {
                                error!(?gateway, ?res, "gateway response");
                            }
                        }
                        Err(ClientError::Call(_)) => {}
                        Err(err) => error!(?gateway, %err, "failed gateway"),
                    }
                }
                .in_current_span(),
            );
        }

        Ok(response)
    }

    #[tracing::instrument(skip_all, err, ret(level = Level::DEBUG), fields(req_id = %uuid()))]
    async fn get_payload_v4(&self, payload_id: PayloadId) -> RpcResult<OpExecutionPayloadEnvelopeV4> {
        debug!(%payload_id, "new request");

        let op_geth_engine_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_engine_client.clone()
        };
        let fallback_fut = tokio::spawn({
            let client = op_geth_engine_client.clone();

            async move { client.get_payload_v4(payload_id).await }
        });

        let Some(gateway) = self.current_gateway.lock().await.clone() else { return Ok(fallback_fut.await??) };

        let gateway_fut: tokio::task::JoinHandle<Result<OpExecutionPayloadEnvelopeV4, _>> = tokio::spawn(
            {
                // only get payload from previously picked gateway
                let fallback_client = op_geth_engine_client.clone();

                async move {
                    let gateway_payload = gateway
                        .client
                        .get_payload_v4(payload_id)
                        .await
                        .inspect_err(|err| error!(%err, "failed gateway"))?;

                    let payload_status = fallback_client
                        .new_payload_v4(
                            OpExecutionPayloadV4 {
                                payload_inner: gateway_payload.execution_payload.payload_inner.clone(),
                                withdrawals_root: gateway_payload.execution_payload.withdrawals_root,
                            },
                            vec![],
                            gateway_payload.parent_beacon_block_root,
                            RequestsOrHash::default(),
                        )
                        .await
                        .inspect_err(|err| error!(%err, "failed fallback validation"))?;

                    if payload_status.is_valid() {
                        trace!(?gateway, ?gateway_payload, ?payload_status, "gateway response");
                        Ok(gateway_payload)
                    } else {
                        error!(?gateway, ?gateway_payload, ?payload_status, "gateway response");
                        Err(RpcError::Internal)
                    }
                }
            }
            .in_current_span(),
        );

        let (fallback, gateway) = tokio::join!(fallback_fut, gateway_fut);

        // ignore join errors
        let fallback = fallback?;
        let gateway = gateway?;
        if let Ok(gateway) = gateway.as_ref() {
            info!(
                "block {}: successfully served from based-gateway {:?}",
                gateway.execution_payload.payload_inner.payload_inner.payload_inner.block_number,
                self.current_gateway.lock().await.as_ref().unwrap()
            );
        } else if let Ok(fallback) = fallback.as_ref() {
            info!(
                "block {}: successfully served from fallback",
                fallback.execution_payload.payload_inner.payload_inner.payload_inner.block_number
            );
        } else {
            error!("couldn't serve a block from fallback or gateway");
        }

        let payload = gateway.or(fallback)?;
        Ok(payload)
    }
}

#[async_trait]
impl PortalApiServer for PortalServerInner {
    /// The network id of the l2
    async fn l2_chain_id(&self) -> RpcResult<u64> {
        let op_node_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_node_client.clone()
        };
        Ok(op_node_client.rollup_config().await.map(|config| config.l2_chain_id)?)
    }

    /// The network id of the l1
    async fn l1_chain_id(&self) -> RpcResult<u64> {
        let op_node_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_node_client.clone()
        };
        Ok(op_node_client.rollup_config().await.map(|config| config.l1_chain_id)?)
    }

    /// rollup.json file
    async fn file_rollup(&self) -> RpcResult<String> {
        let genesis_path = self.args.config_dir.join("rollup.json");
        Ok(std::fs::read_to_string(genesis_path)?)
    }

    /// genesis.json file
    async fn file_genesis(&self) -> RpcResult<String> {
        let genesis_path = self.args.config_dir.join("genesis.json");
        Ok(std::fs::read_to_string(genesis_path)?)
    }

    /// The gossip static address string used by the op-node
    async fn op_node_gossip_static(&self) -> RpcResult<String> {
        let op_node_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_node_client.clone()
        };
        Ok(op_node_client.peer_info().await.and_then(|p| {
            p.addresses.last().cloned().map(Ok).unwrap_or(Err(ClientError::Custom("empty peer addresses".to_string())))
        })?)
    }

    /// The enr that can be used to sync with the op-node
    async fn op_node_bootnode_enr(&self) -> RpcResult<String> {
        let op_node_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_node_client.clone()
        };
        Ok(op_node_client.peer_info().await.map(|p| p.enr)?)
    }

    /// The enode that can be used to sync with the op-geth
    async fn op_geth_bootnode_enode(&self) -> RpcResult<String> {
        let op_geth_client = {
            let guard = self.current_proxy.lock().await;
            guard.as_ref().expect("No current proxy set").inner.op_geth_client.clone()
        };
        Ok(op_geth_client.node_info().await.map(|p| p.enode)?)
    }
}

// TODO: Implement this properly
#[async_trait]
impl RegistryApiServer for PortalServerInner {
    async fn get_future_gateway(&self, n_blocks_into_future: u64) -> RpcResult<(u64, Url, Address, B256)> {
        match self.registry_client.get_future_gateway(n_blocks_into_future).await {
            Ok(gateway) => Ok(gateway),
            Err(err) => {
                error!(%err, "Failed to get future gateway");
                Err(RpcError::Internal)
            }
        }
    }

    async fn current_gateway(&self) -> RpcResult<(u64, Url, Address, B256)> {
        match self.current_gateway.lock().await.as_ref() {
            Some(gateway) => {
                let block_number = self.current_block_number.load(Ordering::Relaxed);
                let url = gateway.id.clone();
                let address = gateway.address;
                let jwt_as_b256 = gateway.jwt.as_bytes().try_into().map_err(|_| RpcError::Internal)?;

                Ok((block_number, url, address, jwt_as_b256))
            }
            None => Err(RpcError::Internal),
        }
    }

    async fn registered_gateways(&self) -> RpcResult<Vec<(Url, Address, B256)>> {
        match self.registry_client.registered_gateways().await {
            Ok(gateways) => Ok(gateways),
            Err(err) => {
                error!(%err, "Failed to get registered gateways");
                Err(RpcError::Internal)
            }
        }
    }

    async fn register_gateway(&self, gateway: (Url, Address, B256)) -> RpcResult<()> {
        match self.registry_client.register_gateway(gateway).await {
            Ok(()) => Ok(()),
            Err(err) => {
                error!(%err, "Failed to register gateway");
                Err(RpcError::Internal)
            }
        }
    }
}

fn create_gateway_client(url: Url, jwt_str: String, address: Address, timeout: Duration) -> eyre::Result<Gateway> {
    let jwt = JwtSecret::from_hex(&jwt_str).map_err(|_| eyre::eyre!("Invalid JWT secret"))?;
    let client = create_auth_client(url.clone(), jwt, timeout)?;
    let gateway_client = Gateway::new(url, client, jwt_str, address);
    Ok(gateway_client)
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
