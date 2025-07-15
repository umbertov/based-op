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
    engine::{ExecutionPayloadV3, ForkchoiceState, ForkchoiceUpdated, PayloadId, PayloadStatus},
};
use bop_common::{
    api::{
        ControlApiClient, EngineApiClient, EngineApiServer, EthApiClient, EthApiServer, OpGethAdminApiClient, OpNodeApiClient, OpNodeP2PApiClient, OpRpcBlock, PortalApiServer, RegistryApiClient, RegistryApiServer, PORTAL_CAPABILITIES
    }, communication::messages::{RpcError, RpcResult}, debug_panic, time::{Duration, Instant}, utils::{uuid, wait_for_signal}
};
use jsonrpsee::{
    core::{async_trait, ClientError},
    http_client::{transport::HttpBackend, HttpClientBuilder},
    server::{RpcServiceBuilder, ServerBuilder}, Methods,
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

use crate::{cli::PortalArgs, middleware::EngineApiRouter};
use indexmap::IndexMap;

pub type RpcClient = jsonrpsee::http_client::HttpClient;
pub type AuthRpcClient = jsonrpsee::http_client::HttpClient<AuthClientService<HttpBackend>>;

#[derive(Clone)]
struct Gateway {
    url: Url,
    jwt: JwtSecret,
    address: Address,
    client: AuthRpcClient,
    ping: Arc<RwLock<Duration>>,
    active: Arc<AtomicBool>,
    registry_index: Arc<AtomicU64>,
}

impl Gateway {
    pub async fn health_check(&self) {
        let ping_start = Instant::now();
        match ControlApiClient::heartbeat(&self.client).await {
            Ok(_) => {
                let ping_duration = ping_start.elapsed();
                *self.ping.write() = ping_duration;
                self.active.store(true, Ordering::Relaxed);
                info!("successfully pinged gateway={} ping={:>9}", self.url, ping_duration.to_string());
            }
            Err(err) => {
                error!(%err, ?self, "failed to ping gateway");
                self.active.store(false, Ordering::Relaxed);
            }
        }
    }

    pub fn is_active(&self) -> bool {
        return self.active.load(Ordering::Relaxed);
    }
}

impl fmt::Debug for Gateway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.url)
    }
}

impl Gateway {
    fn new(url: Url, client: AuthRpcClient, jwt: JwtSecret, address: Address, registry_index: usize) -> Self {
        Self {
            url,
            jwt,
            address,
            client,
            ping: Arc::new(RwLock::new(Duration::from_millis(0))),
            active: Arc::new(AtomicBool::new(true)),
            registry_index: Arc::new(AtomicU64::new(registry_index as u64)),
        }
    }
}

type GatewayInstance = Arc<RwLock<Gateway>>;

pub struct GatewayManager {
    gateways: Arc<RwLock<IndexMap<Url, GatewayInstance>>>,
    registry_client: RpcClient,
    current_gateway: Arc<RwLock<Option<GatewayInstance>>>,
}

impl GatewayManager {
    pub fn new(registry_client: RpcClient) -> Self {
        Self {
            gateways: Arc::new(RwLock::new(IndexMap::new())),
            registry_client,
            current_gateway: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn update_gateway_list(&self) -> eyre::Result<()> {
        let raw_gateways = self.registry_client.registered_gateways().await?;
        let mut gateways = self.gateways.write();

        let timeout = Duration::from_millis(1000);

        for (index, (url, address, jwt_as_b256)) in raw_gateways.iter().enumerate() {
            let jwt_as_str = hex::encode(jwt_as_b256);
            let jwt = JwtSecret::from_hex(&jwt_as_str).map_err(|_| eyre::eyre!("Invalid JWT secret"))?;
            match gateways.get(url) {
                Some(gateway) => {
                    let mut gateway = gateway.write();
                    gateway.jwt = jwt;
                    gateway.address = address.clone();
                    gateway.registry_index.store(index as u64, Ordering::Relaxed);
                }
                None => {
                    let client = create_auth_client(url.clone(), jwt, timeout)?;
                    let gateway = Gateway::new(url.clone(), client, jwt, address.clone(), index);
                    gateways.insert(url.clone(), Arc::new(RwLock::new(gateway)));
                }
            }
        }

        gateways.retain(|url, _| raw_gateways.iter().any(|(u, _, _)| u == url));

        if gateways.len() != raw_gateways.len() {
            error!("Mismatch in number of gateways: expected {}, found {}", raw_gateways.len(), gateways.len());
            debug_panic!("Mismatch in number of gateways: expected {}, found {}", raw_gateways.len(), gateways.len());
        }

        gateways.sort_by(|_, v1, _, v2| {
            let index1 = v1.read().registry_index.load(Ordering::Relaxed);
            let index2 = v2.read().registry_index.load(Ordering::Relaxed);
            index1.cmp(&index2)
        });

        Ok(())
    }

    fn get_next_available_gateway(&self, start_index: usize) -> Option<Arc<RwLock<Gateway>>> {
        let gateways = self.gateways.read();
        let len = gateways.len();
        for i in 0..len {
            let index = (start_index + i) % len;
            if let Some((_, gateway)) = gateways.get_index(index) {
                if gateway.read().is_active() {
                    return Some(Arc::clone(gateway));
                }
            }
        }
        None
    }

    pub async fn decide_current_gateway(&self) -> Option<Arc<RwLock<Gateway>>> {
        let gateways = self.gateways.read();
        match self.registry_client.current_gateway().await {
            Ok((_, current_registry_gateway_url, _, _)) => {
                match gateways.get_index_of(&current_registry_gateway_url){
                    Some(index) => {
                        let result = self.get_next_available_gateway(index);
                        if let Some(gateway) = &result {
                            self.current_gateway.write().replace(Arc::clone(gateway));
                        }
                        result
                    }
                    None => {
                        error!("Current registry gateway not found in local list: {}", current_registry_gateway_url);
                        None
                    }
                }
            }
            Err(_) => {
                error!("Failed to fetch current gateway from registry");
                None
            }
        }
    }

    pub fn current_gateway(&self) -> Option<GatewayInstance> {
        self.current_gateway.read().as_ref().cloned()
    }
}

pub struct NodeGeth {
    pub op_node_client: RpcClient,
    pub op_geth_client: RpcClient,
    pub op_geth_engine_client: AuthRpcClient,
    pub op_geth_engine_jwt: JwtSecret,
    pub engine_proxy_ingress_port: u16,
}

impl NodeGeth {
    pub fn new(
        op_node_client_url: Url,
        op_geth_client_url: Url,
        op_geth_engine_client_url: Url,
        op_geth_engine_jwt: JwtSecret,
        engine_proxy_ingress_port: u16,
    ) -> Self {
        let timeout = Duration::from_millis(1000);
        Self {
            op_node_client: create_client(op_node_client_url, timeout).unwrap(),
            op_geth_client: create_client(op_geth_client_url, timeout).unwrap(),
            op_geth_engine_client: create_auth_client(op_geth_engine_client_url, op_geth_engine_jwt, timeout).unwrap(),
            op_geth_engine_jwt,
            engine_proxy_ingress_port,
        }
    }
}

pub fn create_client(url: Url, timeout: Duration) -> eyre::Result<RpcClient> {
    let client = HttpClientBuilder::default()
        .max_request_size(u32::MAX)
        .max_response_size(u32::MAX)
        .request_timeout(timeout.into())
        .build(url)?;
    Ok(client)
}

pub fn create_auth_client(url: Url, jwt: JwtSecret, timeout: Duration) -> eyre::Result<AuthRpcClient> {
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
