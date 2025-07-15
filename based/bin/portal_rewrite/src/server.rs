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
    server::{RpcServiceBuilder, ServerBuilder},
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

use crate::{
    cli::PortalArgs,
    clients::{create_client, GatewayManager, NodeGeth},
    middleware::EngineApiRouter,
};

#[derive(Clone)]
pub struct PortalServer {
    node_geth: Arc<NodeGeth>,
    gateway_manager: Arc<GatewayManager>,
    args: Arc<PortalArgs>,
}

impl PortalServer {
    pub async fn new(args: PortalArgs) -> Self{
        let node_geth = NodeGeth::new_from_args(&args);
        let gateway_manager = GatewayManager::new_from_args(&args);
        Self {
            node_geth: Arc::new(node_geth),
            gateway_manager: Arc::new(gateway_manager),
            args: Arc::new(args),
        }
    }
    pub async fn run(&self){

    }
}

#[async_trait]
impl PortalApiServer for PortalServer {
    /// The network id of the l2
    async fn l2_chain_id(&self) -> RpcResult<u64> {
        Ok(self.node_geth.op_node_client.rollup_config().await.map(|config| config.l2_chain_id)?)
    }

    /// The network id of the l1
    async fn l1_chain_id(&self) -> RpcResult<u64> {
        Ok(self.node_geth.op_node_client.rollup_config().await.map(|config| config.l1_chain_id)?)
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
        Ok(self.node_geth.op_node_client.peer_info().await.and_then(|p| {
            p.addresses.last().cloned().map(Ok).unwrap_or(Err(ClientError::Custom("empty peer addresses".to_string())))
        })?)
    }

    /// The enr that can be used to sync with the op-node
    async fn op_node_bootnode_enr(&self) -> RpcResult<String> {
        Ok(self.node_geth.op_node_client.peer_info().await.map(|p| p.enr)?)
    }

    /// The enode that can be used to sync with the op-geth
    async fn op_geth_bootnode_enode(&self) -> RpcResult<String> {
        Ok(self.node_geth.op_geth_client.node_info().await.map(|p| p.enode)?)
    }
}

impl NodeGeth {
    pub fn new_from_args(args: &PortalArgs) -> Self {
        let geth_engine_jwt = args.fallback_jwt();

        let op_node_client_url = args.op_node_url.clone();
        let op_geth_client_url = args.fallback_eth_url.clone();
        let op_geth_engine_client_url = args.fallback_url.clone();

        Self::new(
            op_node_client_url,
            op_geth_client_url,
            op_geth_engine_client_url,
            geth_engine_jwt,
            args.portal_port, // TODO: create a separate port for engine proxy.
        )
    }
}

impl GatewayManager {
    pub fn new_from_args(args: &PortalArgs) -> Self {
        let registry_client_url = args.registry_url.clone();
        let timeout = Duration::from_millis(args.fallback_timeout_ms);
        let registry_client = create_client(registry_client_url, timeout).unwrap();
        Self::new(registry_client)
    }
}