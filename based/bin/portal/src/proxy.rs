use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr}, ops::Deref, sync::
        Arc
    
};

use alloy_primitives::B256;
use bop_common::{
    api::{
        EthApiClient, OpNodeAdminApiClient, OpNodeApiClient, OpNodeP2PApiClient,
    }, time::Duration}
;
use futures::{future::BoxFuture, FutureExt};
use jsonrpsee::{
    http_client::{transport::HttpBackend, HttpClient},
    server::{middleware::rpc::RpcServiceT, RpcServiceBuilder, ServerBuilder, ServerHandle}, types::{error::{INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG}, ErrorObject, Params, Request, ResponsePayload}, Methods,
};
use reqwest::Url;
use reth_rpc_layer::{AuthClientService, JwtSecret};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::Mutex;
use tracing::warn;

use jsonrpsee::{
    MethodResponse,
    core::{client::ClientT, traits::ToRpcParams},
    };

use crate::{server::{AuthRpcClient, RpcClient}, utils::{create_auth_client, create_client}};


#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NodeGethPairArgs {
    pub op_geth_url: String,
    pub op_node_url: String,
    pub op_geth_engine_url: String,
    pub op_geth_engine_jwt: String,
    pub port: u16,
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
                let ingress_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), proxy.port);

                let client_op_node = create_client(op_node_url, timeout).ok()?;
                let client_op_geth = create_client(op_geth_url, timeout).ok()?;
                let client_op_geth_engine = create_auth_client(op_geth_engine_url, op_geth_engine_jwt, timeout).ok()?;
                let inner = NodeGethPairInner {
                        client_op_node,
                        client_op_geth,
                        client_op_geth_engine,
                        ingress_addr,
                    };


                    Some(NodeGethPair(Arc::new(inner)))
                }).collect()
    }
}
#[derive(Clone)]
pub struct NodeGethPairInner {
    pub client_op_geth:        RpcClient,
    pub client_op_geth_engine: AuthRpcClient,
    pub client_op_node:        RpcClient,
    pub ingress_addr:          SocketAddr
}

#[derive(Clone)]
pub struct NodeGethPair(Arc<NodeGethPairInner>);

impl Deref for NodeGethPair {
    type Target = Arc<NodeGethPairInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl NodeGethPair {
        pub async fn run(&self, forwarding_engine_geth_client: Arc<Mutex<AuthRpcClient>>, ingress_addr: SocketAddr) -> eyre::Result<ServerHandle> {
        // Clone the necessary fields before moving into the closure
        let rpc_middleware = RpcServiceBuilder::new().layer_fn(move |_s| {
            ActualProxyService::new(forwarding_engine_geth_client.clone())
        });

        let server = ServerBuilder::default()
            .max_request_body_size(u32::MAX)
            .max_response_body_size(u32::MAX)
            .set_rpc_middleware(rpc_middleware)
            .build(ingress_addr)
            .await?;


        let server_handle = server.start(Methods::new());
        Ok(server_handle)
    }

    pub async fn get_current_unsafe_l2(&self) -> B256 {
        match self.client_op_node.sync_status().await {
            Ok(status) => status.unsafe_l2.hash,
            Err(_err) => B256::ZERO,
        }
    }

    pub async fn get_chain_id(&self) -> eyre::Result<String> {
        let info = self.client_op_geth.chain_id().await;
        match info {
            Ok(info) => Ok(info),
            Err(err) => Err(eyre::eyre!("Failed to get chain ID: {}", err)),
        }
    }

    pub async fn pair_node_p2p(&self, other: &NodeGethPair) -> eyre::Result<()> {
        let multi_address_self = self.client_op_node.peer_info().await?.addresses[0].clone();
        let multi_address_other = other.client_op_node.peer_info().await?.addresses[0].clone();
        self.client_op_node.connect_peer(multi_address_other.clone()).await?;
        other.client_op_node.connect_peer(multi_address_self.clone()).await?;
        Ok(())
    }

    pub async fn start_sequencer(&self, head: B256) -> eyre::Result<()> {
        match self.client_op_node.start_sequencer(head).await {
            Ok(_) => Ok(()),
            Err(err) => Err(eyre::eyre!("Failed to start sequencer: {}", err)),
        }
    }

    pub async fn stop_sequencer(&self) -> eyre::Result<()> {
        match self.client_op_node.stop_sequencer().await {
            Ok(_) => Ok(()),
            Err(err) => Err(eyre::eyre!("Failed to stop sequencer: {}", err)),
        }
    }

    pub async fn sequencer_active(&self) -> bool {
        match self.client_op_node.sequencer_active().await {
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

// TODO: remove this
struct WrapParams<'a>(Params<'a>);
impl ToRpcParams for WrapParams<'_> {
    fn to_rpc_params(self) -> Result<Option<Box<RawValue>>, serde_json::Error> {
        // FIXME: we should not clone here
        self.0.as_str().map(String::from).map(RawValue::from_string).transpose()
    }
}
#[derive(Clone)]
pub struct ActualProxyService {
    forward_to: Arc<Mutex<AuthRpcClient>>,
}

impl ActualProxyService{
    pub fn new(forward_to: Arc<Mutex<HttpClient<AuthClientService<HttpBackend>>>>) -> Self {
        Self { forward_to }
    }
}

impl<'a> RpcServiceT<'a> for ActualProxyService
{
    type Future = BoxFuture<'a, MethodResponse>;

    #[tracing::instrument(skip_all, name = "middleware")]
    fn call(&self, req: Request<'a>) -> Self::Future {
        //TODO: is this really the best way to do this?
        let forward_to = self.forward_to.clone();
        async move {
            let params = WrapParams(req.params());
            let r: Result<serde_json::Value, jsonrpsee::core::ClientError> = {
                forward_to.lock().await.request(req.method_name(), params).await
            };

            match r {
                Ok(r) => {
                    let payload = ResponsePayload::success(r);
                    MethodResponse::response(req.id, payload.into(), 4_000_000_000usize)
                }
                Err(_err) => {
                    MethodResponse::error(req.id, ErrorObject::borrowed(INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, None))
                }
            }
        }
        .boxed()
    }
}
