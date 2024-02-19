//! StreamingFast Blockmeta gRPC client.

use std::time::Duration;

use alloy_primitives::bytes::Bytes;
use thegraph::types::{BlockHash, BlockNumber};
use tonic::codegen::{Body, InterceptedService, StdError};
use tonic::transport::{Channel, Uri};

pub use self::auth::AuthInterceptor;
use self::gen::block_client::BlockClient;
pub use self::gen::BlockResp as Block;
use self::gen::Empty;
use self::gen::IdToNumReq;
use self::gen::NumToIdReq;

/// These files are **generated** by the `build.rs` script when compiling the crate with the
/// `proto-gen` feature enabled. The `build.rs` script uses the `tonic-build` crate to generate
/// the files.
///
/// ```shell
/// cargo build -p gateway-framework --features proto-gen
/// ```
mod gen {
    include!("blockmeta/sf.blockmeta.v2.rs");
}

mod auth {
    use tonic::{Request, Status};

    /// The `AuthInterceptor` is a gRPC interceptor that adds an `authorization` header to the request
    /// metadata.
    ///
    /// This middleware inserts the `authorization` header into the request metadata. The header is
    /// expected to be in the format `Bearer <token>`.
    ///
    /// It is used to authenticate requests to the StreamingFast Blockmeta service.
    pub struct AuthInterceptor {
        header_value: String,
    }

    impl AuthInterceptor {
        /// Create a new `AuthInterceptor` with the given authorization token.
        pub(in crate::chains) fn with_token(token: &str) -> Self {
            Self {
                header_value: format!("bearer {}", token),
            }
        }
    }

    impl tonic::service::Interceptor for AuthInterceptor {
        fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
            // The `authorization` header is expected to be in the format `Bearer <token>`
            let auth = self.header_value.parse().map_err(|err| {
                Status::new(
                    tonic::Code::Unauthenticated,
                    format!("invalid authorization token: {}", err),
                )
            })?;

            // Insert the `authorization` header into the request metadata
            request.metadata_mut().insert("authorization", auth);
            Ok(request)
        }
    }
}

/// StreamingFast Blockmeta gRPC client.
///
/// The [`BlockmetaClient`] is a gRPC client for the StreamingFast Blockmeta service. It provides
/// methods to fetch blocks by hash, number, and the latest block.
#[derive(Debug, Clone)]
pub struct BlockmetaClient<T> {
    rpc_client: BlockClient<T>,
}

impl BlockmetaClient<Channel> {
    /// Create a new [`BlockmetaClient`] with the given gRPC endpoint.
    ///
    /// The service will connect once the first request is made. It will attempt to connect for
    /// 5 seconds before timing out.
    pub fn new(endpoint: Uri) -> Self {
        let channel = Channel::builder(endpoint)
            .tls_config(Default::default())
            .expect("failed to configure TLS")
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy();
        Self {
            rpc_client: BlockClient::new(channel),
        }
    }
}

impl BlockmetaClient<InterceptedService<Channel, AuthInterceptor>> {
    /// Create a new [`BlockmetaClient`] with the given gRPC endpoint and authorization token.
    ///
    /// The cliient will connect to the given endpoint and authenticate requests with the given
    /// authorization token inserted into the `authorization` header by the [`AuthInterceptor`].
    ///
    /// The service will connect once the first request is made. It will attempt to connect for
    /// 5 seconds before timing out.
    pub fn new_with_auth(endpoint: Uri, auth: impl AsRef<str>) -> Self {
        let interceptor = AuthInterceptor::with_token(auth.as_ref());
        let channel = Channel::builder(endpoint)
            .tls_config(Default::default())
            .expect("failed to configure TLS")
            .connect_timeout(Duration::from_secs(5))
            .connect_lazy();

        Self {
            rpc_client: BlockClient::with_interceptor(channel, interceptor),
        }
    }
}

impl<T> BlockmetaClient<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// Fetch the latest block from the StreamingFast Blockmeta service.
    ///
    /// Returns `None` if the block does not exist.
    pub async fn get_latest_block(&mut self) -> anyhow::Result<Option<Block>> {
        let request = Empty {};

        match self.rpc_client.head(request).await {
            Ok(res) => Ok(Some(res.into_inner())),
            Err(err) if err.code() == tonic::Code::NotFound => Ok(None),
            Err(err) => Err(anyhow::anyhow!("request failed: {}", err.message())),
        }
    }

    /// Fetch the block with the given hash from the StreamingFast Blockmeta service.
    ///
    /// - `hash`: The block hash to fetch.
    ///
    /// Returns `None` if the block does not exist.
    pub async fn get_block_by_hash(&mut self, hash: BlockHash) -> anyhow::Result<Option<Block>> {
        let request = IdToNumReq {
            block_id: format!("{:x}", hash), // Convert the block hash to the non-0x hex string
        };

        match self.rpc_client.id_to_num(request).await {
            Ok(res) => Ok(Some(res.into_inner())),
            Err(err) if err.code() == tonic::Code::NotFound => Ok(None),
            Err(err) => Err(anyhow::anyhow!("request failed: {}", err.message())),
        }
    }

    /// Fetch the block with the given number from the StreamingFast Blockmeta service.
    ///
    /// - `number`: The block number to fetch.
    ///
    /// Returns `None` if the block does not exist.
    pub async fn get_block_by_number(
        &mut self,
        number: BlockNumber,
    ) -> anyhow::Result<Option<Block>> {
        let request = NumToIdReq { block_num: number };

        match self.rpc_client.num_to_id(request).await {
            Ok(res) => Ok(Some(res.into_inner())),
            Err(err) if err.code() == tonic::Code::NotFound => Ok(None),
            Err(err) => Err(anyhow::anyhow!("request failed: {}", err.message())),
        }
    }
}
