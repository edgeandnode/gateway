#[cfg(test)]
mod tests;

pub use crate::indexer_selection::{Indexing, UtilityConfig};
use crate::{
    indexer_selection::{self, IndexerQuery, Indexers, SelectionError, UnresolvedBlock},
    prelude::{
        shared_lookup::{SharedLookup, SharedLookupWriter},
        *,
    },
};
use async_trait::async_trait;
pub use graphql_client::Response;
use std::{error::Error, sync::Arc};
use tokio::{sync::RwLock, time};

#[derive(Clone, Debug)]
pub struct ClientQuery {
    pub query: String,
    pub variables: Option<String>,
    pub network: String,
    pub subgraph: String,
    pub budget: USD,
}

#[derive(Debug)]
pub struct QueryResponse {
    pub query: IndexerQuery,
    pub response: Response<String>,
}

#[derive(Debug)]
pub enum QueryEngineError {
    SubgraphNotFound,
    NoIndexerSelected,
    MalformedQuery,
    MissingBlock(UnresolvedBlock),
}

#[async_trait]
pub trait Resolver {
    async fn resolve_block(
        &self,
        network: &str,
        block: &UnresolvedBlock,
    ) -> Option<(BlockPointer, Vec<Bytes32>)>;

    async fn query_indexer(&self, query: &IndexerQuery)
        -> Result<Response<String>, Box<dyn Error>>;
}

pub struct Config {
    pub indexer_selection_limit: usize,
    pub utility: UtilityConfig,
}

pub struct Deployment {
    id: Eventual<SubgraphDeploymentID>,
    indexers: Arc<RwLock<Vec<Address>>>,
}

pub struct DeploymentWriter {
    pub id: EventualWriter<SubgraphDeploymentID>,
    pub indexers: Arc<RwLock<Vec<Address>>>,
}

impl shared_lookup::Reader for Deployment {
    type Writer = DeploymentWriter;
    fn new() -> (Self::Writer, Self) {
        let (id_writer, id) = Eventual::new();
        let indexers = Arc::<RwLock<Vec<Address>>>::default();
        (
            Self::Writer {
                id: id_writer,
                indexers: indexers.clone(),
            },
            Self { id, indexers },
        )
    }
}

pub struct Inputs {
    indexers: Arc<Indexers>,
    deployments: SharedLookup<(String, String), Deployment>,
}

pub struct InputWriters {
    pub indexer_inputs: indexer_selection::InputWriters,
    pub indexers: Arc<Indexers>,
    pub deployments: SharedLookupWriter<(String, String), Deployment, DeploymentWriter>,
}

impl Inputs {
    fn new() -> (InputWriters, Self) {
        let (indexer_input_writers, indexer_inputs) = Indexers::inputs();
        let indexers = Arc::new(Indexers::new(indexer_inputs));
        let (deployments_writer, deployments) = SharedLookup::new();
        (
            InputWriters {
                indexer_inputs: indexer_input_writers,
                indexers: indexers.clone(),
                deployments: deployments_writer,
            },
            Inputs {
                indexers,
                deployments,
            },
        )
    }
}

pub struct QueryEngine<R: Resolver> {
    indexers: Arc<Indexers>,
    deployments: SharedLookup<(String, String), Deployment>,
    resolver: R,
    config: Config,
}

impl<R: Resolver> QueryEngine<R> {
    pub fn new(config: Config, resolver: R, inputs: Inputs) -> Self {
        Self {
            indexers: inputs.indexers,
            deployments: inputs.deployments,
            resolver,
            config,
        }
    }

    pub async fn execute_query(
        &self,
        query: ClientQuery,
    ) -> Result<QueryResponse, QueryEngineError> {
        use std::ops::Deref;
        use QueryEngineError::*;
        let deployment = self
            .deployments
            .get(&(query.network.clone(), query.subgraph))
            .await
            .ok_or(SubgraphNotFound)?;
        let deployment_id = deployment.id.value_immediate().ok_or(SubgraphNotFound)?;
        let mut indexers = deployment.indexers.read().await.deref().clone();
        drop(deployment);
        for _ in 0..self.config.indexer_selection_limit {
            let selection_result = self
                .indexers
                .select_indexer(
                    &self.config.utility,
                    &query.network,
                    &deployment_id,
                    &indexers,
                    query.query.clone(),
                    query.variables.clone(),
                    query.budget,
                )
                .await;
            let indexer_query = match selection_result {
                Ok(Some(indexer_query)) => indexer_query,
                Ok(None) => return Err(NoIndexerSelected),
                Err(SelectionError::BadInput) => return Err(MalformedQuery),
                Err(SelectionError::MissingNetworkParams) => return Err(NoIndexerSelected),
                Err(SelectionError::BadIndexer(reason)) => return Err(NoIndexerSelected),
                Err(SelectionError::MissingBlock(unresolved)) => {
                    let (block, uncles) = self
                        .resolver
                        .resolve_block(&query.network, &unresolved)
                        .await
                        .ok_or(MissingBlock(unresolved))?;
                    self.indexers.set_block(&query.network, block).await;
                    for uncle in uncles {
                        self.indexers.remove_block(&query.network, &uncle).await;
                    }
                    continue;
                }
            };
            let t0 = time::Instant::now();
            let result = self.resolver.query_indexer(&indexer_query).await;
            let query_duration = time::Instant::now() - t0;

            let response = match result {
                Ok(response) => response,
                Err(err) => {
                    self.indexers
                        .observe_failed_query(&indexer_query.indexing, &indexer_query.receipt, true)
                        .await;
                    indexers.remove(
                        indexers
                            .iter()
                            .position(|indexer| indexer == &indexer_query.indexing.indexer)
                            .unwrap(),
                    );
                    continue;
                }
            };
            let indexer_behind_err =
                "Failed to decode `block.hash` value: `no block with that hash found`";
            if response
                .errors
                .as_ref()
                .map(|errs| errs.iter().any(|err| err.message == indexer_behind_err))
                .unwrap_or(false)
            {
                self.indexers.observe_indexing_behind(&indexer_query).await;
                indexers.remove(
                    indexers
                        .iter()
                        .position(|indexer| indexer == &indexer_query.indexing.indexer)
                        .unwrap(),
                );
                continue;
            }

            // TODO: fisherman

            self.indexers
                .observe_successful_query(
                    &indexer_query.indexing,
                    query_duration,
                    &indexer_query.receipt,
                )
                .await;
            return Ok(QueryResponse {
                query: indexer_query,
                response,
            });
        }
        Err(NoIndexerSelected)
    }
}
