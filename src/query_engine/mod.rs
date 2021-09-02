#[cfg(test)]
mod tests;

use crate::{
    indexer_selection::{self, IndexerQuery, Indexers, SelectionError, UnresolvedBlock},
    prelude::shared_lookup::{SharedLookup, SharedLookupWriter},
};
pub use crate::{
    indexer_selection::{Indexing, UtilityConfig},
    prelude::*,
};
use async_trait::async_trait;
pub use graphql_client::Response;
use im;
use std::{error::Error, sync::Arc};
use tokio::time::Instant;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QualifiedSubgraph {
    network: String,
    subgraph: String,
}

#[derive(Clone, Debug)]
pub struct ClientQuery {
    pub id: u64,
    pub query: String,
    pub variables: Option<String>,
    pub subgraph: QualifiedSubgraph,
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
    MissingBlocks(Vec<UnresolvedBlock>),
}

#[derive(Debug)]
pub struct BlockHead {
    block: BlockPointer,
    uncles: Vec<Bytes32>,
}

#[async_trait]
pub trait Resolver {
    async fn resolve_blocks(&self, network: &str, unresolved: &[UnresolvedBlock])
        -> Vec<BlockHead>;

    async fn query_indexer(&self, query: &IndexerQuery)
        -> Result<Response<String>, Box<dyn Error>>;
}

pub struct Config {
    pub indexer_selection_limit: usize,
    pub utility: UtilityConfig,
}

pub struct Deployment {
    id: Eventual<SubgraphDeploymentID>,
    indexers: Eventual<im::Vector<Address>>,
}

pub struct DeploymentWriter {
    pub id: EventualWriter<SubgraphDeploymentID>,
    pub indexers: EventualWriter<im::Vector<Address>>,
}

impl Reader for Deployment {
    type Writer = DeploymentWriter;
    fn new() -> (Self::Writer, Self) {
        let (id_writer, id) = Eventual::new();
        let (indexers_writer, indexers) = Eventual::new();
        (
            Self::Writer {
                id: id_writer,
                indexers: indexers_writer,
            },
            Self { id, indexers },
        )
    }
}

pub struct Inputs {
    indexers: Arc<Indexers>,
    deployments: SharedLookup<QualifiedSubgraph, Deployment>,
}

pub struct InputWriters {
    pub indexer_inputs: indexer_selection::InputWriters,
    pub indexers: Arc<Indexers>,
    pub deployments: SharedLookupWriter<QualifiedSubgraph, Deployment, DeploymentWriter>,
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
    deployments: SharedLookup<QualifiedSubgraph, Deployment>,
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
        use QueryEngineError::*;
        let _trace = tracing::info_span!("execute_query", query.id).entered();
        tracing::debug!(
            ?query.subgraph,
            indexer_selection_limit = ?self.config.indexer_selection_limit);
        let deployment = self
            .deployments
            .get(&query.subgraph)
            .await
            .ok_or(SubgraphNotFound)?;
        let deployment_id = deployment.id.value_immediate().ok_or(SubgraphNotFound)?;
        let mut indexers = deployment.indexers.value_immediate().unwrap_or_default();
        drop(deployment);
        tracing::debug!(?deployment_id, deployment_indexers = indexers.len());
        for _ in 0..self.config.indexer_selection_limit {
            let selection_result = self
                .indexers
                .select_indexer(
                    &self.config.utility,
                    &query.subgraph.network,
                    &deployment_id,
                    &indexers,
                    query.query.clone(),
                    query.variables.clone(),
                    query.budget,
                )
                .await;
            match &selection_result {
                Ok(None) => tracing::info!(err = ?NoIndexerSelected),
                Err(err) => tracing::info!(?err),
                _ => (),
            };
            let indexer_query = match selection_result {
                Ok(Some(indexer_query)) => indexer_query,
                Ok(None)
                | Err(SelectionError::MissingNetworkParams)
                | Err(SelectionError::BadIndexer(_)) => return Err(NoIndexerSelected),
                Err(SelectionError::BadInput) => return Err(MalformedQuery),
                Err(SelectionError::MissingBlocks(unresolved)) => {
                    self.resolve_blocks(&query, unresolved).await?;
                    continue;
                }
            };
            tracing::info!(indexer = ?indexer_query.indexing.indexer);
            let t0 = Instant::now();
            let result = self.resolver.query_indexer(&indexer_query).await;
            let query_duration = Instant::now() - t0;
            tracing::debug!(?query_duration);

            let response = match result {
                Ok(response) => response,
                Err(err) => {
                    tracing::info!(%err);
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
                tracing::info!("indexing behind");
                self.indexers.observe_indexing_behind(&indexer_query).await;
                continue;
            }

            // TODO: fisherman

            tracing::info!("query successful");
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

    async fn resolve_blocks(
        &self,
        query: &ClientQuery,
        mut unresolved: Vec<UnresolvedBlock>,
    ) -> Result<(), QueryEngineError> {
        tracing::debug!(unresolved_blocks = ?unresolved);
        let heads = self
            .resolver
            .resolve_blocks(&query.subgraph.network, &unresolved)
            .await;
        for head in heads {
            unresolved.swap_remove(
                unresolved
                    .iter()
                    .position(|b| match b {
                        UnresolvedBlock::WithHash(h) => h == &head.block.hash,
                        UnresolvedBlock::WithNumber(n) => n == &head.block.number,
                    })
                    .unwrap(),
            );
            self.indexers
                .set_block(&query.subgraph.network, head.block)
                .await;
            for uncle in head.uncles {
                self.indexers
                    .remove_block(&query.subgraph.network, &uncle)
                    .await;
            }
        }
        if !unresolved.is_empty() {
            return Err(QueryEngineError::MissingBlocks(unresolved));
        }
        Ok(())
    }
}
