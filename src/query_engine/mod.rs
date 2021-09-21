#[cfg(test)]
mod tests;

use crate::indexer_selection::{self, IndexerQuery, Indexers, SelectionError, UnresolvedBlock};
pub use crate::{
    indexer_selection::{Indexing, UtilityConfig},
    prelude::*,
};
use async_trait::async_trait;
pub use graphql_client::Response;
use im;
use std::{error::Error, sync::Arc};
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub enum Subgraph {
    Name(String),
    Deployment(SubgraphDeploymentID),
}

#[derive(Clone, Debug)]
pub struct ClientQuery {
    pub id: u64,
    pub query: String,
    pub variables: Option<String>,
    pub network: String,
    pub subgraph: Subgraph,
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

pub struct Inputs {
    indexers: Arc<Indexers>,
    deployments: Eventual<im::HashMap<String, SubgraphDeploymentID>>,
    deployment_indexers: Eventual<im::HashMap<SubgraphDeploymentID, im::Vector<Address>>>,
}

pub struct InputWriters {
    pub indexer_inputs: indexer_selection::InputWriters,
    pub indexers: Arc<Indexers>,
    pub deployments: EventualWriter<im::HashMap<String, SubgraphDeploymentID>>,
    pub deployment_indexers: EventualWriter<im::HashMap<SubgraphDeploymentID, im::Vector<Address>>>,
}

impl Inputs {
    pub fn new() -> (InputWriters, Self) {
        let (indexer_input_writers, indexer_inputs) = Indexers::inputs();
        let indexers = Arc::new(Indexers::new(indexer_inputs));
        let (deployments_writer, deployments) = Eventual::new();
        let (deployment_indexers_writer, deployment_indexers) = Eventual::new();
        (
            InputWriters {
                indexer_inputs: indexer_input_writers,
                indexers: indexers.clone(),
                deployments: deployments_writer,
                deployment_indexers: deployment_indexers_writer,
            },
            Inputs {
                indexers,
                deployments,
                deployment_indexers,
            },
        )
    }
}

pub struct QueryEngine<R: Resolver> {
    indexers: Arc<Indexers>,
    deployments: Eventual<im::HashMap<String, SubgraphDeploymentID>>,
    deployment_indexers: Eventual<im::HashMap<SubgraphDeploymentID, im::Vector<Address>>>,
    resolver: R,
    config: Config,
}

impl<R: Resolver> QueryEngine<R> {
    pub fn new(config: Config, resolver: R, inputs: Inputs) -> Self {
        Self {
            indexers: inputs.indexers,
            deployments: inputs.deployments,
            deployment_indexers: inputs.deployment_indexers,
            resolver,
            config,
        }
    }

    #[tracing::instrument(skip(self, query), fields(%query.id))]
    pub async fn execute_query(
        &self,
        query: ClientQuery,
    ) -> Result<QueryResponse, QueryEngineError> {
        use QueryEngineError::*;
        tracing::debug!(
            query.network = %query.network,
            query.subgraph = ?query.subgraph,
            indexer_selection_limit = ?self.config.indexer_selection_limit);
        let deployment = match &query.subgraph {
            Subgraph::Deployment(deployment) => deployment.clone(),
            Subgraph::Name(name) => self
                .deployments
                .value_immediate()
                .and_then(|map| map.get(name).cloned())
                .ok_or(SubgraphNotFound)?,
        };
        let mut indexers = self
            .deployment_indexers
            .value_immediate()
            .and_then(|map| map.get(&deployment).cloned())
            .unwrap_or_default();
        tracing::debug!(?deployment, deployment_indexers = indexers.len());
        for _ in 0..self.config.indexer_selection_limit {
            let selection_result = self
                .indexers
                .select_indexer(
                    &self.config.utility,
                    &query.network,
                    &deployment,
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
            tracing::debug!(query_duration = ?query_duration);

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
            .resolve_blocks(&query.network, &unresolved)
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
            self.indexers.set_block(&query.network, head.block).await;
            for uncle in head.uncles {
                self.indexers.remove_block(&query.network, &uncle).await;
            }
        }
        if !unresolved.is_empty() {
            return Err(QueryEngineError::MissingBlocks(unresolved));
        }
        Ok(())
    }
}
