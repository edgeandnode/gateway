use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::Address;
use axum::async_trait;
use eventuals::{Eventual, EventualExt as _, Ptr};
use gateway_common::types::Indexing;
use gateway_framework::{
    auth::methods::api_keys::APIKey,
    gateway::http::{ApiKeys, GatewayImpl},
    network::{discovery::Status, network_subgraph},
    topology::network::{Deployment, Indexer},
};
use thegraph_core::{client as subgraph_client, types::DeploymentId};

use crate::{
    config::Config, indexer_client::IndexerClient, indexers::indexing, indexings_blocklist,
    indexings_blocklist::indexings_blocklist, subgraph_studio,
};

pub struct SubgraphGatewayOptions {
    pub config: Config,
}

pub struct SubgraphGateway {
    pub config: Config,
    pub indexer_client: IndexerClient,
    pub http_client: reqwest::Client,
}

impl SubgraphGateway {
    pub fn new(options: SubgraphGatewayOptions) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        SubgraphGateway {
            config: options.config,
            indexer_client: IndexerClient {
                client: http_client.clone(),
            },
            http_client,
        }
    }
}

#[async_trait]
impl GatewayImpl for SubgraphGateway {
    async fn publications(
        &self,
        network_subgraph: subgraph_client::Client,
        l2_transfer_support: bool,
    ) -> Eventual<Ptr<Vec<network_subgraph::Subgraph>>> {
        network_subgraph::Client::create(network_subgraph, l2_transfer_support).await
    }

    async fn indexing_statuses(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    ) -> Eventual<Ptr<HashMap<Indexing, Status>>> {
        indexing::statuses(
            deployments.clone(),
            self.http_client.clone(),
            self.config.min_graph_node_version.clone(),
            self.config.min_indexer_version.clone(),
        )
        .await
    }

    async fn legacy_indexers(
        &self,
        indexing_statuses: Eventual<Ptr<HashMap<Indexing, Status>>>,
    ) -> Eventual<Ptr<HashSet<Address>>> {
        indexing_statuses.clone().map(|statuses| async move {
            let legacy_indexers: HashSet<Address> = statuses
                .iter()
                .filter(|(_, status)| status.legacy_scalar)
                .map(|(indexing, _)| indexing.indexer)
                .collect();
            Ptr::new(legacy_indexers)
        })
    }

    async fn indexings_blocklist(
        &self,
        deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
        indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
    ) -> Eventual<Ptr<HashSet<Indexing>>> {
        // Indexer blocklist
        //
        // Periodically check the defective POIs list against the network indexers
        // and update the indexers blocklist accordingly.
        match &self.config.poi_blocklist {
            Some(blocklist) if !blocklist.pois.is_empty() => {
                let pois = blocklist.pois.clone();
                let update_interval = blocklist
                    .update_interval
                    .map_or(indexings_blocklist::DEFAULT_UPDATE_INTERVAL, |min| {
                        Duration::from_secs(min * 60)
                    });

                indexings_blocklist(
                    self.http_client.clone(),
                    deployments.clone(),
                    indexers.clone(),
                    pois,
                    update_interval,
                )
                .await
            }
            _ => Eventual::from_value(Ptr::default()),
        }
    }

    async fn api_keys(&self) -> Eventual<Ptr<HashMap<String, Arc<APIKey>>>> {
        match self.config.common.api_keys.clone() {
            Some(ApiKeys::Endpoint { url, auth, .. }) => {
                subgraph_studio::api_keys(self.http_client.clone(), url, auth.0)
            }
            Some(ApiKeys::Fixed(api_keys)) => Eventual::from_value(Ptr::new(
                api_keys
                    .into_iter()
                    .map(|k| (k.key.clone(), k.into()))
                    .collect(),
            )),
            None => Eventual::from_value(Ptr::default()),
        }
    }
}
