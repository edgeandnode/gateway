use eventuals::{Eventual, EventualExt, Ptr};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use itertools::Itertools;
use thegraph::types::DeploymentId;
use tokio::sync::Mutex;
use toolshed::url::Url;

use indexer_selection::Indexing;

use crate::indexers::public_poi;
use crate::poi::ProofOfIndexingInfo;
use crate::topology::{Deployment, Indexer};

pub const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(20 * 60); // 20 minutes

/// Creates a new indexings blocklist eventual.
///
/// The blocklist is initialized by checking the POIs of all indexers in the network and returning
/// the indexings (addresses of the indexers and deploment ID) that have at least one public POI
/// that matches the ones marked as poisoned.
///
/// This function also starts a recurrent task that updates the blocklist every
/// `update_interval`.
pub async fn indexings_blocklist(
    client: reqwest::Client,
    deployments: Eventual<Ptr<HashMap<DeploymentId, Arc<Deployment>>>>,
    indexers: Eventual<Ptr<HashMap<Address, Arc<Indexer>>>>,
    pois_info: Vec<ProofOfIndexingInfo>,
    update_interval: Duration,
) -> Eventual<Ptr<HashSet<Indexing>>> {
    let (writer, rx) = Eventual::new();
    let writer = Arc::new(Mutex::new(writer));

    // Start the recurrent task
    eventuals::timer(update_interval)
        .pipe_async(move |_| {
            let client = client.clone();
            let pois_info = pois_info.clone();

            let deployments = deployments.clone();
            let indexers = indexers.clone();

            let writer = writer.clone();

            async move {
                // The GraphNetwork eventuals are guaranteed to be initialized by the time this
                // function is called.
                // 9fd85a49-06c3-4e74-9f61-4cc277bd1181
                let deployments = deployments.value().await.unwrap_or_default();
                let indexers = indexers.value().await.unwrap_or_default();

                let indexers_pois_map = check_indexers_public_pois(
                    client,
                    deployments.as_ref(),
                    indexers.as_ref(),
                    pois_info,
                )
                .await;

                let indexings = indexers_pois_map
                    .into_iter()
                    .flat_map(|(addr, pois)| {
                        pois.into_iter().map(move |info| (addr, info.deployment_id))
                    })
                    .map(|(indexer, deployment)| Indexing {
                        indexer,
                        deployment,
                    })
                    .collect::<HashSet<_>>();

                writer.lock().await.write(Ptr::new(indexings));
            }
        })
        .forever();

    rx
}

pub async fn check_indexer_pois(
    client: reqwest::Client,
    indexer_addr: Address,
    indexer_url: Url,
    pois_info: Vec<ProofOfIndexingInfo>,
    batch_size: usize,
) -> (Address, Vec<ProofOfIndexingInfo>) {
    // Send the public POIs queries and merge the results into a table
    let requests = pois_info.iter().map(|info| info.meta());
    let response_map = public_poi::merge_queries(client, indexer_url, requests, batch_size).await;

    // Check the POIs against the indexer's POIs response map and generate
    // a list of matching POI
    let result = pois_info
        .iter()
        .filter(|&info| {
            let info_meta = info.meta();
            let info_poi = info.poi();

            // Check if the POI is contained in the response map. If there is a match
            // mark the POI (and the indexer) as "poisoned"
            response_map
                .get(&info_meta)
                .map(|poi| poi == &info_poi)
                .unwrap_or_default()
        })
        .cloned()
        .collect::<Vec<_>>();

    (indexer_addr, result)
}

/// For a given deployment, return the addresses of all indexers that are currently
/// indexing it.
pub fn deployment_indexer_addresses(
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    deployment_id: &DeploymentId,
) -> Vec<Address> {
    deployments
        .get(deployment_id)
        .map(|deployment| {
            deployment
                .indexers
                .iter()
                .map(|indexer| indexer.id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// Create a table with the POIs to be checked for each indexer. The table is indexed by indexer
/// address and contains a list of POIs to be checked for that indexer.
///
/// Note that each indexer is indexing only a subset of the deployments in the network. Therefore,
/// the POIs to be checked for each indexer are a subset of the POIs in the network. This function
/// filters out the POIs that are not indexed by any indexer.
pub fn indexer_poi_info_map(
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    pois_info: &[ProofOfIndexingInfo],
) -> HashMap<Address, Vec<ProofOfIndexingInfo>> {
    pois_info
        .iter()
        .flat_map(|info| {
            deployment_indexer_addresses(deployments, &info.deployment_id)
                .into_iter()
                .map(|addr| (addr, info.clone()))
        })
        .into_group_map()
}

/// Check if the network indexers have any of the poisoned POIs.
///
/// This function returns a table indicating if the indexer contains at least one of the poisioned
/// POIs (`true`) or not (`false`).
pub async fn check_indexers_public_pois(
    client: reqwest::Client,
    deployments: &HashMap<DeploymentId, Arc<Deployment>>,
    indexers: &HashMap<Address, Arc<Indexer>>,
    pois_info: Vec<ProofOfIndexingInfo>,
) -> HashMap<Address, Vec<ProofOfIndexingInfo>> {
    // Map indexers to POIs to be checked
    let indexer_info = indexer_poi_info_map(deployments, &pois_info);

    let indexer_poi_queries = indexer_info
        .into_iter()
        .filter_map(|(addr, pois_info)| {
            let indexer = indexers.get(&addr)?;
            Some((indexer.id, indexer.status_url(), pois_info))
        })
        .map(|(indexer_addr, indexer_url, pois_info)| {
            let client = client.clone();
            check_indexer_pois(
                client,
                indexer_addr,
                indexer_url,
                pois_info,
                1, // TODO: Increase to 10 after indexer v0.32 is released.
            )
        });

    // Concurrently wait for all the indexer POI queries to finish
    futures::future::join_all(indexer_poi_queries)
        .await
        .into_iter()
        .collect()
}
