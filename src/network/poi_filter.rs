use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use serde_with::serde_as;
use thegraph_core::{DeploymentId, ProofOfIndexing, alloy::primitives::BlockNumber};
use thegraph_graphql_http::http_client::ReqwestExt;
use tokio::{sync::watch, time::Instant};
use url::Url;

use super::GraphQlRequest;

pub struct PoiFilter {
    http: reqwest::Client,
    blocklist: watch::Receiver<HashMap<DeploymentId, Vec<(BlockNumber, ProofOfIndexing)>>>,
    cache: parking_lot::RwLock<HashMap<String, IndexerEntry>>,
}

struct IndexerEntry {
    pois: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing>,
    last_refresh: Instant,
}

impl PoiFilter {
    pub fn new(
        http: reqwest::Client,
        blocklist: watch::Receiver<HashMap<DeploymentId, Vec<(BlockNumber, ProofOfIndexing)>>>,
    ) -> Self {
        Self {
            http,
            blocklist,
            cache: Default::default(),
        }
    }

    pub async fn blocked_deployments(
        &self,
        status_url: &Url,
        deployments: BTreeSet<DeploymentId>,
    ) -> BTreeSet<DeploymentId> {
        let blocklist: HashMap<DeploymentId, Vec<(u64, ProofOfIndexing)>> = self
            .blocklist
            .borrow()
            .iter()
            .filter(|(d, _)| deployments.contains(*d))
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        if blocklist.is_empty() {
            return Default::default();
        }

        let requests: Vec<(DeploymentId, BlockNumber)> = blocklist
            .iter()
            .flat_map(|(deployment, entries)| {
                entries.iter().map(|(block, _)| (*deployment, *block))
            })
            .collect();
        if requests.is_empty() {
            return Default::default();
        }

        let pois = self.resolve(status_url, requests).await;
        deployments
            .into_iter()
            .filter(|deployment| match blocklist.get(deployment) {
                None => false,
                Some(blocklist) => blocklist.iter().any(|(block, poi)| {
                    pois.get(&(*deployment, *block))
                        .map(|poi_| poi == poi_)
                        .unwrap_or(true)
                }),
            })
            .collect()
    }

    /// Fetch public PoIs, such that results are cached indefinitely and refreshed every 20 minutes.
    async fn resolve(
        &self,
        status_url: &Url,
        requests: Vec<(DeploymentId, BlockNumber)>,
    ) -> HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> {
        let url_string = status_url.to_string();

        let mut results: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> = Default::default();
        let mut refresh = false;
        {
            let cache = self.cache.read();
            if let Some(indexer) = cache.get(&url_string.to_string()) {
                refresh = indexer.last_refresh.elapsed() > Duration::from_secs(20 * 60);
                for key in &requests {
                    if let Some(poi) = indexer.pois.get(key) {
                        results.insert(*key, *poi);
                    }
                }
            }
        }

        let updates: Vec<(DeploymentId, u64)> = if refresh {
            requests.clone()
        } else {
            requests
                .iter()
                .filter(|r| !results.contains_key(r))
                .cloned()
                .collect()
        };
        if updates.is_empty() {
            return results;
        }
        let fetched: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> =
            send_requests(&self.http, status_url, updates).await;
        {
            let now = Instant::now();
            let mut cache = self.cache.write();
            let indexer = cache.entry(url_string).or_insert_with(|| IndexerEntry {
                pois: Default::default(),
                last_refresh: now,
            });
            indexer.last_refresh = now;
            for (key, value) in &fetched {
                indexer.pois.insert(*key, *value);
            }
        }
        results.extend(fetched);

        results
    }
}

async fn send_requests(
    http: &reqwest::Client,
    status_url: &Url,
    requests: Vec<(DeploymentId, BlockNumber)>,
) -> HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> {
    let request_batches = requests.chunks(10);
    let requests = request_batches.map(|batch| {
        let status_url = status_url.clone();
        async move {
            let result = send_request(http, status_url.clone(), batch).await;
            match result {
                Ok(response) => response,
                Err(poi_fetch_err) => {
                    tracing::debug!(%poi_fetch_err);
                    Default::default()
                }
            }
        }
    });

    let responses = futures::future::join_all(requests).await;
    responses
        .into_iter()
        .flatten()
        .filter_map(|response| {
            Some((
                (response.deployment, response.block.number),
                response.proof_of_indexing?,
            ))
        })
        .collect()
}

async fn send_request(
    http: &reqwest::Client,
    status_url: Url,
    pois: &[(DeploymentId, BlockNumber)],
) -> anyhow::Result<Vec<PublicProofOfIndexingResponse>> {
    let query = r#"
        query publicPois($requests: [PublicProofOfIndexingRequest!]!) {
            publicProofsOfIndexing(requests: $requests) {
                deployment
                proofOfIndexing
                block { number }
            }
        }"#;
    let request = GraphQlRequest {
        document: query.to_string(),
        variables: serde_json::json!({ "requests": pois.iter().map(|(deployment, block)| serde_json::json!({
            "deployment": deployment,
            "blockNumber": block,
        })).collect::<Vec<_>>() }),
    };
    let response = http
        .post(status_url)
        .send_graphql::<Response>(request)
        .await??;
    Ok(response.public_proofs_of_indexing)
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    public_proofs_of_indexing: Vec<PublicProofOfIndexingResponse>,
}
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicProofOfIndexingResponse {
    pub deployment: DeploymentId,
    pub block: PartialBlockPtr,
    pub proof_of_indexing: Option<ProofOfIndexing>,
}
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct PartialBlockPtr {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub number: BlockNumber,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        sync::Arc,
        time::Duration,
    };

    use thegraph_core::{
        DeploymentId,
        alloy::{hex, primitives::FixedBytes},
    };
    use tokio::sync::watch;
    use url::Url;

    use crate::init_logging;

    #[tokio::test]
    async fn poi_filter() {
        init_logging("poi_filter", false);

        type ResponsePoi = Arc<parking_lot::Mutex<FixedBytes<32>>>;
        let deployment: DeploymentId = "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"
            .parse()
            .unwrap();
        let bad_poi =
            hex!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef").into();
        let response_poi: ResponsePoi = Arc::new(parking_lot::Mutex::new(bad_poi));
        let request_count = Arc::new(parking_lot::Mutex::new(0));

        let route = {
            let response_poi = response_poi.clone();
            let request_count = request_count.clone();
            axum::routing::post(move || async move {
                *request_count.lock() += 1;
                axum::Json(serde_json::json!({
                    "data": {
                        "publicProofsOfIndexing": [
                            {
                                "deployment": deployment,
                                "proofOfIndexing": response_poi.lock().to_string(),
                                "block": { "number": "0" }
                            }
                        ]
                    }
                }))
            })
        };
        let app = axum::Router::new().route("/status", route);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let indexer_url: Url =
            format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port())
                .parse()
                .unwrap();
        eprintln!("listening on {indexer_url}");
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let blocklist = HashMap::from([(deployment, vec![(0, bad_poi.into())])]);
        let poi_filter = super::PoiFilter::new(reqwest::Client::new(), watch::channel(blocklist).1);

        let status_url = indexer_url.join("status").unwrap();
        let assert_blocked = |blocked: Vec<DeploymentId>| async {
            let result = poi_filter
                .blocked_deployments(&status_url, [deployment].into_iter().collect())
                .await;
            assert_eq!(result, BTreeSet::from_iter(blocked));
        };

        assert_blocked(vec![deployment]).await;
        assert_eq!(*request_count.lock(), 1);
        *response_poi.lock() =
            hex!("1111111111111111111111111111111111111111111111111111111111111111").into();
        assert_blocked(vec![deployment]).await;
        assert_eq!(*request_count.lock(), 1);
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(20 * 60)).await;
        assert_blocked(vec![]).await;
        assert_eq!(*request_count.lock(), 2);
    }
}
