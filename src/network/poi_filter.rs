use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use serde_with::serde_as;
use thegraph_core::{alloy::primitives::BlockNumber, DeploymentId, ProofOfIndexing};
use thegraph_graphql_http::http_client::ReqwestExt;
use tokio::time::Instant;
use url::Url;

use super::GraphQlRequest;

pub struct PoiFilter {
    http: reqwest::Client,
    blocklist: HashMap<DeploymentId, Vec<(BlockNumber, ProofOfIndexing)>>,
    cache: parking_lot::RwLock<HashMap<String, IndexerEntry>>,
}

struct IndexerEntry {
    pois: HashMap<(DeploymentId, BlockNumber), ProofOfIndexing>,
    last_refresh: Instant,
}

impl PoiFilter {
    pub fn new(
        http: reqwest::Client,
        blocklist: HashMap<DeploymentId, Vec<(BlockNumber, ProofOfIndexing)>>,
    ) -> Self {
        Self {
            http,
            blocklist,
            cache: Default::default(),
        }
    }

    pub async fn blocked_deployments(
        &self,
        url: &Url,
        deployments: &[DeploymentId],
    ) -> HashSet<DeploymentId> {
        let requests: Vec<(DeploymentId, BlockNumber)> = self
            .blocklist
            .iter()
            .filter(|(deployment, _)| deployments.contains(deployment))
            .flat_map(|(deployment, entries)| {
                entries.iter().map(|(block, _)| (*deployment, *block))
            })
            .collect();
        let pois = self.resolve(url, requests).await;

        deployments
            .iter()
            .filter(|deployment| match self.blocklist.get(deployment) {
                None => false,
                Some(blocklist) => blocklist.iter().any(|(block, poi)| {
                    pois.get(&(**deployment, *block))
                        .map(|poi_| poi == poi_)
                        .unwrap_or(true)
                }),
            })
            .cloned()
            .collect()
    }

    /// Fetch public PoIs, such that results are cached indefinitely and refreshed every 20 minutes.
    async fn resolve(
        &self,
        url: &Url,
        requests: Vec<(DeploymentId, BlockNumber)>,
    ) -> HashMap<(DeploymentId, BlockNumber), ProofOfIndexing> {
        let url_string = url.to_string();

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
            send_requests(&self.http, url, updates).await;
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
                    tracing::warn!(%status_url, %poi_fetch_err);
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
    let response = http
        .post(status_url)
        .send_graphql::<Response>(GraphQlRequest {
            document: query.to_string(),
            variables: serde_json::json!({ "requests": pois.iter().map(|(deployment, block)| serde_json::json!({
                "deployment": deployment,
                "blockNumber": block,
            })).collect::<Vec<_>>() }),
        })
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
    use thegraph_core::{deployment_id, proof_of_indexing as poi};

    use super::Response;

    #[test]
    fn deserialize_public_pois_response() {
        //* Given
        let response = r#"{
            "publicProofsOfIndexing": [
                {
                    "deployment": "QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH",
                    "proofOfIndexing": "0xba8a057796a81e013789789996551bb5b2920fb9947334db956992f7098bd287",
                    "block": {
                        "number": "123"
                    }
                },
                {
                    "deployment": "QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw",
                    "block": {
                        "number": "456"
                    }
                }
            ]
        }"#;

        //* When
        let response = serde_json::from_str::<Response>(response);

        //* Then
        let response = response.expect("deserialization failed");

        assert_eq!(response.public_proofs_of_indexing.len(), 2);
        assert_eq!(
            response.public_proofs_of_indexing[0].deployment,
            deployment_id!("QmeYTH2fK2wv96XvnCGH2eyKFE8kmRfo53zYVy5dKysZtH")
        );
        assert_eq!(
            response.public_proofs_of_indexing[0].proof_of_indexing,
            Some(poi!(
                "ba8a057796a81e013789789996551bb5b2920fb9947334db956992f7098bd287"
            ))
        );
        assert_eq!(response.public_proofs_of_indexing[0].block.number, 123);
        assert_eq!(
            response.public_proofs_of_indexing[1].deployment,
            deployment_id!("QmawxQJ5U1JvgosoFVDyAwutLWxrckqVmBTQxaMaKoj3Lw")
        );
        assert_eq!(
            response.public_proofs_of_indexing[1].proof_of_indexing,
            None
        );
        assert_eq!(response.public_proofs_of_indexing[1].block.number, 456);
    }
}
