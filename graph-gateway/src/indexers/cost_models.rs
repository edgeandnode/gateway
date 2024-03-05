use indoc::indoc;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thegraph_core::types::DeploymentId;
use thegraph_graphql_http::graphql::{Document, IntoDocument, IntoDocumentWithVariables};
use thegraph_graphql_http::http_client::ReqwestExt;

#[derive(Deserialize)]
pub struct CostModelSource {
    pub deployment: DeploymentId,
    pub model: String,
    pub variables: Option<String>,
}

pub async fn query(
    client: &reqwest::Client,
    status_url: reqwest::Url,
    query: CostModelQuery,
) -> anyhow::Result<Vec<CostModelSource>> {
    let res = client.post(status_url).send_graphql(query).await;
    match res {
        Ok(res) => Ok(res?),
        Err(e) => Err(anyhow::anyhow!("Error sending cost model query: {}", e)),
    }
}

const COST_MODEL_QUERY_DOCUMENT: &str = indoc! {
    r#"query ($deployments: [String!]!) {
        costModels(deployments: $deployments) {
            deployment
            model
            variables
        }
    }"#
};

#[serde_as]
#[derive(Clone, Debug, Serialize)]
pub struct CostModelQuery {
    #[serde_as(as = "Vec<DisplayFromStr>")]
    pub deployments: Vec<DeploymentId>,
}

impl IntoDocumentWithVariables for CostModelQuery {
    type Variables = Self;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        (COST_MODEL_QUERY_DOCUMENT.into_document(), self)
    }
}
