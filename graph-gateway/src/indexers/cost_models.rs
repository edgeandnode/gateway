use graphql_http::graphql::{Document, IntoDocument, IntoDocumentWithVariables};
use graphql_http::http_client::ReqwestExt;
use indoc::indoc;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thegraph::types::DeploymentId;
use toolshed::url::Url;

pub async fn query(
    client: reqwest::Client,
    status_url: Url,
    query: CostModelQuery,
) -> anyhow::Result<CostModelResponse> {
    let res = client.post(status_url.0).send_graphql(query).await;
    match res {
        Ok(res) => Ok(res?),
        Err(e) => Err(anyhow::anyhow!("Error sending cost model query: {}", e)),
    }
}

pub(super) const COST_MODEL_QUERY_DOCUMENT: &str = indoc! {
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CostModelResponse {
    pub cost_models: Vec<CostModelSourceResponse>,
}

#[derive(Deserialize)]
pub struct CostModelSourceResponse {
    pub deployment: DeploymentId,
    pub model: String,
    pub variables: Option<String>,
}
