use graphql_http::graphql::{Document, IntoDocument, IntoDocumentWithVariables};
use indoc::indoc;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use toolshed::thegraph::DeploymentId;

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
