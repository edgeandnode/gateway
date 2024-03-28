use indoc::formatdoc;
use itertools::Itertools as _;
use serde::Deserialize;
use thegraph_core::types::DeploymentId;
use thegraph_graphql_http::http_client::ReqwestExt;

#[derive(Debug, Deserialize)]
pub struct CostModelSource {
    pub deployment: DeploymentId,
    pub model: String,
    pub variables: Option<String>,
}

pub async fn query(
    client: &reqwest::Client,
    cost_url: reqwest::Url,
    deployments: &[DeploymentId],
) -> anyhow::Result<Vec<CostModelSource>> {
    let deployments = deployments.iter().map(|d| format!("\"{d}\"")).join(",");
    let query = formatdoc! {
        r#"{{
            costModels(deployments: [{deployments}]) {{
                deployment
                model
                variables
            }}
        }}"#
    };
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Response {
        pub cost_models: Vec<CostModelSource>,
    }
    let response = client
        .post(cost_url)
        .send_graphql::<Response>(query)
        .await
        .map_err(|err| anyhow::anyhow!("Error sending cost model query: {err}"))??;
    Ok(response.cost_models)
}
