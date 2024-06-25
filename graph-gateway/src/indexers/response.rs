use alloy_primitives::{BlockHash, BlockNumber};
use gateway_framework::{blocks::Block, errors::IndexerError};
use serde::{Deserialize, Serialize};
use thegraph_graphql_http::http::response::{Error as GQLError, ResponseBody as GQLResponseBody};

pub fn rewrite_response(
    response: &str,
) -> Result<(String, Vec<GQLError>, Option<Block>), IndexerError> {
    #[derive(Deserialize, Serialize)]
    struct ProbedData {
        #[serde(rename = "_gateway_probe_", skip_serializing)]
        probe: Option<Meta>,
        #[serde(flatten)]
        data: serde_json::Value,
    }
    #[derive(Deserialize)]
    struct Meta {
        block: MaybeBlock,
    }
    #[derive(Deserialize)]
    struct MaybeBlock {
        number: BlockNumber,
        hash: BlockHash,
        timestamp: Option<u64>,
    }
    let mut payload: GQLResponseBody<ProbedData> =
        serde_json::from_str(response).map_err(|err| IndexerError::BadResponse(err.to_string()))?;

    // Avoid processing oversized errors.
    for err in &mut payload.errors {
        err.message.truncate(256);
        err.message.shrink_to_fit();
    }

    let block = payload
        .data
        .as_mut()
        .and_then(|data| data.probe.take())
        .and_then(|meta| {
            Some(Block {
                number: meta.block.number,
                hash: meta.block.hash,
                timestamp: meta.block.timestamp?,
            })
        });
    let client_response = serde_json::to_string(&payload).unwrap();
    Ok((client_response, payload.errors, block))
}
