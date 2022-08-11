use crate::prelude::*;
use std::sync::Arc;
use url::Url;

#[derive(Default)]
pub struct IndexerData {
    pub url: Option<Arc<Url>>,
    pub stake: Option<GRT>,
}
