use prelude::*;
use std::sync::Arc;
use url::Url;

pub struct IndexerDataReader {
    pub url: Eventual<Arc<Url>>,
    pub stake: Eventual<GRT>,
}

pub struct IndexerDataWriter {
    pub url: EventualWriter<Arc<Url>>,
    pub stake: EventualWriter<GRT>,
}

impl Reader for IndexerDataReader {
    type Writer = IndexerDataWriter;
    fn new() -> (Self::Writer, Self) {
        let (url_writer, url) = Eventual::new();
        let (stake_writer, stake) = Eventual::new();
        (
            IndexerDataWriter {
                url: url_writer,
                stake: stake_writer,
            },
            IndexerDataReader { url, stake },
        )
    }
}
