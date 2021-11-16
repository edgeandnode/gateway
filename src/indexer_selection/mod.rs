mod economic_security;
mod indexers;
mod network_cache;
mod performance;
mod price_efficiency;
mod receipts;
mod reputation;
mod selection_factors;
mod utility;

#[cfg(test)]
pub mod test_utils;
#[cfg(test)]
mod tests;

pub use crate::indexer_selection::{
    indexers::{IndexerDataReader, IndexerDataWriter},
    receipts::Receipts,
    selection_factors::{IndexingData, IndexingStatus, SelectionFactors},
};
use crate::prelude::{
    shared_lookup::{SharedLookup, SharedLookupWriter},
    weighted_sample::WeightedSample,
    *,
};
use cost_model;
use economic_security::*;
use graphql_parser::query as graphql_query;
use im;
use indexers::IndexerSnapshot;
use lazy_static::lazy_static;
use network_cache::*;
use num_traits::identities::Zero;
pub use ordered_float::NotNan;
pub use price_efficiency::CostModelSource;
use prometheus;
use rand::{thread_rng, Rng as _};
use receipts::*;
pub use secp256k1::SecretKey;
use selection_factors::*;
use std::{fmt, ops::Deref};
use tokio::{
    sync::{Mutex, RwLock},
    time,
};
use tree_buf::{Decode, Encode};
use utility::*;

pub type Context<'c> = cost_model::Context<'c, &'c str>;

#[derive(Clone, Debug)]
pub struct IndexerQuery {
    pub network: String,
    pub indexing: Indexing,
    pub url: String,
    pub query: String,
    pub receipt: Receipt,
    pub low_collateral_warning: bool,
    pub fee: GRT,
    pub slashable_usd: USD,
    pub utility: NotNan<f64>,
    pub blocks_behind: Option<u64>,
}

#[derive(Clone)]
pub struct Receipt {
    pub commitment: Vec<u8>,
}

impl From<Vec<u8>> for Receipt {
    fn from(commitment: Vec<u8>) -> Self {
        Self { commitment }
    }
}

impl Deref for Receipt {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.commitment
    }
}

impl fmt::Debug for Receipt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "0x{}", hex::encode(&self.commitment))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectionError {
    BadInput,
    MissingNetworkParams,
    MissingBlocks(Vec<UnresolvedBlock>),
    BadIndexer(BadIndexerReason),
    InsufficientCollateral(Indexing, GRT),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BadIndexerReason {
    MissingIndexerStake,
    MissingIndexerDelegatedStake,
    BehindMinimumBlock,
    MissingIndexingStatus,
    MissingCostModel,
    QueryNotCosted,
    FeeTooHigh,
    NaN,
}

impl From<BadIndexerReason> for SelectionError {
    fn from(err: BadIndexerReason) -> Self {
        Self::BadIndexer(err)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum UnresolvedBlock {
    WithNumber(u64),
    WithHash(Bytes32),
}

#[derive(Clone, Debug, Decode, Eq, Hash, Encode, Ord, PartialEq, PartialOrd)]
pub struct Indexing {
    pub indexer: Address,
    pub deployment: SubgraphDeploymentID,
}

#[derive(Debug, Default, Decode, Encode)]
pub struct Snapshot {
    pub slashing_percentage: Bytes32,
    pub usd_to_grt_conversion: Bytes32,
    pub indexers: Vec<IndexerSnapshot>,
    pub indexings: Vec<IndexingSnapshot>,
}

#[derive(Clone)]
pub struct UtilityConfig {
    pub economic_security: f64,
    pub performance: f64,
    pub data_freshness: f64,
    pub price_efficiency: f64,
}

#[derive(Debug)]
pub struct IndexerScore {
    url: String,
    fee: GRT,
    slashable: USD,
    utility: NotNan<f64>,
    sybil: NotNan<f64>,
    blocks_behind: u64,
}

pub struct Inputs {
    pub slashing_percentage: Eventual<PPM>,
    pub usd_to_grt_conversion: Eventual<USD>,
    pub indexers: SharedLookup<Address, IndexerDataReader>,
    pub indexings: SharedLookup<Indexing, SelectionFactors>,
}

pub struct InputWriters {
    pub slashing_percentage: EventualWriter<PPM>,
    pub usd_to_grt_conversion: EventualWriter<USD>,
    pub indexers: SharedLookupWriter<Address, IndexerDataReader, IndexerDataWriter>,
    pub indexings: SharedLookupWriter<Indexing, SelectionFactors, IndexingData>,
}

pub struct Indexers {
    network_params: NetworkParameters,
    network_cache: RwLock<NetworkCache>,
    indexers: SharedLookup<Address, IndexerDataReader>,
    indexings: SharedLookup<Indexing, SelectionFactors>,
    last_decay: Mutex<Option<time::Instant>>,
}

impl Indexers {
    pub fn inputs() -> (InputWriters, Inputs) {
        let (slashing_percentage_writer, slashing_percentage) = Eventual::new();
        let (usd_to_grt_conversion_writer, usd_to_grt_conversion) = Eventual::new();
        let (indexers_writer, indexers) = SharedLookup::new();
        let (indexings_writer, indexings) = SharedLookup::new();
        (
            InputWriters {
                slashing_percentage: slashing_percentage_writer,
                usd_to_grt_conversion: usd_to_grt_conversion_writer,
                indexers: indexers_writer,
                indexings: indexings_writer,
            },
            Inputs {
                slashing_percentage,
                usd_to_grt_conversion,
                indexers,
                indexings,
            },
        )
    }

    pub fn new(inputs: Inputs) -> Indexers {
        Indexers {
            network_params: NetworkParameters {
                slashing_percentage: inputs.slashing_percentage,
                usd_to_grt_conversion: inputs.usd_to_grt_conversion,
            },
            network_cache: RwLock::default(),
            indexers: inputs.indexers,
            indexings: inputs.indexings,
            last_decay: Mutex::default(),
        }
    }

    pub async fn set_block(&self, network: &str, block: BlockPointer) {
        self.network_cache.write().await.set_block(network, block);
    }

    pub async fn remove_block(&self, network: &str, block_hash: &Bytes32) {
        self.network_cache
            .write()
            .await
            .remove_block(network, block_hash);
    }

    pub async fn set_block_head(&self, network: &str, head: BlockHead) {
        self.set_block(&network, head.block).await;
        for uncle in head.uncles {
            self.remove_block(&network, &uncle).await;
        }
    }

    pub async fn latest_block(&self, network: &str) -> Result<BlockPointer, UnresolvedBlock> {
        self.network_cache.read().await.latest_block(network, 0)
    }

    pub async fn add_transfer(
        &self,
        indexing: &Indexing,
        transfer_id: Bytes32,
        collateral: &GRT,
        secret: SecretKey,
    ) {
        let selection_factors = match self.indexings.get(indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        selection_factors
            .add_transfer(transfer_id, collateral, secret)
            .await;
    }

    pub async fn observe_successful_query(
        &self,
        indexing: &Indexing,
        duration: time::Duration,
        receipt: &[u8],
    ) {
        let selection_factors = match self.indexings.get(indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        selection_factors
            .observe_successful_query(duration, receipt)
            .await;
    }

    pub async fn observe_failed_query(
        &self,
        indexing: &Indexing,
        receipt: &[u8],
        is_unknown: bool,
    ) {
        let status = if is_unknown {
            QueryStatus::Unknown
        } else {
            QueryStatus::Failure
        };
        let selection_factors = match self.indexings.get(indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        selection_factors
            .observe_failed_query(receipt, status)
            .await;
    }

    pub async fn observe_indexing_behind(&self, query: &IndexerQuery) {
        // Get this early to be closer to the time when the query was made so
        // that race conditions occur less frequently. They will still occur,
        // but less is better.
        let latest = self
            .network_cache
            .write()
            .await
            .latest_block(&query.network, 0)
            .map(|block| block.number)
            .unwrap_or(0);
        let q = serde_json::from_str::<network_cache::SerializableQuery>(&query.query)
            .expect("observe_indexing_behind should only take sanitized queries");
        let mut operations = graphql_query::parse_query::<&str>(&q.query)
            .expect("observe_indexing_behind should only take sanitized queries")
            .definitions
            .into_iter()
            .filter_map(|d| match d {
                graphql_query::Definition::Operation(o) => Some(o),
                _ => None,
            })
            .collect::<Vec<graphql_query::OperationDefinition<&str>>>();
        let freshness_requirements = self
            .network_cache
            .read()
            .await
            .freshness_requirements(&mut operations, &query.network);
        let selection_factors = match self.indexings.get(&query.indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        selection_factors
            .observe_indexing_behind(&freshness_requirements, latest)
            .await;
    }

    pub async fn snapshot(&self) -> Snapshot {
        let (slashing_percentage, usd_to_grt_conversion) = {
            (
                self.network_params
                    .slashing_percentage
                    .value_immediate()
                    .unwrap_or_default(),
                self.network_params
                    .usd_to_grt_conversion
                    .value_immediate()
                    .unwrap_or_default(),
            )
        };
        Snapshot {
            slashing_percentage: slashing_percentage.to_little_endian().into(),
            usd_to_grt_conversion: usd_to_grt_conversion.to_little_endian().into(),
            indexers: self.indexers.snapshot().await,
            indexings: self.snapshot_indexings().await,
        }
    }

    async fn snapshot_indexings(&self) -> Vec<IndexingSnapshot> {
        use futures::stream::{FuturesUnordered, StreamExt as _};
        self.indexings
            .read()
            .await
            .iter()
            .map(|(k, v)| v.snapshot(k))
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await
    }

    #[cfg(test)]
    pub async fn restore(&self, inputs: &mut InputWriters, snapshot: Snapshot) {
        inputs
            .slashing_percentage
            .write(PPM::from_little_endian(&snapshot.slashing_percentage));
        inputs
            .usd_to_grt_conversion
            .write(GRT::from_little_endian(&snapshot.usd_to_grt_conversion));
        inputs.indexers.restore(snapshot.indexers).await;
        inputs
            .indexings
            .restore(self.restore_indexings(snapshot.indexings).await)
            .await;
    }

    #[cfg(test)]
    async fn restore_indexings(
        &self,
        snapshots: Vec<IndexingSnapshot>,
    ) -> Vec<(Indexing, SelectionFactors, IndexingData)> {
        use futures::stream::{FuturesUnordered, StreamExt as _};
        snapshots
            .into_iter()
            .map(|snapshot| SelectionFactors::restore(snapshot))
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await
    }

    pub async fn decay(&self) {
        let mut log = match self.last_decay.try_lock() {
            Ok(log) => log,
            Err(_) => return,
        };
        let time = Instant::now();
        let last_decay = match log.replace(time) {
            Some(last_decay) => last_decay,
            None => return,
        };
        drop(log);
        let passed_hours = (time - last_decay).as_secs_f64() / 3600.0;
        // Information half-life of ~24 hours.
        let retain = 0.973f64.powf(passed_hours);
        let indexings: Vec<Indexing> = self.indexings.keys().await;
        for indexing in indexings {
            match self.indexings.get(&indexing).await {
                Some(selection_factors) => selection_factors.decay(retain).await,
                None => continue,
            };
        }
    }

    // TODO: Specify budget in terms of a cost model -
    // the budget should be different per query
    pub async fn select_indexer(
        &self,
        config: &UtilityConfig,
        network: &str,
        subgraph: &SubgraphDeploymentID,
        indexers: &im::Vector<Address>,
        query: String,
        variables: Option<String>,
        budget: USD,
    ) -> Result<Option<IndexerQuery>, SelectionError> {
        let budget: GRT = self
            .network_params
            .usd_to_grt(budget)
            .ok_or(SelectionError::MissingNetworkParams)?;
        // Performance: Use a shared context to avoid duplicating query parsing,
        // which is one of the most expensive operations.
        let mut context = Context::new(&query, variables.as_deref().unwrap_or_default())
            .map_err(|_| SelectionError::BadInput)?;
        let freshness_requirements = self
            .network_cache
            .read()
            .await
            .freshness_requirements(&mut context.operations, network)?;

        let (indexing, score, receipt) = match self
            .make_selection(
                config,
                network,
                subgraph,
                &mut context,
                &indexers,
                budget,
                &freshness_requirements,
            )
            .await
        {
            Ok(Some(result)) => result,
            Ok(None) => return Ok(None),
            Err(err) => return Err(err),
        };

        let make_query_deterministic_timer =
            METRICS.make_query_deterministic_duration.start_timer();
        let query = self.network_cache.read().await.make_query_deterministic(
            network,
            context,
            score.blocks_behind,
        )?;
        make_query_deterministic_timer.observe_duration();

        Ok(Some(IndexerQuery {
            network: network.into(),
            indexing,
            url: score.url,
            query: query.query,
            receipt: receipt.commitment.into(),
            low_collateral_warning: receipt.low_collateral_warning,
            fee: score.fee,
            slashable_usd: score.slashable,
            utility: score.utility,
            blocks_behind: query.blocks_behind,
        }))
    }

    /// Select random indexer, weighted by utility. Indexers with incomplete data or that do not
    /// meet the minimum requirements will be excluded.
    async fn make_selection(
        &self,
        config: &UtilityConfig,
        network: &str,
        deployment: &SubgraphDeploymentID,
        context: &mut Context<'_>,
        indexers: &im::Vector<Address>,
        budget: USD,
        freshness_requirements: &BlockRequirements,
    ) -> Result<Option<(Indexing, IndexerScore, ReceiptBorrow)>, SelectionError> {
        let _make_selection_timer = METRICS.make_selection_duration.start_timer();
        let mut scores = Vec::new();
        for indexer in indexers {
            let indexing = Indexing {
                indexer: *indexer,
                deployment: *deployment,
            };
            let result = self
                .score_indexer(
                    network,
                    &indexing,
                    context,
                    budget,
                    config,
                    freshness_requirements,
                )
                .await;
            match &result {
                Ok(score) => tracing::trace!(
                    ?indexing.deployment,
                    ?indexing.indexer,
                    ?score.fee,
                    ?score.slashable,
                    %score.utility,
                    %score.sybil,
                    ?score.blocks_behind,
                ),
                Err(err) => tracing::trace!(
                    ?indexing.deployment,
                    ?indexing.indexer,
                    score_err = ?err,
                ),
            };
            let score = match result {
                Ok(score) if score.utility > NotNan::zero() => score,
                Err(err) => match &err {
                    &SelectionError::BadInput
                    | &SelectionError::MissingNetworkParams
                    | &SelectionError::MissingBlocks(_) => return Err(err),
                    _ => continue,
                },
                _ => continue,
            };
            scores.push((indexing, score));
        }
        let max_utility = match scores.iter().map(|(_, score)| score.utility).max() {
            Some(n) => n,
            _ => return Ok(None),
        };
        // Having a random utility cutoff that is weighted toward 1 normalized
        // to the highest score makes it so that we define our selection based
        // on an expected utility distribution, so that even if there are many
        // bad indexers with lots of stake it may not adversely affect the
        // result. This is important because an Indexer deployed on the other
        // side of the world should not generally bring our expected utility
        // down below the minimum requirements set forth by this equation.
        let mut utility_cutoff = NotNan::<f64>::new(thread_rng().gen()).unwrap();
        // Careful raising this value, it's really powerful. Near 0 and utility
        // is ignored (only stake matters). Near 1 utility matters at ~ x^2
        // (depending on the distribution of stake). Above that and things are
        // getting crazy and we're exploiting the utility strongly.
        const UTILITY_PREFERENCE: f64 = 1.1;
        utility_cutoff = max_utility * (1.0 - utility_cutoff.powf(UTILITY_PREFERENCE));
        let mut selected = WeightedSample::new();
        let scores = scores
            .into_iter()
            .filter(|(_, score)| score.utility >= utility_cutoff);
        for (indexing, score) in scores {
            let sybil = score.sybil.into();
            selected.add((indexing, score), sybil);
        }
        let (indexing, score) = match selected.take() {
            Some(selection) => selection,
            None => return Ok(None),
        };
        // Technically the "algorithm" part ends here, but eventually we want to
        // be able to go back with data already collected if a later step fails.

        // TODO: Depending on how these steps fail, it can make sense to try to
        // kick off resolutions (eg: getting block hashes, or adding collateral)
        // and at the same time try to use another Indexer.

        let fee = score.fee.clone();
        self.indexings
            .get(&indexing)
            .await
            .ok_or(BadIndexerReason::MissingIndexingStatus)?
            .commit(&fee)
            .await
            .map(|receipt| Some((indexing.clone(), score, receipt)))
            .map_err(|_| SelectionError::InsufficientCollateral(indexing.clone(), fee))
    }

    async fn score_indexer(
        &self,
        network: &str,
        indexing: &Indexing,
        context: &mut Context<'_>,
        budget: GRT,
        config: &UtilityConfig,
        freshness_requirements: &BlockRequirements,
    ) -> Result<IndexerScore, SelectionError> {
        let _score_indexers_timer = METRICS.score_indexer_duration.start_timer();
        let mut aggregator = UtilityAggregator::new();
        let load_indexer_data_timer = METRICS.get_indexer_data_duration.start_timer();
        let indexer_data = self
            .indexers
            .get(&indexing.indexer)
            .await
            .map(|data| {
                (
                    data.url.value_immediate(),
                    data.stake.value_immediate(),
                    data.delegated_stake.value_immediate(),
                )
            })
            .unwrap_or_default();
        let indexer_url = indexer_data
            .0
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        let indexer_stake = indexer_data
            .1
            .ok_or(BadIndexerReason::MissingIndexerStake)?;
        let delegated_stake = indexer_data
            .2
            .ok_or(BadIndexerReason::MissingIndexerDelegatedStake)?;
        load_indexer_data_timer.observe_duration();
        let economic_security = self
            .network_params
            .economic_security_utility(indexer_stake, config.economic_security)
            .ok_or(SelectionError::MissingNetworkParams)?;
        aggregator.add(economic_security.utility.clone());

        let get_selection_factors_timer = METRICS.get_selection_factors_duration.start_timer();
        let selection_factors = self
            .indexings
            .get(&indexing)
            .await
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;

        let get_blocks_behind_timer = METRICS.get_blocks_behind_duration.start_timer();
        let blocks_behind = selection_factors.blocks_behind().await?;
        let latest_block = self
            .network_cache
            .read()
            .await
            .latest_block(network, blocks_behind)
            .map_err(|unresolved| SelectionError::MissingBlocks(vec![unresolved]))?;
        get_blocks_behind_timer.observe_duration();

        let (fee, price_efficiency) = selection_factors
            .get_price(context, config.price_efficiency, &budget)
            .await?;
        aggregator.add(price_efficiency);

        let get_collateral_timer = METRICS.get_collateral_duration.start_timer();
        if !selection_factors.has_collateral_for(&fee).await {
            return Err(SelectionError::InsufficientCollateral(
                indexing.clone(),
                fee,
            ));
        }
        get_collateral_timer.observe_duration();

        let get_performance_timer = METRICS.get_performance_duration.start_timer();
        aggregator.add(
            selection_factors
                .expected_performance_utility(config.performance)
                .await,
        );
        get_performance_timer.observe_duration();

        let get_reputation_timer = METRICS.get_reputation_duration.start_timer();
        aggregator.add(selection_factors.expected_reputation_utility().await?);
        get_reputation_timer.observe_duration();

        let get_freshness_timer = METRICS.get_freshness_duration.start_timer();
        aggregator.add(
            selection_factors
                .expected_freshness_utility(
                    freshness_requirements,
                    config.data_freshness,
                    latest_block.number,
                    blocks_behind,
                )
                .await?,
        );
        get_freshness_timer.observe_duration();

        drop(selection_factors);
        get_selection_factors_timer.observe_duration();

        // It's not immediately obvious why this mult works. We want to consider the amount
        // staked over the total amount staked of all Indexers in the running. But, we don't
        // know the total stake. If we did, it would be dividing all of these by that
        // constant. Dividing all weights by a constant has no effect on the selection
        // algorithm. Interestingly, delegating to an indexer just about guarantees that the
        // indexer will receive more queries if they met the minimum criteria above. So,
        // delegating more and then getting more queries is kind of a self-fulfilling
        // prophesy. What balances this, is that any amount delegated is most productive
        // when delegated proportionally to each Indexer's utility for that subgraph.
        let utility = aggregator.crunch();

        Ok(IndexerScore {
            url: indexer_url,
            fee,
            slashable: economic_security.slashable_usd,
            utility: NotNan::new(utility).map_err(|_| BadIndexerReason::NaN)?,
            sybil: Self::sybil(indexer_stake, delegated_stake)?,
            blocks_behind,
        })
    }

    /// Sybil protection
    /// TODO: This is wrong. It should be looking at the total stake of all
    /// allocations on the Indexing, not the total stake for the Indexer. The
    /// allocations are meant to provide a signal about capacity and should be
    /// respected.
    fn sybil(indexer_stake: GRT, delegated_stake: GRT) -> Result<NotNan<f64>, BadIndexerReason> {
        // This unfortunately double-counts indexer own stake, once for economic
        // security and once for sybil.
        let total_stake = delegated_stake.saturating_add(indexer_stake);
        let identity = total_stake.as_f64();
        // To optimize for sybil protection, we want to just mult the utility by the identity
        // weight. But this may run into some economic problems. Consider the following scenario:
        //
        // Two indexers: A, and B have utilities of 45% and 55%, respectively. They are equally
        // delegated at 50% of the total delegation pool, each. Because their delegation is
        // equal, they receive query fees proportional to their utility. A delegator notices that
        // part of their delegation would be more efficiently allocated if they move it to the
        // indexer with higher utility so that delegation reflects query volume. So, now they have
        // utilities of 45% and 55%, and delegation of 45% and 55%. Because these are multiplied,
        // the new selection is 40% and 60%. But, the delegations are 45% and 55%. So… a delegator
        // notices that their delegation is inefficiently allocated and move their delegation to the
        // more selected indexer. Now, delegation is 40% 60%, but utility stayed at 45% and 55%… and
        // the new selections are 35% and 65%… and so the cycle continues. The gap continues to
        // widen until all delegation is moved to the marginally better Indexer. This is the kind of
        // winner-take-all scenario we are trying to avoid. In the absence of cold hard math and
        // reasoning, going to try using log magics.
        let sybil = (identity.max(0.0) + 1.0).log(std::f64::consts::E);
        NotNan::new(sybil).map_err(|_| BadIndexerReason::NaN)
    }
}

// TODO: For the user experience we should turn these into 0-1 values,
// and from those calculate both a utility parameter and a weight
// which should be used when combining utilities.
impl Default for UtilityConfig {
    fn default() -> Self {
        Self {
            /// This value comes from the PRD and is for the web case, where ~50% of users may leave
            /// a webpage if it takes longer than 2s to load. So - 50% utility at that point. After
            /// playing with this I feel that the emphasis on performance should increase. For one,
            /// the existing utility function is as though this one request were the only factor in
            /// loading a page, when it may have other requests that must go before or after it that
            /// should be included in that 2 second metric. The other is that this doesn't account
            /// for the user experience difference in users that do stay around long enough for the
            /// page to load.
            // Before change to PRD -
            // performance: 1.570744,
            // After change to PRD -
            performance: 0.01,
            /// This value comes from the PRD and makes it such that ~80% of utility is achieved at
            /// $1m, and utility increases linearly near $100k
            // economic_security: 0.00000161757,
            /// But, it turns out that it's hard to get that much slashable stake given the limited
            /// supply of GRT spread out across multiple indexers. So instead, put ~91% utility at
            /// $400k - which matches the testnet of Indexers having ~$5m and slashing at 10% but
            /// not using all of their value to stake
            economic_security: 0.000006,
            /// Strongly prefers latest blocks. Utility is at 1 when 0 blocks behind, .95 at 1 block
            /// behind, and 0.5 at 8 blocks behind.
            data_freshness: 4.33,
            // Don't over or under value "getting a good deal"
            // Note: This is not a utility_a parameter, but is a weight.
            price_efficiency: 0.5,
            // TODO
            // reputation: 0.0,
        }
    }
}

struct Metrics {
    get_blocks_behind_duration: prometheus::Histogram,
    get_collateral_duration: prometheus::Histogram,
    get_freshness_duration: prometheus::Histogram,
    get_indexer_data_duration: prometheus::Histogram,
    get_performance_duration: prometheus::Histogram,
    get_reputation_duration: prometheus::Histogram,
    get_selection_factors_duration: prometheus::Histogram,
    make_query_deterministic_duration: prometheus::Histogram,
    make_selection_duration: prometheus::Histogram,
    score_indexer_duration: prometheus::Histogram,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics {
        get_blocks_behind_duration: prometheus::register_histogram!(
            "get_blocks_behind_duration",
            "Duration of calculating blocks behind for indexing"
        )
        .unwrap(),
        get_collateral_duration: prometheus::register_histogram!(
            "get_collateral_duration",
            "Duration of checking collateral for indexing"
        )
        .unwrap(),
        get_freshness_duration: prometheus::register_histogram!(
            "get_freshness_duration",
            "Duration of calculating data freshness for indexing"
        )
        .unwrap(),
        get_indexer_data_duration: prometheus::register_histogram!(
            "get_indexer_data_duration",
            "Duration of loading indexer data"
        )
        .unwrap(),
        get_performance_duration: prometheus::register_histogram!(
            "get_performance_duration",
            "Duration of calculating performance for indexing"
        )
        .unwrap(),
        get_reputation_duration: prometheus::register_histogram!(
            "get_reputation_duration",
            "Duration of calculating reputation for indexing"
        )
        .unwrap(),
        get_selection_factors_duration: prometheus::register_histogram!(
            "get_selection_factors_duration",
            "Duration of loading selection factors for indexing"
        )
        .unwrap(),
        make_query_deterministic_duration: prometheus::register_histogram!(
            "make_query_deterministic_duration",
            "Duration of making query deterministic"
        )
        .unwrap(),
        make_selection_duration: prometheus::register_histogram!(
            "make_selection_duration",
            "Duration of making indexer selection"
        )
        .unwrap(),
        score_indexer_duration: prometheus::register_histogram!(
            "score_indexer_duration",
            "Duration of indexer scoring"
        )
        .unwrap(),
    };
}
