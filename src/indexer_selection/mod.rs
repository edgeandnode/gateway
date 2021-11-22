mod allocations;
mod block_requirements;
mod data_freshness;
mod economic_security;
mod indexers;
mod performance;
mod price_efficiency;
mod reputation;
mod selection_factors;
mod utility;

#[cfg(test)]
pub mod test_utils;
#[cfg(test)]
mod tests;

pub use crate::indexer_selection::{
    allocations::{Allocations, QueryStatus, Receipt},
    block_requirements::BlockRequirements,
    indexers::{IndexerDataReader, IndexerDataWriter},
    price_efficiency::CostModelSource,
    selection_factors::{IndexingData, IndexingStatus, SelectionFactors},
};
use crate::{
    block_resolver::BlockResolver,
    indexer_selection::{block_requirements::*, economic_security::*},
    prelude::{
        shared_lookup::{SharedLookup, SharedLookupWriter},
        weighted_sample::WeightedSample,
        *,
    },
};
use cost_model;
use im;
use lazy_static::lazy_static;
use num_traits::identities::Zero;
pub use ordered_float::NotNan;
use prometheus;
use rand::{thread_rng, Rng as _};
pub use secp256k1::SecretKey;
use tokio::{sync::Mutex, time};
use utility::*;

pub type Context<'c> = cost_model::Context<'c, &'c str>;

#[derive(Clone, Debug)]
pub struct IndexerQuery {
    pub network: String,
    pub indexing: Indexing,
    pub url: String,
    pub query: String,
    pub receipt: Receipt,
    pub fee: GRT,
    pub slashable_usd: USD,
    pub utility: NotNan<f64>,
    pub blocks_behind: Option<u64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SelectionError {
    BadInput,
    MissingNetworkParams,
    MissingBlock(UnresolvedBlock),
    BadIndexer(BadIndexerReason),
    NoAllocation(Indexing),
}

impl From<UnresolvedBlock> for SelectionError {
    fn from(unresolved: UnresolvedBlock) -> Self {
        Self::MissingBlock(unresolved)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BadIndexerReason {
    MissingIndexerStake,
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
    WithHash(Bytes32),
    WithNumber(u64),
}

impl UnresolvedBlock {
    pub fn matches(&self, block: &BlockPointer) -> bool {
        match self {
            Self::WithHash(hash) => &block.hash == hash,
            Self::WithNumber(number) => &block.number == number,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Indexing {
    pub indexer: Address,
    pub deployment: SubgraphDeploymentID,
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
            indexers: inputs.indexers,
            indexings: inputs.indexings,
            last_decay: Mutex::default(),
        }
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

    pub async fn observe_indexing_behind(
        &self,
        context: &mut Context<'_>,
        query: &IndexerQuery,
        block_resolver: &BlockResolver,
    ) {
        // Get this early to be closer to the time when the query was made so
        // that race conditions occur less frequently. They will still occur,
        // but less is better.
        let latest = block_resolver.latest_block().map(|b| b.number).unwrap_or(0);
        let freshness_requirements =
            freshness_requirements(&mut context.operations, block_resolver).await;
        let selection_factors = match self.indexings.get(&query.indexing).await {
            Some(selection_factors) => selection_factors,
            None => return,
        };
        selection_factors
            .observe_indexing_behind(&freshness_requirements, latest)
            .await;
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

    pub async fn freshness_requirements(
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
    ) -> Result<BlockRequirements, SelectionError> {
        freshness_requirements(&mut context.operations, block_resolver).await
    }

    // TODO: Specify budget in terms of a cost model -
    // the budget should be different per query
    pub async fn select_indexer(
        &self,
        config: &UtilityConfig,
        network: &str,
        subgraph: &SubgraphDeploymentID,
        indexers: &im::Vector<Address>,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        freshness_requirements: &BlockRequirements,
        budget: USD,
    ) -> Result<Option<IndexerQuery>, SelectionError> {
        let budget: GRT = self
            .network_params
            .usd_to_grt(budget)
            .ok_or(SelectionError::MissingNetworkParams)?;

        let (indexing, score, receipt) = match self
            .make_selection(
                config,
                subgraph,
                context,
                block_resolver,
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
        let head = block_resolver
            .latest_block()
            .ok_or(SelectionError::MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let latest_block = block_resolver
            .resolve_block(UnresolvedBlock::WithNumber(
                head.number.saturating_sub(score.blocks_behind),
            ))
            .await?;
        let query =
            make_query_deterministic(context, block_resolver, &latest_block, score.blocks_behind)
                .await?;
        make_query_deterministic_timer.observe_duration();

        Ok(Some(IndexerQuery {
            network: network.into(),
            indexing,
            url: score.url,
            query: query.query,
            receipt: receipt.commitment.into(),
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
        deployment: &SubgraphDeploymentID,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        indexers: &im::Vector<Address>,
        budget: USD,
        freshness_requirements: &BlockRequirements,
    ) -> Result<Option<(Indexing, IndexerScore, Receipt)>, SelectionError> {
        let _make_selection_timer = METRICS.make_selection_duration.start_timer();
        let mut scores = Vec::new();
        for indexer in indexers {
            let indexing = Indexing {
                indexer: *indexer,
                deployment: *deployment,
            };
            let result = self
                .score_indexer(
                    &indexing,
                    context,
                    block_resolver,
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
                    | &SelectionError::MissingBlock(_) => return Err(err),
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
            .map_err(|_| SelectionError::NoAllocation(indexing.clone()))
    }

    async fn score_indexer(
        &self,
        indexing: &Indexing,
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        budget: GRT,
        config: &UtilityConfig,
        freshness_requirements: &BlockRequirements,
    ) -> Result<IndexerScore, SelectionError> {
        let _score_indexers_timer = METRICS.score_indexer_duration.start_timer();
        let mut aggregator = UtilityAggregator::new();
        let indexer_data = self
            .indexers
            .get(&indexing.indexer)
            .await
            .map(|data| (data.url.value_immediate(), data.stake.value_immediate()))
            .unwrap_or_default();
        let indexer_url = indexer_data
            .0
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        let indexer_stake = indexer_data
            .1
            .ok_or(BadIndexerReason::MissingIndexerStake)?;
        let economic_security = self
            .network_params
            .economic_security_utility(indexer_stake, config.economic_security)
            .ok_or(SelectionError::MissingNetworkParams)?;
        aggregator.add(economic_security.utility.clone());

        let selection_factors = self
            .indexings
            .get(&indexing)
            .await
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;

        let latest_block = block_resolver
            .latest_block()
            .ok_or(SelectionError::MissingBlock(UnresolvedBlock::WithNumber(0)))?;
        let blocks_behind = selection_factors.blocks_behind().await?;

        let (fee, price_efficiency) = selection_factors
            .get_price(context, config.price_efficiency, &budget)
            .await?;
        aggregator.add(price_efficiency);

        let indexer_allocation = selection_factors.total_allocation().await;
        if indexer_allocation == GRT::zero() {
            return Err(SelectionError::NoAllocation(indexing.clone()));
        }

        aggregator.add(
            selection_factors
                .expected_performance_utility(config.performance)
                .await,
        );

        aggregator.add(selection_factors.expected_reputation_utility().await?);

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

        drop(selection_factors);

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
            sybil: Self::sybil(indexer_allocation)?,
            blocks_behind,
        })
    }

    /// Sybil protection
    fn sybil(indexer_allocation: GRT) -> Result<NotNan<f64>, BadIndexerReason> {
        let identity = indexer_allocation.as_f64();
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
    make_query_deterministic_duration: prometheus::Histogram,
    make_selection_duration: prometheus::Histogram,
    score_indexer_duration: prometheus::Histogram,
}

lazy_static! {
    static ref METRICS: Metrics = Metrics {
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
