use crate::{
    decay::DecayBuffer, economic_security::*, indexing::IndexingState, performance::Performance,
    price_efficiency::price_efficiency, reputation::Reputation, utility::*, BadIndexerReason,
    BlockStatus, FreshnessRequirements, IndexerInfo, Indexing, SelectionError, UtilityConfig,
};
use ordered_float::NotNan;
use prelude::*;

#[derive(Clone, Debug)]
pub struct IndexerScore {
    pub url: URL,
    pub fee: GRT,
    pub slashable: USD,
    pub utility: NotNan<f64>,
    pub utility_scores: UtilityScores,
    pub sybil: NotNan<f64>,
    pub blocks_behind: u64,
}

#[derive(Clone, Debug)]
pub struct UtilityScores {
    pub economic_security: f64,
    pub price_efficiency: f64,
    pub data_freshness: f64,
    pub performance: f64,
    pub reputation: f64,
}

pub trait Merge {
    fn merge(&mut self, other: &Self);
}

impl Merge for BlockStatus {
    fn merge(&mut self, other: &Self) {
        self.reported_number = self.reported_number.min(other.reported_number);
        self.blocks_behind = self.blocks_behind.max(other.blocks_behind);
        self.behind_reported_block |= other.behind_reported_block;
    }
}

/// TODO: docs
pub struct SelectionFactors {
    pub indexer: Address,
    pub url: URL,
    pub fee: GRT,
    stake: GRT,
    allocation: GRT,
    status: BlockStatus,
    performance: DecayBuffer<Performance>,
    reputation: DecayBuffer<Reputation>,
}

impl SelectionFactors {
    pub fn new(
        fee: GRT,
        indexing: &Indexing,
        indexer_info: &IndexerInfo,
        indexing_state: &IndexingState,
    ) -> Result<Self, SelectionError> {
        let allocation = indexing_state.total_allocation();
        if allocation == GRT::zero() {
            return Err(SelectionError::NoAllocation(indexing.clone()));
        }
        Ok(Self {
            indexer: indexing.indexer.clone(),
            url: indexer_info.url.clone(),
            stake: indexer_info.stake.clone(),
            allocation,
            fee,
            status: indexing_state
                .status
                .block
                .clone()
                .ok_or(BadIndexerReason::MissingIndexingStatus)?,
            performance: indexing_state.performance.clone(),
            reputation: indexing_state.reputation.clone(),
        })
    }

    pub fn merge_selection(
        &mut self,
        indexer_info: &IndexerInfo,
        indexing_state: &IndexingState,
    ) -> Result<(), SelectionError> {
        self.stake += indexer_info.stake;
        self.allocation += indexing_state.total_allocation();
        self.status.merge(
            indexing_state
                .status
                .block
                .as_ref()
                .ok_or(BadIndexerReason::MissingIndexingStatus)?,
        );
        self.performance.merge(&indexing_state.performance);
        self.reputation.merge(&indexing_state.reputation);
        Ok(())
    }

    pub fn score(
        &self,
        config: &UtilityConfig,
        network_params: &NetworkParameters,
        freshness_requirements: &FreshnessRequirements,
        budget: &GRT,
        cost: &GRT,
        latest_block: u64,
    ) -> Result<IndexerScore, SelectionError> {
        let EconomicSecurity {
            utility: economic_security,
            slashable,
        } = network_params
            .economic_security_utility(self.stake, config.economic_security)
            .ok_or(SelectionError::MissingNetworkParams)?;
        let price_efficiency =
            price_efficiency(&(self.fee + *cost), config.price_efficiency, &budget);
        let performance = self.performance.expected_utility(config.performance);
        let reputation = self.reputation.expected_utility(UtilityParameters {
            a: 3.0,
            weight: 1.0,
        });
        let data_freshness = Self::expected_freshness_utility(
            &self.status,
            freshness_requirements,
            config.data_freshness,
            latest_block,
        )?;
        // It's not immediately obvious why this mult works. We want to consider the amount staked
        // over the total amount staked of all Indexers in the running. But, we don't know the total
        // stake. If we did, it would be dividing all of these by that constant. Dividing all
        // weights by a constant has no effect on the selection algorithm. Interestingly, delegating
        // to an indexer just about guarantees that the indexer will receive more queries if they
        // met the minimum criteria above. So, delegating more and then getting more queries is kind
        // of a self-fulfilling prophesy. What balances this, is that any amount delegated is most
        // productive when delegated proportionally to each Indexer's utility for that subgraph.
        let utility = weighted_product_model([
            economic_security,
            price_efficiency,
            performance,
            reputation,
            data_freshness,
        ]);
        Ok(IndexerScore {
            url: self.url.clone(),
            fee: self.fee,
            slashable,
            utility: NotNan::new(utility).map_err(|_| BadIndexerReason::NaN)?,
            utility_scores: UtilityScores {
                economic_security: economic_security.utility,
                price_efficiency: price_efficiency.utility,
                performance: performance.utility,
                reputation: reputation.utility,
                data_freshness: data_freshness.utility,
            },
            sybil: Self::sybil(self.allocation)?,
            blocks_behind: self.status.blocks_behind,
        })
    }

    fn expected_freshness_utility(
        status: &BlockStatus,
        requirements: &FreshnessRequirements,
        utility_parameters: UtilityParameters,
        latest_block: u64,
    ) -> Result<SelectionFactor, SelectionError> {
        // Check that the indexer has synced at least up to any minimum block required.
        if let Some(minimum) = requirements.minimum_block {
            let indexer_latest = latest_block.saturating_sub(status.blocks_behind);
            if indexer_latest < minimum {
                return Err(BadIndexerReason::BehindMinimumBlock.into());
            }
        }
        // Add utility if the latest block is requested. Otherwise, data freshness is not a utility,
        // but a binary of minimum block. Note that it can be both.
        let utility = if !requirements.has_latest || (status.blocks_behind == 0) {
            1.0
        } else {
            let freshness = 1.0 / status.blocks_behind as f64;
            concave_utility(freshness, utility_parameters.a)
        };
        Ok(SelectionFactor {
            utility,
            weight: utility_parameters.weight,
        })
    }

    /// Sybil protection
    fn sybil(indexer_allocation: GRT) -> Result<NotNan<f64>, BadIndexerReason> {
        let identity = indexer_allocation.as_f64();

        // There is a GIP out there which would allow for allocations with 0 GRT stake.
        // For example, MIPS. We don't want for those to never be selected. Furthermore,
        // we can account for the cost of an allocation which would contribute to sybil.
        const BONUS: f64 = 1000.0;

        // Don't flatten so quickly, since numbers are large
        const SLOPE: f64 = 100.0;

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
        let sybil = (((identity + BONUS) / SLOPE) + 1.0).log(std::f64::consts::E);
        NotNan::new(sybil).map_err(|_| BadIndexerReason::NaN)
    }
}
