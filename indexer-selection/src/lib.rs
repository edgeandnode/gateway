pub mod actor;
pub mod decay;
mod economic_security;
mod indexing;
mod performance;
mod price_efficiency;
mod reliability;
mod score;
pub mod simulation;
pub mod test_utils;
mod utility;

pub use crate::{
    economic_security::NetworkParameters,
    indexing::{BlockStatus, IndexingState, IndexingStatus},
    utility::ConcaveUtilityParameters,
};
pub use cost_model::{self, CostModel};
pub use ordered_float::NotNan;
pub use receipts;
pub use secp256k1::SecretKey;

use crate::{
    price_efficiency::indexer_fee,
    receipts::BorrowFail,
    score::{select_indexers, SelectionFactors},
};
use prelude::{epoch_cache::EpochCache, *};
use rand::{prelude::SmallRng, SeedableRng as _};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

pub type Context<'c> = cost_model::Context<'c, &'c str>;

#[derive(Clone, Debug)]
pub struct Selection {
    pub indexing: Indexing,
    pub url: URL,
    pub price: GRT,
    pub blocks_behind: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectionError {
    BadInput(InputError),
    BadIndexer(IndexerError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputError {
    MalformedQuery,
    MissingNetworkParams,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum IndexerError {
    NoStatus,
    NoAllocation,
    BehindMinimumBlock,
    QueryNotCosted,
    FeeTooHigh,
    Excluded,
    NaN,
}

impl From<InputError> for SelectionError {
    fn from(err: InputError) -> Self {
        Self::BadInput(err)
    }
}

impl From<IndexerError> for SelectionError {
    fn from(err: IndexerError) -> Self {
        Self::BadIndexer(err)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexerErrorObservation {
    Timeout,
    IndexingBehind {
        latest_query_block: u64,
        latest_block: u64,
    },
    Other,
}

impl From<BorrowFail> for IndexerError {
    fn from(from: BorrowFail) -> Self {
        match from {
            BorrowFail::NoAllocation => Self::NoAllocation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
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

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Indexing {
    pub indexer: Address,
    pub deployment: SubgraphDeploymentID,
}

#[derive(Default, Debug, Eq, PartialEq)]
pub struct FreshnessRequirements {
    /// If specified, the subgraph must have indexed up to at least this number.
    pub minimum_block: Option<u64>,
    /// If true, the query has an unspecified block which means the query benefits from syncing as
    /// far in the future as possible.
    pub has_latest: bool,
}

pub struct IndexerErrors<'a>(pub BTreeMap<IndexerError, BTreeSet<&'a Address>>);

impl<'a> IndexerErrors<'a> {
    fn add(&mut self, err: IndexerError, indexer: &'a Address) {
        self.0.entry(err).or_default().insert(indexer);
    }
}

#[derive(Debug)]
pub struct UtilityParameters {
    pub budget: GRT,
    pub freshness_requirements: FreshnessRequirements,
    pub performance: ConcaveUtilityParameters,
    pub data_freshness: ConcaveUtilityParameters,
    pub economic_security: ConcaveUtilityParameters,
    pub price_efficiency_weight: f64,
}

impl UtilityParameters {
    pub fn new(
        budget: GRT,
        freshness_requirements: FreshnessRequirements,
        performance: f64,
        data_freshness: f64,
        economic_security: f64,
        price_efficiency: f64,
    ) -> Self {
        fn interp(lo: f64, hi: f64, preference: f64) -> f64 {
            if !(0.0..=1.0).contains(&preference) {
                return lo;
            }
            lo + ((hi - lo) * preference)
        }
        Self {
            budget,
            freshness_requirements,
            // https://www.desmos.com/calculator/hegcczzalf
            performance: ConcaveUtilityParameters {
                a: interp(1.1, 1.2, performance),
                weight: interp(1.0, 1.5, performance),
            },
            // https://www.desmos.com/calculator/kwgsyriihk
            data_freshness: ConcaveUtilityParameters {
                a: interp(5.0, 3.0, data_freshness),
                weight: interp(3.0, 4.5, data_freshness),
            },
            // https://www.desmos.com/calculator/g7t53e70lf
            economic_security: ConcaveUtilityParameters {
                a: interp(8e-4, 4e-4, economic_security),
                weight: interp(1.0, 1.5, economic_security),
            },
            price_efficiency_weight: interp(1.0, 2.0, price_efficiency),
        }
    }
}

#[derive(Default)]
pub struct State {
    pub network_params: NetworkParameters,
    indexers: EpochCache<Address, Arc<IndexerInfo>, 2>,
    indexings: EpochCache<Indexing, IndexingState, 2>,
    // Restricted subgraphs only allow listed indexers, and ignore their stake.
    pub restricted_deployments: Arc<HashMap<SubgraphDeploymentID, HashSet<Address>>>,
}

#[derive(Debug)]
pub struct IndexerInfo {
    pub url: URL,
    pub stake: GRT,
}

impl State {
    pub fn insert_indexing(&mut self, indexing: Indexing, status: IndexingStatus) {
        let state = self
            .indexings
            .get_or_insert(indexing, |_| IndexingState::default());
        state.set_status(status);
    }

    pub fn observe_query(
        &mut self,
        indexing: &Indexing,
        duration: Duration,
        result: Result<(), IndexerErrorObservation>,
    ) {
        if let Some(state) = self.indexings.get_mut(indexing) {
            state.observe_query(duration, result);
        }
    }

    pub fn penalize(&mut self, indexing: &Indexing, weight: u8) {
        if let Some(state) = self.indexings.get_mut(indexing) {
            state.penalize(weight);
        }
    }

    pub fn decay(&mut self) {
        self.indexings.apply(|sf| sf.decay());
    }

    pub fn select_indexers<'s, 'a>(
        &'s self,
        deployment: &SubgraphDeploymentID,
        indexers: &'a [Address],
        params: &UtilityParameters,
        context: &mut Context<'_>,
        selection_limit: u8,
    ) -> Result<(Vec<Selection>, IndexerErrors<'a>), InputError> {
        let mut errors = IndexerErrors(BTreeMap::new());
        let mut available = Vec::<SelectionFactors<'s>>::new();
        for indexer in indexers {
            if let Some(allowed) = self.restricted_deployments.get(deployment) {
                if !allowed.contains(indexer) {
                    errors.add(IndexerError::Excluded, indexer);
                    continue;
                }
            }
            let indexing = Indexing {
                indexer: *indexer,
                deployment: *deployment,
            };
            match self.selection_factors(indexing, params, context, selection_limit) {
                Ok(factors) => available.push(factors),
                Err(SelectionError::BadIndexer(err)) => errors.add(err, indexer),
                Err(SelectionError::BadInput(err)) => return Err(err),
            };
        }

        let mut rng = SmallRng::from_entropy();
        let mut selections = select_indexers(&mut rng, params, &available);
        selections.truncate(selection_limit as usize);
        Ok((selections, errors))
    }

    fn selection_factors<'s>(
        &'s self,
        indexing: Indexing,
        params: &UtilityParameters,
        context: &mut Context<'_>,
        selection_limit: u8,
    ) -> Result<SelectionFactors<'s>, SelectionError> {
        let info = self
            .indexers
            .get_unobserved(&indexing.indexer)
            .ok_or(IndexerError::NoStatus)?;
        let state = self
            .indexings
            .get_unobserved(&indexing)
            .ok_or(IndexerError::NoStatus)?;
        let status = state.status.block.as_ref().ok_or(IndexerError::NoStatus)?;
        let slashable_stake = self
            .network_params
            .slashable_usd(info.stake)
            .ok_or(InputError::MissingNetworkParams)?;
        let price = indexer_fee(
            &state.status.cost_model,
            context,
            params.price_efficiency_weight,
            &params.budget,
            selection_limit,
        )?;
        Ok(SelectionFactors {
            indexing,
            url: info.url.clone(),
            reliability: &state.reliability,
            perf_success: &state.perf_success,
            perf_failure: &state.perf_failure,
            blocks_behind: status.blocks_behind,
            slashable_stake,
            price,
            last_use: state.last_use,
            sybil: sybil(&state.total_allocation())?,
        })
    }
}

/// Sybil protection
fn sybil(indexer_allocation: &GRT) -> Result<NotNan<f64>, IndexerError> {
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
    NotNan::new(sybil).map_err(|_| IndexerError::NaN)
}
