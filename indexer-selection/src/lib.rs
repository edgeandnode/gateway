use std::collections::HashMap;
use std::time::Duration;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fmt::Display,
};

use alloy_primitives::{Address, BlockHash, BlockNumber};
pub use cost_model::{self, CostModel};
use num_traits::Zero as _;
pub use ordered_float::NotNan;
use prelude::*;
use rand::{prelude::SmallRng, Rng as _};
use score::{expected_individual_score, ExpectedValue};
use toolshed::thegraph::{BlockPointer, DeploymentId};
use toolshed::url::Url;

pub use crate::{
    economic_security::NetworkParameters,
    indexing::{BlockStatus, IndexingState, IndexingStatus},
    score::SELECTION_LIMIT,
    utility::ConcaveUtilityParameters,
};
use crate::{
    fee::indexer_fee,
    score::{select_indexers, SelectionFactors},
};

pub mod actor;
pub mod decay;
mod economic_security;
mod fee;
mod indexing;
mod performance;
mod reliability;
mod score;
pub mod simulation;
#[cfg(test)]
mod test;
pub mod test_utils;
mod utility;

// We have to use `String` instead of `&'c str` here because of compiler bug triggered when holding
// a context across an await. See https://github.com/rust-lang/rust/issues/71723
pub type Context<'c> = cost_model::Context<'c, String>;

/// If an indexer's score is penalized such that it falls below this proportion of the max indexer
/// score, then the indexer will be discarded from the set of indexers to select from.
const MIN_SCORE_CUTOFF: f64 = 0.25;

#[derive(Clone, Debug)]
pub struct Selection {
    pub indexing: Indexing,
    pub url: Url,
    pub fee: GRT,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum IndexerError {
    NoStatus,
    NoStake,
    NoAllocation,
    MissingRequiredBlock,
    QueryNotCosted,
    FeeTooHigh,
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
        /// Chain head prior to indexer selection execution
        latest_block: u64,
        /// Latest block used for the indexer query
        latest_query_block: u64,
        /// Latest block indexed at the time of query execution, reported by indexer
        reported_block: Option<u64>,
    },
    BadAttestation,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum UnresolvedBlock {
    WithHash(BlockHash),
    WithNumber(BlockNumber),
}

impl UnresolvedBlock {
    pub fn matches(&self, block: &BlockPointer) -> bool {
        match self {
            Self::WithHash(hash) => &block.hash == hash,
            Self::WithNumber(number) => &block.number == number,
        }
    }
}

impl Display for UnresolvedBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithHash(hash) => write!(f, "{hash}"),
            Self::WithNumber(number) => write!(f, "{number}"),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Indexing {
    pub indexer: Address,
    pub deployment: DeploymentId,
}

#[derive(Default, Debug, Eq, PartialEq)]
pub struct BlockRequirements {
    /// Range of blocks specified in the query, inclusive.
    pub range: Option<(u64, u64)>,
    /// If true, the query has an unspecified block which means the query benefits from syncing as
    /// far in the future as possible.
    pub has_latest: bool,
}

#[derive(Debug)]
pub struct IndexerErrors<'a>(pub BTreeMap<IndexerError, BTreeSet<&'a Address>>);

impl<'a> IndexerErrors<'a> {
    fn add(&mut self, err: IndexerError, indexer: &'a Address) {
        self.0.entry(err).or_default().insert(indexer);
    }
}

#[derive(Debug)]
pub struct UtilityParameters {
    pub budget: GRT,
    pub requirements: BlockRequirements,
    pub latest_block: u64,
    pub block_rate_hz: f64,
}

#[derive(Default)]
pub struct State {
    pub network_params: NetworkParameters,
    indexings: HashMap<Indexing, IndexingState>,
}

impl State {
    pub fn insert_indexing(&mut self, indexing: Indexing, status: IndexingStatus) {
        if let Some(entry) = self.indexings.get_mut(&indexing) {
            entry.update_status(status);
        } else {
            self.indexings.insert(indexing, IndexingState::new(status));
        }
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
        for indexing in self.indexings.values_mut() {
            indexing.decay();
        }
    }

    // We use a small-state PRNG (xoroshiro256++) here instead of StdRng (ChaCha12).
    // `select_indexers` does not require the protections provided by a CSPRNG. The Xoroshiro++
    // algorithm provides high statistical quality while reducing the runtime of selection
    // consumed by generating ranom numbers from 4% to 2% at the time of writing.
    //
    // See also: https://docs.rs/rand/latest/rand/rngs/struct.SmallRng.html
    pub fn select_indexers<'a>(
        &self,
        rng: &mut SmallRng,
        candidates: &'a [Indexing],
        params: &UtilityParameters,
        context: &mut Context<'_>,
        selection_limit: u8,
    ) -> Result<(Vec<Selection>, IndexerErrors<'a>), InputError> {
        let mut errors = IndexerErrors(BTreeMap::new());
        let mut available = Vec::<SelectionFactors>::new();

        for candidate in candidates {
            match self.selection_factors(candidate, params, context, selection_limit) {
                Ok(factors) => available.push(factors),
                Err(SelectionError::BadIndexer(err)) => errors.add(err, &candidate.indexer),
                Err(SelectionError::BadInput(err)) => return Err(err),
            };
        }

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(?available);
        } else if rng.gen_bool(0.001) {
            tracing::debug!(?available);
        }

        // Find the maximum expected individual indexer score.
        let max_score = available
            .iter()
            .map(|factors| factors.expected_score)
            .max()
            .unwrap_or(NotNan::zero());
        // `select_indexers` discourages sybils by weighting it's selection based on the `sybil`
        // value. Having a random score cutoff that is weighted toward 1 normalized to the highest
        // score makes it so that we define our selection based on an expected score distribution,
        // so that even if there are many bad indexers with lots of stake it may not adversely
        // affect the result. This is important because an Indexer deployed on the other side of the
        // world should not generally bring our expected score down below the minimum requirements
        // set forth by this equation.
        let mut score_cutoff: NotNan<f64> =
            NotNan::new(rng.gen_range(MIN_SCORE_CUTOFF..1.0)).unwrap();
        score_cutoff = max_score * score_cutoff;
        // Filter out indexers below the cutoff. This avoids a situation where most indexers have
        // terrible scores, only a few have good scores, and the good indexers are often passed over
        // in multi-selection.
        tracing::debug!(score_cutoff = *score_cutoff);
        available.retain(|factors| factors.expected_score >= score_cutoff);

        let mut selections = select_indexers(rng, params, &available, selection_limit);
        selections.truncate(selection_limit as usize);
        Ok((selections, errors))
    }

    fn selection_factors(
        &self,
        candidate: &Indexing,
        params: &UtilityParameters,
        context: &mut Context<'_>,
        selection_limit: u8,
    ) -> Result<SelectionFactors, SelectionError> {
        let state = self
            .indexings
            .get(candidate)
            .ok_or(IndexerError::NoStatus)?;

        let block_status = state.status.block.as_ref().ok_or(IndexerError::NoStatus)?;
        if !block_status.meets_requirements(&params.requirements, params.latest_block) {
            return Err(IndexerError::MissingRequiredBlock.into());
        }

        if state.status.stake == GRT(UDecimal18::from(0)) {
            return Err(IndexerError::NoStake.into());
        }

        let slashable = self
            .network_params
            .slashable_usd(state.status.stake)
            .ok_or(InputError::MissingNetworkParams)?;

        let fee = indexer_fee(
            &state.status.cost_model,
            context,
            &params.budget,
            selection_limit,
        )?;

        let reliability = state.reliability.expected_value();
        let perf_success = state.perf_success.expected_value();
        let slashable_usd = slashable.0.into();

        let expected_score = NotNan::new(expected_individual_score(
            params,
            reliability,
            perf_success,
            state.status.versions_behind,
            block_status.blocks_behind,
            slashable_usd,
            &fee,
        ))
        .unwrap_or(NotNan::zero());

        Ok(SelectionFactors {
            indexing: *candidate,
            url: state.status.url.clone(),
            versions_behind: state.status.versions_behind,
            reliability,
            perf_success,
            perf_failure: state.perf_failure.expected_value(),
            blocks_behind: block_status.blocks_behind,
            slashable_usd,
            expected_score,
            fee,
            last_use: state.last_use,
            sybil: sybil(&state.status.allocation)?,
        })
    }
}

/// Sybil protection
fn sybil(indexer_allocation: &GRT) -> Result<NotNan<f64>, IndexerError> {
    let identity: f64 = indexer_allocation.0.into();

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
