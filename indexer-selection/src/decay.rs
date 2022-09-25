use crate::utility::*;
use prelude::*;

// This could have been done more automatically by using a proc-macro, but this is simpler.
#[macro_export]
macro_rules! impl_struct_decay {
    ($name:ty {$($field:ident),*}) => {
        impl Decay for $name {
            fn shift(&mut self, mut next: Option<&mut Self>, fraction: f64, keep: f64) {
                $(
                    self.$field.shift(
                        next.as_deref_mut().map(|n| &mut n.$field),
                        fraction,
                        keep,
                    );
                )*
            }

            fn clear(&mut self) {
                // Doing a destructure ensures that we don't miss any fields,
                // should they be added in the future. I tried it and the compiler
                // even gives you a nice error message...
                //
                // missing structure fields:
                //    -{name}
                let Self { $($field),* } = self;

                $(
                    $field.clear();
                )*
            }
        }
    };
}

pub trait Decay {
    fn shift(&mut self, next: Option<&mut Self>, fraction: f64, keep: f64);
    fn clear(&mut self);
}

pub trait DecayUtility {
    fn count(&self) -> f64;
    fn expected_utility(&self, u_a: f64) -> f64;
}

/// The DecayBuffer accounts for selection factors over various time-frames. Currently, these time
/// frames are LEN consecutive powers of 4 intervals, i.e. [1m, 4m, 16m, ... 4096m] if LEN is 7 and
/// `decay` is called once every minute.
#[derive(Clone, Debug)]
pub struct DecayBufferUnconfigured<T, const LOSS_POINTS: u16, const LEN: usize> {
    frames: [T; LEN],
}

impl<T, const D: u16, const L: usize> Default for DecayBufferUnconfigured<T, D, L>
where
    [T; L]: Default,
{
    fn default() -> Self {
        Self {
            frames: Default::default(),
        }
    }
}

pub type DecayBuffer<T> = DecayBufferUnconfigured<T, 5, 7>;
pub type FastDecayBuffer<T> = DecayBufferUnconfigured<T, 10, 6>;

impl<T: DecayUtility, const D: u16, const L: usize> DecayBufferUnconfigured<T, D, L> {
    pub fn expected_utility(&self, utility_parameters: UtilityParameters) -> SelectionFactor {
        let agg_utility =
            weighted_product_model(self.frames.iter().enumerate().map(|(i, frame)| {
                // 1/10 query per minute = 85% confident.
                let confidence =
                    concave_utility(frame.count() * 10.0 / 4.0_f64.powf(i as f64), 0.19);

                // Buckets decrease in relevance rapidly, making
                // the most recent buckets contribute the most to the
                // final result.
                let importance = 1.0 / (1.0 + (i as f64));

                SelectionFactor {
                    utility: frame.expected_utility(utility_parameters.a),
                    weight: confidence * importance * utility_parameters.weight,
                }
            }));
        SelectionFactor::one(agg_utility)
    }
}

impl<T, const D: u16, const L: usize> DecayBufferUnconfigured<T, D, L>
where
    Self: Default,
{
    pub fn new() -> Self {
        Default::default()
    }
}

impl<T, const D: u16, const L: usize> DecayBufferUnconfigured<T, D, L> {
    pub fn current_mut(&mut self) -> &mut T {
        &mut self.frames[0]
    }

    pub fn frames(&self) -> &[T] {
        &self.frames
    }
}

impl<T: Decay, const D: u16, const L: usize> DecayBufferUnconfigured<T, D, L> {
    /*
    The idea here is to pretend that we have a whole bunch of buckets of the minimum window size (1 min each)
    A whole 8191 of them! Queries contribute to the bucket they belong, and each time we decay each bucket shifts down
    and retains 99.5% of the information in the bucket.

    The above would be bad for performance, so we achieve almost the same result by creating "frames"
    of increasing size, each of which can hold many buckets. When we decay we do the same operation as above,
    assuming that each bucket within a frame holds the average value of all buckets within that frame. This results in some "smearing"
    of the data, but reduces the size of our array from 8191 to just 7 (assuming a LEN of 7) at the expense of
    slight complexity and loss of accuracy.
    */
    pub fn decay(&mut self) {
        for i in (0..L).rev() {
            // Select buckets [i], [i+]
            let (l, r) = self.frames.split_at_mut(i + 1);
            let (this, next) = (l.last_mut().unwrap(), r.get_mut(0));

            // Decay rate of 0.1% per smallest bucket resolution per point.
            // That is, each time we shift decay 0.1% * points per non-aggregated bucket
            // and aggregate the results into frames.
            let retain = 1.0 - ((D as f64) * 0.001f64);
            let retain = retain.powf(4_f64.powf(i as f64));

            // Shift one bucket, aggregating the results.
            this.shift(next, 0.25_f64.powf(i as f64), retain);
        }
    }
}

impl Decay for f64 {
    fn shift(&mut self, next: Option<&mut Self>, fraction: f64, retain: f64) {
        // Remove some amount of value from this frame
        let take = *self * fraction;
        *self -= take;
        if let Some(next) = next {
            // And add that value to the next frame, destroying some of the value
            // as we go to forget over time.
            *next += take * retain;
        }
    }

    fn clear(&mut self) {
        *self = 0.0;
    }
}

impl Decay for Duration {
    fn shift(&mut self, next: Option<&mut Self>, fraction: f64, keep: f64) {
        let secs = self.as_secs_f64();
        let take = secs * fraction;
        *self = Duration::from_secs_f64(secs - take);
        if let Some(next) = next {
            *next += Duration::from_secs_f64(take * keep);
        }
    }

    fn clear(&mut self) {
        *self = Duration::ZERO;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reputation::Reputation;
    use prelude::test_utils::create_test_output;
    use rand::{rngs::SmallRng, Rng as _, SeedableRng as _};
    use std::{collections::HashMap, io::Write as _};

    /// Success rates, in units of 1e-4
    const SUCCESS_RATES: [u32; 7] = [0000, 4000, 6000, 8000, 9000, 9900, 9999];

    #[test]
    #[ignore = "Writes output to disk"]
    fn reputation_outage_response() {
        let out = create_test_output("reputation-outage-response.csv").unwrap();
        writeln!(&out, "outage_duration_m,success_rate,t_m,utility").unwrap();

        let outage_durations_m = [0, 5, 60, 5 * 60, 15 * 60, 120 * 60];
        let query_volume_hz = 10;
        let outage_start_s = 60 * 4092;
        let mut rand = SmallRng::from_entropy();
        for outage_duration_m in outage_durations_m {
            let mut reputations = SUCCESS_RATES
                .iter()
                .map(|success_rate| (*success_rate, DecayBuffer::default()))
                .collect::<HashMap<u32, DecayBuffer<Reputation>>>();
            let outage_end = outage_start_s + (60 * outage_duration_m);
            for t_s in 0u64..(outage_start_s * 2) {
                let outage = (t_s >= outage_start_s) && (t_s < outage_end);
                for success_rate in SUCCESS_RATES {
                    let reputation = reputations.get_mut(&success_rate).unwrap();
                    let success_rate = success_rate as f64 * 1e-4;
                    for _ in 0..query_volume_hz {
                        if !outage && rand.gen_bool(success_rate) {
                            reputation.current_mut().add_successful_query();
                        } else {
                            reputation.current_mut().add_failed_query();
                        }
                    }
                    // Decay every minute.
                    if (t_s % 60) == 0 {
                        reputation.decay();
                    }
                    // Sample every minute.
                    if (t_s % 60) == 0 {
                        let utility = reputation
                            .expected_utility(UtilityParameters::one(1.0))
                            .utility;
                        writeln!(
                            &out,
                            "{},{},{},{}",
                            outage_duration_m,
                            success_rate,
                            t_s / 60,
                            utility
                        )
                        .unwrap();
                    }
                }
            }
        }
    }

    #[test]
    #[ignore = "Writes output to disk"]
    fn reputation_penalty_response() {
        let out = create_test_output("reputation-penalty-response.csv").unwrap();
        writeln!(&out, "penalty,success_rate,t_m,utility").unwrap();

        let penalties = [0, 40, 80, 120, 140, 160];
        let query_volume_hz = 10;
        let penalty_start_s = 60 * 4092;
        let mut rand = SmallRng::from_entropy();
        for penalty in penalties {
            let mut reputations = SUCCESS_RATES
                .iter()
                .map(|success_rate| (*success_rate, DecayBuffer::default()))
                .collect::<HashMap<u32, DecayBuffer<Reputation>>>();
            for t_s in 0u64..(penalty_start_s * 2) {
                for success_rate in SUCCESS_RATES {
                    let reputation = reputations.get_mut(&success_rate).unwrap();
                    let success_rate = success_rate as f64 * 1e-4;
                    for _ in 0..query_volume_hz {
                        if rand.gen_bool(success_rate) {
                            reputation.current_mut().add_successful_query();
                        } else {
                            reputation.current_mut().add_failed_query();
                        }
                    }
                    if t_s == penalty_start_s {
                        reputation.current_mut().penalize(penalty);
                    }
                    // Decay every minute.
                    if (t_s % 60) == 0 {
                        reputation.decay();
                    }
                    // Sample every minute.
                    if (t_s % 60) == 0 {
                        let utility = reputation
                            .expected_utility(UtilityParameters::one(1.0))
                            .utility;
                        writeln!(
                            &out,
                            "{},{},{},{}",
                            penalty,
                            success_rate,
                            t_s / 60,
                            utility
                        )
                        .unwrap();
                    }
                }
            }
        }
    }
}
