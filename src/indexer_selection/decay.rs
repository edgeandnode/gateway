use crate::indexer_selection::{
    utility::{concave_utility, SelectionFactor},
    SelectionError,
};

/// The utility weight of each frame, as a fucntion of its index. For example, we want to have a
/// greater weight to the utilities of smaller time frames that contain more recent data so that
/// indexer selection can quickly respond to a changing environment. So with a FRAME_INDEX_WIEGHT
/// of 0.5 we will weigh index 0 at 100%, index 1 at 50%, index 2 at 25%, and so on. This number is
/// currently _very_ arbitrary and needs to be tuned.
const FRAME_INDEX_WIEGHT: f64 = 0.5;

/// The DecayBuffer accounts for selection factors over various time-frames. Currently, these time
/// frames are 7 consecutive powers of 4 minute intervals, i.e. [1m, 4m, 16m, ... 4096m]. This
/// assumes that `decay` is called once every minute.
#[derive(Default)]
pub struct DecayBuffer<T: Decay<T> + Default> {
    frames: [T; 7],
    decay_ticks: u64,
}

pub trait Decay<T> {
    fn expected_utility(&self) -> Result<f64, SelectionError>;
    fn shift(&mut self, next: &T, fraction: f64);
}

impl<T: Decay<T> + Default> DecayBuffer<T> {
    pub fn current(&mut self) -> &mut T {
        &mut self.frames[0]
    }

    pub fn expected_utility(&self, u_a: f64) -> Result<SelectionFactor, SelectionError> {
        let index_weight = |i: usize| -> f64 {
            if i == 0 {
                return 1.0;
            }
            FRAME_INDEX_WIEGHT * i as f64
        };
        let agg_utility = self
            .frames
            .iter()
            .map(|frame| frame.expected_utility())
            .collect::<Result<Vec<f64>, SelectionError>>()?
            .into_iter()
            .enumerate()
            .map(|(i, u)| concave_utility(u, u_a) * index_weight(i))
            .sum();
        Ok(SelectionFactor {
            utility: agg_utility,
            // This weight gives about 85% confidence after 10 samples
            // We would like more samples, but the query volume per indexer/deployment
            // pair is so low that it otherwise takes a very long time to converge.
            weight: concave_utility(agg_utility, 0.19),
        })
    }

    pub fn decay(&mut self) {
        // For each frame `frame[i]`,
        // when `decay_ticks` is divisible by `pow(4, i)`,
        // reduce the value of `frame[i]` by 1/4 and add the value of `frame[i-1]` to `frame[i]`.
        self.decay_ticks += 1;
        for i in 1..7 {
            let frame_ticks = 1 << (i * 2);
            if (self.decay_ticks % frame_ticks) != 0 {
                continue;
            }
            let (l, r) = self.frames.split_at_mut(i);
            let (prev, this) = (&l.last().unwrap(), &mut r[0]);
            this.shift(prev, 0.25);
        }
    }
}
