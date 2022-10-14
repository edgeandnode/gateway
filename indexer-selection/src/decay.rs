use crate::score::ExpectedValue;
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
                // Doing a destructure ensures that we don't miss any fields, should they be added
                // in the future. I tried it and the compiler even gives you a nice error message...
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

pub trait FrameWeight {
    fn weight(&self) -> f64;
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

impl<T, const D: u16, const L: usize> ExpectedValue for DecayBufferUnconfigured<T, D, L>
where
    T: ExpectedValue + FrameWeight,
{
    fn expected_value(&self) -> f64 {
        self.frames
            .iter()
            .map(|f| f.expected_value() * (f.weight() + 1.0))
            .sum::<f64>()
            / self.frames.iter().map(|f| f.weight()).sum::<f64>().max(1.0)
    }
}

pub type DecayBuffer<T> = DecayBufferUnconfigured<T, 5, 7>;
pub type FastDecayBuffer<T> = DecayBufferUnconfigured<T, 10, 6>;

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
    /// The idea here is to pretend that we have a whole bunch of buckets of the minimum window size
    /// (1 min each). A whole 8191 of them! Queries contribute to the bucket they belong, and each
    /// time we decay each bucket shifts down and retains 99.5% of the information in the bucket.
    ///
    /// The above would be bad for performance, so we achieve almost the same result by creating
    /// "frames" of increasing size, each of which can hold many buckets. When we decay we do the
    /// same operation as above, assuming that each bucket within a frame holds the average value of
    /// all buckets within that frame. This results in some "smearing" of the data, but reduces the
    /// size of our array from 8191 to just 7 (assuming a LEN of 7) at the expense of slight
    /// complexity and loss of accuracy.
    pub fn decay(&mut self) {
        for i in (0..L).rev() {
            // Select buckets [i], [i+]
            let (l, r) = self.frames.split_at_mut(i + 1);
            let (this, next) = (l.last_mut().unwrap(), r.get_mut(0));

            // Decay rate of 0.1% per smallest bucket resolution per point. That is, each time we
            // shift decay 0.1% * points per non-aggregated bucket and aggregate the results into
            // frames.
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
