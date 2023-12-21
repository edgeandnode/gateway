use std::time::Duration;

/// DecayBuffer approximates a histogram of data points over time to inform a prediction. Data
/// points are collected in the first (current) bin. Each call to `decay` rotates the bins to the
/// right and resets the current bin. The information stored in each bin is decayed away at a rate
/// of `1 - (0.001 * D)` per decay cycle (https://www.desmos.com/calculator/7kfwwvtkc1).
///
/// We'll consider query count for this example:
///     e.g. [c_0, c_1, c_2, ..., c_5461] where c_i is the count time T-i.
/// Imagine we get a query roughly once per decay, we could see something like:
///     [1, 0, 2, 0, 0, 1, ..., 2]
/// As a cycle passes, we shift the data down because T-0 is now T-1 and T-500 is now T-501. So
/// shifting gives us this:
///     [0, 1, 0, 2, 0, 0, ..., 2, 2]
/// (The final 1 disappeared into the first member of the ellipsis, and the 2 popped out from the
/// last member of the ellipsis)
///
/// There is no actual decay yet in the above description. Note though that if we shift multiple
/// times the sum should be the same for a while.
///     e.g. [1, 0, 0, ...] -> [0, 1, 0, ...] -> [0, 0, 1, ...]
/// The sum of all frames here is 1 until the 1 drops off the end.
///
/// The purpose of the decay is to weigh more recent data exponentially more than old data. If the
/// decay per frame is 1% then we would get approximately this:
///     [1, 0, 0, ...] -> [0, .99, 0, ...] -> [0, 0, .98]
/// (This looks linear, but is exponential I've just rounded the numbers).
///
/// Note that every time we call decay, the sum of all values decreases.
///
/// We consider the accuracy of timestamp of recent data is more important than the accuracy of
/// timestamp of old data. For example, it's useful to know if a failed request happened 2 seconds
/// ago vs 12 seconds ago. But less useful to know whether it happened 1002 vs 1012 seconds ago even
/// though that's the same duration. So for the approximation of our histogram, we use time frames
/// with intervals of F consecutive powers of 4.
///     e.g. [1, 4, 16, 64] if F = 4

#[derive(Clone, Debug)]
pub struct DecayBuffer<T, const F: usize, const D: u16> {
    frames: [T; F],
}

pub trait Decay {
    fn decay(&mut self, prev: &Self, retain: f64, take: f64);
}

pub type ISADecayBuffer<T> = DecayBuffer<T, 7, 4>;

impl<T, const F: usize, const D: u16> Default for DecayBuffer<T, F, D>
where
    [T; F]: Default,
{
    fn default() -> Self {
        debug_assert!(F > 0);
        debug_assert!(D < 1000);
        Self {
            frames: Default::default(),
        }
    }
}

impl<T, const F: usize, const D: u16> DecayBuffer<T, F, D>
where
    Self: Default,
{
    pub fn new() -> Self {
        Default::default()
    }
}

impl<T, const F: usize, const D: u16> DecayBuffer<T, F, D> {
    pub fn current_mut(&mut self) -> &mut T {
        &mut self.frames[0]
    }

    pub fn frames(&self) -> &[T] {
        &self.frames
    }

    pub fn map<'a, I>(&'a self, f: impl FnMut(&T) -> I + 'a) -> impl Iterator<Item = I> + 'a {
        self.frames.iter().map(f)
    }
}

impl<T, const F: usize, const D: u16> DecayBuffer<T, F, D>
where
    T: Decay + Default,
{
    pub fn decay(&mut self) {
        // BQN: (1-1e¯3×d)×((1-4⋆-↕f)×⊢)+(«4⋆-↕f)×⊢
        // LLVM should be capable of constant folding & unrolling this loop nicely.
        // https://rust.godbolt.org/z/K13dj78Ge
        for i in (1..self.frames.len()).rev() {
            let retain = 1.0 - 4_f64.powi(-(i as i32));
            let take = 4_f64.powi(-(i as i32 - 1));
            let decay = 1.0 - 1e-3 * D as f64;
            let (cur, prev) = self.frames[..=i].split_last_mut().unwrap();
            cur.decay(prev.last().unwrap(), retain * decay, take * decay);
        }
        self.frames[0] = T::default();
    }
}

impl Decay for f64 {
    fn decay(&mut self, prev: &Self, retain: f64, take: f64) {
        *self = (*self * retain) + (prev * take);
    }
}

impl Decay for Duration {
    fn decay(&mut self, prev: &Self, retain: f64, take: f64) {
        let mut v = self.as_secs_f64();
        v.decay(&prev.as_secs_f64(), retain, take);
        *self = Duration::from_secs_f64(v);
    }
}

// This could have been done more automatically by using a proc-macro, but this is simpler.
#[macro_export]
macro_rules! impl_struct_decay {
    ($name:ty {$($field:ident),*}) => {
        impl Decay for $name {
            fn decay(&mut self, prev: &Self, retain: f64, take: f64) {
                // Doing a destructure ensures that we don't miss any fields, should they be added
                // in the future. I tried it and the compiler even gives you a nice error message:
                //
                //   missing structure fields:
                //     -{name}
                let Self { $($field: _),* } = self;
                $(
                    self.$field.decay(&prev.$field, retain, take);
                )*
            }
        }
    };
}

#[cfg(test)]
mod test {
    use std::iter;

    use arrayvec::ArrayVec;

    use gateway_common::utils::testing::assert_within;

    use super::*;

    struct Model<const F: usize, const D: u16>(Vec<f64>);

    impl<const F: usize, const D: u16> Model<F, D> {
        fn new() -> Self {
            Self((0..F).flat_map(|i| iter::repeat(0.0).take(w(i))).collect())
        }

        fn decay(&mut self) {
            // BQN: »(1-d×1e¯3)×⊢
            for x in &mut self.0 {
                *x *= 1.0 - 1e-3 * D as f64;
            }
            self.0.rotate_right(1);
            self.0[0] = 0.0;
        }

        fn frames(&self) -> ArrayVec<f64, F> {
            (0..F)
                .scan(0, |i, f| {
                    let offset = *i;
                    let len = w(f);
                    *i += len;
                    Some(self.0[offset..][..len].iter().sum::<f64>())
                })
                .collect()
        }
    }

    fn w(i: usize) -> usize {
        4_u64.pow(i as u32) as usize
    }

    #[test]
    fn test() {
        model_check::<7, 0>();
        model_check::<7, 1>();
        model_check::<7, 5>();
        model_check::<7, 10>();
    }

    fn model_check<const F: usize, const D: u16>()
    where
        [f64; F]: Default,
    {
        let mut model = Model::<F, D>::new();
        let mut buf = DecayBuffer::<f64, F, D>::default();

        for _ in 0..1000 {
            model.0[0] = 1.0;
            model.decay();
            *buf.current_mut() = 1.0;
            buf.decay();

            let value = buf.frames().iter().sum::<f64>();
            let expected = model.frames().iter().sum::<f64>();

            println!("---",);
            println!("{:.2e} {}", expected, show(&model.frames()));
            println!("{:.2e} {}", value, show(buf.frames()));
            println!("{}", (value - expected) / expected);

            assert_within(value, expected, 0.013 * expected);
        }
    }

    fn show(v: &[f64]) -> String {
        format!(
            "[{}]",
            v.iter()
                .map(|f| format!("{f:.2e}"))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}
