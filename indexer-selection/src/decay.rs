use prelude::*;

/// DecayBuffer accounts for data sets over increasing time frames. Currently, these time frames are
/// in intervals of L consecutive powers of 4. e.g. [1, 4, 16, 64] if L = 4. Data is collected in
/// real-time into the current frame (index 0). Each execution of `decay` distributes its value
/// across the remaining frames and then clears the current frame. The amount of information lost
/// is determined by `D`, using the function `1 / (1 + 0.01 * D * i)` where i is the frame index.
/// Higher values of `D` increase the rate of information decay as value if moved to larger frames,
/// which also lowers the "weight" of those observations when averaging the expected values across
/// the frames.
///
/// https://www.desmos.com/calculator/g1jpunuro5
/// 717f8434-6759-4888-85c3-1e1e77057e50
#[derive(Clone, Debug)]
pub struct DecayBuffer<T, const L: usize, const D: u16> {
    frames: [T; L],
}

fn decay<const D: u16>(mut f: f64, i: u8, value: f64) -> f64 {
    f *= 1.0 - 4.0_f64.powi(-(i as i32));
    f + (value / (1.0 + D as f64 * 0.01 * i as f64))
}

pub trait Decay {
    fn decay<const D: u16>(&mut self, i: u8, value: &Self);
    fn clear(&mut self);
}

pub type ISADecayBuffer<T> = DecayBuffer<T, 7, 100>;
pub type FastDecayBuffer<T> = DecayBuffer<T, 6, 10000>;

impl<T, const L: usize, const D: u16> Default for DecayBuffer<T, L, D>
where
    [T; L]: Default,
{
    fn default() -> Self {
        Self {
            frames: Default::default(),
        }
    }
}

impl<T, const L: usize, const D: u16> DecayBuffer<T, L, D>
where
    Self: Default,
{
    pub fn new() -> Self {
        Default::default()
    }
}

impl<T, const L: usize, const D: u16> DecayBuffer<T, L, D> {
    pub fn current_mut(&mut self) -> &mut T {
        &mut self.frames[0]
    }

    pub fn frames(&self) -> &[T] {
        &self.frames
    }

    pub fn map<'a, F, I>(&'a self, f: F) -> impl Iterator<Item = I> + 'a
    where
        F: FnMut(&T) -> I + 'a,
    {
        self.frames.iter().map(f)
    }
}

impl<T, const L: usize, const D: u16> DecayBuffer<T, L, D>
where
    T: Decay,
{
    pub fn decay(&mut self) {
        let (head, rest) = match self.frames.split_first_mut() {
            Some(split) => split,
            None => return,
        };
        for (i, frame) in rest.into_iter().enumerate() {
            frame.decay::<D>(i as u8 + 1, head);
        }
        head.clear();
    }
}

impl Decay for f64 {
    fn decay<const D: u16>(&mut self, i: u8, value: &Self) {
        *self = decay::<D>(*self, i, *value);
    }

    fn clear(&mut self) {
        *self = 0.0;
    }
}

impl Decay for Duration {
    fn decay<const D: u16>(&mut self, i: u8, value: &Self) {
        *self = Duration::from_secs_f64(decay::<D>(self.as_secs_f64(), i, value.as_secs_f64()));
    }

    fn clear(&mut self) {
        *self = Duration::ZERO;
    }
}

// This could have been done more automatically by using a proc-macro, but this is simpler.
#[macro_export]
macro_rules! impl_struct_decay {
    ($name:ty {$($field:ident),*}) => {
        impl Decay for $name {
            fn decay<const D: u16>(&mut self, i: u8, value: &Self) {
                $(
                    self.$field.decay::<D>(i, &value.$field);
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
