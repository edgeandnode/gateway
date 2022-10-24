use prelude::*;

/// DecayBuffer accounts for data sets over increasing time frames. Currently, these time frames are
/// in intervals of F consecutive powers of 4. e.g. [1, 4, 16, 64] if F = 4. Data is collected in
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
pub struct DecayBuffer<T, const F: usize, const D: u16> {
    frames: [T; F],
}

fn decay<const D: u16>(mut f: f64, i: u8, value: f64) -> f64 {
    f *= 1.0 - 4_f64.powi(-(i as i32));
    f + value / (1.0 + 0.01 * (D as u64 * i as u64) as f64)
}

pub trait Decay {
    fn decay<const D: u16>(&mut self, i: u8, value: &Self);
    fn clear(&mut self);
}

pub type ISADecayBuffer<T> = DecayBuffer<T, 7, 100>;
pub type FastDecayBuffer<T> = DecayBuffer<T, 6, 10000>;

impl<T, const F: usize, const D: u16> Default for DecayBuffer<T, F, D>
where
    [T; F]: Default,
{
    fn default() -> Self {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        property_check::<4, 0>();
        property_check::<4, 1>();
        property_check::<4, 10>();
        property_check::<4, 100>();
        property_check::<4, 1000>();
    }

    fn property_check<const F: usize, const D: u16>()
    where
        [f64; F]: Default,
    {
        let expected = (1..F)
            .map(|i| 4_f64.powi(i as i32) / (1.0 + 0.01 * (D as u64 * i as u64) as f64))
            .collect::<Vec<f64>>();
        println!("{}", show(&expected));

        let mut buf = DecayBuffer::<f64, F, D>::default();
        let mut prev_err = (1..F).map(|_| 1.0).collect::<Vec<f64>>();
        while prev_err.iter().any(|e| *e > 0.001) {
            *buf.current_mut() = 1.0;
            buf.decay();

            let f = &buf.frames()[1..];
            let new_err = f
                .iter()
                .zip(&expected)
                .map(|(a, b)| (b - a).abs() / b)
                .collect::<Vec<f64>>();

            println!(
                "F={F},D={D}: sum={:.2} err={:.2e} f={}",
                f.iter().sum::<f64>(),
                new_err.iter().sum::<f64>(),
                show(&f),
            );

            // approaches expected vector when given unit value (1)
            assert!(f.iter().zip(&expected).all(|(a, b)| a <= b));
            assert!(f.iter().sum::<f64>() <= expected.iter().sum::<f64>());
            assert!(new_err.iter().zip(&prev_err).all(|(a, b)| a <= b));

            prev_err = new_err;
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
