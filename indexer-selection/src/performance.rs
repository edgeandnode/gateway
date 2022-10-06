use crate::{
    decay::{Decay, FrameWeight},
    score::ExpectedValue,
};
use prelude::*;

/// Histogram of response times
#[derive(Clone, Debug, Default)]
pub struct Performance {
    latency: Vec<u32>,
    count: Vec<f64>,
}

impl Performance {
    pub fn observe(&mut self, duration: Duration) {
        let bin = nearest_triangle_number(duration.as_secs_f64() * 1000.0) as u32;
        *self.bucket_mut(bin) += 1.0;
    }

    fn bucket_mut(&mut self, bin: u32) -> &mut f64 {
        let index = match self.latency.binary_search(&bin) {
            Ok(index) => index,
            Err(index) => {
                self.latency.insert(index, bin);
                self.count.insert(index, 0.0);
                index
            }
        };
        &mut self.count[index]
    }
}

impl ExpectedValue for Performance {
    fn expected_value(&self) -> f64 {
        self.latency
            .iter()
            .zip(&self.count)
            .map(|(&l, &c)| l as f64 * c)
            .sum::<f64>()
            / self.count.iter().sum::<f64>().max(1.0)
    }
}

impl FrameWeight for Performance {
    fn weight(&self) -> f64 {
        self.count.iter().sum()
    }
}

impl Decay for Performance {
    fn shift(&mut self, mut next: Option<&mut Self>, fraction: f64, keep: f64) {
        // For each quantized bucket, find the corresponding quantized bucket in the next frame, and
        // shift information into it.
        for (count, latency) in self.count.iter_mut().zip(self.latency.iter().copied()) {
            let next_latency = next.as_deref_mut().map(|n| n.bucket_mut(latency));
            count.shift(next_latency, fraction, keep);
        }
    }

    fn clear(&mut self) {
        self.latency.clear();
        self.count.clear();
    }
}

fn nearest_triangle_number(n: f64) -> f64 {
    let m = (0.5 * ((1.0 + 8.0 * n).sqrt() - 1.0)).floor();
    let tan0 = (m * m + m) / 2.0;
    let tan1 = (m * m + 3.0 * m + 2.0) / 2.0;
    if (n - tan0) > (tan1 - n) {
        tan1
    } else {
        tan0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_triangle_numbers() {
        let cases = [(1, 1), (16, 15), (14, 15), (1000, 990)];
        for (start, expect) in cases.iter() {
            assert_eq!(nearest_triangle_number(*start as f64), *expect as f64);
        }
    }
}
