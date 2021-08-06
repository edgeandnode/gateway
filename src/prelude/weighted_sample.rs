use rand::{thread_rng, Rng as _};
use std::collections::HashMap;

/// Selects a random item from a stream of items with a weight
#[derive(Debug)]
pub struct WeightedSample<T> {
    item: Option<T>,
    weight_sum: f64,
}

impl<T> WeightedSample<T> {
    pub fn new() -> Self {
        Self {
            weight_sum: 0.0,
            item: None,
        }
    }

    pub fn add(&mut self, item: T, weight: f64) {
        // A-Chao algorithm for weighted reservoir sampling of a stream simplified for 1 output.
        // https://en.wikipedia.org/wiki/Reservoir_sampling#Weighted_random_sampling
        self.weight_sum += weight;
        let p = weight / self.weight_sum;
        let j = thread_rng().gen::<f64>();
        if j <= p {
            self.item = Some(item);
        }
    }

    pub fn take(self) -> Option<T> {
        let Self { item, .. } = self;
        item
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let sampler = WeightedSample::<u32>::new();
        assert!(sampler.take().is_none());
    }

    #[test]
    fn one_item() {
        let mut sampler = WeightedSample::new();
        sampler.add('a', 0.1);
        assert_eq!(Some('a'), sampler.take());
    }

    #[test]
    fn many_items() {
        let mut counts = HashMap::<_, i32>::new();
        for _ in 0..100000 {
            let mut sampler = WeightedSample::new();

            sampler.add('a', 0.2);
            sampler.add('b', 0.6);
            sampler.add('c', 1.2);

            let chosen = sampler.take();

            let count = counts.entry(chosen.unwrap()).or_default();
            *count += 1;
        }

        let diff_a = (10000 - counts.get(&'a').unwrap()).abs();
        assert!(diff_a < 1000);

        let diff_b = (30000 - counts.get(&'b').unwrap()).abs();
        assert!(diff_b < 1000);

        let diff_c = (60000 - counts.get(&'c').unwrap()).abs();
        assert!(diff_c < 1000);
    }
}
