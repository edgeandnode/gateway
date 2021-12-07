use crate::indexer_selection::{
    utility::{concave_utility, SelectionFactor},
    SelectionError,
};

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
    fn clear(&mut self);
}

impl<T: Decay<T> + Default> DecayBuffer<T> {
    pub fn current(&mut self) -> &mut T {
        &mut self.frames[0]
    }

    pub fn expected_utility(&self, u_a: f64) -> Result<SelectionFactor, SelectionError> {
        let agg_utility = self
            .frames
            .iter()
            .map(|frame| frame.expected_utility())
            .collect::<Result<Vec<f64>, SelectionError>>()?
            .into_iter()
            .sum::<f64>()
            / self.frames.len() as f64;

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
        // Clear the current frame.
        self.frames[0].clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer_selection::reputation::Reputation;
    use plotlib::{
        page::Page,
        repr::Plot,
        style::{PointMarker, PointStyle},
        view::ContinuousView,
    };
    use rand::{thread_rng, Rng as _};

    #[test]
    fn reputation_response() {
        let outage_durations_m = [0, 5, 60, 5 * 60, 30 * 60, 120 * 60];
        let query_volume_hz = 20;
        let success_rate = 0.9900;
        let mut reputation = DecayBuffer::<Reputation>::default();
        let mut rand = thread_rng();
        let outage_start = 60 * 4092;
        for outage_duration_m in outage_durations_m {
            let mut data = Vec::new();
            let outage_end = outage_start + (60 * outage_duration_m);
            for t_s in 0u64..(outage_start * 2) {
                let outage = (t_s >= outage_start) && (t_s < outage_end);
                for _ in 0..query_volume_hz {
                    if !outage && rand.gen_bool(success_rate) {
                        reputation.current().add_successful_query();
                    } else {
                        reputation.current().add_failed_query();
                    }
                }
                // Decay every minute.
                if (t_s % 60) == 0 {
                    reputation.decay();
                }
                // Sample every minute.
                if (t_s % 60) == 0 {
                    let utility = reputation
                        .expected_utility(1.0)
                        .map(|selection_factor| selection_factor.utility)
                        .unwrap_or(f64::NAN);
                    data.push(((t_s / 60) as f64, utility));
                }
            }

            let trace =
                Plot::new(data.clone()).point_style(PointStyle::new().marker(PointMarker::Cross));
            let view = ContinuousView::new()
                .add(trace)
                .x_range(3500.0, 4092.0 * 2.0)
                .x_label("t (minutes)")
                .y_range(0.0, 1.0)
                .y_label("utility");
            println!(
                "{} minute outage:\n{}",
                outage_duration_m,
                Page::single(&view).dimensions(80, 20).to_text().unwrap()
            );
        }
    }
}
