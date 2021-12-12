use crate::indexer_selection::utility::{concave_utility, SelectionFactor, UtilityAggregator};

pub trait Decay {
    fn expected_utility(&self, u_a: f64) -> f64;
    fn shift(&mut self, next: &Self, fraction: f64);
    fn clear(&mut self);
    fn count(&self) -> f64;
}

/// The DecayBuffer accounts for selection factors over various time-frames. Currently, these time
/// frames are 7 consecutive powers of 4 minute intervals, i.e. [1m, 4m, 16m, ... 4096m]. This
/// assumes that `decay` is called once every minute.
#[derive(Default)]
pub struct DecayBuffer<T: Default + Decay> {
    frames: [T; 7],
    decay_ticks: u64,
}

impl<T: Default + Decay> DecayBuffer<T> {
    pub fn current_mut(&mut self) -> &mut T {
        &mut self.frames[0]
    }

    pub fn expected_utility(&self, u_a: f64) -> SelectionFactor {
        // TODO: This weight seems to have no appreciable effect
        const FRAME_INDEX_WIEGTH: f64 = 0.5;
        let mut aggregator = UtilityAggregator::new();
        for (i, frame) in self.frames.iter().enumerate() {
            let index_weight = FRAME_INDEX_WIEGTH * (i + 1) as f64;
            aggregator.add(SelectionFactor {
                utility: frame.expected_utility(u_a),
                weight: concave_utility(index_weight * frame.count(), u_a),
            });
        }
        let agg_utility = aggregator.crunch();
        SelectionFactor {
            utility: agg_utility,
            weight: 1.0,
        }
    }

    pub fn decay(&mut self) {
        // For each frame `frame[i]`,
        // when `decay_ticks` is divisible by `pow(4, i-1)`,
        // reduce the value of `frame[i]` by 1/4 and add the value of `frame[i-1]` to `frame[i]`.
        self.decay_ticks += 1;
        for i in (1..7).rev() {
            let next_frame_ticks = 1 << ((i - 1) * 2);
            if (self.decay_ticks % next_frame_ticks) != 0 {
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
    use crate::{indexer_selection::reputation::Reputation, prelude::test_utils::create_dir};
    use plotters::prelude::*;
    use rand::{rngs::SmallRng, Rng as _, SeedableRng as _};
    use std::collections::HashMap;

    /// Success rates, in units of 1e-4
    const SUCCESS_RATES: [u32; 7] = [0000, 4000, 6000, 8000, 9000, 9900, 9999];

    struct ResponseConfig<E> {
        title: &'static str,
        event_description: &'static str,
        event_values: [E; 6],
    }

    struct ResponsePoint<E> {
        event_value: E,
        success_rate: f64,
        t_m: u64,
        utility: f64,
    }

    fn generate_response_plots<E: Eq + ToString>(
        config: &ResponseConfig<E>,
        points: &[ResponsePoint<E>],
    ) {
        create_dir("test-outputs");
        let file_path = format!("test-outputs/{}.svg", config.title);
        let root = SVGBackend::new(&file_path, (1600, 800)).into_drawing_area();
        let plot_areas = root.split_evenly((3, 2));
        for (plot_area, event_value) in plot_areas.into_iter().zip(&config.event_values) {
            let mut plot = plotters::prelude::ChartBuilder::on(&plot_area)
                .margin(5)
                .x_label_area_size(35)
                .y_label_area_size(35)
                .caption(
                    format!("{} {}", event_value.to_string(), config.event_description),
                    ("sans-serif", 18),
                )
                .build_cartesian_2d(0.0..(4092.0 * 2.0), 0.0..1.0)
                .unwrap();
            plot.configure_mesh()
                .x_desc("t (minute)")
                .y_desc("utility")
                .draw()
                .unwrap();
            for (i, success_rate) in SUCCESS_RATES.iter().enumerate().rev() {
                let success_rate = *success_rate as f64 * 1e-4;
                let color = Palette99::pick(i);
                let data = LineSeries::new(
                    points
                        .iter()
                        .filter(|point| &point.event_value == event_value)
                        .filter(|point| point.success_rate == success_rate)
                        .map(|point| (point.t_m as f64, point.utility)),
                    &color,
                );
                plot.draw_series(data)
                    .unwrap()
                    .label(format!("{:.4}", success_rate))
                    .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &color));
            }
            plot.configure_series_labels()
                .background_style(&WHITE.mix(0.8))
                .border_style(&BLACK)
                .draw()
                .unwrap();
        }
    }

    #[test]
    fn reputation_response() {
        let config = ResponseConfig {
            title: "reputation-outage-response",
            event_description: "minute outage",
            event_values: [0, 5, 60, 5 * 60, 15 * 60, 120 * 60],
        };
        let query_volume_hz = 10;
        let outage_start_s = 60 * 4092;
        let mut rand = SmallRng::from_entropy();
        let mut data = Vec::<ResponsePoint<u64>>::new();
        for outage_duration_m in config.event_values {
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
                        data.push(ResponsePoint {
                            event_value: outage_duration_m,
                            success_rate,
                            t_m: (t_s / 60),
                            utility: reputation.expected_utility(1.0).utility,
                        });
                    }
                }
            }
        }
        generate_response_plots(&config, &data);
    }

    #[test]
    fn penalty_response() {
        let config = ResponseConfig {
            title: "reputation-penalty-response",
            event_description: "penalty",
            event_values: [0, 40, 80, 120, 140, 160],
        };
        let query_volume_hz = 10;
        let penalty_start_s = 60 * 4092;
        let mut rand = SmallRng::from_entropy();
        let mut data = Vec::<ResponsePoint<u8>>::new();
        for penalty in config.event_values {
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
                        data.push(ResponsePoint {
                            event_value: penalty,
                            success_rate,
                            t_m: (t_s / 60),
                            utility: reputation.expected_utility(1.0).utility,
                        });
                    }
                }
            }
        }
        generate_response_plots(&config, &data);
    }
}
