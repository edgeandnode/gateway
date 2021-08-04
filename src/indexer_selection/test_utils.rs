use crate::indexer_selection::utility::SelectionFactor;

pub fn assert_within(utility: SelectionFactor, expected: f64, tolerance: f64) {
  let diff = (utility.utility - expected).abs();
  assert!(
    diff <= tolerance,
    "Expected utility of {} +- {} but got {} which is off by {}",
    expected,
    tolerance,
    utility.utility,
    diff
  );
}
