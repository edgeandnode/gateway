pub use thegraph::types::{InvalidDecimalString, UDecimal18};

// The following are cumbersome by design. It's better to be forced to think hard about converting
// between these types.

/// GRT with 18 fractional digits
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct GRT(pub UDecimal18);

/// USD with 18 fractional digits
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct USD(pub UDecimal18);
