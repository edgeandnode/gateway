mod decimal;

pub use crate::prelude::decimal::*;
pub use eventuals::{Eventual, EventualWriter};

/// Decimal Parts-Per-Million with 6 fractional digits
pub type PPM = Decimal<6>;
/// Decimal USD with 18 fractional digits
pub type USD = Decimal<18>;
/// Decimal GRT with 18 fractional digits
pub type GRT = Decimal<18>;
