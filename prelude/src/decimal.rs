use primitive_types::U256;
use std::{cmp::Ordering, fmt, iter, ops, str};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParseStrError {
    InvalidInput,
}

impl fmt::Display for ParseStrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Failed to parse decimal value")
    }
}

/// Represents a positive decimal value with some fractional digit precision, P.
/// Using U256 as storage.
#[derive(Copy, Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UDecimal<const P: u8> {
    internal: U256,
}

macro_rules! impl_from_uints {
    ($($t:ty),+) => {$(
        impl<const P: u8> std::convert::TryFrom<$t> for UDecimal<P> {
            type Error = &'static str;
            fn try_from(from: $t) -> Result<UDecimal<P>, Self::Error> {
                let internal = U256::from(from)
                    .checked_mul(U256::exp10(P as usize))
                    .ok_or("overflow")?;
                Ok(UDecimal { internal })
            }
        }
    )*};
}

impl_from_uints!(u8, u16, u32, u64, u128, usize, U256);

impl<const P: u8> str::FromStr for UDecimal<P> {
    type Err = ParseStrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ParseStrError::*;
        // We require at least one ASCII digit. Otherwise `U256::from_dec_str` will return 0 for
        // some inputs we consider invalid.
        if !s.chars().any(|c: char| -> bool { ('0'..'9').contains(&c) }) {
            return Err(InvalidInput);
        }
        let (int, frac) = s.split_at(s.chars().position(|c| c == '.').unwrap_or(s.len()));
        let p = P as usize;
        let digits = int
            .chars()
            // append fractional digits (after decimal point)
            .chain(frac.chars().skip(1).chain(iter::repeat('0')).take(p))
            .collect::<String>();
        Ok(UDecimal {
            internal: U256::from_dec_str(&digits).map_err(|_| InvalidInput)?,
        })
    }
}

impl<const P: u8> fmt::Display for UDecimal<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.internal == 0.into() {
            return write!(f, "0");
        }
        let p = P as usize;
        let digits = self.internal.to_string().into_bytes();
        let ctz = digits
            .iter()
            .rev()
            .take_while(|&&b| b == b'0')
            .count()
            .min(p);
        if digits.len() < p {
            let fill = "0".repeat(p - digits.len());
            let frac = &digits[0..digits.len() - ctz];
            write!(f, "0.{}{}", fill, unsafe { str::from_utf8_unchecked(frac) })
        } else {
            let (mut int, mut frac) = digits.split_at(digits.len() - p);
            frac = &frac[0..frac.len() - ctz];
            if int.is_empty() {
                int = &[b'0'];
            }
            if ctz == p {
                write!(f, "{}", unsafe { str::from_utf8_unchecked(int) })
            } else {
                write!(
                    f,
                    "{}.{}",
                    unsafe { str::from_utf8_unchecked(int) },
                    unsafe { str::from_utf8_unchecked(frac) }
                )
            }
        }
    }
}

impl<const P: u8> fmt::Debug for UDecimal<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self}")
    }
}

// TODO: The following mathematical operations may result in overflow. This is
// fine for our current use-case. But should be handled if we want to release as
// a separate library.

impl<const P: u8> ops::Mul for UDecimal<P> {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self::Output {
        Self {
            internal: (self.internal * rhs.internal) / U256::exp10(P as usize),
        }
    }
}

impl<const P: u8> ops::Mul<U256> for UDecimal<P> {
    type Output = Self;
    fn mul(self, rhs: U256) -> Self::Output {
        Self {
            internal: self.internal * rhs,
        }
    }
}

impl<const P: u8> ops::Div for UDecimal<P> {
    type Output = Self;
    fn div(self, rhs: Self) -> Self::Output {
        Self {
            internal: (self.internal * U256::exp10(P as usize)) / rhs.internal,
        }
    }
}

impl<const P: u8> ops::Add for UDecimal<P> {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self {
            internal: self.internal + rhs.internal,
        }
    }
}

impl<const P: u8> ops::Sub for UDecimal<P> {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            internal: self.internal - rhs.internal,
        }
    }
}

impl<const P: u8> ops::AddAssign for UDecimal<P> {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

impl<const P: u8> ops::SubAssign for UDecimal<P> {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
    }
}

impl<const P: u8> iter::Sum for UDecimal<P> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::zero(), |sum, x| sum + x)
    }
}

#[allow(dead_code)]
impl<const P: u8> UDecimal<P> {
    pub fn zero() -> Self {
        Self { internal: 0.into() }
    }

    pub fn from_little_endian(bytes: &[u8; 32]) -> Self {
        Self {
            internal: U256::from_little_endian(bytes),
        }
    }

    pub fn change_precision<const N: u8>(self) -> UDecimal<N> {
        UDecimal {
            internal: match N.cmp(&P) {
                Ordering::Greater => self.internal * (U256::exp10((N - P) as usize)),
                Ordering::Less => self.internal / (U256::exp10((P - N) as usize)),
                Ordering::Equal => self.internal,
            },
        }
    }

    pub fn shift<const N: u8>(self) -> UDecimal<N> {
        UDecimal {
            internal: self.internal,
        }
    }

    pub fn as_u256(&self) -> U256 {
        self.internal / U256::exp10(P as usize)
    }

    pub fn as_f64(&self) -> f64 {
        // Collect the little-endian bytes of the U256 value.
        let mut le_u8 = [0u8; 32];
        self.internal.to_little_endian(&mut le_u8);
        // Merge the 32 bytes into 4 u64 values to reduce the amount of float
        // operations required to calculate the final value.
        let mut le_u64 = [0u64; 4];
        for (i, entry) in le_u64.iter_mut().enumerate() {
            *entry = u64::from_le_bytes(le_u8[(i * 8)..((i + 1) * 8)].try_into().unwrap());
        }
        // Count trailing u64 zero values. This is used to avoid unnecessary
        // multiplications by zero.
        let ctz = le_u64.iter().rev().take_while(|&&b| b == 0).count();
        // Sum the terms and then divide by 10^P, where each term equals
        // 2^(64i) * n.
        le_u64
            .iter()
            .enumerate()
            .take(le_u64.len() - ctz)
            .map(|(i, &n)| 2.0f64.powi(i as i32 * 64) * n as f64)
            .sum::<f64>()
            / 10.0f64.powi(P as i32)
    }

    pub fn to_little_endian(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        self.internal.to_little_endian(&mut buf);
        buf
    }

    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            internal: self.internal.saturating_add(other.internal),
        }
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            internal: self.internal.saturating_sub(other.internal),
        }
    }
}

impl<const P: u8> TryFrom<f64> for UDecimal<P> {
    type Error = FromF64Error;
    fn try_from(mut from: f64) -> Result<Self, Self::Error> {
        if from.is_nan() || (from < 0.0) {
            return Err(FromF64Error::InvalidInput);
        }
        const U128_MAX: f64 = u128::MAX as f64;
        from *= 10.0f64.powi(P as i32);
        let lower = from.min(U128_MAX);
        from -= lower;
        let lower = lower as u128;
        // This can result in some nasty loss of precision for low (nonzero) values of upper.
        let upper = (from / U128_MAX).round() as u128;
        let mut le_u8 = [0u8; 32];
        le_u8[0..16].copy_from_slice(&lower.to_le_bytes());
        le_u8[16..32].copy_from_slice(&upper.to_le_bytes());
        Ok(Self {
            internal: U256::from_little_endian(&le_u8),
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FromF64Error {
    InvalidInput,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr as _;

    #[test]
    fn udecimal() {
        test_udecimal::<6>(&[
            ("", None),
            ("?", None),
            (".", None),
            ("1.1.1", None),
            ("10.10?1", None),
            ("1?0.01", None),
            ("0", Some(("0", 0))),
            ("0.0", Some(("0", 0))),
            (".0", Some(("0", 0))),
            ("0.", Some(("0", 0))),
            ("00.00", Some(("0", 0))),
            ("1", Some(("1", 1_000_000))),
            ("1.0", Some(("1", 1_000_000))),
            ("1.", Some(("1", 1_000_000))),
            ("0.1", Some(("0.1", 100_000))),
            (".1", Some(("0.1", 100_000))),
            ("0.0000012", Some(("0.000001", 1))),
            ("0.001001", Some(("0.001001", 1_001))),
            ("0.001", Some(("0.001", 1_000))),
            ("100.001", Some(("100.001", 100_001_000))),
            ("100.000", Some(("100", 100_000_000))),
            ("123.0", Some(("123", 123_000_000))),
            ("123", Some(("123", 123_000_000))),
            (
                "123456789.123456789",
                Some(("123456789.123456", 123_456_789_123_456)),
            ),
        ]);
        test_udecimal::<0>(&[
            ("0", Some(("0", 0))),
            ("1", Some(("1", 1))),
            ("0.1", Some(("0", 0))),
            ("123456789", Some(("123456789", 123_456_789))),
            ("123.1", Some(("123", 123))),
        ]);
    }

    fn test_udecimal<const P: u8>(tests: &[(&str, Option<(&str, u64)>)]) {
        for (input, expected) in tests {
            println!("input: \"{}\"", input);
            let d = UDecimal::<P>::from_str(input);
            match expected {
                &Some((repr, internal)) => {
                    assert_eq!(d.as_ref().map(|d| d.internal), Ok(internal.into()));
                    assert_eq!(d.as_ref().map(ToString::to_string), Ok(repr.into()));
                }
                None => assert_eq!(d, Err(ParseStrError::InvalidInput)),
            }
        }
    }

    #[test]
    fn udecimal_from_f64() {
        let tests = [
            0.0,
            0.5,
            0.01,
            0.0042,
            1.0,
            123.456,
            1e14,
            1e18,
            2.0f64.powi(128) - 1.0,
            2.0f64.powi(128),
            1e26,
            // 1e27, // -> 2.085% error @ P = 12
            // 1e28, // -> 1.318% error @ P = 12
            1e29,
            1e30,
            1e32,
        ];
        for test in tests {
            test_udecimal_from_f64::<0>(test);
            test_udecimal_from_f64::<1>(test);
            test_udecimal_from_f64::<6>(test);
            test_udecimal_from_f64::<12>(test);
        }
    }

    fn test_udecimal_from_f64<const P: u8>(value: f64) {
        let expected = (value * 10.0f64.powi(P as i32)).floor();
        let decimal = UDecimal::<P>::try_from(value).unwrap();
        let output = decimal.internal.to_string().parse::<f64>().unwrap();
        let error = (expected - output).abs() / expected.max(1e-30);
        println!(
            "expected: {}\n decimal: {}\n   error: {:.3}%\n---",
            expected / 10.0f64.powi(P as i32),
            decimal,
            error * 100.0
        );
        assert!(error < 0.005);
    }
}
