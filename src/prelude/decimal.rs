use primitive_types::U256;
use std::{fmt, iter, ops, str};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParseStrError {
    InvalidInput,
}

/// Represents a positive decimal value with some fractional digit precision, P.
/// Using U256 as storage.
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct UDecimal<const P: u8> {
    internal: U256,
}

macro_rules! impl_from_uints {
    ($($t:ty),+) => {
        $(impl<const P: u8> std::convert::TryInto<UDecimal<P>> for $t {
            type Error = &'static str;
            fn try_into(self) -> Result<UDecimal<P>, Self::Error> {
                let internal = U256::from(self)
                    .checked_mul(U256::exp10(P as usize))
                    .ok_or("overflow")?;
                Ok(UDecimal { internal })
            }
        })*
    };
}

impl_from_uints!(u8, u16, u32, u64, u128, usize, U256);

impl<const P: u8> str::FromStr for UDecimal<P> {
    type Err = ParseStrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ParseStrError::*;
        let ascii_digit = |c: char| -> bool { ('0' <= c) && (c <= '9') };
        if !s.chars().any(ascii_digit) {
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
            let fill = iter::repeat('0').take(p - digits.len()).collect::<String>();
            let frac = &digits[0..digits.len() - ctz];
            write!(f, "0.{}{}", fill, unsafe { str::from_utf8_unchecked(frac) })
        } else {
            let (mut int, mut frac) = digits.split_at(digits.len() - p);
            frac = &frac[0..frac.len() - ctz];
            if int.len() == 0 {
                int = &[b'0'];
            }
            if ctz == p {
                write!(f, "{}", unsafe { str::from_utf8_unchecked(&int) })
            } else {
                write!(
                    f,
                    "{}.{}",
                    unsafe { str::from_utf8_unchecked(&int) },
                    unsafe { str::from_utf8_unchecked(&frac) }
                )
            }
        }
    }
}

impl<const P: u8> fmt::Debug for UDecimal<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl<const P: u8> ops::Mul for UDecimal<P> {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self::Output {
        Self {
            internal: (self.internal * rhs.internal) / U256::exp10(P as usize),
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

impl<const P: u8> UDecimal<P> {
    pub fn change_precision<const N: u8>(&self) -> UDecimal<N> {
        UDecimal {
            internal: if N > P {
                self.internal * (U256::exp10((N - P) as usize))
            } else if N < P {
                self.internal / (U256::exp10((P - N) as usize))
            } else {
                self.internal
            },
        }
    }

    pub fn as_f64(&self) -> f64 {
        // TODO: avoid relying on string conversions for this
        self.to_string().parse().unwrap()
    }
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
}
