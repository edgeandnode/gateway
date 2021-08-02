use primitive_types::U256;
use std::{fmt, iter, ops, str};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParseStrError {
    InvalidInput,
}

/// Represents a positive decimal value with some fractional digit precision, P.
/// Using U256 as storage.
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Decimal<const P: usize> {
    internal: U256,
}

impl<I: Into<U256>, const P: usize> From<I> for Decimal<P> {
    fn from(from: I) -> Self {
        let internal = from.into() * U256::exp10(P);
        Self { internal }
    }
}

impl<const P: usize> str::FromStr for Decimal<P> {
    type Err = ParseStrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ParseStrError::*;
        let ascii_digit = |c: char| -> bool { ('0' <= c) && (c <= '9') };
        let valid_char = |c: char| -> bool { (c == '.') || ascii_digit(c) };
        if !s.chars().all(valid_char)
            || !s.chars().any(ascii_digit)
            || (s.chars().filter(|&c| c == '.').count() > 1)
        {
            return Err(InvalidInput);
        }

        let (int, mut frac) = s.split_at(s.chars().position(|c| c == '.').unwrap_or(s.len()));
        if frac.len() > 0 {
            frac = &frac[1..frac.len().min(P + 1)];
        }
        let digits = if frac.chars().all(|c| c == '0') {
            let fill = iter::repeat('0').take(P).collect::<String>();
            format!("{}{}", int, fill)
        } else {
            let fill = iter::repeat('0').take(P - frac.len()).collect::<String>();
            format!("{}{}{}", int, frac, fill)
        };
        Ok(Decimal {
            internal: U256::from_dec_str(&digits).map_err(|_| InvalidInput)?,
        })
    }
}

impl<const P: usize> fmt::Display for Decimal<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.internal == 0.into() {
            return write!(f, "0");
        }
        let digits = self.internal.to_string().into_bytes();
        let ctz = digits
            .iter()
            .rev()
            .take_while(|&&b| b == b'0')
            .count()
            .min(P);
        if digits.len() < P {
            let fill = iter::repeat('0').take(P - digits.len()).collect::<String>();
            let frac = &digits[0..digits.len() - ctz];
            write!(f, "0.{}{}", fill, unsafe { str::from_utf8_unchecked(frac) })
        } else {
            let (mut int, mut frac) = digits.split_at(digits.len() - P);
            frac = &frac[0..frac.len() - ctz];
            if int.len() == 0 {
                int = &[b'0'];
            }
            if ctz == P {
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

impl<const P: usize> fmt::Debug for Decimal<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl<const P: usize> ops::Mul for Decimal<P> {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self::Output {
        Self {
            internal: (self.internal * rhs.internal) / U256::exp10(P),
        }
    }
}

impl<const P: usize> ops::Div for Decimal<P> {
    type Output = Self;
    fn div(self, rhs: Self) -> Self::Output {
        Self {
            internal: (self.internal * U256::exp10(P)) / rhs.internal,
        }
    }
}

impl<const P: usize> Decimal<P> {
    pub fn conv<const N: usize>(&self) -> Decimal<N> {
        Decimal {
            internal: if N > P {
                self.internal * (U256::exp10(N - P))
            } else {
                self.internal / (U256::exp10(P - N))
            },
        }
    }

    pub fn as_f64(&self) -> f64 {
        self.to_string().parse().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr as _;

    #[test]
    fn decimal() {
        test_decimal::<6>(&[
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
        test_decimal::<0>(&[
            ("0", Some(("0", 0))),
            ("1", Some(("1", 1))),
            ("0.1", Some(("0", 0))),
            ("123456789", Some(("123456789", 123_456_789))),
            ("123.1", Some(("123", 123))),
        ]);
    }

    fn test_decimal<const P: usize>(tests: &[(&str, Option<(&str, u64)>)]) {
        for (input, expected) in tests {
            println!("input: \"{}\"", input);
            let d = Decimal::<P>::from_str(input);
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
