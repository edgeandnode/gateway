use std::hash::{Hash, Hasher as _};

use siphasher::sip::SipHasher24;

pub fn sip24_hash(value: &impl Hash) -> u64 {
    let mut hasher = SipHasher24::default();
    value.hash(&mut hasher);
    hasher.finish()
}
