use rand::distributions::Alphanumeric;
use rand::Rng;

/// Generates and returns bytes of length `len`.
pub fn generate_bytes(len: usize) -> Vec<u8> {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(|c| c as u8)
        .collect::<Vec<u8>>()
}
