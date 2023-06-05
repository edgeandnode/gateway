use std::ops::Deref;
use std::str::FromStr;

use hdwallet::KeyChain;
use prelude::anyhow;
use secp256k1::{self, SecretKey};
use zeroize::Zeroize;

use crate::key_path;

#[derive(Clone)]
pub struct SignerKey(SecretKey);

impl SignerKey {
    pub fn from_slice(bytes: &[u8]) -> Result<Self, anyhow::Error> {
        let secret_key = SecretKey::from_slice(bytes)?;
        Ok(Self(secret_key))
    }
}

impl Default for SignerKey {
    fn default() -> Self {
        Self(secp256k1::ONE_KEY)
    }
}

impl From<SecretKey> for SignerKey {
    fn from(key: SecretKey) -> Self {
        Self(key)
    }
}

impl FromStr for SignerKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Wallet seed zeroized on drop
        let wallet_seed = bip39::Seed::new(
            &bip39::Mnemonic::from_phrase(s, bip39::Language::English)?,
            "",
        );
        let signer_key = hdwallet::DefaultKeyChain::new(
            hdwallet::ExtendedPrivKey::with_seed(wallet_seed.as_bytes()).expect("Invalid mnemonic"),
        )
        .derive_private_key(key_path("scalar/allocations").into())
        .expect("Failed to derive signer key")
        .0
        .private_key;

        // Convert between versions of secp256k1 lib.
        Ok(SignerKey::from_slice(signer_key.as_ref()).unwrap())
    }
}

impl Deref for SignerKey {
    type Target = SecretKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Zeroize for SignerKey {
    fn zeroize(&mut self) {
        self.0 = secp256k1::ONE_KEY;
    }
}

impl zeroize::ZeroizeOnDrop for SignerKey {}

impl Drop for SignerKey {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl std::fmt::Debug for SignerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SignerKey(..)")
    }
}

#[cfg(test)]
mod tests {
    use std::default::Default;

    use assert_matches::assert_matches;

    use super::*;

    #[test]
    fn from_slice() {
        //// Given
        let secret_key = secp256k1::ONE_KEY;

        //// When
        let signer_key = SignerKey::from_slice(secret_key.as_ref());

        //// Then
        assert_matches!(signer_key, Ok(key) => {
            assert_eq!(*key, secret_key);
        });
    }

    #[test]
    fn from_secret_key() {
        //// Given
        let secret_key = secp256k1::ONE_KEY;

        //// When
        let signer_key: SignerKey = secret_key.into();

        //// Then
        assert_eq!(*signer_key, secret_key);
    }

    /// It's a bit hard to verify the inner workings like zeroing without relying on undefined behavior.
    /// But, we can check that this at least runs.
    #[test]
    pub fn no_panic() {
        //// Given
        let secret_key = secp256k1::ONE_KEY;
        let signing_key = SignerKey::from(secret_key);

        //// When
        drop(signing_key);
    }

    #[test]
    fn fmt_debug() {
        //// Given
        let signer_key: SignerKey = SignerKey::default();

        //// When
        let signer_key_debug = format!("{:?}", signer_key);

        //// Then
        assert_eq!(signer_key_debug, "SignerKey(..)");
    }
}
