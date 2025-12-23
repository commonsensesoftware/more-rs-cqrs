use rc2::{
    cipher::{generic_array::GenericArray, BlockDecrypt, BlockEncrypt, InvalidLength},
    Rc2,
};
use uuid::Uuid;

/// Defines the behavior of a mask that can be used to obfuscate the value a [version](Version).
///
/// # Remarks
///
/// Encrypting a [version](Version) is **not** about security. It is about obfuscation to ensure the
/// encoded value is opaque and only understood by the storage provider that generated it. Generating
/// a compromised value that would be accepted by a store assumes the risk of breaking the concurrency
/// guarantees provided by the storage provider. There is nothing inherently secret about the value.
pub trait Mask: Send + Sync {
    /// Encrypts the provided data.
    ///
    /// # Arguments
    ///
    /// * `data` - the data to encrypt
    fn mask(&self, data: [u8; 8]) -> [u8; 8];

    /// Decrypts the provided data.
    ///
    /// # Arguments
    ///
    /// * `data` - the data to encrypt
    fn unmask(&self, data: [u8; 8]) -> [u8; 8];
}

/* REMARKS
 * An entity version is a binary-encoded 64-bit value that is only meant to be known to the storage that provided
 * it and is opaque to all consumers; however, if you know or discover the algorithm used by the storage provider,
 * the value can easily be reproduced. The ciphers used here for masking are NOT about security. They are used
 * purely for obfuscation. Given the block size, the following ciphers are candidates:
 *
 * - DES
 * - RC2
 * - Blowfish
 * - XTEA
 *
 * Processing is always a single block so the algorithm that is used almost irrelevant. RC2 was selected for its
 * key size and speed.
 *
 * If the underlying key is compromised, the worst thing that can happen is that a user can potentially construct a
 * value that would violate the concurrency semantics of the store that generated it. A version does not contain any
 * secrets. The obfuscated version is never stored. A storage provider is allowed to change the masking approach it
 * uses, if at all, at any time.
 */

/// Represents a default, secure mask.
///
/// # Remarks
///
/// This [mask](Mask) uses [RC2](https://en.wikipedia.org/wiki/RC2) as described in
/// [RFC 2268](https://www.rfc-editor.org/rfc/rfc2268.html) to encrypt and decrypt the encoded value.
#[derive(Clone)]
pub struct SecureMask(Rc2);

impl SecureMask {
    /// Creates a new instance of the [SecureMask].
    ///
    /// # Arguments
    ///
    /// * `key` - the key to initialize the mask with
    ///
    /// # Returns
    ///
    /// The new [SecureMask] or [InvalidLength] if the key is not
    /// between 1 and 128 bytes.
    pub fn new<K: AsRef<[u8]>>(key: K) -> Result<Self, InvalidLength> {
        let key = key.as_ref();

        if key.is_empty() || key.len() > 128 {
            Err(InvalidLength)
        } else {
            Ok(Self::new_unchecked(key))
        }
    }

    /// Initializes a new [SecureMask] without checking the key size.
    ///
    /// # Arguments
    ///
    /// * `key` - the key, 1-128 bytes, to initialize the mask with
    #[inline]
    pub fn new_unchecked<K: AsRef<[u8]>>(key: K) -> Self {
        Self(Rc2::new_with_eff_key_len(key.as_ref(), key.as_ref().len()))
    }

    /// Creates a new key.
    pub fn new_key() -> [u8; 64] {
        let mut key = [0u8; 64];
        
        key[00..16].copy_from_slice(&Uuid::new_v4().to_bytes_le());
        key[16..32].copy_from_slice(&Uuid::new_v4().to_bytes_le());
        key[32..48].copy_from_slice(&Uuid::new_v4().to_bytes_le());
        key[48..64].copy_from_slice(&Uuid::new_v4().to_bytes_le());
        
        key
    }

    /// Initializes a new [SecureMask] using an ephemeral key.
    #[inline]
    pub fn ephemeral() -> Self {
        Self::new_unchecked(Self::new_key())
    }
}

impl Mask for SecureMask {
    fn mask(&self, data: [u8; 8]) -> [u8; 8] {
        let mut array: GenericArray<u8, _> = data.into();
        self.0.encrypt_block(&mut array);
        array.into()
    }

    fn unmask(&self, data: [u8; 8]) -> [u8; 8] {
        let mut array: GenericArray<u8, _> = data.into();
        self.0.decrypt_block(&mut array);
        array.into()
    }
}

impl AsRef<dyn Mask> for SecureMask {
    fn as_ref(&self) -> &(dyn Mask + 'static) {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Version;

    #[test]
    fn masked_version_should_not_equal_unmasked_version() {
        // arrange
        let unmasked = (42u64 << 32) | 1u64;
        let original = Version::from(unmasked);
        let mask = SecureMask::ephemeral();

        // act
        let masked = u64::from(original.mask(&mask));

        // assert
        assert_ne!(masked, unmasked);
    }

    #[test]
    fn version_should_roundtrip_using_secure_mask() {
        // arrange
        let value = (42u64 << 32) | 1u64;
        let original = Version::from(value);
        let mask = SecureMask::ephemeral();

        // act
        let masked = original.mask(&mask);
        let unmasked = masked.unmask(mask);

        // assert
        assert_eq!(unmasked, original);
    }

    #[test]
    fn version_should_roundtrip_using_secure_mask_as_smart_pointer() {
        // arrange
        let value = (42u64 << 32) | 1u64;
        let original = Version::from(value);
        let mask: Box<dyn Mask> = Box::new(SecureMask::ephemeral());

        // act
        let masked = original.mask(&mask);
        let unmasked = masked.unmask(&mask);

        // assert
        assert_eq!(unmasked, original);
    }
}
