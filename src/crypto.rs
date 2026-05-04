/// AES-256-GCM envelope encryption.
///
/// Layout of every encrypted blob: `[12-byte nonce][ciphertext][16-byte GCM tag]`
/// The GCM tag is appended automatically by the `aes-gcm` crate.
///
/// Envelope scheme
/// ───────────────
///   • A random 256-bit DEK is generated for each record.
///   • The DEK is encrypted with the caller-supplied KEK → stored in `dek_encrypted`.
///   • The record payload is encrypted with the DEK → stored in `data`.
///   • `format_version = 1` in the DB row flags the record as encrypted.
#[cfg(feature = "encryption")]
mod inner {
    use aes_gcm::{
        Aes256Gcm, Key, Nonce,
        aead::{Aead, AeadCore, KeyInit, OsRng},
    };

    use crate::error::{Result, SquirrelError};

    const NONCE_LEN: usize = 12;
    const MIN_CIPHERTEXT_LEN: usize = NONCE_LEN + 16; // nonce + GCM tag

    pub fn encrypt(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
        let nonce  = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| SquirrelError::Other(format!("encrypt: {e}")))?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(nonce.as_slice());
        out.extend(ciphertext);
        Ok(out)
    }

    pub fn decrypt(key: &[u8; 32], data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < MIN_CIPHERTEXT_LEN {
            return Err(SquirrelError::Other("ciphertext too short".into()));
        }
        let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
        let nonce  = Nonce::from_slice(nonce_bytes);
        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| SquirrelError::Other(format!("decrypt: {e}")))
    }

    /// Derive a 256-bit KEK from a passphrase using Argon2id.
    /// `salt` must be at least 8 bytes; use [`random_salt`] to generate one.
    pub fn derive_kek(passphrase: &str, salt: &[u8]) -> Result<[u8; 32]> {
        let mut kek = [0u8; 32];
        argon2::Argon2::default()
            .hash_password_into(passphrase.as_bytes(), salt, &mut kek)
            .map_err(|e| SquirrelError::Other(format!("key derivation: {e}")))?;
        Ok(kek)
    }

    pub fn random_key() -> [u8; 32] {
        use rand::RngCore;
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }

    pub fn random_salt() -> [u8; 16] {
        use rand::RngCore;
        let mut salt = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut salt);
        salt
    }

    /// Generate a random DEK, encrypt `data` with it, then wrap the DEK with `kek`.
    /// Returns `(encrypted_data, encrypted_dek)`.
    pub fn encrypt_record(kek: &[u8; 32], data: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        let dek     = random_key();
        let enc_data = encrypt(&dek, data)?;
        let enc_dek  = encrypt(kek, &dek)?;
        Ok((enc_data, enc_dek))
    }

    /// Unwrap the DEK with `kek`, then decrypt `enc_data`.
    pub fn decrypt_record(kek: &[u8; 32], enc_data: &[u8], enc_dek: &[u8]) -> Result<Vec<u8>> {
        let dek_bytes = decrypt(kek, enc_dek)?;
        let dek: [u8; 32] = dek_bytes
            .try_into()
            .map_err(|_| SquirrelError::Other("decrypted DEK has wrong length".into()))?;
        decrypt(&dek, enc_data)
    }
}

#[cfg(feature = "encryption")]
pub(crate) use inner::{decrypt_record, derive_kek, encrypt_record};
