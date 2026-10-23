//! Encryption at rest: AES-256-GCM (authenticated), plus read-only support for
//! the AES-256-CTR files of earlier versions (no MAC: format chunk v2 / WAL v2
//! with the ENCRYPTED flag).
//!
//! Keys come from the server's key management plugin through a provider
//! callback installed by the FFI layer. Every encrypted file carries the key
//! id, key version and a random 128-bit IV in a plaintext header, so files
//! stay readable after key rotation (old versions remain fetchable) and two
//! files never share a keystream. CTR is seekable, so any byte range — a
//! single column block, a WAL entry — is decrypted independently.

use std::sync::{Arc, RwLock};

use crate::bytes::{put_u32, ByteReader};
use crate::error::{corrupt, Error, Result};

pub const KEY_LEN: usize = 32;
pub const IV_LEN: usize = 16;
/// Serialized size of [`CipherParams`] (key id, version, IV, CRC).
pub const PARAMS_LEN: usize = 32;
/// GCM authentication tag appended to every sealed unit.
pub const TAG_LEN: usize = 16;

#[cfg(feature = "encryption")]
type Gcm = aes_gcm::AesGcm<aes::Aes256, aes_gcm::aead::consts::U16>;

/// `(key_id, version)` → `(version, key)`. `version == None` asks for the latest.
pub type KeyProvider = dyn Fn(u32, Option<u32>) -> Result<(u32, [u8; KEY_LEN])> + Send + Sync;

static PROVIDER: RwLock<Option<Arc<KeyProvider>>> = RwLock::new(None);

pub fn set_key_provider(p: Option<Arc<KeyProvider>>) {
    if let Ok(mut slot) = PROVIDER.write() {
        *slot = p;
    }
}

fn fetch_key(key_id: u32, version: Option<u32>) -> Result<(u32, [u8; KEY_LEN])> {
    let provider = PROVIDER
        .read()
        .ok()
        .and_then(|p| p.clone())
        .ok_or_else(|| Error::Unsupported("no encryption key provider (load a key management plugin)".into()))?;
    provider(key_id, version)
}

/// Latest key version the provider offers for `key_id`.
pub(crate) fn latest_version(key_id: u32) -> Result<u32> {
    fetch_key(key_id, None).map(|(v, _)| v)
}

/// Encryption parameters of one file, plus the resolved key.
#[derive(Clone)]
pub struct CipherParams {
    pub key_id: u32,
    pub key_version: u32,
    iv: [u8; IV_LEN],
    key: [u8; KEY_LEN],
    /// Present for authenticated (GCM) files: what the tags are bound to.
    auth: Option<Arc<Auth>>,
}

#[cfg_attr(not(feature = "encryption"), allow(dead_code))]
struct Auth {
    bind: Vec<u8>,
    #[cfg(feature = "encryption")]
    gcm: Gcm,
}

impl std::fmt::Debug for CipherParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CipherParams").field("key_id", &self.key_id).field("key_version", &self.key_version).finish()
    }
}

impl CipherParams {
    /// Parameters for a new file: latest key version, fresh random IV.
    pub(crate) fn for_new_file(key_id: u32) -> Result<CipherParams> {
        let (key_version, key) = fetch_key(key_id, None)?;
        let mut iv = [0u8; IV_LEN];
        getrandom::getrandom(&mut iv).map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        Ok(CipherParams { key_id, key_version, iv, key, auth: None })
    }

    /// The file's random IV (identifies the file; the MANIFEST records it).
    pub(crate) fn iv(&self) -> [u8; IV_LEN] {
        self.iv
    }

    pub(crate) fn encode(&self) -> [u8; PARAMS_LEN] {
        let mut b = Vec::with_capacity(PARAMS_LEN);
        put_u32(&mut b, self.key_id);
        put_u32(&mut b, self.key_version);
        b.extend_from_slice(&self.iv);
        put_u32(&mut b, 0);
        let crc = crc32fast::hash(&b);
        put_u32(&mut b, crc);
        let mut out = [0u8; PARAMS_LEN];
        out.copy_from_slice(&b);
        out
    }

    /// Parses a header written by `encode` and fetches the matching key.
    pub(crate) fn decode(b: &[u8]) -> Result<CipherParams> {
        if b.len() < PARAMS_LEN || crc32fast::hash(&b[..28]).to_le_bytes() != b[28..32] {
            return Err(corrupt("encryption header checksum mismatch"));
        }
        let mut r = ByteReader::new(b);
        let key_id = r.u32()?;
        let version = r.u32()?;
        let mut iv = [0u8; IV_LEN];
        iv.copy_from_slice(r.take(IV_LEN)?);
        let (key_version, key) = fetch_key(key_id, Some(version))?;
        Ok(CipherParams { key_id, key_version, iv, key, auth: None })
    }

    /// Makes these parameters authenticated: units are sealed with AES-256-GCM
    /// and the tags are bound to `bind` (the container header bytes).
    #[cfg(feature = "encryption")]
    pub(crate) fn authenticated(mut self, bind: Vec<u8>) -> CipherParams {
        use aes_gcm::aead::KeyInit;
        let gcm = Gcm::new(&self.key.into());
        self.auth = Some(Arc::new(Auth { bind, gcm }));
        self
    }

    #[cfg(not(feature = "encryption"))]
    pub(crate) fn authenticated(self, _bind: Vec<u8>) -> CipherParams {
        unreachable!("CipherParams cannot be built without the encryption feature")
    }

    /// False for files of the earlier, unauthenticated CTR format.
    pub(crate) fn is_authenticated(&self) -> bool {
        self.auth.is_some()
    }

    /// Encrypts `data` in place (the unit that starts at byte `pos` of the
    /// file) and returns its tag. `ctx` is extra associated data.
    #[cfg(feature = "encryption")]
    pub(crate) fn seal(&self, data: &mut [u8], pos: u64, ctx: &[u8]) -> Result<[u8; TAG_LEN]> {
        use aes_gcm::aead::AeadInPlace;
        let a = self.auth.as_ref().ok_or_else(|| Error::Crypto("sealing with unauthenticated parameters".into()))?;
        let aad = [&a.bind[..], ctx].concat();
        let tag = a
            .gcm
            .encrypt_in_place_detached(&self.nonce(pos).into(), &aad, data)
            .map_err(|_| Error::Crypto("unit too large to encrypt".into()))?;
        Ok(tag.into())
    }

    /// Verifies `tag` and decrypts `data` in place. `false` = the unit was
    /// altered, moved, or the key is wrong (not distinguishable); `data` is
    /// then unspecified and must not be used.
    #[cfg(feature = "encryption")]
    pub(crate) fn open(&self, data: &mut [u8], tag: &[u8], pos: u64, ctx: &[u8]) -> bool {
        use aes_gcm::aead::AeadInPlace;
        let (Some(a), Ok(tag)) = (self.auth.as_ref(), <[u8; TAG_LEN]>::try_from(tag)) else { return false };
        let aad = [&a.bind[..], ctx].concat();
        a.gcm.decrypt_in_place_detached(&self.nonce(pos).into(), &aad, data, &tag.into()).is_ok()
    }

    #[cfg(not(feature = "encryption"))]
    pub(crate) fn seal(&self, _data: &mut [u8], _pos: u64, _ctx: &[u8]) -> Result<[u8; TAG_LEN]> {
        unreachable!("CipherParams cannot be built without the encryption feature")
    }

    #[cfg(not(feature = "encryption"))]
    pub(crate) fn open(&self, _data: &mut [u8], _tag: &[u8], _pos: u64, _ctx: &[u8]) -> bool {
        unreachable!("CipherParams cannot be built without the encryption feature")
    }

    #[cfg(feature = "encryption")]
    fn nonce(&self, pos: u64) -> [u8; IV_LEN] {
        let mut n = self.iv;
        for (b, p) in n[IV_LEN - 8..].iter_mut().zip(pos.to_be_bytes()) {
            *b ^= p;
        }
        n
    }

    /// Legacy AES-256-CTR (no authentication): XORs `data` with the keystream
    /// starting at byte `offset` of the stream. Only used to read old files.
    #[cfg(feature = "encryption")]
    pub(crate) fn apply_ctr(&self, data: &mut [u8], offset: u64) {
        use aes::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
        let mut c = ctr::Ctr128BE::<aes::Aes256>::new(&self.key.into(), &self.iv.into());
        c.seek(offset);
        c.apply_keystream(data);
    }

    #[cfg(not(feature = "encryption"))]
    pub(crate) fn apply_ctr(&self, _data: &mut [u8], _offset: u64) {
        unreachable!("CipherParams cannot be built without the encryption feature")
    }
}

/// Fails early when encryption was requested but is not compiled in.
pub(crate) fn ensure_available() -> Result<()> {
    if cfg!(feature = "encryption") {
        Ok(())
    } else {
        Err(Error::Unsupported("MooseDB was built without the `encryption` feature".into()))
    }
}

#[cfg(test)]
pub(crate) mod test_keys {
    //! Deterministic key provider for tests: key = [id ^ version; 32].
    use super::*;

    pub(crate) fn install() {
        set_key_provider(Some(Arc::new(|id: u32, version: Option<u32>| {
            if id == 99 {
                return Err(Error::NotFound(format!("key {id}")));
            }
            let v = version.unwrap_or(2);
            // Key 7 is the one the moosedb-tests fixtures were written with.
            let b = if id == 7 { (7u8.wrapping_mul(31)) ^ v as u8 } else { (id ^ v) as u8 };
            Ok((v, [b; KEY_LEN]))
        })));
    }
}

#[cfg(all(test, feature = "encryption"))]
mod tests {
    use super::*;

    #[test]
    fn ctr_is_seekable_and_reversible() {
        test_keys::install();
        let p = CipherParams::for_new_file(1).unwrap();
        assert_eq!(p.key_version, 2);
        let plain: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        let mut whole = plain.clone();
        p.apply_ctr(&mut whole, 0);
        assert_ne!(whole, plain);
        // Decrypting a middle slice on its own gives the same plaintext.
        let mut part = whole[333..777].to_vec();
        p.apply_ctr(&mut part, 333);
        assert_eq!(part, &plain[333..777]);

        let q = CipherParams::decode(&p.encode()).unwrap();
        let mut back = whole.clone();
        q.apply_ctr(&mut back, 0);
        assert_eq!(back, plain);

        let mut bad = p.encode();
        bad[5] ^= 1;
        assert!(CipherParams::decode(&bad).is_err());
        assert!(CipherParams::for_new_file(99).is_err());
    }

    #[test]
    fn gcm_units_bind_position_context_and_header() {
        test_keys::install();
        let p = CipherParams::for_new_file(1).unwrap().authenticated(b"header".to_vec());
        let plain: Vec<u8> = (0..300u32).map(|i| i as u8).collect();
        let mut unit = plain.clone();
        let tag = p.seal(&mut unit, 64, b"x").unwrap();
        assert_ne!(unit, plain);

        let mut ok = unit.clone();
        assert!(p.open(&mut ok, &tag, 64, b"x"));
        assert_eq!(ok, plain);
        for (pos, ctx) in [(65u64, &b"x"[..]), (64, b"y"), (0, b"x")] {
            assert!(!p.open(&mut unit.clone(), &tag, pos, ctx), "moved unit accepted");
        }
        let other = CipherParams::decode(&p.encode()).unwrap().authenticated(b"headeR".to_vec());
        assert!(!other.open(&mut unit.clone(), &tag, 64, b"x"), "different header accepted");
        let same = CipherParams::decode(&p.encode()).unwrap().authenticated(b"header".to_vec());
        assert!(same.open(&mut unit.clone(), &tag, 64, b"x"));
        let mut flipped = unit.clone();
        flipped[17] ^= 1;
        assert!(!p.open(&mut flipped, &tag, 64, b"x"));
        let mut bad_tag = tag;
        bad_tag[0] ^= 1;
        assert!(!p.open(&mut unit.clone(), &bad_tag, 64, b"x"));
    }
}
