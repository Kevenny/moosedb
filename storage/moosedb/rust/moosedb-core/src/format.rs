//! Version numbers of every on-disk format, in one place (see "Formato em
//! disco v1.0" in docs/architecture.md). A reader accepts the versions listed
//! next to its constant and refuses any other with an explicit error; a
//! writer emits the *lowest* version that can express what it writes, so an
//! older binary keeps opening files that use no newer feature.

/// Chunk `.tfl`: 2 = plaintext, or AES-256-CTR without authentication
/// (legacy, read-only); 3 = AES-256-GCM, only with the ENCRYPTED flag.
pub(crate) const CHUNK_PLAIN: u16 = 2;
pub(crate) const CHUNK_AEAD: u16 = 3;

/// WAL segment: 1 = bare rows (read-only), 2 = batch entries, plaintext or
/// AES-256-CTR (legacy encrypted), 3 = batch entries, AES-256-GCM.
pub(crate) const WAL_V1: u16 = 1;
pub(crate) const WAL_PLAIN: u16 = 2;
pub(crate) const WAL_AEAD: u16 = 3;

/// MANIFEST: 1 = no `replay_seq` (read-only), 2 = no tombstones, 3 = with
/// tombstones (written from the first DELETE on), 4 = AES-256-GCM sealed
/// (only for encrypted tables; plaintext tables never write it).
pub(crate) const MANIFEST_V1: u16 = 1;
pub(crate) const MANIFEST_PLAIN: u16 = 2;
pub(crate) const MANIFEST_TOMB: u16 = 3;
pub(crate) const MANIFEST_AEAD: u16 = 4;

// Chunk flag `ZONES` (chunk.rs `flags::ZONES`): zone-map section at the end of
// the index+Bloom unit. Not a version: v2/v3 readers stop after the Bloom
// filter, so they ignore both the bytes and the flag.
