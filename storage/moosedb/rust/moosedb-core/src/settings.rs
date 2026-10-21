//! Process-wide tunables, mirrored from the `moosedb_*` system variables.
//! Every value is an atomic so the server can change them at runtime.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

/// Default of `moosedb_batch_memory_budget`: 1 GiB for the buffers of every
/// open statement batch of the process together.
pub const DEFAULT_BATCH_MEMORY_BUDGET: u64 = 1 << 30;

pub struct Settings {
    retention_check_interval_secs: AtomicU64,
    compaction_trigger_chunks: AtomicU32,
    bloom_fpr_bits: AtomicU64,
    chunk_cache_bytes: AtomicU64,
    max_open_chunks: AtomicU32,
    batch_memory_budget_bytes: AtomicU64,
    unique_state_bytes: AtomicU64,
}

static SETTINGS: Settings = Settings {
    retention_check_interval_secs: AtomicU64::new(3600),
    compaction_trigger_chunks: AtomicU32::new(10),
    bloom_fpr_bits: AtomicU64::new(0x3F84_7AE1_47AE_147B), // 0.01
    chunk_cache_bytes: AtomicU64::new(128 << 20),
    max_open_chunks: AtomicU32::new(100),
    batch_memory_budget_bytes: AtomicU64::new(DEFAULT_BATCH_MEMORY_BUDGET),
    unique_state_bytes: AtomicU64::new(0),
};

pub fn get() -> &'static Settings {
    &SETTINGS
}

impl Settings {
    /// Seconds between retention sweeps of one table; 0 disables background sweeps.
    pub fn retention_check_interval_secs(&self) -> u64 {
        self.retention_check_interval_secs.load(Relaxed)
    }
    pub fn set_retention_check_interval_secs(&self, v: u64) {
        self.retention_check_interval_secs.store(v, Relaxed);
    }

    /// Chunks in one time bucket that trigger automatic compaction (min 2).
    pub fn compaction_trigger_chunks(&self) -> u32 {
        self.compaction_trigger_chunks.load(Relaxed).max(2)
    }
    pub fn set_compaction_trigger_chunks(&self, v: u32) {
        self.compaction_trigger_chunks.store(v, Relaxed);
    }

    pub fn bloom_fpr(&self) -> f64 {
        f64::from_bits(self.bloom_fpr_bits.load(Relaxed))
    }
    pub fn set_bloom_fpr(&self, v: f64) {
        if v.is_finite() && v > 0.0 && v < 1.0 {
            self.bloom_fpr_bits.store(v.to_bits(), Relaxed);
        }
    }

    /// Bytes of decoded chunk blocks kept in memory; 0 disables the cache.
    pub fn chunk_cache_bytes(&self) -> u64 {
        self.chunk_cache_bytes.load(Relaxed)
    }
    pub fn set_chunk_cache_bytes(&self, v: u64) {
        self.chunk_cache_bytes.store(v, Relaxed);
        crate::cache::resize(v);
    }

    /// Chunk file descriptors kept open between reads (min 1).
    pub fn max_open_chunks(&self) -> u32 {
        self.max_open_chunks.load(Relaxed).max(1)
    }
    pub fn set_max_open_chunks(&self, v: u32) {
        self.max_open_chunks.store(v, Relaxed);
    }

    /// Bytes the row buffers of all open batches of the process may hold
    /// together; a batch that crosses it spills early. 0 selects the default.
    pub fn batch_memory_budget_bytes(&self) -> u64 {
        match self.batch_memory_budget_bytes.load(Relaxed) {
            0 => DEFAULT_BATCH_MEMORY_BUDGET,
            v => v,
        }
    }
    pub fn set_batch_memory_budget_bytes(&self, v: u64) {
        self.batch_memory_budget_bytes.store(v, Relaxed);
    }

    /// Bytes of UNIQUE-key state (an accelerator, see `unique.rs`) one table
    /// may keep; past it the coldest series are dropped. 0 (the default) = a
    /// quarter of the batch memory budget. Not a system variable.
    pub fn unique_state_bytes(&self) -> u64 {
        match self.unique_state_bytes.load(Relaxed) {
            0 => self.batch_memory_budget_bytes() / 4,
            v => v,
        }
    }
    pub fn set_unique_state_bytes(&self, v: u64) {
        self.unique_state_bytes.store(v, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn default_fpr_is_one_percent() {
        assert_eq!(super::get().bloom_fpr(), 0.01);
    }
}
