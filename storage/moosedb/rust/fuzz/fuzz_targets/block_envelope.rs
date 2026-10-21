#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| moosedb_core::fuzz_api::block_envelope(data));
