//! Shared by the read benchmarks: a counting global allocator.
//!
//! Lives in `moosedb-ffi` because `unsafe impl GlobalAlloc` needs `unsafe`,
//! which only this crate may use. Counts allocations made by the *calling
//! thread* while counting is on, so other test threads and the maintenance
//! pool do not add noise.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Counting;

thread_local! {
    // `const` initializers with no destructor: no lazy registration, so it is
    // safe to touch from inside the allocator.
    static ON: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

fn bump() {
    let _ = ON.try_with(|on| {
        if on.get() {
            let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        }
    });
}

// SAFETY: forwards every call to `System`; only adds a thread-local counter.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        bump();
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        bump();
        // SAFETY: same contract as the caller's.
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Runs `f` and returns its result and the number of allocations (malloc and
/// realloc) the calling thread made meanwhile.
pub fn count_allocs<T>(f: impl FnOnce() -> T) -> (T, u64) {
    ALLOCS.with(|n| n.set(0));
    ON.with(|on| on.set(true));
    let out = f();
    ON.with(|on| on.set(false));
    (out, ALLOCS.with(Cell::get))
}
