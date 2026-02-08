use std::hint::{black_box, spin_loop};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};
use std::thread::{self};
use wait_on_address::AtomicWait;

/// Gets the first index of `value` within `slice`, or returns [`None`] if
/// it was not found.
pub fn index_of<T: PartialEq>(value: &T, slice: &[T]) -> Option<usize> {
    slice.iter().position(|x| x == value)
}

/// A synchronization primitive for blocking threads until an event occurs.
/// The primitive is reusable and will wake up all pending listeners each time
/// it is signaled.
#[derive(Debug, Default)]
pub struct Event {
    /// The inner atomic representation of the event.
    /// Stores the version number in the lower bits.
    /// The uppermost bit is reserved for [`Self::WAITER_FLAG`],
    /// which indicates whether threads are sleeping on this event.
    atomic: AtomicU64,
}

impl Event {
    /// When set, this bit indicates that threads are waiting
    /// on the event counter atomic.
    const WAITER_FLAG: u64 = 1 << 63;

    /// Initializes a new event.
    pub const fn new() -> Self {
        Self {
            atomic: AtomicU64::new(0),
        }
    }

    /// Notifies all listeners that this event has changed.
    /// This operation guarantees a happens-before relationship between
    /// all preceeding code and all subsequent calls to [`Self::listen`].
    pub fn notify(&self) {
        let atomic = self.atomic.fetch_add(1, Ordering::SeqCst);
        let new_value = atomic + 1;

        if (atomic & Self::WAITER_FLAG) != 0 {
            let _ = self.atomic.compare_exchange(
                new_value,
                new_value & !Event::WAITER_FLAG,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            self.atomic.notify_all();
        }
    }

    /// Begins listening for changes to `self`. The event listener
    /// will block until [`Self::notify`] is called by another thread.
    /// This operation guarantees a happens-before relationship between
    /// the last call to [`Self::listen`] and all subsequent code.
    pub fn listen(&self) -> EventListener<'_> {
        EventListener {
            event: self,
            version: self.atomic.load(Ordering::SeqCst) & !Self::WAITER_FLAG,
        }
    }
}

/// Allows for waiting for an [`Event`] to change.
#[derive(Copy, Clone, Debug)]
pub struct EventListener<'a> {
    /// The event in question.
    event: &'a Event,
    /// The version number of the event when this listener was created.
    version: u64,
}

impl<'a> EventListener<'a> {
    /// Whether [`Event::notify`] has been called at least once after this
    /// object's creation.
    pub fn signaled(&self) -> bool {
        let atomic = self.event.atomic.load(Ordering::Relaxed);
        if self.version == (atomic & !Event::WAITER_FLAG) {
            false
        } else {
            // Since the version incremented, memory writes from signaling threads
            // must be made visible to this thread.
            fence(Ordering::Acquire);
            true
        }
    }

    /// Blocks the current thread, spinning in a loop for `cycles` before
    /// falling back to blocking with the operating system scheduler.
    /// Returns `true` only if the thread was put to sleep by the operating
    /// system.
    pub fn spin_wait(&self, cycles: usize) -> bool {
        let mut i = 0;
        while i < cycles {
            if self.signaled() {
                return false;
            }

            black_box(i);
            spin_loop();
            i += 1;
        }

        self.wait();
        true
    }

    /// Blocks the current thread. Returns when [`Event::notify`] has been
    /// called at least once since this object's creation.
    pub fn wait(&self) {
        loop {
            let (Ok(atomic) | Err(atomic)) = self.event.atomic.compare_exchange(
                self.version,
                self.version | Event::WAITER_FLAG,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );

            if self.version == (atomic & !Event::WAITER_FLAG) {
                self.event.atomic.wait(self.version | Event::WAITER_FLAG);
            } else {
                break;
            }
        }

        // Since the version incremented, memory writes from signaling threads
        // must be made visible to this thread.
        fence(Ordering::Acquire);
    }
}

/// Invokes the closure `f`. If `f` panics and unwinding reaches this stack
/// frame, then aborts the process.
pub fn abort_on_panic<R, F: FnOnce() -> R>(f: F) -> R {
    /// Ensures that the program aborts on any unhandled panic where this object
    /// is in scope. The provided message will be printed.
    struct PanicGuard;

    impl Drop for PanicGuard {
        fn drop(&mut self) {
            if thread::panicking() {
                std::process::abort();
            }
        }
    }

    let _guard = PanicGuard;
    f()
}