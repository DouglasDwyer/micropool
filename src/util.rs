use std::hint::{black_box, spin_loop};
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
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
    /// The version number - incremented each time the event changes.
    version: AtomicU64,
    /// The number of listeners that have begun sleeping.
    waiting_listeners: AtomicU32,
}

impl Event {
    /// Initializes a new event.
    pub const fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            waiting_listeners: AtomicU32::new(0),
        }
    }

    /// Notifies all listeners that this event has changed.
    pub fn notify(&self) {
        self.version.fetch_add(1, Ordering::Release);
        if 0 < self.waiting_listeners.load(Ordering::Acquire) {
            self.version.notify_all();
        }
    }

    /// Begins listening for changes to `self`. The event listener
    /// will block until [`Self::notify`] is called by another thread.
    pub fn listen(&self) -> EventListener<'_> {
        EventListener {
            event: self,
            version: self.version.load(Ordering::Acquire),
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
        self.event.version.load(Ordering::Acquire) != self.version
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
        self.event.waiting_listeners.fetch_add(1, Ordering::Release);
        while !self.signaled() {
            self.event.version.wait(self.version);
        }
        self.event.waiting_listeners.fetch_sub(1, Ordering::Release);
    }
}

/// Ensures that the program aborts on any unhandled panic where this object is
/// in scope. The provided message will be printed.
pub struct PanicGuard(pub &'static str);

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if thread::panicking() {
            // Panicking within another panic causes termination.
            panic!("{}", self.0);
        }
    }
}

/// Allows for erasing lifetimes and sharing references on the stack.
pub struct ScopedRef<T>(NonNull<ScopedHolder<T>>);

impl<T> ScopedRef<T> {
    /// Wraps `value` in a [`ScopedRef`], which is provided to `scope`.
    /// Once `scope` completes, this function will block until all
    /// clones of the reference are dropped.
    pub fn of<R>(value: T, scope: impl FnOnce(Self) -> R) -> R {
        let holder = ScopedHolder {
            ref_count: AtomicUsize::new(1),
            value,
        };
        let _dropper = ScopedHolderDropper(&holder);
        scope(ScopedRef(NonNull::from_ref(&holder)))
    }

    /// Determines whether two scoped references refer to the same underlying
    /// object.
    pub fn ptr_eq(lhs: &Self, rhs: &Self) -> bool {
        lhs.0 == rhs.0
    }
}

impl<T> Clone for ScopedRef<T> {
    fn clone(&self) -> Self {
        unsafe {
            self.0.as_ref().ref_count.fetch_add(1, Ordering::Release);
            Self(self.0)
        }
    }
}

impl<T> Deref for ScopedRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &self.0.as_ref().value }
    }
}

impl<T> Drop for ScopedRef<T> {
    fn drop(&mut self) {
        unsafe {
            self.0.as_ref().ref_count.fetch_sub(1, Ordering::Release);
        }
    }
}

unsafe impl<T: Send + Sync> Send for ScopedRef<T> {}
unsafe impl<T: Send + Sync> Sync for ScopedRef<T> {}

/// Stores the inner state for a [`ScopedRef`].
struct ScopedHolder<T> {
    /// The number of active references to this holder.
    ref_count: AtomicUsize,
    /// The underlying value.
    value: T,
}

/// Ensures that [`ScopedHolder`] is not freed until all references
/// to it are dropped.
struct ScopedHolderDropper<'a, T>(&'a ScopedHolder<T>);

impl<T> Drop for ScopedHolderDropper<'_, T> {
    fn drop(&mut self) {
        while 0 < self.0.ref_count.load(Ordering::Acquire) {
            spin_loop();
        }
    }
}
