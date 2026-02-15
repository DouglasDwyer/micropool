use std::sync::{Arc, Weak};

use takecell::TakeOwnCell;

use crate::ThreadPoolState;

/// A task whose result is exclusively owned by the caller.
pub struct OwnedTask<T: 'static + Send>(Arc<TypedTaskInner<TakeOwnCell<T>>>);

impl<T: 'static + Send> OwnedTask<T> {
    /// Spawns a new task on the given pool.
    #[inline(always)]
    pub(crate) fn spawn(
        pool: &Arc<ThreadPoolState>,
        f: impl 'static + FnOnce() -> T + Send,
    ) -> Self {
        let inner = Arc::new(TypedTaskInner {
            func: TakeOwnCell::new(Box::new(|| TakeOwnCell::new(f()))),
            pool: Arc::downgrade(pool),
            result: spin::Once::new(),
        });

        pool.push_task(inner.clone());

        Self(inner)
    }

    /// Cancels this task, preventing it from running if it was not yet started.
    #[inline(always)]
    pub fn cancel(self) {
        if let Some(pool) = self.0.pool.upgrade() {
            pool.cancel_task(&self.0);
        }
    }

    /// Whether the task has been completed yet.
    #[inline(always)]
    pub fn complete(&self) -> bool {
        self.0.result.is_completed()
    }

    /// Steals any outstanding work on this task.
    /// Returns immediately if there are remaining units in progress on other
    /// threads.
    #[inline(always)]
    pub fn help(&self) {
        self.0.run();
    }

    /// Joins the current thread with this task, completing all remaining work.
    /// After all work is complete, yields the result.
    #[inline(always)]
    pub fn join(self) -> T {
        self.0.run();
        self.0
            .result
            .wait()
            .take()
            .expect("Failed to get result of task")
    }

    /// Attempts to get the result of this task if it has been completed.
    /// Otherwise, returns the original task.
    #[inline(always)]
    pub fn try_join(self) -> Result<T, Self> {
        if self.complete() {
            Ok(self.join())
        } else {
            Err(self)
        }
    }
}

impl<T: 'static + Send> std::fmt::Debug for OwnedTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedTask")
            .field("complete", &self.complete())
            .finish_non_exhaustive()
    }
}

/// A clonable task whose result can be shared by reference.
pub struct SharedTask<T: 'static + Send + Sync>(Arc<TypedTaskInner<T>>);

impl<T: 'static + Send + Sync> SharedTask<T> {
    /// Spawns a new task on the given pool.
    #[inline(always)]
    pub(crate) fn spawn(
        pool: &Arc<ThreadPoolState>,
        f: impl 'static + FnOnce() -> T + Send,
    ) -> Self {
        let inner = Arc::new(TypedTaskInner {
            func: TakeOwnCell::new(Box::new(f)),
            pool: Arc::downgrade(pool),
            result: spin::Once::new(),
        });

        pool.push_task(inner.clone());

        Self(inner)
    }

    /// Cancels this task, preventing it from running if it was not yet started.
    ///
    /// If this task was cloned, then [`Self::cancel`] must be called on all
    /// clones to have an effect.
    #[inline(always)]
    pub fn cancel(self) {
        // A shared task may only be cancelled if `self` is the only reference.
        // If there are multiple references **and** the task is still enqueued,
        // then the `strong_count` will be at least three (one for `self`, one for
        // the other reference, and one for the reference in the queue).
        // If the task has been removed from the queue, then the `strong_count` may be
        // less than three, but in that case `cancel_task` is a no-op. So, this
        // is correct.
        if Arc::strong_count(&self.0) < 3
            && let Some(pool) = self.0.pool.upgrade()
        {
            pool.cancel_task(&self.0);
        }
    }

    /// Whether the task has been completed yet.
    #[inline(always)]
    pub fn complete(&self) -> bool {
        self.0.result.is_completed()
    }

    /// Steals any outstanding work on this task.
    /// Returns immediately if there are remaining units in progress on other
    /// threads.
    #[inline(always)]
    pub fn help(&self) {
        self.0.run();
    }

    /// Joins the current thread with this task, completing all remaining work.
    /// After all work is complete, yields the result.
    #[inline(always)]
    pub fn join(&self) -> &T {
        self.0.run();
        self.0.result.wait()
    }

    /// Attempts to get the result of this task if it has been completed.
    /// Otherwise, returns the original task.
    #[inline(always)]
    pub fn try_join(&self) -> Option<&T> {
        if self.complete() {
            Some(self.join())
        } else {
            None
        }
    }
}

impl<T: 'static + Send + Sync> Clone for SharedTask<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: 'static + Send + Sync> std::fmt::Debug for SharedTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedTask")
            .field("complete", &self.complete())
            .finish_non_exhaustive()
    }
}

/// Allows for thread pools to manipulate the inner task state.
pub(crate) trait TaskInner: Send {
    /// Executes the inner task.
    /// Returns `true` if the task executed, or `false`
    /// if it was already taken by a different thread.
    fn run(&self) -> bool;
}

/// Manages the state of a task.
struct TypedTaskInner<T: Send + Sync> {
    /// The function to invoke.
    func: TakeOwnCell<Box<dyn FnOnce() -> T + Send>>,
    /// The thread pool on which this work is spawned.
    pool: Weak<ThreadPoolState>,
    /// The result of the task, if any.
    result: spin::Once<T>,
}

impl<T: Send + Sync> TaskInner for TypedTaskInner<T> {
    #[inline(always)]
    fn run(&self) -> bool {
        if let Some(f) = self.func.take() {
            self.result.call_once(f);
            true
        } else {
            false
        }
    }
}
