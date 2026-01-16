use std::sync::Arc;

use takecell::TakeOwnCell;

use crate::ThreadPoolState;
use crate::join_point::JoinPoint;

/// A handle to some background work.
///
/// Unlike [`SharedTask`], this type cannot be cloned. However,
/// it returns a `T` rather than `&T` as its result.
pub struct Task<T: 'static + Send>(Arc<dyn TypedTaskInner<TakeOwnCell<T>>>);

impl<T: 'static + Send> Task<T> {
    /// Spawns a new task on the given pool.
    pub(crate) fn spawn(
        pool: &'static ThreadPoolState,
        f: impl 'static + FnOnce() -> T + Send,
    ) -> Self {
        let inner = Arc::new(TaskInnerHolder {
            func: TakeOwnCell::new(|| TakeOwnCell::new(f())),
            pool,
            state: spin::RwLock::new(TaskState::NotStarted),
            result: spin::Once::new(),
        });

        pool.push_task(inner.clone());

        Self(inner)
    }

    /// Cancels this task, preventing it from running if it was not yet started.
    pub fn cancel(self) {
        self.0.pool().cancel_task(&self.0);
    }

    /// Whether the task has been completed yet.
    pub fn complete(&self) -> bool {
        self.0.complete()
    }

    /// Steals any outstanding work on this task.
    /// Returns immediately if there are remaining units in progress on other
    /// threads.
    pub fn help(&self) {
        JoinPoint::join_task(&*self.0, true);
    }

    /// Joins the current thread with this task, completing all remaining work.
    /// After all work is complete, yields the result.
    pub fn join(self) -> T {
        JoinPoint::join_task(&*self.0, false);
        self.0
            .result()
            .expect("Failed to get result of task")
            .take()
            .expect("Failed to get result of task")
    }

    /// Attempts to get the result of this task if it has been completed.
    /// Otherwise, returns the original task.
    pub fn try_join(self) -> Result<T, Self> {
        if self.complete() {
            Ok(self.join())
        } else {
            Err(self)
        }
    }
}

/// A handle to some background work.
///
/// Unlike [`Task`], this type can be cloned. It returns a
/// `&T` rather than a `T` as its result.
pub struct SharedTask<T: 'static + Send + Sync>(Arc<dyn TypedTaskInner<T>>);

impl<T: 'static + Send + Sync> SharedTask<T> {
    /// Spawns a new task on the given pool.
    pub(crate) fn spawn(
        pool: &'static ThreadPoolState,
        f: impl 'static + FnOnce() -> T + Send,
    ) -> Self {
        let inner = Arc::new(TaskInnerHolder {
            func: TakeOwnCell::new(f),
            pool,
            state: spin::RwLock::new(TaskState::NotStarted),
            result: spin::Once::new(),
        });

        pool.push_task(inner.clone());

        Self(inner)
    }

    /// Cancels this task, preventing it from running if it was not yet started.
    ///
    /// If this task was cloned, then [`Self::cancel`] must be called on all
    /// clones to have an effect.
    pub fn cancel(self) {
        // A shared task may only be cancelled if `self` is the only reference.
        // If there are multiple references **and** the task is still enqueued,
        // then the `strong_count` will be at least three (one for `self`, one for
        // the other reference, and one for the reference in the queue).
        // If the task has been removed from the queue, then the `strong_count` may be
        // less than three, but in that case `cancel_task` is a no-op. So, this
        // is correct.
        if Arc::strong_count(&self.0) < 3 {
            self.0.pool().cancel_task(&self.0);
        }
    }

    /// Whether the task has been completed yet.
    pub fn complete(&self) -> bool {
        self.0.complete()
    }

    /// Steals any outstanding work on this task.
    /// Returns immediately if there are remaining units in progress on other
    /// threads.
    pub fn help(&self) {
        JoinPoint::join_task(&*self.0, true);
    }

    /// Joins the current thread with this task, completing all remaining work.
    /// After all work is complete, yields the result.
    pub fn join(&self) -> &T {
        if self.0.result().is_none() {
            JoinPoint::join_task(&*self.0, false);
        }

        self.0.result().expect("Failed to get result of task")
    }

    /// Attempts to get the result of this task if it has been completed.
    /// Otherwise, returns the original task.
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

/// Allows for thread pools to manipulate the inner task state.
pub(crate) trait TaskInner: Send {
    /// Whether the task has finished execution.
    fn complete(&self) -> bool;

    /// The thread pool on which this work is spawned.
    fn pool(&self) -> &'static ThreadPoolState;

    /// Gets a reference to the state tracker for the task.
    fn state(&self) -> &spin::RwLock<TaskState>;

    /// Executes the inner task. This should be called once by a single thread
    /// in the correct pool context.
    fn run(&self);
}

/// Abstracts over [`TaskInnerHolder`]s with different function types.
trait TypedTaskInner<T: Send>: TaskInner + Send + Sync {
    /// Gets the result of the task, or returns [`None`] if the task has not
    /// finished.
    fn result(&self) -> Option<&T>;
}

/// Manages the state of a task.
struct TaskInnerHolder<T: Send + Sync, F: FnOnce() -> T + Send> {
    /// The function to invoke.
    func: TakeOwnCell<F>,
    /// The thread pool on which this work is spawned.
    pool: &'static ThreadPoolState,
    /// The result of the task, if any.
    result: spin::Once<T>,
    /// The state tracker for the task.
    state: spin::RwLock<TaskState>,
}

impl<T: Send + Sync, F: FnOnce() -> T + Send> TaskInner for TaskInnerHolder<T, F> {
    fn complete(&self) -> bool {
        self.result.is_completed()
    }

    fn pool(&self) -> &'static ThreadPoolState {
        self.pool
    }

    fn state(&self) -> &spin::RwLock<TaskState> {
        &self.state
    }

    fn run(&self) {
        let result = self
            .func
            .take()
            .expect("TaskInner::run called multiple times")();
        self.result.call_once(|| result);
    }
}

impl<T: Send + Sync, F: FnOnce() -> T + Send> TypedTaskInner<T> for TaskInnerHolder<T, F> {
    fn result(&self) -> Option<&T> {
        self.result.get()
    }
}

/// Tracks the state of a running task.
#[derive(Clone)]
pub(crate) enum TaskState {
    /// The task has not yet been started.
    NotStarted,
    /// The task is running with the given root join point.
    Running(JoinPoint),
    /// The task has finished execution.
    Complete,
}
