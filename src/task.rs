use std::sync::Arc;

use crate::ThreadPoolState;
use crate::join_point::JoinPoint;

/// A handle to a queued group of work units, which output a single result.
pub struct Task<T: 'static>(Arc<dyn TypedTaskInner<T>>);

impl<T: 'static> Task<T> {
    /// Spawns a new task on the given pool.
    pub(crate) fn spawn(pool: &'static ThreadPoolState, f: impl 'static + FnOnce() -> T + Send) -> Self {
        let inner = Arc::new(TaskInnerHolder {
            func: spin::Mutex::new(Some(f)),
            pool,
            state: spin::RwLock::new(TaskState::NotStarted),
            result: spin::Mutex::new(None)
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

    /// Attempts to get the result of this task if it has been completed.
    /// Otherwise, returns the original task.
    pub fn result(self) -> Result<T, Self> {
        if self.complete() {
            Ok(self.join())
        } else {
            Err(self)
        }
    }

    /// Joins the current thread with this task, completing all remaining work.
    /// After all work is complete, yields the result.
    pub fn join(self) -> T {
        JoinPoint::join_task(&*self.0, false);
        self.0.take_result()
    }
    
    /// Joins with the remaining work on this task.
    /// Returns immediately if there are outstanding units in progress on other threads.
    pub fn join_work(&self) {
        JoinPoint::join_task(&*self.0, true);
    }
}

/// Allows for thread pools to manipulate the inner task state.
pub(crate) trait TaskInner {
    /// The thread pool on which this work is spawned.
    fn pool(&self) -> &'static ThreadPoolState;

    /// Gets a reference to the state tracker for the task.
    fn state(&self) -> &spin::RwLock<TaskState>;

    /// Executes the inner task. This should be called once by a single thread
    /// in the correct pool context.
    fn run(&self);
}

trait TypedTaskInner<T>: TaskInner {
    /// Whether the task has finished execution.
    fn complete(&self) -> bool;

    /// Gets the result of the task. Panics if the task has not yet finished,
    /// or if this was called multiple times.
    fn take_result(&self) -> T;
}

struct TaskInnerHolder<T, F: FnOnce() -> T + Send> {
    /// The function to invoke.
    func: spin::Mutex<Option<F>>,
    /// The thread pool on which this work is spawned.
    pool: &'static ThreadPoolState,
    /// The result of the task, if any.
    result: spin::Mutex<Option<T>>,
    /// The state tracker for the task.
    state: spin::RwLock<TaskState>
}

impl<T, F: FnOnce() -> T + Send> TaskInner for TaskInnerHolder<T, F> {
    fn pool(&self) -> &'static ThreadPoolState {
        &self.pool
    }

    fn state(&self) -> &spin::RwLock<TaskState> {
        &self.state
    }

    fn run(&self) {
        let result = self.func.lock().take().expect("TaskInner::run called multiple times")();
        *self.result.lock() = Some(result);
    }
}

impl<T, F: FnOnce() -> T + Send> TypedTaskInner<T> for TaskInnerHolder<T, F> {
    fn complete(&self) -> bool {
        self.result.lock().is_some()
    }

    fn take_result(&self) -> T {
        self.result.lock().take().expect("Failed to get result of task")
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
    Complete
}