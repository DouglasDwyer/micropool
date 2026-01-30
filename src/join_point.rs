use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering, fence};

use smallvec::SmallVec;

use crate::thread_pool::ThreadPoolState;
use crate::util::*;
use crate::{TaskInner, TaskState};

thread_local! {
    /// The last join point entered by this thread, if any.
    static CURRENT_JOIN_POINT: Cell<Option<JoinPoint>> = const { Cell::new(None) };
}

/// References a point in the call stack where control flow was split across
/// multiple threads. Allows for waiting for the call to complete.
///
/// **Note:** control flow will not leave a join point until all [`JoinPoint`]
/// references are dropped!
#[derive(Clone)]
pub struct JoinPoint(ScopedRef<JoinPointInner>);

impl JoinPoint {
    /// The amount of space to reserve for child pointers on the stack.
    const CHILD_BUFFER_CAPACITY: usize = 32;

    /// Calls `f` with values `0..total_invocations` in parallel.
    ///
    /// # Safety
    ///
    /// The function `f` must be safe to call from multiple threads in parallel.
    /// That is, it should be [`Sync`], but the bound is elided here for
    /// convenience.
    pub unsafe fn invoke(pool: &'static ThreadPoolState, f: impl Fn(usize), times: usize) {
        unsafe {
            match times {
                0 => {}
                1 => f(0),
                _ => {
                    let maybe_on_change = Event::new();
                    let parent = Self::current();

                    if parent
                        .as_ref()
                        .is_some_and(|x| !std::ptr::eq(x.0.pool, pool))
                    {
                        panic!(
                            "Attempted to parallelize work with two different thread pools simultaneously"
                        );
                    }

                    let f = std::mem::transmute::<
                        *const (dyn Fn(usize) + '_),
                        *const (dyn Fn(usize) + 'static),
                    >(&f as *const dyn Fn(usize));

                    ScopedRef::of(
                        JoinPointInner {
                            children: spin::RwLock::new(SmallVec::new()),
                            completed_invocations: AtomicU64::new(0),
                            func: f,
                            on_change: parent
                                .as_ref()
                                .map(|x| x.0.on_change)
                                .unwrap_or(&maybe_on_change),
                            parent,
                            pool,
                            started_invocations: AtomicU64::new(1),
                            total_invocations: times as u64,
                        },
                        |inner| {
                            let join_point = JoinPoint(inner);

                            if let Some(parent) = &join_point.0.parent {
                                parent.0.children.write().push(join_point.clone());
                                (*join_point.0.on_change).notify();
                            } else {
                                join_point.0.pool.roots.write().push(join_point.clone());
                            }

                            join_point.0.pool.on_change.notify();

                            Self::set_current(Some(join_point.clone()));

                            join_point.invoke_work_unit(0);
                            join_point.join();

                            Self::set_current(join_point.0.parent.clone());
                        },
                    );
                }
            }
        }
    }

    /// Begins running `task`. If the task was already started on another
    /// thread, then joins with that task.
    ///
    /// If `help_only` is true, then this function may return early
    /// (before the task is complete) while other threads finish their work.
    pub fn join_task(task: &dyn TaskInner, help_only: bool) {
        unsafe {
            let mut state_lock = task.state().write();

            match state_lock.clone() {
                TaskState::NotStarted => {
                    let previous = Self::current();
                    let on_change = Event::new();
                    let f = |_| task.run();
                    let f = std::mem::transmute::<
                        *const (dyn Fn(usize) + '_),
                        *const (dyn Fn(usize) + 'static),
                    >(&f as *const _);

                    ScopedRef::of(
                        JoinPointInner {
                            children: spin::RwLock::new(SmallVec::new()),
                            completed_invocations: AtomicU64::new(0),
                            func: f,
                            on_change: &on_change,
                            parent: None,
                            pool: task.pool(),
                            started_invocations: AtomicU64::new(1),
                            total_invocations: 1,
                        },
                        move |inner| {
                            let join_point = JoinPoint(inner);

                            *state_lock = TaskState::Running(join_point.clone());
                            drop(state_lock);

                            join_point.0.pool.roots.write().push(join_point.clone());

                            Self::set_current(Some(join_point.clone()));

                            join_point.invoke_work_unit(0);

                            *task.state().write() = TaskState::Complete;
                            Self::set_current(previous);
                        },
                    );
                }
                TaskState::Running(join_point) => {
                    drop(state_lock);
                    if help_only {
                        join_point.help();
                    } else {
                        join_point.join();
                    }
                }
                TaskState::Complete => {}
            }
        }
    }

    /// Gets the join point associated with the current context, if any.
    pub fn current() -> Option<JoinPoint> {
        let result = CURRENT_JOIN_POINT.take();
        CURRENT_JOIN_POINT.set(result.clone());
        result
    }

    /// Sets the join point associated with the current context, if any.
    pub fn set_current(point: Option<JoinPoint>) {
        CURRENT_JOIN_POINT.set(point);
    }

    /// Processes work unit `i`, and wakes up any waiting threads if work is
    /// complete.
    ///
    /// # Safety
    ///
    /// This function may be called exactly once for each `i` on the range
    /// `0..self.0.total_invocations`. Any other calls are undefined
    /// behavior.
    pub unsafe fn invoke_work_unit(&self, i: u64) {
        abort_on_panic(|| unsafe {
            debug_assert!(
                i < self.0.total_invocations,
                "Invoked out-of-bounds work unit"
            );

            (*self.0.func)(i as usize);

            // Use release ordering to ensure that the operation result is visible
            let now_finished = self.0.completed_invocations.fetch_add(1, Ordering::Release) + 1;

            if now_finished == self.0.total_invocations {
                if let Some(parent) = &self.0.parent {
                    let mut siblings = parent.0.children.write();
                    let index = index_of(self, &siblings).unwrap_unchecked();
                    siblings.swap_remove(index);
                } else {
                    let mut roots = self.0.pool.roots.write();
                    let index = index_of(self, &roots).unwrap_unchecked();
                    roots.swap_remove(index);
                }

                (*self.0.on_change).notify();
            }
        })
    }

    /// Exhausts all work for this join point (but not any of its children).
    /// Finishes once all available work has been **started**, but not
    /// necessarily finished. Returns `true` if at least one available work
    /// unit was run to completion.
    fn invoke_immediate_work(&self) -> bool {
        let mut ran_item = false;
        loop {
            let next_index = self.0.started_invocations.fetch_add(1, Ordering::Relaxed);
            if next_index < self.0.total_invocations {
                unsafe {
                    self.invoke_work_unit(next_index);
                }
                ran_item = true;
            } else {
                return ran_item;
            }
        }
    }

    /// Attempts to steal work units from children.
    /// Finishes once all available work has been **started**, but not
    /// necessarily finished. Returns `true` if at least one available work
    /// unit was run to completion.
    fn invoke_child_work(&self) -> bool {
        if let Some((child, i)) = self.select_child_work_unit() {
            unsafe {
                Self::invoke_work_unit(&child, i);
            }
            Self::help(&child);
            true
        } else {
            false
        }
    }

    /// Steals a work unit from a child. The unit is marked as taken, so it is
    /// the caller's responsibility to invoke [`Self::invoke_work_unit`] on
    /// the child.
    ///
    /// Returns a reference to the child and the index of the work unit.
    fn select_child_work_unit(&self) -> Option<(JoinPoint, u64)> {
        Self::select_work_unit_from(&self.0.children.read())
    }

    /// Attempts to steal work units from this join point.
    /// Finishes once all available work has been **started**, but not
    /// necessarily finished. Returns `true` if at least one available work
    /// unit was run to completion.
    pub fn help(&self) -> bool {
        let invoked_immediate = self.invoke_immediate_work();
        let invoked_children = self.invoke_child_work();
        invoked_immediate || invoked_children
    }

    /// Fetches work from this join point and its children.
    /// Returns only when all work is complete for the join point.
    pub fn join(&self) {
        self.invoke_immediate_work();

        let mut spin_before_sleep = true;
        loop {
            let listener = unsafe { (*self.0.on_change).listen() };
            if self.0.completed_invocations.load(Ordering::Relaxed) < self.0.total_invocations {
                if self.invoke_child_work() {
                    spin_before_sleep = true;
                } else {
                    // Only spin if something was found to do since the last sleep
                    let spin_cycles = if spin_before_sleep {
                        self.0.pool.idle_spin_cycles
                    } else {
                        0
                    };
                    spin_before_sleep = !listener.spin_wait(spin_cycles);
                }
            } else {
                // Ensure that results from all work items are visible to the calling thread.
                fence(Ordering::Acquire);
                break;
            }
        }
    }

    /// Steals a work unit from one of the join points in the list.
    /// The unit is marked as taken, so it is the caller's
    /// responsibility to invoke [`Self::invoke_work_unit`] on the point.
    ///
    /// Returns a reference to the point and the index of the work unit.
    pub fn select_work_unit_from(items: &[JoinPoint]) -> Option<(JoinPoint, u64)> {
        for child in items {
            let next_index = child.0.started_invocations.fetch_add(1, Ordering::Relaxed);
            if next_index < child.0.total_invocations {
                return Some((child.clone(), next_index));
            } else if let Some(result) = Self::select_child_work_unit(child) {
                return Some(result);
            }
        }
        None
    }
}

impl PartialEq for JoinPoint {
    fn eq(&self, other: &Self) -> bool {
        ScopedRef::ptr_eq(&self.0, &other.0)
    }
}

unsafe impl Send for JoinPoint {}
unsafe impl Sync for JoinPoint {}

/// Holds the inner state for a [`JoinPoint`].
struct JoinPointInner {
    /// Places where control has split during parallel execution of this
    /// [`JoinPoint`].
    children: spin::RwLock<SmallVec<[JoinPoint; JoinPoint::CHILD_BUFFER_CAPACITY]>>,
    /// The number of invocations that have finished.
    completed_invocations: AtomicU64,
    /// The function to invoke.
    func: *const (dyn 'static + Fn(usize)),
    /// An event that is signaled when work is available or completed.
    on_change: *const Event,
    /// The point that spawned this one.
    parent: Option<JoinPoint>,
    /// The pool that owns the join point tree.
    pool: &'static ThreadPoolState,
    /// The number of invocations that have started.
    started_invocations: AtomicU64,
    /// The total number of invocations to perform.
    total_invocations: u64,
}
