#![allow(warnings)]
#![warn(missing_docs)]

use std::{cell::{Cell, UnsafeCell}, hint::{black_box, spin_loop, unreachable_unchecked}, marker::PhantomData, mem::MaybeUninit, ops::{Deref, Index}, ptr::NonNull, sync::{atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering}, Arc}, thread::{self, available_parallelism}};

use paralight::iter::{BaseParallelIterator, GenericThreadPool, ParallelIterator, ParallelSource, ParallelSourceExt};
use smallvec::{smallvec, SmallVec};

/// A handle to the global threadpool. This can be passed to [`paralight::iter::ParallelSourceExt::with_thread_pool`]
/// to create parallel iterators.
pub const POOL: TodoThreadPool = TodoThreadPool;

/// Determines how a thread pool will behave.
pub struct ThreadPoolBuilder {
    /// Threads waiting for work will spin for at least this many cycles before sleeping.
    idle_spin_cycles: usize,
    /// The number of threads to spawn.
    num_threads: usize,
    /// The function to use when spawning new threads.
    spawn_handler: Box<dyn FnMut(usize, Box<dyn FnOnce() + Send>)>,
}

impl ThreadPoolBuilder {
    /// Threads waiting for work will spin for at least this many cycles before sleeping.
    pub fn idle_spin_cycles(self, idle_spin_cycles: usize) -> Self {
        Self {
            idle_spin_cycles,
            ..self
        }
    }

    /// Sets the number of threads to be used in the thread pool.
    pub fn num_threads(self, num_threads: usize) -> Self {
        Self {
            num_threads,
            ..self
        }
    }

    /// Sets a custom function for spawning threads.
    pub fn spawn_handler<F>(self, spawn: F) -> Self
    where
        F: FnMut(usize, Box<dyn FnOnce() + Send>) + 'static
    {
        Self {
            spawn_handler: Box::new(spawn),
            ..Default::default()
        }
    }

    /// Begins the thread pool by starting all background tasks.
    fn start(mut self) {
        for i in 0..self.num_threads {
            (self.spawn_handler)(i, Box::new(ThreadPoolState::worker_entrypoint));
        }
    }
}

impl Default for ThreadPoolBuilder {
    fn default() -> Self {
        Self {
            idle_spin_cycles: 150000,
            num_threads: available_parallelism().map(usize::from).unwrap_or(1),
            spawn_handler: Box::new(|_, x| { thread::spawn(x); })
        }
    }
}

struct TodoThreadPool;

impl GenericThreadPool for TodoThreadPool {
    fn upper_bounded_pipeline<Output: Send, Accum>(
        self,
        input_len: usize,
        init: impl Fn() -> Accum + Sync,
        process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
        finalize: impl Fn(Accum) -> Output + Sync,
        reduce: impl Fn(Output, Output) -> Output,
        cleanup: &(impl paralight::iter::SourceCleanup + Sync),
    ) -> Output {
        todo!()
    }

    fn iter_pipeline<Output: Send>(
        self,
        input_len: usize,
        accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
        reduce: impl paralight::iter::Accumulator<Output, Output>,
        cleanup: &(impl paralight::iter::SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let mut output = SmallVec::<[Output; 16]>::new();  //todo: dynamic size to keep stack const
            output.reserve_exact(input_len);
            let output_buffer = output.as_mut_ptr();

            JoinPoint::invoke(|i| {
                output_buffer.add(i).write(accum.accumulate(i..i + 1));
            }, input_len);
            
            output.set_len(input_len);

            reduce.accumulate(output.into_iter())
        }
    }
}

/// Tracks whether the global thread pool has been started.
static THREAD_POOL_INITIALIZED: spin::Once = spin::Once::new();

/// Explicitly initializes the global thread pool.
/// This function will panic if the global pool was already created.
/// 
/// Note that, if this function is _not_ called, the global pool will
/// be implicitly created with default settings when it is first used.
pub fn init(builder: ThreadPoolBuilder) {
    let mut run = false;

    THREAD_POOL_INITIALIZED.call_once(|| {
        builder.start();
        run = true;
    });

    if !run {
        panic!("TODO::init called after thread pool was active");
    }
}

#[derive(Default)]
struct ThreadPoolState {
    /// An event that is invoked whenever new work is available.
    on_change: Event,
    /// Join points where pool threads should look for work.
    roots: spin::RwLock<Vec<JoinPoint>>
}

impl ThreadPoolState {
    /// Gets a reference to the global pool state. Ensures that the global thread pool
    /// has been initialized.
    pub fn global() -> &'static Self {
        /// The inner state object.
        static STATE: ThreadPoolState = ThreadPoolState::new();
        THREAD_POOL_INITIALIZED.call_once(|| ThreadPoolBuilder::default().start());
        &STATE
    }

    /// Creates a new state object.
    pub const fn new() -> Self {
        Self {
            on_change: Event::new(),
            roots: spin::RwLock::new(Vec::new())
        }
    }

    /// Enters this pool as a worker thread.
    pub fn worker_entrypoint() {
        Self::global().join();
    }

    /// Polls for available work on the thread pool, and goes to sleep if none is available.
    fn join(&self) {
        assert!(JoinPoint::current().is_none(), "Attempted to enter pool from within another context");
        
        let mut spin_before_sleep = false;

        loop {
            let listener = self.on_change.listen();
            let item = JoinPoint::select_work_unit_from(&self.roots.read());

            if let Some((point, i)) = item {
                spin_before_sleep = true;
                JoinPoint::set_current(Some(point.clone()));
                unsafe { point.invoke_work_unit(i); }
                point.join_work();
                JoinPoint::set_current(None);
            }
            else {
                // Only spin if something was found to do since the last sleep
                let spin_cycles = if spin_before_sleep { todo!() } else { 0 };
                spin_before_sleep = !listener.spin_wait(spin_cycles);
            }
        }
    }
}

unsafe impl Send for ThreadPoolState {}
unsafe impl Sync for ThreadPoolState {}

/// Takes two closures and *potentially* runs them in parallel. It
/// returns a pair of the results from those closures.
pub fn fork<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send {
    unsafe {
        let oper_a_holder = MaybeUninit::new(oper_a);
        let oper_b_holder = MaybeUninit::new(oper_b);

        let result_a = UnsafeCell::new(MaybeUninit::uninit());
        let result_b = UnsafeCell::new(MaybeUninit::uninit());

        JoinPoint::invoke(|i| match i {
            0 => { (*result_a.get()).write(oper_a_holder.assume_init_read()()); },
            1 => { (*result_b.get()).write(oper_b_holder.assume_init_read()()); },
            _ => unreachable_unchecked()
        }, 2);

        (result_a.into_inner().assume_init(), result_b.into_inner().assume_init())
    }
}

thread_local! {
    /// The last join point entered by this thread, if any.
    static CURRENT_JOIN_POINT: UnsafeCell<Option<JoinPoint>> = UnsafeCell::new(None);
}

/// Tracks a point in the call stack where control flow was split across multiple threads.
#[derive(Clone)]
struct JoinPoint(ScopedRef<JoinPointInner>);

impl JoinPoint {
    /// The amount of space to reserve for child pointers.
    const CHILD_LIST_CAPACITY: usize = 32;

    /// Calls `f` with values `0..total_invocations` in parallel.
    /// 
    /// # Safety
    /// 
    /// The function `f` must be safe to call from multiple threads in parallel.
    /// That is, it should be [`Sync`], but the bound is elided here for convenience.
    pub unsafe fn invoke(f: impl Fn(usize), times: usize) {
        unsafe {
            match times {
                0 => {},
                1 => f(0),
                _ => {
                    let maybe_on_change = Event::new();
                    let parent = Self::current();

                    ScopedRef::of(JoinPointInner {
                        children: spin::RwLock::new(SmallVec::new()),
                        completed_invocations: AtomicU64::new(0),
                        func: &f as *const _ as *const _,
                        on_change: parent.as_ref().map(|x| x.0.on_change).unwrap_or(&maybe_on_change),
                        parent,
                        started_invocations: AtomicU64::new(1),
                        total_invocations: times as u64
                    }, |inner| {
                        let join_point = JoinPoint(inner);

                        let pool_state = ThreadPoolState::global();

                        if let Some(parent) = &join_point.0.parent {
                            parent.0.children.write().push(join_point.clone());
                            (*join_point.0.on_change).notify();
                        }
                        else {
                            pool_state.roots.write().push(join_point.clone());
                        }

                        pool_state.on_change.notify();
                        
                        Self::set_current(Some(join_point.clone()));

                        join_point.invoke_work_unit(0);
                        join_point.join();

                        Self::set_current(join_point.0.parent.clone());
                    });                    
                }
            }
        }
    }

    /// Gets the join point associated with the current context, if any.
    pub fn current() -> Option<JoinPoint> {
        unsafe { CURRENT_JOIN_POINT.with(|x| (*x.get()).clone()) }
    }

    /// Sets the join point associated with the current context, if any.
    pub fn set_current(point: Option<JoinPoint>) {
        unsafe { CURRENT_JOIN_POINT.with(|x| *x.get() = point) }
    }

    /// Processes work unit `i`, and wakes up any waiting threads if work is complete.
    /// 
    /// # Safety
    /// 
    /// todo
    unsafe fn invoke_work_unit(&self, i: u64) {
        let _guard = PanicGuard;
        
        debug_assert!(i < self.0.total_invocations, "Invoked out-of-bounds work unit");
        
        (*self.0.func)(i as usize);
        let now_finished = self.0.completed_invocations.fetch_add(1, Ordering::Release) + 1;

        if now_finished == self.0.total_invocations {
            if let Some(parent) = &self.0.parent {
                let mut siblings = parent.0.children.write();
                let index = index_of(self, &siblings).unwrap_unchecked();
                siblings.swap_remove(index);
            }
            else {
                let mut roots = ThreadPoolState::global().roots.write();
                let index = index_of(self, &roots).unwrap_unchecked();
                roots.swap_remove(index);
            }

            (*self.0.on_change).notify();
        }
    }

    /// Exhausts all work for this join point (but not any of its children).
    /// Finishes once all available work has been **started**, but not necessarily finished.
    /// Returns `true` if at least one available work unit was run to completion.
    fn invoke_immediate_work(&self) -> bool {
        let mut ran_item = false;
        loop {
            let next_index = self.0.started_invocations.fetch_add(1, Ordering::Relaxed);
            if next_index < self.0.total_invocations {
                unsafe { self.invoke_work_unit(next_index); }
                ran_item = true;
            }
            else {
                return ran_item;
            }
        }
    }

    /// Attempts to steal work units from children.
    /// Finishes once all available work has been **started**, but not necessarily finished.
    /// Returns `true` if at least one available work unit was run to completion.
    fn invoke_child_work(&self) -> bool {
        if let Some((child, i)) = self.select_child_work_unit() {
            unsafe { Self::invoke_work_unit(&child, i); }
            Self::join_work(&child);
            true
        }
        else {
            false
        }
    }

    /// Steals a work unit from a child. The unit is marked as taken, so it is the caller's
    /// responsibility to invoke [`Self::invoke_work_unit`] on the child.
    /// 
    /// Returns a reference to the child and the index of the work unit.
    fn select_child_work_unit(&self) -> Option<(JoinPoint, u64)> {
        Self::select_work_unit_from(&self.0.children.read())
    }

    /// Attempts to steal work units from this join point.
    /// Finishes once all available work has been **started**, but not necessarily finished.
    /// Returns `true` if at least one available work unit was run to completion.
    fn join_work(&self) -> bool {
        let invoked_immediate = self.invoke_immediate_work();
        let invoked_children = self.invoke_child_work();
        invoked_immediate || invoked_children
    }

    /// Fetches work from this join point and its children.
    /// Returns only when all work is complete for the join point.
    fn join(&self) {
        self.invoke_immediate_work();

        let mut spin_before_sleep = false;

        while self.0.completed_invocations.load(Ordering::Acquire) < self.0.total_invocations {
            let listener = unsafe { (*self.0.on_change).listen() }; // todo: no more raw ptr
            if self.invoke_child_work() {
                spin_before_sleep = true;
            }
            else {
                // Only spin if something was found to do since the last sleep
                let spin_cycles = if spin_before_sleep { todo!() } else { 0 };
                spin_before_sleep = !listener.spin_wait(spin_cycles);
            }
        }
    }

    /// Steals a work unit from one of the join points in the list.
    /// The unit is marked as taken, so it is the caller's
    /// responsibility to invoke [`Self::invoke_work_unit`] on the point.
    /// 
    /// Returns a reference to the point and the index of the work unit.
    fn select_work_unit_from(items: &[JoinPoint]) -> Option<(JoinPoint, u64)> {
        for child in items {
            let next_index = child.0.started_invocations.fetch_add(1, Ordering::Relaxed);
            if next_index < child.0.total_invocations {
                return Some((child.clone(), next_index));
            }
            else if let Some(result) = Self::select_child_work_unit(child) {
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

/// Holds the inner state for a [`JoinPoint`].
struct JoinPointInner {
    /// Places where control has split during parallel execution of this [`JoinPoint`].
    children: spin::RwLock<SmallVec<[JoinPoint; JoinPoint::CHILD_LIST_CAPACITY]>>,
    /// The number of invocations that have finished.
    completed_invocations: AtomicU64,
    /// The function to invoke.
    func: *const (dyn 'static + Fn(usize)),
    /// An event that is signaled when work is available or completed.
    on_change: *const Event,
    /// The point that spawned this one.
    parent: Option<JoinPoint>,
    /// The number of invocations that have started.
    started_invocations: AtomicU64,
    /// The total number of invocations to perform.
    total_invocations: u64
}

/// A synchronization primitive for blocking threads until an event occurs.
/// The primitive is reusable and will wake up all pending listeners each time
/// it is signaled.
#[derive(Debug, Default)]
pub struct Event {
    /// The version number - incremented each time the event changes.
    version: AtomicU64
}

impl Event {
    /// Initializes a new event.
    pub const fn new() -> Self {
        Self { version: AtomicU64::new(0) }
    }

    /// Notifies all listeners that this event has changed.
    pub fn notify(&self) {
        self.version.fetch_add(1, Ordering::Release);
        atomic_wait::wake_all(self.atomic_address());
    }

    /// Begins listening for changes to `self`. The event listener
    /// will block until [`Self::notify`] is called by another thread.
    pub fn listen(&self) -> EventListener<'_> {
        EventListener { event: self, version: self.version.load(Ordering::Acquire) }
    }

    /// Gets the atomic address on which to wait for events.
    fn atomic_address(&self) -> &AtomicU32 {
        unsafe { &*(&self.version as *const _ as *const _) }
    }
}

/// Allows for waiting for an [`Event`] to change.
#[derive(Copy, Clone, Debug)]
pub struct EventListener<'a> {
    /// The event in question.
    event: &'a Event,
    /// The version number of the event when this listener was created.
    version: u64
}

impl<'a> EventListener<'a> {
    /// Whether [`Event::notify`] has been called at least once after this object's creation.
    pub fn signaled(&self) -> bool {
        self.event.version.load(Ordering::Acquire) != self.version
    }

    /// Blocks the current thread, spinning in a loop for `cycles` before falling back
    /// to blocking with the operating system scheduler. Returns `true` only if the
    /// thread was put to sleep by the operating system.
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

    /// Blocks the current thread. Returns when [`Event::notify`] has been called at least once
    /// since this object's creation.
    pub fn wait(&self) {
        unsafe {
            while !self.signaled() {
                atomic_wait::wait(self.event.atomic_address(), self.version as u32);
            }
        }
    }
}

/// Allows for erasing lifetimes and sharing references on the stack.
struct ScopedRef<T>(NonNull<ScopedHolder<T>>);

impl<T> ScopedRef<T> {
    /// Wraps `value` in a [`ScopedRef`], which is provided to `scope`.
    /// Once `scope` completes, this function will block until all
    /// clones of the reference are dropped.
    pub fn of<R>(value: T, scope: impl FnOnce(Self) -> R) -> R {
        let holder = ScopedHolder { ref_count: AtomicUsize::new(1), value };
        scope(ScopedRef(NonNull::from_ref(&holder)))
    }

    /// Determines whether two scoped references refer to the same underlying object.
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
        unsafe { self.0.as_ref().ref_count.fetch_sub(1, Ordering::Release); }
    }
}

/// Stores the inner state for a [`ScopedRef`].
struct ScopedHolder<T> {
    /// The number of active references to this holder.
    ref_count: AtomicUsize,
    /// The underlying value.
    value: T
}

impl<T> Drop for ScopedHolder<T> {
    fn drop(&mut self) {
        while 0 < self.ref_count.load(Ordering::Acquire) {
            spin_loop();
        }
    }
}

/// Ensures that the program aborts on unhandled panics across [`JoinPoint`]s.
struct PanicGuard;

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if thread::panicking() {
            panic!("Panic was not caught at join point boundary; aborting.");
        }
    }
}

/// Gets the first index of `value` within `slice`, or returns [`None`] if
/// it was not found.
fn index_of<T: PartialEq>(value: &T, slice: &[T]) -> Option<usize> {
    slice.iter().position(|x| x == value)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_it() {
        init(ThreadPoolBuilder::default().num_threads(1));

        use paralight::iter::*;
        /*fork(|| {
            std::thread::sleep_ms(10);
            println!("A on {:?}", std::thread::current().id());
        }, || {
            println!("B on {:?}", std::thread::current().id());
            std::thread::sleep_ms(120);
            fork(|| {
                std::thread::sleep_ms(250);
                println!("C on {:?}", std::thread::current().id());
            }, || {
                println!("D on {:?}", std::thread::current().id());
            });
        });*///
        let mut output = 0..1000;
        let mut indx = [None; 1000];

        ((0..1000).into_par_iter(), indx.par_iter_mut())
            .zip_eq()
            .with_thread_pool(POOL)
            .for_each(|(i, out)| *out = Some(std::thread::current().id()));
        println!("GO {indx:?}");
    }

    /*use futures_executor::*;

    async fn execute_background() {
        let queue = TaskQueue::<Fifo>::default();
        TaskPool::new(queue.clone(), 4).forget();

        assert_eq!(
            queue
                .spawn(once(|| {
                    println!("This will execute on background thread.");
                    2
                }))
                .await,
            2
        );
    }

    #[test]
    fn execute_many() {
        let queue_a = TaskQueue::<Fifo>::default();
        assert_eq!(
            &[0, 3, 8, 15, 24][..],
            &queue_a.join(many((1..=5).map(|x| move || x * x - 1)))
        );
    }

    #[test]
    fn execute_double() {
        let queue_a = TaskQueue::<Fifo>::default();
        let queue_b = TaskQueue::<Lifo>::default();

        let first_task = queue_a.spawn(once(|| 2));
        let second_task = queue_b.spawn(once(|| 2));

        TaskPool::new(
            ChainedWorkProvider::default()
                .with(queue_a.clone())
                .with(queue_b.clone()),
            4,
        )
        .forget();

        assert_eq!(first_task.join(), second_task.join());
    }

    #[test]
    fn execute_double_twice() {
        let queue_a = TaskQueue::<Fifo>::default();
        let queue_b = TaskQueue::<Lifo>::default();

        let first_task = queue_a.spawn(once(|| 2));
        let second_task = queue_b.spawn(once(|| 2));

        TaskPool::new(
            ChainedWorkProvider::default()
                .with(queue_a.clone())
                .with(queue_b.clone()),
            1,
        )
        .forget();

        assert_eq!(first_task.join(), second_task.join());

        for _i in 0..1000 {
            let third_task =
                queue_a.spawn(once(|| std::thread::sleep(std::time::Duration::new(0, 10))));
            let fourth_task = queue_b.spawn(once(|| {
                std::thread::sleep(std::time::Duration::new(0, 200))
            }));
            assert_eq!(third_task.join(), fourth_task.join());
        }
    }

    #[test]
    fn execute_background_blocking() {
        block_on(execute_background());
    }*/
}
