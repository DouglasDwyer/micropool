use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::hint::{spin_loop, unreachable_unchecked};
use std::mem::MaybeUninit;
use std::ops::ControlFlow;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::thread::{self, JoinHandle, available_parallelism};

use paralight::iter::{Accumulator, ExactSizeAccumulator, GenericThreadPool, SourceCleanup};
use smallvec::SmallVec;
use vecdeque_stableix::Deque;

use crate::util::*;
use crate::{OwnedTask, SharedTask, TaskInner};

/// The global thread pool.
static GLOBAL_POOL: spin::Once<ThreadPool> = spin::Once::new();

thread_local! {
    /// The thread pool that is locally active due to [`ThreadPool::install`].
    static LOCAL_POOL: Cell<*const ThreadPool> = const { Cell::new(std::ptr::null()) };
}

/// The function signature for the [`ThreadPoolBuilder::spawn_handler`]
/// function.
type ThreadSpawnerFn = dyn FnMut(usize, Box<dyn FnOnce() + Send>) -> JoinHandle<()>;

/// Determines how a thread pool will behave.
pub struct ThreadPoolBuilder {
    /// Threads waiting for work will spin at least this many cycles before
    /// sleeping.
    idle_spin_cycles: usize,
    /// The number of threads to spawn.
    num_threads: usize,
    /// The function to use when spawning new threads.
    spawn_handler: Box<ThreadSpawnerFn>,
}

impl ThreadPoolBuilder {
    /// Creates a new [`ThreadPool`] initialized using this configuration.
    pub fn build(self) -> ThreadPool {
        ThreadPool::new(self)
    }

    /// Initializes the global thread pool. This initialization is
    /// **optional**.  If you do not call this function, the thread pool
    /// will be automatically initialized with the default
    /// configuration.
    ///
    /// Panics if the global thread pool was already initialized.
    pub fn build_global(self) {
        let mut run = false;
        GLOBAL_POOL.call_once(|| {
            run = true;
            self.build()
        });

        if !run {
            panic!("ThreadPoolBuilder::build_global called after global pool was already active");
        }
    }

    /// Threads waiting for work will spin at least this many cycles before
    /// sleeping.
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
        F: FnMut(usize, Box<dyn FnOnce() + Send>) -> JoinHandle<()> + 'static,
    {
        Self {
            spawn_handler: Box::new(spawn),
            ..self
        }
    }
}

impl Default for ThreadPoolBuilder {
    fn default() -> Self {
        Self {
            idle_spin_cycles: 150000,
            num_threads: available_parallelism()
                .map(usize::from)
                .unwrap_or_default()
                .saturating_sub(1)
                .max(1),
            spawn_handler: Box::new(|_, x| thread::spawn(x)),
        }
    }
}

/// Represents a user-created thread pool.
///
/// Use a [`ThreadPoolBuilder`] to specify the number and/or names of threads
/// in the pool. After calling [`ThreadPoolBuilder::build()`], you can then
/// execute functions explicitly within this [`ThreadPool`] using
/// [`ThreadPool::install()`]. By contrast, top-level functions
/// (like `join()`) will execute implicitly within the current thread pool.
pub struct ThreadPool {
    /// Handles for stopping the pool threads.
    join_handles: Vec<JoinHandle<()>>,
    /// The shared state for the threadpool.
    state: &'static ThreadPoolState,
}

impl ThreadPool {
    /// The amount of space (in units of elements) to reserve on the stack
    /// for parallel pipeline outputs.
    const OUTPUT_BUFFER_CAPACITY: usize = 256;

    /// Initializes a new pool with `builder`.
    fn new(mut builder: ThreadPoolBuilder) -> Self {
        let state = Box::leak(Box::new(ThreadPoolState::new(&builder)));

        let mut join_handles = Vec::with_capacity(builder.num_threads);
        for i in 0..builder.num_threads {
            join_handles.push((builder.spawn_handler)(i, Box::new(|| state.join())));
        }

        Self {
            join_handles,
            state,
        }
    }

    /// Changes the current context to this thread pool. Any attempts to use
    /// [`crate::join`] or parallel iterators will operate within this pool.
    ///
    /// Panics if called from within a parallel iterator or other asynchronous
    /// task.
    pub fn install<R>(&self, f: impl FnOnce() -> R) -> R {
        abort_on_panic(|| {
            let previous = LOCAL_POOL.get();
            LOCAL_POOL.set(self);
            let result = f();
            LOCAL_POOL.set(previous);
            result
        })
    }

    /// Spawns an asynchronous task on the global thread pool.
    /// The returned handle can be used to obtain the result.
    pub fn spawn_owned<T: 'static + Send>(
        &self,
        f: impl 'static + FnOnce() -> T + Send,
    ) -> OwnedTask<T> {
        OwnedTask::spawn(self.state, f)
    }

    /// Spawns a shared asynchronous task on the global thread pool.
    /// The returned handle can be used to obtain the result.
    pub fn spawn_shared<T: 'static + Send + Sync>(
        &self,
        f: impl 'static + FnOnce() -> T + Send,
    ) -> SharedTask<T> {
        SharedTask::spawn(self.state, f)
    }

    /// Executes `f` within the context of the current thread pool.
    /// Initializes the global thread pool if no other pool is active.
    pub(crate) fn with_current<R>(f: impl FnOnce(&ThreadPool) -> R) -> R {
        unsafe {
            let mut pool_ptr = LOCAL_POOL.get();

            if pool_ptr.is_null() {
                pool_ptr = GLOBAL_POOL.call_once(|| ThreadPoolBuilder::default().build());
                LOCAL_POOL.set(pool_ptr);
            }

            f(&*pool_ptr)
        }
    }

    /// The total number of worker threads in this pool.
    pub fn num_threads(&self) -> usize {
        self.join_handles.len()
    }

    /// Takes two closures and *potentially* runs them in parallel. It
    /// returns a pair of the results from those closures.
    pub fn join<A, B, RA, RB>(&self, oper_a: A, oper_b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        unsafe {
            let oper_a_holder = MaybeUninit::new(oper_a);
            let oper_b_holder = MaybeUninit::new(oper_b);

            let result_a = UnsafeCell::new(MaybeUninit::uninit());
            let result_b = UnsafeCell::new(MaybeUninit::uninit());

            WorkQueue::invoke(
                self.state,
                |i| match i {
                    0 => {
                        (*result_a.get()).write(oper_a_holder.assume_init_read()());
                    }
                    1 => {
                        (*result_b.get()).write(oper_b_holder.assume_init_read()());
                    }
                    _ => unreachable_unchecked(),
                },
                2,
            );

            (
                result_a.into_inner().assume_init(),
                result_b.into_inner().assume_init(),
            )
        }
    }

    /// Execute [`paralight`] iterators with maximal parallelism.
    /// Every iterator item may be processed on a separate thread.
    ///
    /// Note: by maximizing parallelism, this also maximizes overhead.
    /// This is best used with computationally-heavy iterators that have few
    /// elements. For alternatives, see [`Self::split_per`],
    /// [`Self::split_by`], and [`Self::split_by_threads`].
    pub fn split_per_item(&self) -> impl '_ + GenericThreadPool {
        SplitPerItem(self)
    }

    /// Execute [`paralight`] iterators by batching elements.
    /// Each group of `chunk_size` elements may be processed by a single thread.
    pub fn split_per(&self, chunk_size: usize) -> impl '_ + GenericThreadPool {
        SplitPer {
            chunk_units_calculator: move |x| (chunk_size.max(1), x.div_ceil(chunk_size.max(1))),
            pool: self,
        }
    }

    /// Execute [`paralight`] iterators by batching elements.
    /// Every iterator will be broken up into `chunks`
    /// separate work units, which may be processed in parallel.
    pub fn split_by(&self, chunks: usize) -> impl '_ + GenericThreadPool {
        SplitPer {
            chunk_units_calculator: move |x| (x.div_ceil(chunks.max(1)), chunks.max(1)),
            pool: self,
        }
    }

    /// Execute [`paralight`] iterators by batching elements.
    /// Every iterator will be broken up into [`Self::num_threads`]
    /// separate work units, which may be processed in parallel.
    pub fn split_by_threads(&self) -> impl '_ + GenericThreadPool {
        // Add one additional chunk for cases where a non-pool
        // thread is invoking the work
        self.split_by(self.num_threads() + 1)
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.state.should_stop.store(true, Ordering::Relaxed);
        self.state.on_change.notify();

        for handle in self.join_handles.drain(..) {
            let _ = handle.join();
        }
    }
}

/// Implementation for [`ThreadPool::split_per_item`].
struct SplitPerItem<'a>(&'a ThreadPool);

unsafe impl<'a> GenericThreadPool for SplitPerItem<'a> {
    fn upper_bounded_pipeline<Output: Send, Accum>(
        self,
        input_len: usize,
        init: impl Fn() -> Accum + Sync,
        process_item: impl Fn(Accum, usize) -> ControlFlow<Accum, Accum> + Sync,
        finalize: impl Fn(Accum) -> Output + Sync,
        reduce: impl Fn(Output, Output) -> Output,
        _: &(impl SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let mut output = SmallVec::<[Output; ThreadPool::OUTPUT_BUFFER_CAPACITY]>::new();
            output.reserve_exact(input_len);
            let output_buffer = output.as_mut_ptr();

            WorkQueue::invoke(
                self.0.state,
                |i| {
                    output_buffer
                        .add(i)
                        .write(finalize(match process_item(init(), i) {
                            ControlFlow::Break(x) | ControlFlow::Continue(x) => x,
                        }));
                },
                input_len,
            );

            output.set_len(input_len);
            output
                .into_iter()
                .reduce(reduce)
                .expect("Iterator was empty")
        }
    }

    fn iter_pipeline<Output, Accum: Send>(
        self,
        input_len: usize,
        accum: impl Accumulator<usize, Accum> + Sync,
        reduce: impl ExactSizeAccumulator<Accum, Output>,
        _: &(impl SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let mut output = SmallVec::<[Accum; ThreadPool::OUTPUT_BUFFER_CAPACITY]>::new();
            output.reserve_exact(input_len);
            let output_buffer = output.as_mut_ptr();

            WorkQueue::invoke(
                self.0.state,
                |i| {
                    output_buffer.add(i).write(accum.accumulate(i..i + 1));
                },
                input_len,
            );

            output.set_len(input_len);
            reduce.accumulate_exact(output.into_iter())
        }
    }
}

/// Implementation for [`ThreadPool::split_per`], [`ThreadPool::split_by`], and
/// [`ThreadPool::split_by_threads`].
struct SplitPer<'a, F: Fn(usize) -> (usize, usize)> {
    /// Maps the input iterator size to a chunk size and work unit count.
    chunk_units_calculator: F,
    /// The pool on which to spawn the work.
    pool: &'a ThreadPool,
}

unsafe impl<'a, F: Fn(usize) -> (usize, usize)> GenericThreadPool for SplitPer<'a, F> {
    fn upper_bounded_pipeline<Output: Send, Accum>(
        self,
        input_len: usize,
        init: impl Fn() -> Accum + Sync,
        process_item: impl Fn(Accum, usize) -> ControlFlow<Accum, Accum> + Sync,
        finalize: impl Fn(Accum) -> Output + Sync,
        reduce: impl Fn(Output, Output) -> Output,
        cleanup: &(impl SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let (chunk_size, work_units) = (self.chunk_units_calculator)(input_len);

            let mut output = SmallVec::<[Output; ThreadPool::OUTPUT_BUFFER_CAPACITY]>::new();
            output.reserve_exact(work_units);

            let break_early = AtomicBool::new(false);
            let output_buffer = output.as_mut_ptr();

            WorkQueue::invoke(
                self.pool.state,
                |i| {
                    let start = chunk_size * i;
                    let end = (start + chunk_size).min(input_len);

                    let mut accumulator = init();

                    for j in start..end {
                        if break_early.load(Ordering::Relaxed) {
                            cleanup.cleanup_item_range(j..end);
                            break;
                        }

                        match process_item(accumulator, j) {
                            ControlFlow::Break(x) => {
                                accumulator = x;
                                break_early.store(true, Ordering::Release);
                                cleanup.cleanup_item_range(j + 1..end);
                                break;
                            }
                            ControlFlow::Continue(x) => accumulator = x,
                        };
                    }

                    output_buffer.add(i).write(finalize(accumulator));
                },
                work_units,
            );

            output.set_len(work_units);
            output
                .into_iter()
                .reduce(reduce)
                .expect("Iterator was empty")
        }
    }

    fn iter_pipeline<Output, Accum: Send>(
        self,
        input_len: usize,
        accum: impl Accumulator<usize, Accum> + Sync,
        reduce: impl ExactSizeAccumulator<Accum, Output>,
        _: &(impl SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let (chunk_size, work_units) = (self.chunk_units_calculator)(input_len);

            let mut output = SmallVec::<[Accum; ThreadPool::OUTPUT_BUFFER_CAPACITY]>::new();
            output.reserve_exact(work_units);
            let output_buffer = output.as_mut_ptr();

            WorkQueue::invoke(self.pool.state,
                |i| {
                    let start = chunk_size * i;
                    let end = (start + chunk_size).min(input_len);
                    output_buffer.add(i).write(accum.accumulate(start..end));
                }, work_units);

            output.set_len(work_units);
            reduce.accumulate_exact(output.into_iter())
        }
    }
}

/// Stores the inner state for a [`ThreadPool`] and coordinates work across
/// multiple threads.
pub(crate) struct ThreadPoolState {
    /// Threads waiting for work will spin for at least this many cycles before
    /// sleeping.
    pub idle_spin_cycles: usize,
    /// Raised whenever work is available from queues or tasks.
    pub on_change: Event,
    /// Queues that have pending work.
    pub roots: spin::Mutex<Deque<*mut WorkQueue, isize>>,
    /// Whether the parent [`ThreadPool`] is being dropped.
    /// This indicates that workers should exit.
    pub should_stop: AtomicBool,
    /// The tasks that are currently in progress.
    pub tasks: spin::Mutex<VecDeque<Arc<dyn TaskInner>>>,
}

impl ThreadPoolState {
    /// Creates a new state object.
    pub fn new(builder: &ThreadPoolBuilder) -> Self {
        Self {
            idle_spin_cycles: builder.idle_spin_cycles,
            on_change: Event::new(),
            roots: spin::Mutex::default(),
            should_stop: AtomicBool::new(false),
            tasks: spin::Mutex::new(VecDeque::new()),
        }
    }

    /// Removes the provided task from the queue if it is found.
    pub fn cancel_task<U: ?Sized>(&self, task: &Arc<U>) {
        let mut tasks = self.tasks.lock();
        if let Some(index) = tasks
            .iter()
            .position(|x| Arc::as_ptr(x).cast::<()>() == Arc::as_ptr(task).cast::<()>())
        {
            tasks.swap_remove_back(index);
        }
    }

    /// Schedules a task to be run on the pool.
    pub fn push_task(&self, task: Arc<dyn TaskInner>) {
        println!("spawn task");
        let mut tasks = self.tasks.lock();
        let no_prior_work = tasks.is_empty();
        tasks.push_back(task);
        drop(tasks);
        println!("end spawn taks");
        
        if no_prior_work {
            self.on_change.notify();
        }
    }

    /// Polls for available work on the thread pool, and goes to sleep if none
    /// is available.
    fn join(&self) {
        let mut spin_before_sleep = false;

        loop {
            let listener = self.on_change.listen();
            
            if let Some((queue_ptr, item_ptr, first_unit)) = self.reserve_work_unit() {
                // Safety: the queue will remain valid until the reserved item completes
                let queue = unsafe { queue_ptr.as_ref() };
                WorkQueueLocalStorage::enter(queue, |queue| {
                    // Safety: the item was reserved from a valid queue beforehand
                    let complete = unsafe { WorkQueue::help_work_item(item_ptr, first_unit) };
                    if complete {
                        queue.on_change.notify();
                    }
                });
                
                spin_before_sleep = true;
            } else if self.should_stop.load(Ordering::Relaxed) {
                return;
            } else if let Some(task) = self.pop_task() {
                task.run();
            } else {
                // Only spin if something was found to do since the last sleep
                let spin_cycles = if spin_before_sleep {
                    self.idle_spin_cycles
                } else {
                    0
                };
                spin_before_sleep = !listener.spin_wait(spin_cycles);
            }
        }
    }

    /// Removes a task from the queue, if one is available.
    fn pop_task(&self) -> Option<Arc<dyn TaskInner>> {
        self.tasks.lock().pop_front()
    }

    fn reserve_work_unit(&self) -> Option<(NonNull<WorkQueue>, NonNull<WorkItem>, usize)> {
        println!("rwu {:?}", std::thread::current().id());
        let mut roots = self.roots.lock();
        while let Some(maybe_ptr) = roots.front() {
            if let Some(queue_ptr) = NonNull::new(*maybe_ptr) {
                // Safety: as long as the item exists in the queue, its pointer is valid
                let queue = unsafe { queue_ptr.as_ref() };
                if let Some((item_ptr, first_unit)) = queue.reserve_work_unit_or_clear_root() {
                    println!("rwud {:?}", std::thread::current().id());
                    return Some((queue_ptr, item_ptr, first_unit));
                }
            }
            
            roots.pop_front();
        }

        println!("rwuf {:?}", std::thread::current().id());
        None
    }
}

impl std::fmt::Debug for ThreadPoolState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ThreadPoolState")
            .finish_non_exhaustive()
    }
}

unsafe impl Send for ThreadPoolState {}
unsafe impl Sync for ThreadPoolState {}

#[derive(Debug, Default)]
struct WorkQueue {
    /// Raised whenever a work item is created or finished.
    on_change: Event,
    /// The associated thread pool.
    pool: Option<&'static ThreadPoolState>,
    /// Inner state used for work tracking.
    state: spin::Mutex<WorkQueueState>
}

impl WorkQueue {
    pub unsafe fn invoke(pool: &'static ThreadPoolState, f: impl Fn(usize), times: usize) {
        match times {
            0 => {}
            1 => f(0),
            _ => WorkQueueLocalStorage::with(pool, |queue| queue.register_and_invoke(f, times))
        }
    }

    fn dequeue_item(&self, index: isize) {
        let mut inner = self.state.lock();
        if let Some(entry) = inner.queue.get_mut(index) {
            *entry = std::ptr::null_mut();
            inner.pending_items -= 1;
        }
    }

    fn enqueue_item(&self, item_ptr: NonNull<WorkItem>) -> isize {
        let mut inner = self.state.lock();
        let no_prior_work = inner.pending_items == 0;
        let index = inner.queue.push_back(item_ptr.as_ptr());
        inner.pending_items += 1;

        if no_prior_work {
            self.on_change.notify();

            if inner.root_index.is_none() {
                drop(inner);
                let mut roots = self.pool().roots.lock();
                let mut inner = self.state.lock();
                if inner.root_index.is_none() {
                    inner.root_index = Some(roots.push_back(self as *const _ as *mut _));
                    drop((roots, inner));
                    self.pool().on_change.notify();
                }
            }
        }

        index
    }

    fn help(&self) -> bool {
        if let Some((item_ptr, first_unit)) = self.reserve_work_unit() {
            // Safety: `item_ptr` is valid until the reserved work is complete
            let complete = unsafe { Self::help_work_item(item_ptr, first_unit) };

            if complete {
                self.on_change.notify();
            }

            true
        }
        else {
            false
        }
    }

    fn register_and_invoke(&self, f: impl Fn(usize), times: usize) {
        // Safety: it is always sound to create a pointer
        let func = unsafe {
            std::mem::transmute::<
                *const (dyn Fn(usize) + '_),
                *const (dyn Fn(usize) + 'static),
            >(&f as *const dyn Fn(usize))
        };

        let item = WorkItem {
            completed_invocations: AtomicU64::new(0),
            enqueued: AtomicBool::new(true),
            func,
            started_invocations: AtomicU64::new(1),
            total_invocations: times as u64
        };
        let item_ptr = NonNull::from_ref(&item);

        let index = self.enqueue_item(item_ptr);

        // Safety: `item_ptr` is valid for the duration of the call
        let complete = unsafe { Self::help_work_item(item_ptr, 0) };

        if !complete {
            self.wait_until_complete(&item);
        }

        // Ensure that results from all work items are visible to the calling thread.
        fence(Ordering::Acquire);

        if item.enqueued.load(Ordering::Relaxed) {
            self.dequeue_item(index);
        }
    }
    
    /// Searches for available work at the beginning of the queue.
    /// If found, the work unit is marked as taken, so it is the caller's
    /// responsibility to invoke [`Self::help_work_item`] on it.
    fn reserve_work_unit(&self) -> Option<(NonNull<WorkItem>, usize)> {
        Self::reserve_work_unit_inner(&mut self.state.lock())
    }
    
    /// Searches for available work at the beginning of the queue.
    /// If found, the work unit is marked as taken, so it is the caller's
    /// responsibility to invoke [`Self::help_work_item`] on it.
    fn reserve_work_unit_or_clear_root(&self) -> Option<(NonNull<WorkItem>, usize)> {
        let mut state = self.state.lock();
        let result = Self::reserve_work_unit_inner(&mut state);

        if result.is_none() {
            state.root_index = None;
        }

        result
    }

    /// Waits for pending units on `item` to finish processing.
    /// In the meantime, calls [`Self::help`] to steal other work items.
    fn wait_until_complete(&self, item: &WorkItem) {
        let mut spin_before_sleep = true;
        let mut first_time = false;
        loop {
            let listener = self.on_change.listen();
            if item.completed_invocations.load(Ordering::Relaxed) < item.total_invocations {
                if self.help() {
                    spin_before_sleep = true;
                }
                else {
                    // Only spin if something was found to do since the last sleep
                    let spin_cycles = if spin_before_sleep {
                        self.pool().idle_spin_cycles
                    } else {
                        0
                    };
                    spin_before_sleep = !listener.spin_wait(spin_cycles);
                }
            }
            else {
                break;
            }
        }
    }

    /// Gets the thread pool currently associated with this queue.
    fn pool(&self) -> &'static ThreadPoolState {
        self.pool.expect("pool was not initialized")
    }

    /// Executes pending work for `item_ptr`, starting with `first_unit` (which
    /// should already have been reserved). Returns whether the work item was fully completed
    /// as a result of this call.
    /// 
    /// # Safety
    /// 
    /// `item_ptr` must refer to a valid item that resides in this queue.
    /// `first_unit` must be a unique index not processsed by any other thread.
    /// Once this function completes, `item_ptr` might no longer be valid
    /// (since the owning thread may deallocate it).
    unsafe fn help_work_item(item_ptr: NonNull<WorkItem>, first_unit: usize) -> bool {
        // Safety: the pointer is valid until we increment `completed_invocations`
        let item = unsafe { item_ptr.as_ref() };
        let total_invocations = item.total_invocations;

        let mut locally_completed = 1;
        // Safety: the function is valid and has not been called with `first_unit` yet
        unsafe { (*item.func)(first_unit); }

        loop {
            let next_unit = item.started_invocations.fetch_add(1, Ordering::Relaxed);
            if next_unit < item.total_invocations {
                // Safety: the function is valid and has not been called with `next_unit` yet
                unsafe { (*item.func)(next_unit as usize) };
                locally_completed += 1;
            }
            else {
                break;
            }
        }

        let now_finished = item.completed_invocations.fetch_add(locally_completed, Ordering::Release) + locally_completed;
        now_finished == total_invocations
    }
    
    /// Searches for available work at the beginning of the queue.
    /// If found, the work unit is marked as taken, so it is the caller's
    /// responsibility to invoke [`Self::help_work_item`] on it.
    fn reserve_work_unit_inner(inner: &mut WorkQueueState) -> Option<(NonNull<WorkItem>, usize)> {
        while let Some(maybe_ptr) = inner.queue.front_mut() {
            if let Some(item_ptr) = NonNull::new(*maybe_ptr) {
                // Safety: as long as the item exists in the queue, its pointer is valid
                let item = unsafe { item_ptr.as_ref() };
                let first_unit = item.started_invocations.fetch_add(1, Ordering::Relaxed);

                if item.total_invocations <= first_unit + 1 {
                    item.enqueued.store(false, Ordering::Relaxed);
                    inner.queue.pop_front();
                    inner.pending_items -= 1;
                }

                if first_unit < item.total_invocations {
                    return Some((item_ptr, first_unit as usize));
                }
            }
            else {
                inner.queue.pop_front();
            }            
        }

        None
    }
}

/// The mutable inner state for a [`WorkQueue`].
#[derive(Debug, Default)]
struct WorkQueueState {
    /// The total number of non-null entries in [`Self::queue`].
    pub pending_items: usize,
    /// Holds work items that (potentially) have units that need to start.
    /// Once an item is fully started, its pointer will be overwritten with null here.
    pub queue: Deque<*mut WorkItem, isize>,
    pub root_index: Option<isize>,
}

/// Tracks a parallel computation.
#[derive(Debug)]
struct WorkItem {
    /// The number of units that have finished.
    pub completed_invocations: AtomicU64,
    /// Whether the work item is still registered in the [`WorkQueueState::queue`].
    pub enqueued: AtomicBool,
    /// The function to invoke. Accepts the unit index as an argument.
    pub func: *const (dyn 'static + Fn(usize)),
    /// The number of units that have started.
    pub started_invocations: AtomicU64,
    /// The total number of units to invoke.
    pub total_invocations: u64,
}

thread_local! {
    static CURRENT_QUEUE: Cell<*const WorkQueue> = const { Cell::new(std::ptr::null()) };
    static QUEUE_STORAGE: UnsafeCell<WorkQueue> = UnsafeCell::default();
}

#[derive(Debug, Default)]
struct WorkQueueLocalStorage;

impl WorkQueueLocalStorage {
    pub fn with(pool: &'static ThreadPoolState, f: impl FnOnce(&WorkQueue)) {
        // Safety: CURRENT_QUEUE is only non-null within a call to `Self::enter`
        // (which will be somewhere up the stack) so this reference should be valid.
        if let Some(queue) = unsafe { CURRENT_QUEUE.get().as_ref() } {
            assert!(std::ptr::eq(queue.pool(), pool), "attempted to parallelize work with two different thread pools simultaneously");
            f(queue)
        }
        else {
            QUEUE_STORAGE.with(|storage| {
                // Safety: `Self::enter` will set the current queue, so storage
                // will not be borrowed mutably as the result of a recursive call
                let queue = unsafe { &mut *storage.get() };
                queue.pool = Some(pool);
                Self::enter(queue, f);

                {
                    let mut inner = queue.state.lock();
                    if let Some(index) = inner.root_index {
                        drop(inner);
                        if let Some(queue_ptr) = pool.roots.lock().get_mut(index) {
                            *queue_ptr = std::ptr::null_mut();
                        }
                    }
                }
            })
        }
    }

    pub fn enter(queue: &WorkQueue, f: impl FnOnce(&WorkQueue)) {
        assert!(CURRENT_QUEUE.get().is_null(), "attempted to enter queue more than once");
        CURRENT_QUEUE.set(queue);
        f(queue);
        CURRENT_QUEUE.set(std::ptr::null());
    }
}