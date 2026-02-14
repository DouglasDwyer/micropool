#![allow(unused)]

use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::hint::unreachable_unchecked;
use std::iter::repeat_with;
use std::mem::{MaybeUninit, transmute};
use std::ops::ControlFlow;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering, fence};
use std::thread::{self, JoinHandle, available_parallelism};

use paralight::iter::{Accumulator, ExactSizeAccumulator, GenericThreadPool, SourceCleanup};
use smallvec::SmallVec;

use crate::util::*;
use crate::{OwnedTask, SharedTask, TaskInner};

/// The global thread pool.
static GLOBAL_POOL: spin::Once<ThreadPool> = spin::Once::new();

thread_local! {
    /// A mask related to the top-level work item being executed by this thread.
    /// This mask is used to restrict which jobs get taken during work-stealing.
    /// This prevents external threads from taking each other's work
    /// (which would increase latency).
    static LOCAL_ADVERTISE_MASK: Cell<*const AtomicBits> = const { Cell::new(std::ptr::null()) };

    /// The thread pool that is locally active due to [`ThreadPool::install`].
    static LOCAL_POOL: Cell<*const ThreadPool> = const { Cell::new(std::ptr::null()) };

    /// todo
    static ADVERTISE_MASK_STORE: UnsafeCell<AtomicBits> = UnsafeCell::new(AtomicBits::default());
}

/// The function signature for the [`ThreadPoolBuilder::spawn_handler`]
/// function.
type ThreadSpawnerFn = dyn FnMut(usize, Box<dyn FnOnce() + Send>) -> JoinHandle<()>;

/// Determines how a thread pool will behave.
pub struct ThreadPoolBuilder {
    /// Threads waiting for work will spin at least this many cycles before
    /// sleeping.
    idle_spin_cycles: usize,
    /// The maximum supported number of concurrent jobs (created through parallel iterators or calls to [`ThreadPool::join`]).
    /// By default, this is set to `8 * std::thread::available_parallelism()`.
    max_jobs: usize,
    /// The number of threads to spawn.
    /// By default, this is set to `std::thread::available_parallelism().saturating_sub(1).max(1)`.
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
            idle_spin_cycles: 3000,
            max_jobs: 8 * available_parallelism().map(usize::from).unwrap_or(1),
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
    /// Handles for stopping the pool threads (if this is an owned pool),
    /// or [`None`] (if this is a pool referenced by a worker).
    join_handles: Option<Vec<JoinHandle<()>>>,
    /// The shared state for the threadpool.
    state: Arc<ThreadPoolState>,
}

impl ThreadPool {
    /// The amount of space (in units of elements) to reserve on the stack
    /// for parallel pipeline outputs.
    const OUTPUT_BUFFER_CAPACITY: usize = 256;

    /// Initializes a new pool with `builder`.
    fn new(mut builder: ThreadPoolBuilder) -> Self {
        let state = Arc::new(ThreadPoolState::new(&builder));

        let mut join_handles = Vec::with_capacity(builder.num_threads);
        for i in 0..builder.num_threads {
            let state_cloned = state.clone();
            join_handles.push((builder.spawn_handler)(i, Box::new(move || {
                let thread_pool = Self { join_handles: None, state: state_cloned };
                LOCAL_POOL.set(&thread_pool);
                thread_pool.state.join()
            })));
        }

        Self {
            join_handles: Some(join_handles),
            state,
        }
    }

    /// Changes the current context to this thread pool. Any attempts to use
    /// [`crate::join`] or parallel iterators will operate within this pool.
    /// Panics if called recursively.
    pub fn install<R>(&self, f: impl FnOnce() -> R) -> R {
        abort_on_panic(|| {
            assert!(
                LOCAL_POOL.get().is_null(),
                "cannot call install recursively"
            );
            
            LOCAL_POOL.set(self);
            let result = f();
            LOCAL_POOL.set(std::ptr::null());
            result
        })
    }

    /// Spawns an asynchronous task on the global thread pool.
    /// The returned handle can be used to obtain the result.
    pub fn spawn_owned<T: 'static + Send>(
        &self,
        f: impl 'static + FnOnce() -> T + Send,
    ) -> OwnedTask<T> {
        OwnedTask::spawn(&self.state, f)
    }

    /// Spawns a shared asynchronous task on the global thread pool.
    /// The returned handle can be used to obtain the result.
    pub fn spawn_shared<T: 'static + Send + Sync>(
        &self,
        f: impl 'static + FnOnce() -> T + Send,
    ) -> SharedTask<T> {
        SharedTask::spawn(&self.state, f)
    }

    /// Executes `f` within the context of the current thread pool.
    /// Initializes the global thread pool if no other pool is active.
    pub(crate) fn with_current<R>(f: impl FnOnce(&ThreadPool) -> R) -> R {
        abort_on_panic(|| unsafe {
            let mut pool_ptr = LOCAL_POOL.get();

            if pool_ptr.is_null() {
                pool_ptr = GLOBAL_POOL.call_once(|| ThreadPoolBuilder::default().build());
                LOCAL_POOL.set(pool_ptr);
                let result = f(&*pool_ptr);
                LOCAL_POOL.set(std::ptr::null());
                result
            }
            else {
                f(&*pool_ptr)
            }
        })
    }

    /// The total number of worker threads in this pool.
    pub fn num_threads(&self) -> usize {
        self.state.num_threads
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

            self.state.invoke_sync_unchecked(
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
        if let Some(join_handles) = &mut self.join_handles {
            self.state.should_stop.store(true, Ordering::Relaxed);
            self.state.on_change.notify();

            for handle in join_handles.drain(..) {
                let _ = handle.join();
            }
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

            self.0.state.invoke_sync_unchecked(
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

            self.0.state.invoke_sync_unchecked(
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

            self.pool.state.invoke_sync_unchecked(
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

            self.pool.state.invoke_sync_unchecked(
                |i| {
                    let start = chunk_size * i;
                    let end = (start + chunk_size).min(input_len);
                    output_buffer.add(i).write(accum.accumulate(start..end));
                },
                work_units,
            );

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
    idle_spin_cycles: usize,
    global_advertise_mask: AtomicBits,
    /// Shared information about scheduled jobs. Threads reserve slots in this array
    /// using the [`Self::running_jobs`] member. Other threads can then search this
    /// array to look for work.
    jobs: Vec<JobSlot>,
    num_threads: usize,
    /// An event that is invoked whenever new work is available.
    on_change: Event,
    running_jobs: AtomicBits,
    /// Whether the parent [`ThreadPool`] is being dropped.
    /// This indicates that workers should exit.
    should_stop: AtomicBool,
    /// The tasks that are currently in progress.
    tasks: spin::Mutex<VecDeque<Arc<dyn TaskInner>>>,
}

impl ThreadPoolState {
    /// Creates a new state object.
    pub fn new(builder: &ThreadPoolBuilder) -> Self {
        let global_advertise_mask = AtomicBits::new(builder.max_jobs);
        let running_jobs = AtomicBits::new(builder.max_jobs);

        Self {
            global_advertise_mask,
            idle_spin_cycles: builder.idle_spin_cycles,
            jobs: repeat_with(JobSlot::default).take(running_jobs.len()).collect(),
            num_threads: builder.num_threads,
            on_change: Event::new(),
            running_jobs,
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

    pub unsafe fn invoke_sync_unchecked(&self, f: impl Fn(usize), times: usize) {
        /// Forces the compiler to accept that `f` is `Sync`.
        struct AssertSync<F>(F);
        
        impl<F> AssertSync<F> {
            /// Gets the inner value.
            pub unsafe fn get(&self) -> &F {
                &self.0
            }
        }
        
        // Safety: guaranteed by the outer function invariant
        unsafe impl<F> Sync for AssertSync<F> {}

        let f_sync = AssertSync(f);
        
        self.invoke(move |i| unsafe { (f_sync.get())(i) }, times)
    }

    pub fn invoke(&self, f: impl Fn(usize) + Sync, times: usize) {
        match times {
            0 => {},
            1 => f(0),
            _ => if let Some(index) = self.reserve_job_slot() {
                let func = Self::create_job_func(f);
                let slot = &self.jobs[index];
                let times = times as i64;

                let previous_local_advertise_mask = LOCAL_ADVERTISE_MASK.get();
                let search_mask = if previous_local_advertise_mask.is_null() {
                    // Safety: todo
                    unsafe {
                        &*ADVERTISE_MASK_STORE.with(|x| {
                            let array = unsafe { &mut *x.get() };
                            if array.len() < self.global_advertise_mask.len() {
                                *array = AtomicBits::new(self.global_advertise_mask.len());
                            }

                            x.get()
                        })
                    }
                }
                else {
                    // Safety: todo
                    unsafe { &*previous_local_advertise_mask }
                };

                let advertise_masks = [&self.global_advertise_mask, search_mask];

                let descriptor = JobDescriptor {
                    clear_masks: &advertise_masks,
                    func: &func,
                    incomplete_units: AtomicI64::new(times),
                    search_mask
                };

                // Safety: the slot has been reserved but the job count is not yet published,
                // so no other threads will be reading the descriptor.
                unsafe { *slot.descriptor.get() = &descriptor as *const _ as *const _ };

                // The descriptor that was written must be made visible to other threads
                slot.available_units.store(times - 1, Ordering::Release);

                for mask in &advertise_masks {
                    mask.set(index, true, Ordering::Relaxed);
                }

                // Notify other threads of available work
                self.on_change.notify();

                let locally_completed = func(JobInvocation {
                    available_units: &slot.available_units,
                    clear_masks: &advertise_masks,
                    reserved_unit: times - 1,
                    slot: index
                });

                if locally_completed < times {
                    let mut spin_before_sleep = true;
                    let mut listener = self.on_change.listen();

                    let remaining = descriptor.incomplete_units.fetch_sub(locally_completed, Ordering::Relaxed) - locally_completed;

                    if 0 < remaining {
                        while 0 < descriptor.incomplete_units.load(Ordering::Relaxed) {
                            if self.help_one_job(search_mask, false) {
                                spin_before_sleep = true;
                            }
                            else {
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

                    fence(Ordering::Acquire);
                }

                LOCAL_ADVERTISE_MASK.set(previous_local_advertise_mask);
                self.release_job_slot(index);
            }
            else {
                // No job slot available - perform the work without parallelism
                for i in 0..times {
                    f(i);
                }
            }
        }
    }

    /// Schedules a task to be run on the pool.
    pub fn push_task(&self, task: Arc<dyn TaskInner>) {
        self.tasks.lock().push_back(task);
        self.on_change.notify();
    }

    /// Polls for available work on the thread pool, and goes to sleep if none
    /// is available.
    fn join(&self) {
        let mut listener = self.on_change.listen();
        let mut spin_before_sleep = false;

        loop {
            if self.help_global_jobs() {
                spin_before_sleep = true;
            } else if self.should_stop.load(Ordering::Relaxed) {
                return;
            } else if let Some(task) = self.pop_task() {
                //JoinPoint::join_task(&*task, true);
                task.run();
                spin_before_sleep = true;
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

    fn help_global_jobs(&self) -> bool {
        let mut ran_item = false;
        
        while self.help_one_job(&self.global_advertise_mask, true) {
            ran_item = true;
        }

        ran_item
    }

    fn help_one_job(&self, search_mask: &AtomicBits, overwrite_local_advertise_mask: bool) -> bool {
        for index in search_mask.iter_ones() {
            let slot = &self.jobs[index];
            let reserved_unit = slot.available_units.fetch_sub(1, Ordering::Relaxed) - 1;

            if reserved_unit < 0 {
                continue;
            }
            else {
                // Safety: we were able to reserve a work unit, so the job is valid
                // and will remain so until the unit is processed.
                let descriptor = unsafe { &*slot.descriptor.get().read().cast::<JobDescriptor>() };

                if !std::ptr::eq(descriptor.search_mask, search_mask) {
                    // The work unit corresponds to some other search set now. Cancel this task,
                    // and make visible the memory reads from this thread
                    slot.available_units.fetch_add(1, Ordering::Release);
                    self.on_change.notify();
                    continue;
                }
                
                let previous_local_advertise_mask = LOCAL_ADVERTISE_MASK.get();
                if overwrite_local_advertise_mask {
                    LOCAL_ADVERTISE_MASK.set(descriptor.search_mask);
                }

                let locally_completed = (descriptor.func)(JobInvocation {
                    available_units: &slot.available_units,
                    clear_masks: descriptor.clear_masks,
                    reserved_unit,
                    slot: index
                });

                if overwrite_local_advertise_mask {
                    LOCAL_ADVERTISE_MASK.set(previous_local_advertise_mask);
                }

                // The parallelized results must be made visible to the original thread
                let remaining = descriptor.incomplete_units.fetch_sub(locally_completed, Ordering::Release) - locally_completed;
                
                if remaining == 0 {
                    self.on_change.notify();
                }

                return true;
            }
        }

        false
    }

    fn release_job_slot(&self, index: usize) {
        // Release ordering: now that we are finished using the job slot,
        // we need to guarantee that any memory operations on it are visible.
        self.running_jobs.set(index, false, Ordering::Release);
    }

    fn reserve_job_slot(&self) -> Option<usize> {
        for index in self.running_jobs.iter_zeroes() {
            if !self.running_jobs.set(index, true, Ordering::Relaxed) {
                // Acquire ordering: now that the job slot is reserved, we need to
                // guarantee that any previous operations using the same slot have finished.
                fence(Ordering::Acquire);
                return Some(index);
            }
        }

        None
    }

    fn create_job_func(f: impl Fn(usize) + Sync) -> impl Fn(JobInvocation) -> i64 {
        move |invocation| {
            let mut locally_completed = 0;
            let mut next_unit = invocation.reserved_unit;

            loop {
                if next_unit < 0 {
                    break;
                }
                else if next_unit == 0 {
                    for mask in invocation.clear_masks {
                        mask.set(invocation.slot, false, Ordering::Relaxed);
                    }
                }
                
                f(next_unit as usize);
                locally_completed += 1;

                next_unit = invocation.available_units.fetch_sub(1, Ordering::Relaxed) - 1;
            }

            locally_completed as i64
        }
    }
}

unsafe impl Send for ThreadPoolState {}
unsafe impl Sync for ThreadPoolState {}

#[derive(Debug, Default)]
struct JobSlot {
    available_units: AtomicI64,
    descriptor: UnsafeCell<*const ()>
}

struct JobDescriptor<'a> {
    clear_masks: &'a [&'a AtomicBits],
    func: &'a dyn Fn(JobInvocation) -> i64,
    incomplete_units: AtomicI64,
    search_mask: &'a AtomicBits,
}

#[derive(Debug)]
struct JobInvocation<'a> {
    available_units: &'a AtomicI64,
    clear_masks: &'a [&'a AtomicBits],
    reserved_unit: i64,
    slot: usize
}

/// Holds a shared array of `bool`s. Each value is atomically modifiable.
#[derive(Clone, Debug, Default)]
struct AtomicBits(Arc<[AtomicU64]>);

impl AtomicBits {
    /// The number of bits per [`AtomicU64`] in the backing array.
    const ELEMENT_BITS: usize = u64::BITS as usize;

    /// Creates a new bit array that can store at least `capacity` values.
    /// The actual length of the array may be larger.
    pub fn new(capacity: usize) -> Self {
        let elements = capacity.div_ceil(Self::ELEMENT_BITS);
        Self(repeat_with(AtomicU64::default).take(elements).collect())
    }

    /// Loads the value of the bit at `index`.
    /// Valid atomic orderings are [`Ordering::SeqCst`],
    /// [`Ordering::Acquire`], and [`Ordering::Relaxed`].
    pub fn get(&self, index: usize, order: Ordering) -> bool {
        let (element, bit) = Self::element_bit(index);
        let mask = 1 << bit;

        (self.0[element].load(order) & mask) != 0
    }

    /// Returns `true` if there are no values in this array.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over all `true` values in the array.
    /// This is always an [`Ordering::Relaxed`] operation, since
    /// different parts of the array may be loaded at different times.
    pub fn iter_ones(&self) -> impl Iterator<Item = usize> {
        AtomicBitsIter {
            base_index: 0,
            bits: self,
            current_value: 0,
            invert_mask: 0,
            next_element: 0
        }
    }

    /// The number of bits in the array.
    pub fn len(&self) -> usize {
        Self::ELEMENT_BITS * self.0.len()
    }

    /// Returns an iterator over all `true` values in the array.
    /// This is always an [`Ordering::Relaxed`] operation, since
    /// different parts of the array may be loaded at different times.
    pub fn iter_zeroes(&self) -> impl Iterator<Item = usize> {
        AtomicBitsIter {
            base_index: 0,
            bits: self,
            current_value: 0,
            invert_mask: u64::MAX,
            next_element: 0
        }
    }

    /// Sets the value of the bit at `index`. Returns the previous value.
    /// All atomic orderings are possible.
    pub fn set(&self, index: usize, value: bool, order: Ordering) -> bool {
        let (element, bit) = Self::element_bit(index);
        let mask = 1 << bit;
        let element = &self.0[element];

        let previous = if value {
            element.fetch_or(mask, order)
        }
        else {
            element.fetch_and(!mask, order)
        };

        (previous & mask) != 0
    }

    /// Decomposes `index` into two parts:
    /// 
    /// - The location of the [`AtomicU64`] within the backing array
    /// - The location of the bit within that number
    fn element_bit(index: usize) -> (usize, usize) {
        (index / Self::ELEMENT_BITS, index % Self::ELEMENT_BITS)
    }
}

/// Enumerates the indices of `true` values or `false` values in an [`AtomicBits`] array.
struct AtomicBitsIter<'a> {
    /// The overall index of the starting bit in [`Self::current_value`].
    base_index: usize,
    /// The array from which to load data.
    bits: &'a AtomicBits,
    /// The current group of values that was loaded.
    current_value: u64,
    /// If set to `0`, then this iterates over all `true` values in the array.
    /// If set to [`u64::MAX`], then this iterates over all `false` values.
    invert_mask: u64,
    /// The next element to load from the underlying [`AtomicU64`] array.
    next_element: usize,
}

impl Iterator for AtomicBitsIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_value == 0 {
            if self.next_element < self.bits.0.len() {
                let element = self.bits.0[self.next_element].load(Ordering::Relaxed);
                self.current_value = element ^ self.invert_mask;

                self.base_index = AtomicBits::ELEMENT_BITS * self.next_element;
                self.next_element += 1;
            }
            else {
                return None;
            }
        }
        
        let bit = self.current_value.trailing_zeros();
        self.current_value &= (u64::MAX << 1) << bit;
        Some(self.base_index + bit as usize)
    }
}