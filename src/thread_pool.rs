use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::hint::unreachable_unchecked;
use std::mem::MaybeUninit;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle, available_parallelism};

use paralight::iter::GenericThreadPool;
use smallvec::SmallVec;

use crate::join_point::*;
use crate::util::*;
use crate::{Task, TaskInner};

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
            ThreadPoolBuilder::default().build()
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
            ..Default::default()
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
        assert!(
            JoinPoint::current().is_none(),
            "Attempted to enter pool from within another context."
        );

        let _guard = PanicGuard("Panic was not caught at ThreadPool install boundary; aborting.");
        let previous = LOCAL_POOL.get();
        LOCAL_POOL.set(self);
        let result = f();
        LOCAL_POOL.set(previous);
        result
    }

    /// Spawns an asynchronous task on the global thread pool.
    /// The returned handle can be used to obtain the result.
    pub fn spawn<T: 'static + Send>(&self, f: impl 'static + FnOnce() -> T + Send) -> Task<T> {
        Task::spawn(self.state, f)
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

            JoinPoint::invoke(
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
        self.split_by(self.num_threads())
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.state.should_stop.store(true, Ordering::Release);
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
        process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
        finalize: impl Fn(Accum) -> Output + Sync,
        reduce: impl Fn(Output, Output) -> Output,
        _: &(impl paralight::iter::SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let mut output = SmallVec::<[Output; ThreadPool::OUTPUT_BUFFER_CAPACITY]>::new();
            output.reserve_exact(input_len);
            let output_buffer = output.as_mut_ptr();

            JoinPoint::invoke(
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

    fn iter_pipeline<Output: Send>(
        self,
        input_len: usize,
        accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
        reduce: impl paralight::iter::Accumulator<Output, Output>,
        _: &(impl paralight::iter::SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let mut output = SmallVec::<[Output; ThreadPool::OUTPUT_BUFFER_CAPACITY]>::new();
            output.reserve_exact(input_len);
            let output_buffer = output.as_mut_ptr();

            JoinPoint::invoke(
                self.0.state,
                |i| {
                    output_buffer.add(i).write(accum.accumulate(i..i + 1));
                },
                input_len,
            );

            output.set_len(input_len);
            reduce.accumulate(output.into_iter())
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
        process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
        finalize: impl Fn(Accum) -> Output + Sync,
        reduce: impl Fn(Output, Output) -> Output,
        _: &(impl paralight::iter::SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let (chunk_size, work_units) = (self.chunk_units_calculator)(input_len);

            let mut output = SmallVec::<[Output; ThreadPool::OUTPUT_BUFFER_CAPACITY]>::new();
            output.reserve_exact(work_units);
            let output_buffer = output.as_mut_ptr();

            JoinPoint::invoke(
                self.pool.state,
                |i| {
                    let start = chunk_size * i;
                    let end = (start + chunk_size).min(input_len);

                    let mut accumulator = init();

                    for j in start..end {
                        accumulator = match process_item(accumulator, j) {
                            ControlFlow::Break(x) | ControlFlow::Continue(x) => x,
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

    fn iter_pipeline<Output: Send>(
        self,
        input_len: usize,
        accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
        reduce: impl paralight::iter::Accumulator<Output, Output>,
        _: &(impl paralight::iter::SourceCleanup + Sync),
    ) -> Output {
        unsafe {
            let (chunk_size, work_units) = (self.chunk_units_calculator)(input_len);

            let mut output = SmallVec::<[Output; ThreadPool::OUTPUT_BUFFER_CAPACITY]>::new();
            output.reserve_exact(work_units);
            let output_buffer = output.as_mut_ptr();

            JoinPoint::invoke(
                self.pool.state,
                |i| {
                    let start = chunk_size * i;
                    let end = (start + chunk_size).min(input_len);
                    output_buffer.add(i).write(accum.accumulate(start..end));
                },
                work_units,
            );

            output.set_len(work_units);
            reduce.accumulate(output.into_iter())
        }
    }
}

/// Stores the inner state for a [`ThreadPool`] and coordinates work across
/// multiple threads.
#[derive(Default)]
pub(crate) struct ThreadPoolState {
    /// Threads waiting for work will spin for at least this many cycles before
    /// sleeping.
    pub idle_spin_cycles: usize,
    /// An event that is invoked whenever new work is available.
    pub on_change: Event,
    /// Join points where pool threads should look for work.
    pub roots: spin::RwLock<Vec<JoinPoint>>,
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
            roots: spin::RwLock::new(Vec::new()),
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
        self.tasks.lock().push_back(task);
        self.on_change.notify();
    }

    /// Polls for available work on the thread pool, and goes to sleep if none
    /// is available.
    fn join(&self) {
        assert!(
            JoinPoint::current().is_none(),
            "Attempted to enter pool from within another context"
        );

        let mut spin_before_sleep = false;

        loop {
            let listener = self.on_change.listen();
            let item = JoinPoint::select_work_unit_from(&self.roots.read());

            if let Some((point, i)) = item {
                spin_before_sleep = true;
                JoinPoint::set_current(Some(point.clone()));
                unsafe {
                    point.invoke_work_unit(i);
                }
                point.join_work();
                JoinPoint::set_current(None);
            } else if self.should_stop.load(Ordering::Acquire) {
                return;
            } else if let Some(task) = self.pop_task() {
                JoinPoint::join_task(&*task, true);
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
}

unsafe impl Send for ThreadPoolState {}
unsafe impl Sync for ThreadPoolState {}
