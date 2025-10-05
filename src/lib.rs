#![warn(missing_docs)]
#![allow(warnings)]

use std::cell::UnsafeCell;
use std::hint::unreachable_unchecked;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, available_parallelism, JoinHandle};

use paralight::iter::GenericThreadPool;
use smallvec::SmallVec;

use self::join_point::*;
pub use self::thread_pool::*;
use self::util::*;

/// Internal tracking for work trees executing on a thread pool.
mod join_point;

/// The primary thread pool interface.
mod thread_pool;

/// Synchronization primitives and helper types used in the implementation.
mod util;

/// Execute [`paralight`] iterators with maximal parallelism.
/// Every iterator item may be processed on a separate thread.
/// 
/// Note: by maximizing parallelism, this also maximizes overhead.
/// This is best used with computationally-heavy iterators that have few elements.
/// For alternatives, see [`split_per`], [`split_by`], and [`split_by_threads`].
pub fn split_per_item() -> impl GenericThreadPool {
    struct SplitPerItem;

    impl GenericThreadPool for SplitPerItem {
        fn upper_bounded_pipeline<Output: Send, Accum>(
            self,
            input_len: usize,
            init: impl Fn() -> Accum + Sync,
            process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
            finalize: impl Fn(Accum) -> Output + Sync,
            reduce: impl Fn(Output, Output) -> Output,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| f.split_per_item().upper_bounded_pipeline(input_len, init, process_item, finalize, reduce, cleanup))
        }
    
        fn iter_pipeline<Output: Send>(
            self,
            input_len: usize,
            accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
            reduce: impl paralight::iter::Accumulator<Output, Output>,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| f.split_per_item().iter_pipeline(input_len, accum, reduce, cleanup))
        }
    }

    SplitPerItem
}

/// Execute [`paralight`] iterators by batching elements.
/// Each group of `chunk_size` elements may be processed by a single thread.
pub fn split_per(chunk_size: usize) -> impl GenericThreadPool {
    struct ThreadPerChunk(usize);

    impl GenericThreadPool for ThreadPerChunk {
        fn upper_bounded_pipeline<Output: Send, Accum>(
            self,
            input_len: usize,
            init: impl Fn() -> Accum + Sync,
            process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
            finalize: impl Fn(Accum) -> Output + Sync,
            reduce: impl Fn(Output, Output) -> Output,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| f.split_by(self.0).upper_bounded_pipeline(input_len, init, process_item, finalize, reduce, cleanup))
        }
    
        fn iter_pipeline<Output: Send>(
            self,
            input_len: usize,
            accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
            reduce: impl paralight::iter::Accumulator<Output, Output>,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| f.split_by(self.0).iter_pipeline(input_len, accum, reduce, cleanup))
        }
    }

    ThreadPerChunk(chunk_size)
}

/// Execute [`paralight`] iterators by batching elements.
/// Every iterator will be broken up into `chunks`
/// separate work units, which may be processed in parallel.
pub fn split_by(chunks: usize) -> impl GenericThreadPool {
    struct Chunks(usize);

    impl GenericThreadPool for Chunks {
        fn upper_bounded_pipeline<Output: Send, Accum>(
            self,
            input_len: usize,
            init: impl Fn() -> Accum + Sync,
            process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
            finalize: impl Fn(Accum) -> Output + Sync,
            reduce: impl Fn(Output, Output) -> Output,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| f.split_by(self.0).upper_bounded_pipeline(input_len, init, process_item, finalize, reduce, cleanup))
        }
    
        fn iter_pipeline<Output: Send>(
            self,
            input_len: usize,
            accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
            reduce: impl paralight::iter::Accumulator<Output, Output>,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| f.split_by(self.0).iter_pipeline(input_len, accum, reduce, cleanup))
        }
    }

    Chunks(chunks)
}

/// Execute [`paralight`] iterators by batching elements.
/// Every iterator will be broken up into `N` separate work units,
/// where `N` is the number of pool threads. Each unit may be processed in parallel.
pub fn split_by_threads() -> impl GenericThreadPool {
    struct SplitByThreads;

    impl GenericThreadPool for SplitByThreads {
        fn upper_bounded_pipeline<Output: Send, Accum>(
            self,
            input_len: usize,
            init: impl Fn() -> Accum + Sync,
            process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
            finalize: impl Fn(Accum) -> Output + Sync,
            reduce: impl Fn(Output, Output) -> Output,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| f.split_by_threads().upper_bounded_pipeline(input_len, init, process_item, finalize, reduce, cleanup))
        }
    
        fn iter_pipeline<Output: Send>(
            self,
            input_len: usize,
            accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
            reduce: impl paralight::iter::Accumulator<Output, Output>,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| f.split_by_threads().iter_pipeline(input_len, accum, reduce, cleanup))
        }
    }

    SplitByThreads
}

/// Takes two closures and *potentially* runs them in parallel. It
/// returns a pair of the results from those closures.
pub fn fork<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
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
            todo!(),
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

/*
/// Spawns an asynchronous task on the global thread pool.
/// The returned handle can be used to obtain the result.
pub fn spawn<T>(f: impl 'static + Send + FnOnce() -> T) -> Task<T> {
    todo!()
}

/// A handle to a queued group of work units, which output a single result.
pub struct Task<T>(Arc<TaskInnerImpl<T>>);

impl<T> Task<T> {
    /// Cancels this task, preventing it from running if it was not yet started.
    pub fn cancel(self) {
        todo!()
    }

    /// Whether the task has been completed yet.
    pub fn complete(&self) -> bool {
        todo!()
    }

    /// Attempts to get the result of this task if it has been completed. Otherwise, returns
    /// the original task.
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
        let join_point: &JoinPoint = todo!();
        join_point.join();

        unsafe { (*self.0.result.get()).take().unwrap_unchecked() }
    }

    /// Joins with the remaining work on this task, completing all units while they are
    /// available. Returns immediately if there are outstanding units in progress on other threads.
    pub fn join_work(&self) {
        let join_point: &JoinPoint = todo!();
        join_point.join_work();
    }
}

/*
impl<T> Future for Task<T> {
    type Output = T;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        /*unsafe {
            if self.complete() {
                Poll::Ready(self.get_result())
            } else {
                self.control.set_result_waker(cx.waker().clone());
                Poll::Pending
            }
        }*/
        todo!("Poll work and stih?")
    }
} */

trait TaskInner {
    fn set_join_point(&self, point: Option<JoinPoint>);

    unsafe fn invoke_work_unit(&self, i: usize);

    fn work_units(&self) -> usize;
}

struct TaskInnerImpl<T> {
    //root: spin::RwLock<JoinPoint>,
    /// The result of the task, if it is available.
    result: UnsafeCell<Option<T>>
}

/*
/// A structure which interally alerts a condvar upon wake.
#[derive(Clone, Default)]
struct CondvarWaker {
    /// The inner backing for the waker.
    inner: Arc<CondvarWakerInner>,
}

impl CondvarWaker {
    /// Converts this to a waker.
    pub fn as_waker(&self) -> Waker {
        unsafe {
            Waker::from_raw(Self::clone_waker(
                &self.inner as *const Arc<CondvarWakerInner> as *const (),
            ))
        }
    }

    /// Clones the waker.
    ///
    /// # Safety
    ///
    /// For this function to be sound, inner must be a valid pointer to an `Arc<CondvarWakerInner>`.
    unsafe fn clone_waker(inner: *const ()) -> RawWaker {
        unsafe {
            let value = &*(inner as *const Arc<CondvarWakerInner>);
            let data = Box::into_raw(Box::new(value.clone()));

            RawWaker::new(
                data as *const (),
                &RawWakerVTable::new(
                    Self::clone_waker,
                    Self::wake_waker,
                    Self::wake_by_ref_waker,
                    Self::drop_waker,
                ),
            )
        }
    }

    /// Wakes the waker, and consumes the pointer.
    ///
    /// # Safety
    ///
    /// For this function to be sound, inner must be a valid owned pointer to an `Arc<CondvarWakerInner>`.
    unsafe fn wake_waker(inner: *const ()) {
        Self::wake_by_ref_waker(inner);
        Self::drop_waker(inner);
    }

    /// Wakes the waker.
    ///
    /// # Safety
    ///
    /// For this function to be sound, inner must be a valid pointer to an `Arc<CondvarWakerInner>`.
    #[allow(unused_variables)]
    unsafe fn wake_by_ref_waker(inner: *const ()) {
        let inner = &*(inner as *const Arc<CondvarWakerInner>);
        let guard = inner.lock.lock().expect("Could not lock mutex");
        inner.on_wake.notify_all();
    }

    /// Drops the waker, consuming the given pointer.
    ///
    /// # Safety
    ///
    /// For this function to be sound, inner must be a valid owned pointer to an `Arc<CondvarWakerInner>`.
    unsafe fn drop_waker(inner: *const ()) {
        drop(Box::from_raw(inner as *mut Arc<CondvarWakerInner>));
    }
}

impl Deref for CondvarWaker {
    type Target = CondvarWakerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Stores the inner state for a condition variable waker.
#[derive(Default)]
struct CondvarWakerInner {
    /// The lock that should be used for waiting.
    lock: sync_impl::Mutex<()>,
    /// The condition variable that is alerted on wake.
    on_wake: sync_impl::Condvar,
} */ */

#[cfg(test)]
mod tests {
    use super::*;
    use paralight::iter::*;

    /// Tests that a parallel iterator can add things.
    #[test]
    fn test_add() {
        let len = 10_000;
        let mut output = vec![0; len];
        let left = (0..len as u64).collect::<Vec<u64>>();
        let right = (0..len as u64).collect::<Vec<u64>>();
        let expected_output = (0..len as u64).map(|x| 2 * x).collect::<Vec<u64>>();

        let output_slice = output.as_mut_slice();
        let left_slice = left.as_slice();
        let right_slice = right.as_slice();

        (
            std::hint::black_box(output_slice.par_iter_mut()),
            std::hint::black_box(left_slice).par_iter(),
            std::hint::black_box(right_slice).par_iter(),
        )
            .zip_eq()
            .with_thread_pool(crate::split_by_threads())
            .for_each(|(out, &a, &b)| *out = a + b);

        assert_eq!(output, expected_output);
    }

    /// Tests that a parallel iterator can sum things.
    #[test]
    fn test_sum() {
        let len = 10_000;
        let input = (0..len as u64).collect::<Vec<u64>>();
        let input_slice = input.as_slice();
        let result = input_slice
            .par_iter()
            .with_thread_pool(crate::split_by_threads())
            .sum::<u64>();
        assert_eq!(result, 49995000);
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
