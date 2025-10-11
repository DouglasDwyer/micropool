#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

pub use paralight::iter;
use paralight::iter::GenericThreadPool;

pub use self::task::*;
pub use self::thread_pool::*;

/// Internal tracking for work trees executing on a thread pool.
mod join_point;

/// Implementation of tasks for the [`spawn`] API.
mod task;

/// The primary thread pool interface.
mod thread_pool;

/// Synchronization primitives and helper types used in the implementation.
mod util;

/// Execute [`paralight`] iterators with maximal parallelism.
/// Every iterator item may be processed on a separate thread.
///
/// Note: by maximizing parallelism, this also maximizes overhead.
/// This is best used with computationally-heavy iterators that have few
/// elements. For alternatives, see [`split_per`], [`split_by`], and
/// [`split_by_threads`].
pub fn split_per_item() -> impl GenericThreadPool {
    struct SplitPerItem;

    unsafe impl GenericThreadPool for SplitPerItem {
        fn upper_bounded_pipeline<Output: Send, Accum>(
            self,
            input_len: usize,
            init: impl Fn() -> Accum + Sync,
            process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
            finalize: impl Fn(Accum) -> Output + Sync,
            reduce: impl Fn(Output, Output) -> Output,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| {
                f.split_per_item().upper_bounded_pipeline(
                    input_len,
                    init,
                    process_item,
                    finalize,
                    reduce,
                    cleanup,
                )
            })
        }

        fn iter_pipeline<Output: Send>(
            self,
            input_len: usize,
            accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
            reduce: impl paralight::iter::Accumulator<Output, Output>,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| {
                f.split_per_item()
                    .iter_pipeline(input_len, accum, reduce, cleanup)
            })
        }
    }

    SplitPerItem
}

/// Execute [`paralight`] iterators by batching elements.
/// Each group of `chunk_size` elements may be processed by a single thread.
pub fn split_per(chunk_size: usize) -> impl GenericThreadPool {
    struct ThreadPerChunk(usize);

    unsafe impl GenericThreadPool for ThreadPerChunk {
        fn upper_bounded_pipeline<Output: Send, Accum>(
            self,
            input_len: usize,
            init: impl Fn() -> Accum + Sync,
            process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
            finalize: impl Fn(Accum) -> Output + Sync,
            reduce: impl Fn(Output, Output) -> Output,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| {
                f.split_by(self.0).upper_bounded_pipeline(
                    input_len,
                    init,
                    process_item,
                    finalize,
                    reduce,
                    cleanup,
                )
            })
        }

        fn iter_pipeline<Output: Send>(
            self,
            input_len: usize,
            accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
            reduce: impl paralight::iter::Accumulator<Output, Output>,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| {
                f.split_by(self.0)
                    .iter_pipeline(input_len, accum, reduce, cleanup)
            })
        }
    }

    ThreadPerChunk(chunk_size)
}

/// Execute [`paralight`] iterators by batching elements.
/// Every iterator will be broken up into `chunks`
/// separate work units, which may be processed in parallel.
pub fn split_by(chunks: usize) -> impl GenericThreadPool {
    struct Chunks(usize);

    unsafe impl GenericThreadPool for Chunks {
        fn upper_bounded_pipeline<Output: Send, Accum>(
            self,
            input_len: usize,
            init: impl Fn() -> Accum + Sync,
            process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
            finalize: impl Fn(Accum) -> Output + Sync,
            reduce: impl Fn(Output, Output) -> Output,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| {
                f.split_by(self.0).upper_bounded_pipeline(
                    input_len,
                    init,
                    process_item,
                    finalize,
                    reduce,
                    cleanup,
                )
            })
        }

        fn iter_pipeline<Output: Send>(
            self,
            input_len: usize,
            accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
            reduce: impl paralight::iter::Accumulator<Output, Output>,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| {
                f.split_by(self.0)
                    .iter_pipeline(input_len, accum, reduce, cleanup)
            })
        }
    }

    Chunks(chunks)
}

/// Execute [`paralight`] iterators by batching elements.
/// Every iterator will be broken up into `N` separate work units,
/// where `N` is the number of pool threads. Each unit may be processed in
/// parallel.
pub fn split_by_threads() -> impl GenericThreadPool {
    struct SplitByThreads;

    unsafe impl GenericThreadPool for SplitByThreads {
        fn upper_bounded_pipeline<Output: Send, Accum>(
            self,
            input_len: usize,
            init: impl Fn() -> Accum + Sync,
            process_item: impl Fn(Accum, usize) -> std::ops::ControlFlow<Accum, Accum> + Sync,
            finalize: impl Fn(Accum) -> Output + Sync,
            reduce: impl Fn(Output, Output) -> Output,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| {
                f.split_by_threads().upper_bounded_pipeline(
                    input_len,
                    init,
                    process_item,
                    finalize,
                    reduce,
                    cleanup,
                )
            })
        }

        fn iter_pipeline<Output: Send>(
            self,
            input_len: usize,
            accum: impl paralight::iter::Accumulator<usize, Output> + Sync,
            reduce: impl paralight::iter::Accumulator<Output, Output>,
            cleanup: &(impl paralight::iter::SourceCleanup + Sync),
        ) -> Output {
            ThreadPool::with_current(|f| {
                f.split_by_threads()
                    .iter_pipeline(input_len, accum, reduce, cleanup)
            })
        }
    }

    SplitByThreads
}

/// Takes two closures and *potentially* runs them in parallel. It
/// returns a pair of the results from those closures.
pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    ThreadPool::with_current(|pool| pool.join(oper_a, oper_b))
}

/// Spawns an asynchronous task on the global thread pool.
/// The returned handle can be used to obtain the result.
pub fn spawn<T: 'static + Send>(f: impl 'static + Send + FnOnce() -> T) -> Task<T> {
    ThreadPool::with_current(|pool| pool.spawn(f))
}

/// Tests for `micropool`.
#[cfg(test)]
mod tests {
    use crate::iter::*;

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

    /// Tests a simple for each loop.
    #[test]
    fn test_for_each() {
        let mut result = [0; 5];
        (result.par_iter_mut(), (1..=5).into_par_iter())
            .zip_eq()
            .with_thread_pool(crate::split_by_threads())
            .for_each(|(out, x)| *out = x * x - 1);
        assert_eq!([0, 3, 8, 15, 24], result);
    }

    /// Spawns and joins many tasks.
    #[test]
    fn execute_many() {
        let first_task = crate::spawn(|| 2);
        let second_task = crate::spawn(|| 2);
        assert_eq!(first_task.join(), second_task.join());

        for _ in 0..1000 {
            let third_task = crate::spawn(|| std::thread::sleep(std::time::Duration::new(0, 10)));
            let fourth_task = crate::spawn(|| std::thread::sleep(std::time::Duration::new(0, 200)));
            assert_eq!(third_task.join(), fourth_task.join());
        }
    }
}
