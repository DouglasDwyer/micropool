# 🌊 micropool: low-latency thread pool with parallel iterator support

[![Crates.io](https://img.shields.io/crates/v/micropool.svg)](https://crates.io/crates/micropool)
[![Docs.rs](https://docs.rs/micropool/badge.svg)](https://docs.rs/micropool)

`micropool` is a [`rayon`](https://github.com/rayon-rs/rayon)-style thread pool designed for games and other low-latency scenarios. It implements the ability to spread work across multiple CPU threads in blocking and non-blocking ways. It also has full support for [`paralight`](https://github.com/gendx/paralight)'s parallel iterators, which cleanly facilitate multithreading in a synchronous codebase. `micropool` uses a work-stealing scheduling system, but is unique in several aspects:

1. **🧵🤝 External threads participate:** when a non-pool thread is blocked on `micropool` (from calling `join` or using a parallel iterator), it will actively help complete the work. This eliminates the overhead of a context switch.
1. **⏩✅ Forward progress is guaranteed:** any synchronous work that uses `micropool` is guaranteed to have at least one thread processing it, from the moment of creation.
1. **🎯🛡️ Scope-based work stealing:** a thread that is blocked _will only_ steal work related to its current task. Blocked threads _will never_ steal unrelated work, which might take an unpredictable amount of time to finish.
1. **🎚️⚡ Two priority tiers:** foreground work created by a blocking call is always prioritized over background tasks created via `spawn`.
1. **🔄💤 Spinning before sleeping:** threads will spin for a configurable interval before sleeping with the operating system scheduler.

### Usage

Parallel iterators allow for splitting common list operations across multiple threads. `micropool` re-exports the [`paralight`](https://github.com/gendx/paralight) library:

```rust
use micropool::iter::*;

let len = 10_000;
let input = (0..len as u64).collect::<Vec<u64>>();
let input_slice = input.as_slice();
let result = input_slice
    .par_iter()
    .with_thread_pool(micropool::split_by_threads())
    .sum::<u64>();

assert_eq!(result, 49995000);
```

The `.with_thread_pool` line specifies that the current `micropool` instance should be used, and `split_by_threads` indicates that each pool thread should process an equal-sized chunk of the data. Other data-splitting strategies available are `split_by`, `split_per_item`, and `split_per`.

### Scheduling system

The following example illustrates the properties of the `micropool` scheduling system:

```rust
println!("A {:?}", std::thread::current().id());
let background_task = micropool::spawn(|| println!("B {:?}", std::thread::current().id()));

micropool::join(|| {
    std::thread::sleep(std::time::Duration::from_millis(20));
    println!("C {:?}", std::thread::current().id())
}, || {
    println!("D {:?}", std::thread::current().id());
    micropool::join(|| {
        std::thread::sleep(std::time::Duration::from_millis(200));
        println!("E {:?}", std::thread::current().id());
    }, || {
        println!("F {:?}", std::thread::current().id());
    });
});
```

One possible output of this code might be:

```text
A ThreadId(1)      // The main thread is #1
D ThreadId(2)      // Thread #2 begins helping the outer micropool::join call
C ThreadId(1)      // Thread #1 helps to finish the outer micropool::join call
F ThreadId(1)      // Thread #1 steals work from thread #2, to help complete the inner micropool::join call 
E ThreadId(2)      // Thread #2 finishes the inner micropool::join call
B ThreadId(2)      // Thread #2 grabs and completes the background task; thread #1 will *never* execute this
```

There are several key differences between `micropool`'s behavior and `rayon`, for instance:

1. The outer call to `join` occurs on an external thread. With `rayon`, this call would simply block and the main thread would wait for pool threads to finish both halves of `join`. With `micropool`, the external thread helps.
1. Because the calling thread always helps complete its work, progress on a synchronous task never stalls. In contrast, if the `rayon` thread pool is saturated with tasks, the call to `join` might be long and unpredictable - the `rayon` workers would need to finish their current tasks first, even if those tasks are unrelated.
1. When the external thread finishes its work, and is blocking on the result of `join`, there is other work available: the `background_task`. In this case, completion of `background_task` is not required for `join` to return. As such, the external thread will **never** run it. In contrast, if a `rayon` thread is blocked, it may run unrelated work in the meantime, so it may take a long/unpredictable amount of time before control flow returns from the `join`.
1. Worker threads will always help with synchronous work (like `join`) before processing asynchronous tasks created via `spawn`. This natural separation of foreground and background work ensures that the most important foreground tasks - like per-frame rendering or physics in a agame engine - happen first.
1.  [According to Dennis Gustafsson, workers that spin while waiting for new tasks sometimes perform better than workers that sleep.](https://www.youtube.com/watch?v=Kvsvd67XUKw) When many short tasks are scheduled, the overhead of operating system calls for sleeping can outweight the wasted compute. `micropool` compensates for this by spinning threads before they sleep.