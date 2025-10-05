# 🌊 micropool: low-latency thread pool with parallel iterator support

`micropool` is a [`rayon`](https://github.com/rayon-rs/rayon)-like thread pool implementation designed for games and other low-latency scenarios. It implements the ability to spread work across multiple CPU threads in blocking and non-blocking ways. It also has full support for [`paralight`](https://github.com/gendx/paralight)'s parallel iterators, which cleanly facilitate multithreading in a synchronous codebase. `micropool` uses a work-stealing scheduling system, but is unique in several aspects:

1. **🧵🤝 External threads participate:** when a non-pool thread is blocked on `micropool` (from calling `join` or using a parallel iterator), it will actively help complete the work.
1. **⏩✅ Forward progress is guaranteed:** any synchronous work that uses `micropool` is guaranteed to have at least one thread processing it, from the moment of creation.
1. **🎯🛡️ Scope-based work stealing:** a thread that is blocked _will only_ steal work related to its current task. Blocked threads _will never_ work-steal unrelated tasks, which might take an unpredictable amount of time to finish.
1. **🎚️⚡ Two priority tiers:** foreground work created by a blocking call is always prioritized over background tasks created via `spawn`.

The following example illustrates these properties:

```rust
println!("A {:?}", std::thread::current().id())
let background_task = micropool::spawn(|| println!("B {:?}", std::thread::current().id()));

micropool::join(|| {
    std::thread::sleep(std::time::Duration::from_millis(20));
    println!("C {:?}", std::thread::current().id())
}, || {
    println!("D {:?}", std::thread::current().id());
    micropool::join(|| {
        std::thread::sleep(std::time::Duration::from_millis(200));
        println!("E {:?}", std::thread::current().id())
    }, || {
        println!("F {:?}", std::thread::current().id());
    });
});
```

One possible output of this code might be:

```
A ThreadId(1)      // The main thread is #1
D ThreadId(2)      // Thread #2 begins helping the outer micropool::join call
C ThreadId(1)      // Thread #1 helps to finish the outer micropool::join call
F ThreadId(1)      // Thread #1 steals work from thread #2, to help complete the inner micropool::join call 
E ThreadId(2)      // Thread #2 finishes the inner micropool::join call
B ThreadId(2)      // Thread #2 grabs and completes the background task; thread #1 will *never* execute this
```

There are several key differences between `micropool`'s behavior and `rayon`, for instance:

1. The outer call to `join` occurs on an external thread. With `rayon`, this call would simply block and the main thread would wait for pool threads to finish both halves of `join`. With `micropool`, the external thread helps.
1. Because the calling thread always helps complete its work, progress on a synchronous task never stalls. If the `rayon` thread pool is saturated with tasks, the call to `join` might be long and unpredictable - the `rayon` workers would need to finish their current tasks first, even if those tasks are unrelated. CONTRAST MICROPOOL
1. 