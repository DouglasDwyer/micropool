// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::mem::size_of;

//const NUM_THREADS: &[usize] = &[1, 2, 4, 8];
//const LENGTHS: &[usize] = &[10_000, 100_000, 1_000_000, 10_000_000];

const NUM_THREADS: &[usize] = &[6];
const LENGTHS: &[usize] = &[1_000_000];

fn sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum");
    for len in LENGTHS {
        group.throughput(Throughput::Bytes((len * size_of::<u64>()) as u64));
        for &num_threads in NUM_THREADS {
            group.bench_with_input(
                BenchmarkId::new(format!("micropool@{num_threads}"), len),
                len,
                |bencher, len| micropool::sum(bencher, num_threads, len),
            );
        }
    }
    group.finish();
}

fn add(c: &mut Criterion) {
    let mut group = c.benchmark_group("add");
    for len in LENGTHS {
        group.throughput(Throughput::Bytes((len * size_of::<u64>()) as u64));
        for &num_threads in NUM_THREADS {
            group.bench_with_input(
                BenchmarkId::new(format!("micropool@{num_threads}"), len),
                len,
                |bencher, len| micropool::add(bencher, num_threads, len),
            );
        }
    }
    group.finish();
}

/// Benchmarks using Paralight.
mod micropool {
    use criterion::Bencher;
    use paralight::iter::*;
    use std::hint::black_box;

    pub fn sum(bencher: &mut Bencher, num_threads: usize, len: &usize) {
        micropool::ThreadPoolBuilder::default()
            .num_threads(num_threads - 1)
            .build()
            .install(|| {
                let input = (0..*len as u64).collect::<Vec<u64>>();
                let input_slice = input.as_slice();

                bencher.iter(|| {
                    black_box(input_slice)
                        .par_iter()
                        .with_thread_pool(micropool::split_by_threads())
                        .sum::<u64>()
                });
            });
    }

    pub fn add(bencher: &mut Bencher, num_threads: usize, len: &usize) {
        micropool::ThreadPoolBuilder::default()
            .num_threads(num_threads - 1)
            .build()
            .install(|| {
                let mut output = vec![0; *len];
                let left = (0..*len as u64).collect::<Vec<u64>>();
                let right = (0..*len as u64).collect::<Vec<u64>>();

                let output_slice = output.as_mut_slice();
                let left_slice = left.as_slice();
                let right_slice = right.as_slice();

                bencher.iter(|| {
                    (
                        black_box(output_slice.par_iter_mut()),
                        black_box(left_slice).par_iter(),
                        black_box(right_slice).par_iter(),
                    )
                        .zip_eq()
                        .with_thread_pool(micropool::split_by_threads())
                        .for_each(|(out, &a, &b)| *out = a + b)
                });
            });
    }
}

criterion_group!(benches, sum, add);
criterion_main!(benches);
