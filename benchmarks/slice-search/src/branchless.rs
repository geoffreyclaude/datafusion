// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Const-generic branchless filter for membership testing.
//!
//! The key optimization is using const generics so the compiler knows the exact
//! array size at compile time, enabling full loop unrolling and SIMD codegen.
//!
//! The API is batch-oriented: dispatch once on haystack size (match overhead),
//! then process thousands of needles with the fully-optimized monomorphic code.
//! This mirrors production IN LIST processing where dispatch cost is amortized.

use seq_macro::seq;

/// Branchless membership check using bitwise OR fold.
/// With const N, the compiler fully unrolls this loop.
#[inline(always)]
fn branchless_check<T: Copy + PartialEq, const N: usize>(
    haystack: &[T; N],
    needle: T,
) -> bool {
    haystack.iter().fold(false, |acc, &v| acc | (v == needle))
}

/// Batch branchless membership check.
/// Checks each needle against the haystack and counts matches.
/// This is representative of production IN LIST processing.
#[inline(always)]
fn branchless_batch<T: Copy + PartialEq, const N: usize>(
    haystack: &[T; N],
    needles: &[T],
) -> usize {
    needles
        .iter()
        .filter(|&&needle| branchless_check(haystack, needle))
        .count()
}

/// Batch branchless lookup: checks all needles against a haystack.
/// Returns the count of matches (to prevent dead code elimination).
///
/// This avoids vtable dispatch by selecting the const-generic instantiation
/// via match, then processing the entire batch with the monomorphic function.
pub fn branchless_batch_lookup<T: Copy + PartialEq + Default>(
    haystack: &[T],
    needles: &[T],
) -> Option<usize> {
    macro_rules! dispatch {
        ($n:literal) => {{
            let mut arr = [T::default(); $n];
            arr.copy_from_slice(haystack);
            Some(branchless_batch(&arr, needles))
        }};
    }

    seq!(N in 2..=64 {
        match haystack.len() {
            #( N => dispatch!(N), )*
            // Intermediate sizes up to 256
            80 => dispatch!(80),
            96 => dispatch!(96),
            112 => dispatch!(112),
            128 => dispatch!(128),
            160 => dispatch!(160),
            192 => dispatch!(192),
            224 => dispatch!(224),
            256 => dispatch!(256),
            _ => None,
        }
    })
}
