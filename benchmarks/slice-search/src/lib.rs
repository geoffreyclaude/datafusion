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

pub mod branchless;

/// Slice lengths up to 256 for i8 benchmarks (extended range for cutoff analysis).
pub const LENGTHS_256: [usize; 55] = [
    // Every value 2-31 for fine-grained crossover analysis (skip 1, useless)
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, // Even values 32-64
    32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64,
    // Intermediate sizes up to 256
    80, 96, 112, 128, 160, 192, 224, 256,
];

/// Slice lengths up to 128 for i16 benchmarks.
pub const LENGTHS_128: [usize; 51] = [
    // Every value 2-31 for fine-grained crossover analysis (skip 1, useless)
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, // Even values 32-64
    32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64,
    // Intermediate sizes up to 128
    80, 96, 112, 128,
];

/// Slice lengths up to 64 for i32/i64 benchmarks.
pub const LENGTHS_64: [usize; 47] = [
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58,
    60, 62, 64,
];

/// Slice lengths up to 16 for i128/str benchmarks.
pub const LENGTHS_16: [usize; 15] = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

/// Builds an even-numbered sequence with a missing midpoint value to place the
/// target between two central elements for a worst-case binary search path.
///
/// The returned vector contains the values `[0, 2, 4, ...]`, and the target is
/// the midpoint value plus one, so the lookup explores the deepest part of the
/// search tree while guaranteed to miss.
pub fn generate_numeric_even<T, F>(len: usize, to_type: F) -> (Vec<T>, T)
where
    T: Ord + Clone,
    F: Fn(usize) -> T,
{
    let data: Vec<T> = (0..len).map(|i| to_type(i * 2)).collect();
    let midpoint = len / 2;
    let target = to_type(midpoint * 2 + 1);

    (data, target)
}

/// Builds an even-numbered string sequence with a missing midpoint value to
/// force the binary search down its longest path.
/// Strings are 32+ characters to be representative of real-world data.
/// The unique number is at the end to force full string comparison.
pub fn generate_even_strings(len: usize) -> (Vec<String>, String) {
    // Format: "item_padding_________________{:08}" (32 chars total)
    let data: Vec<String> = (0..len)
        .map(|i| format!("item_padding_________________{:08}", i * 2))
        .collect();
    let midpoint = len / 2;
    let target = format!("item_padding_________________{:08}", midpoint * 2 + 1);

    (data, target)
}
