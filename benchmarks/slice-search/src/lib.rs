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

/// Power-of-two slice lengths evaluated by the benchmarks.
pub const LENGTHS: [usize; 11] = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];

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
pub fn generate_even_strings(len: usize) -> (Vec<String>, String) {
    let data: Vec<String> = (0..len).map(|i| format!("item_{:04}", i * 2)).collect();
    let midpoint = len / 2;
    let target = format!("item_{:04}", midpoint * 2 + 1);

    (data, target)
}
