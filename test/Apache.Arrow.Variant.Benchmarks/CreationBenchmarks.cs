// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Variant.Benchmarks
{
    /// <summary>
    /// Measures per-call construction cost: class (heap alloc + box) vs
    /// struct (zero alloc for numerics, inline storage).
    /// </summary>
    [MemoryDiagnoser]
    public class CreationBenchmarks
    {
        // --- Int32 ---

        [Benchmark(Baseline = true)]
        public VariantValue Class_CreateInt32() => VariantValue.FromInt32(42);

        [Benchmark]
        public StructVariantValue Struct_CreateInt32() => StructVariantValue.FromInt32(42);

        // --- Double ---

        [Benchmark]
        public VariantValue Class_CreateDouble() => VariantValue.FromDouble(3.14);

        [Benchmark]
        public StructVariantValue Struct_CreateDouble() => StructVariantValue.FromDouble(3.14);

        // --- Float ---

        [Benchmark]
        public VariantValue Class_CreateFloat() => VariantValue.FromFloat(2.71f);

        [Benchmark]
        public StructVariantValue Struct_CreateFloat() => StructVariantValue.FromFloat(2.71f);

        // --- Int64 ---

        [Benchmark]
        public VariantValue Class_CreateInt64() => VariantValue.FromInt64(123456789L);

        [Benchmark]
        public StructVariantValue Struct_CreateInt64() => StructVariantValue.FromInt64(123456789L);

        // --- String ---

        [Benchmark]
        public VariantValue Class_CreateString() => VariantValue.FromString("hello");

        [Benchmark]
        public StructVariantValue Struct_CreateString() => StructVariantValue.FromString("hello");

        // --- Boolean ---

        [Benchmark]
        public VariantValue Class_CreateBoolean() => VariantValue.FromBoolean(true);

        [Benchmark]
        public StructVariantValue Struct_CreateBoolean() => StructVariantValue.FromBoolean(true);
    }
}
