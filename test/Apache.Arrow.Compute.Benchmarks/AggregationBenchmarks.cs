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

using System;
using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Compute.Benchmarks
{
    /// <summary>
    /// Compares the SIMD-accelerated aggregation kernels (which dispatch to TensorPrimitives on the
    /// null-free fast path) against a straightforward managed scalar loop over the same Arrow array.
    /// This is the evidence that the fast path earns its place.
    /// </summary>
    [MemoryDiagnoser]
    public class AggregationBenchmarks
    {
        [Params(1024, 1_000_000)]
        public int Size { get; set; }

        private DoubleArray _values = null!;

        [GlobalSetup]
        public void Setup()
        {
            var builder = new DoubleArray.Builder();
            builder.Reserve(Size);
            var rng = new Random(17);
            for (int i = 0; i < Size; i++)
            {
                builder.Append(rng.NextDouble() * 1000.0);
            }
            _values = builder.Build();
        }

        // SIMD fast path (null-free => TensorPrimitives).
        [Benchmark(Baseline = true)]
        public double Sum_Kernel() => _values.Sum();

        // Straightforward managed scalar loop over the same values buffer.
        [Benchmark]
        public double Sum_NaiveScalar()
        {
            ReadOnlySpan<double> values = _values.Values;
            double acc = 0d;
            for (int i = 0; i < values.Length; i++)
            {
                acc += values[i];
            }
            return acc;
        }

        [Benchmark]
        public double Min_Kernel() => _values.Min();

        [Benchmark]
        public double Min_NaiveScalar()
        {
            ReadOnlySpan<double> values = _values.Values;
            double min = values[0];
            for (int i = 1; i < values.Length; i++)
            {
                if (values[i] < min) min = values[i];
            }
            return min;
        }

        [Benchmark]
        public double Mean_Kernel() => _values.Mean();
    }
}
