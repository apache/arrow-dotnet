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
using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;
using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Benchmarks
{
    /// <summary>
    /// Compares the ways of filling a fixed-width builder whose final length is known.
    /// Every case builds and disposes a complete <see cref="Int32Array"/>, so allocation and
    /// zeroing are included; <see cref="FillOnly"/> isolates the cost of the writes themselves.
    /// </summary>
    [MemoryDiagnoser]
    public class FixedWidthBuilderBenchmark
    {
        [Params(100_000)]
        public int Count;

        /// <summary>
        /// "None" passes no allocator: managed working memory copied into the default allocator,
        /// as builders have always behaved. "Native" passes the default allocator explicitly,
        /// opting into the hand-over; it reports GC memory pressure and so triggers collections
        /// (see issue #261). "CallerOwned" reports none, isolating the builder's own cost.
        /// </summary>
        [Params("None", "Native", "CallerOwned")]
        public string Allocator;

        private MemoryAllocator _allocator;

        private int[] _source;
        private IMemoryOwner<byte> _scratch;

        [GlobalSetup]
        public void Setup()
        {
            _allocator = Allocator switch
            {
                "CallerOwned" => new CallerOwnedNativeAllocator(),
                "Native" => MemoryAllocator.Default.Value,
                _ => null,
            };

            _source = new int[Count];
            for (int i = 0; i < Count; i++)
            {
                _source[i] = i;
            }

            _scratch = MemoryAllocator.Default.Value.Allocate(Count * sizeof(int));
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _scratch.Dispose();
        }

        /// <summary>
        /// Lower bound: the same writes into native memory that was allocated once, up front.
        /// </summary>
        [Benchmark(Baseline = true)]
        public int FillOnly()
        {
            Span<int> span = MemoryMarshal.Cast<byte, int>(_scratch.Memory.Span);
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = i;
            }
            return span[span.Length - 1];
        }

        [Benchmark]
        public int AppendPerElement()
        {
            Int32Array.Builder builder = new Int32Array.Builder(Count, _allocator);
            for (int i = 0; i < Count; i++)
            {
                builder.Append(i);
            }
            return Finish(builder);
        }

        [Benchmark]
        public int AppendSpan()
        {
            Int32Array.Builder builder = new Int32Array.Builder(Count, _allocator);
            builder.Append(_source);
            return Finish(builder);
        }

        [Benchmark]
        public int GetSpanAdvanceWhole() => GetSpanAdvance(Count);

        [Benchmark]
        public int GetSpanAdvanceChunk1024() => GetSpanAdvance(1024);

        [Benchmark]
        public int GetSpanAdvanceChunk16() => GetSpanAdvance(16);

        [Benchmark]
        public int GetSpanAdvanceChunk1() => GetSpanAdvance(1);

        private int GetSpanAdvance(int chunk)
        {
            Int32Array.Builder builder = new Int32Array.Builder(Count, _allocator);
            int value = 0;
            while (value < Count)
            {
                int n = Math.Min(chunk, Count - value);
                Span<int> span = builder.GetSpan(n);
                for (int i = 0; i < span.Length; i++)
                {
                    span[i] = value++;
                }
                builder.Advance(n);
            }
            return Finish(builder);
        }

        private static int Finish(Int32Array.Builder builder)
        {
            // Disposing the builder matters only for the caller-owned allocator: after a
            // hand-over the builder holds a reference to the array's memory.
            using (builder)
            {
                using Int32Array array = builder.Build();
                return array.Length;
            }
        }
    }
}
