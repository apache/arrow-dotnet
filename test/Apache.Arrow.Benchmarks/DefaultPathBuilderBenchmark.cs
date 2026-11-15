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
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Benchmarks
{
    /// <summary>
    /// Builders used the way existing code uses them: no allocator, no capacity. Uses only APIs
    /// that have existed for a long time, so the same file can be run against older versions to
    /// check that the default path has not become slower.
    /// </summary>
    [MemoryDiagnoser]
    public class DefaultPathBuilderBenchmark
    {
        [Params(100, 100_000)]
        public int Count;

        private int[] _ints;
        private string[] _strings;

        [GlobalSetup]
        public void Setup()
        {
            _ints = new int[Count];
            _strings = new string[Math.Min(Count, 10_000)];
            for (int i = 0; i < _ints.Length; i++)
            {
                _ints[i] = i;
            }
            for (int i = 0; i < _strings.Length; i++)
            {
                _strings[i] = "value-" + i;
            }
        }

        [Benchmark]
        public int Int32AppendPerElement()
        {
            Int32Array.Builder builder = new Int32Array.Builder();
            for (int i = 0; i < Count; i++)
            {
                builder.Append(i);
            }
            using Int32Array array = builder.Build();
            return array.Length;
        }

        [Benchmark]
        public int Int32ReserveThenAppend()
        {
            Int32Array.Builder builder = new Int32Array.Builder();
            builder.Reserve(Count);
            for (int i = 0; i < Count; i++)
            {
                builder.Append(i);
            }
            using Int32Array array = builder.Build();
            return array.Length;
        }

        [Benchmark]
        public int Int32AppendSpan()
        {
            Int32Array.Builder builder = new Int32Array.Builder();
            builder.Append(_ints);
            using Int32Array array = builder.Build();
            return array.Length;
        }

        [Benchmark]
        public int Int32WithNulls()
        {
            Int32Array.Builder builder = new Int32Array.Builder();
            for (int i = 0; i < Count; i++)
            {
                if (i % 10 == 0)
                {
                    builder.AppendNull();
                }
                else
                {
                    builder.Append(i);
                }
            }
            using Int32Array array = builder.Build();
            return array.Length;
        }

        /// <summary>
        /// The same appends as <see cref="Int32WithNulls"/> without building, so no native
        /// memory or GC memory pressure is involved: this measures only the builder's own work.
        /// </summary>
        [Benchmark]
        public int Int32WithNullsAppendOnly()
        {
            Int32Array.Builder builder = new Int32Array.Builder();
            for (int i = 0; i < Count; i++)
            {
                if (i % 10 == 0)
                {
                    builder.AppendNull();
                }
                else
                {
                    builder.Append(i);
                }
            }
            return builder.Length;
        }

        [Benchmark]
        public int StringAppend()
        {
            StringArray.Builder builder = new StringArray.Builder();
            foreach (string s in _strings)
            {
                builder.Append(s);
            }
            using StringArray array = builder.Build();
            return array.Length;
        }

        /// <summary>
        /// Many threads growing builders at once, without building, so that no native memory or
        /// GC memory pressure is involved: this isolates contention on state shared by the
        /// builders' growth path.
        /// </summary>
        [Benchmark]
        public int ParallelGrowthOnly()
        {
            int total = 0;
            Parallel.For(0, Environment.ProcessorCount * 64, _ =>
            {
                ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>();
                for (int i = 0; i < 2000; i++)
                {
                    builder.Append(i);
                }
                System.Threading.Interlocked.Add(ref total, builder.Length);
            });
            return total;
        }

        /// <summary>
        /// Many threads building small arrays at once, to expose contention on any state shared
        /// between builders. Each build also makes a native allocation that reports GC memory
        /// pressure, which makes this case noisy.
        /// </summary>
        [Benchmark]
        public int ParallelSmallBuilds()
        {
            int total = 0;
            Parallel.For(0, Environment.ProcessorCount * 64, _ =>
            {
                Int32Array.Builder builder = new Int32Array.Builder();
                for (int i = 0; i < 100; i++)
                {
                    builder.Append(i);
                }
                using Int32Array array = builder.Build();
                System.Threading.Interlocked.Add(ref total, array.Length);
            });
            return total;
        }
    }
}
