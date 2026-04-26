// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace Apache.Arrow.Benchmarks
{
    [MemoryDiagnoser]
    [ShortRunJob]
    public class StringArrayGetStringBenchmark
    {
        private StringArray _array;
        private StringArray _slice;

        [Params(1_024)]
        public int Count { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            var builder = new StringArray.Builder();
            builder.Append("prefix");

            for (int i = 0; i < Count; i++)
            {
                if ((i & 7) == 0)
                {
                    builder.AppendNull();
                }
                else if ((i & 7) == 1)
                {
                    builder.Append(string.Empty);
                }
                else
                {
                    builder.Append($"value-{i:0000}-payload");
                }
            }

            builder.Append("suffix");

            _array = builder.Build();
            _slice = (StringArray)_array.Slice(1, Count);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _slice.Dispose();
            _array.Dispose();
        }

        [Benchmark(Baseline = true)]
        public int LegacyGetString()
        {
            int totalLength = 0;
            for (int i = 0; i < _array.Length; i++)
            {
                totalLength += GetStringLegacy(_array, i)?.Length ?? 0;
            }

            return totalLength;
        }

        [Benchmark]
        public int GetString()
        {
            int totalLength = 0;
            for (int i = 0; i < _array.Length; i++)
            {
                totalLength += _array.GetString(i)?.Length ?? 0;
            }

            return totalLength;
        }

        [Benchmark]
        public int LegacyGetStringFromSlice()
        {
            int totalLength = 0;
            for (int i = 0; i < _slice.Length; i++)
            {
                totalLength += GetStringLegacy(_slice, i)?.Length ?? 0;
            }

            return totalLength;
        }

        [Benchmark]
        public int GetStringFromSlice()
        {
            int totalLength = 0;
            for (int i = 0; i < _slice.Length; i++)
            {
                totalLength += _slice.GetString(i)?.Length ?? 0;
            }

            return totalLength;
        }

        private static string GetStringLegacy(StringArray array, int index)
        {
            ReadOnlySpan<byte> bytes = array.GetBytes(index, out bool isNull);

            if (isNull)
            {
                return null;
            }

            if (bytes.Length == 0)
            {
                return string.Empty;
            }

            return Encoding.UTF8.GetString(bytes);
        }
    }
}
