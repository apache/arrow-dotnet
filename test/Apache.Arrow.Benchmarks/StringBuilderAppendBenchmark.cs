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

using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Benchmarks
{
    [MemoryDiagnoser]
    [ShortRunJob]
    public class StringBuilderAppendBenchmark
    {
        private const int Count = 10_000;
        private string _payload;
        private string[] _values;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _payload = new string('a', 32);
            _values = new string[Count];

            for (int i = 0; i < _values.Length; i++)
            {
                _values[i] = _payload;
            }
        }

        [Benchmark]
        public int AppendSmallStrings()
        {
            var builder = new StringArray.Builder();

            for (int i = 0; i < Count; i++)
            {
                builder.Append(_payload);
            }

            using StringArray array = builder.Build();
            return array.Length;
        }

        [Benchmark]
        public int AppendRangeSmallStrings()
        {
            using StringArray array = new StringArray.Builder()
                .AppendRange(_values)
                .Build();

            return array.Length;
        }
    }
}
