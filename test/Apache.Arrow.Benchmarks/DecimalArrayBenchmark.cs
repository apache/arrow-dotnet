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
using System.Data.SqlTypes;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Benchmarks
{
    [MemoryDiagnoser]
    public class DecimalArrayBenchmark
    {
        [Params(10_000)]
        public int Count { get; set; }

        private Decimal128Array _decimal128LowScale;
        private Decimal128Array _decimal128HighScale;
        private Decimal256Array _decimal256LowScale;
        private Decimal256Array _decimal256HighScale;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var random = new Random(42);

            _decimal128LowScale = BuildDecimal128Array(new Decimal128Type(14, 4), random);
            _decimal128HighScale = BuildDecimal128Array(new Decimal128Type(38, 20), random);
            _decimal256LowScale = BuildDecimal256Array(new Decimal256Type(14, 4), random);
            _decimal256HighScale = BuildDecimal256Array(new Decimal256Type(76, 38), random);
        }

        private Decimal128Array BuildDecimal128Array(Decimal128Type type, Random random)
        {
            var builder = new Decimal128Array.Builder(type);
            for (int i = 0; i < Count; i++)
            {
                builder.Append((decimal)Math.Round(random.NextDouble() * 10000, Math.Min(type.Scale, 10)));
            }
            return builder.Build();
        }

        private Decimal256Array BuildDecimal256Array(Decimal256Type type, Random random)
        {
            var builder = new Decimal256Array.Builder(type);
            for (int i = 0; i < Count; i++)
            {
                builder.Append((decimal)Math.Round(random.NextDouble() * 10000, Math.Min(type.Scale, 10)));
            }
            return builder.Build();
        }

        [Benchmark]
        public decimal? Decimal128_GetValue_LowScale()
        {
            decimal? sum = 0;
            for (int i = 0; i < _decimal128LowScale.Length; i++)
            {
                sum += _decimal128LowScale.GetValue(i);
            }
            return sum;
        }

        [Benchmark]
        public decimal? Decimal128_GetValue_HighScale()
        {
            decimal? sum = 0;
            for (int i = 0; i < _decimal128HighScale.Length; i++)
            {
                sum += _decimal128HighScale.GetValue(i);
            }
            return sum;
        }

        [Benchmark]
        public decimal? Decimal256_GetValue_LowScale()
        {
            decimal? sum = 0;
            for (int i = 0; i < _decimal256LowScale.Length; i++)
            {
                sum += _decimal256LowScale.GetValue(i);
            }
            return sum;
        }

        [Benchmark]
        public decimal? Decimal256_GetValue_HighScale()
        {
            decimal? sum = 0;
            for (int i = 0; i < _decimal256HighScale.Length; i++)
            {
                sum += _decimal256HighScale.GetValue(i);
            }
            return sum;
        }

        [Benchmark]
        public SqlDecimal? Decimal128_GetSqlDecimal()
        {
            SqlDecimal? sum = 0;
            for (int i = 0; i < _decimal128LowScale.Length; i++)
            {
                sum += _decimal128LowScale.GetSqlDecimal(i);
            }
            return sum;
        }

        [Benchmark]
        public string Decimal256_GetString_HighScale()
        {
            string last = null;
            for (int i = 0; i < _decimal256HighScale.Length; i++)
            {
                last = _decimal256HighScale.GetString(i);
            }
            return last;
        }
    }
}
