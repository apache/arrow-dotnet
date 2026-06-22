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
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Compute;
using Xunit;

namespace Apache.Arrow.Compute.Tests
{
    public class AggregationsTests
    {
        private static DoubleArray Doubles(params double?[] values)
        {
            var builder = new DoubleArray.Builder();
            foreach (double? v in values)
            {
                if (v.HasValue) builder.Append(v.Value);
                else builder.AppendNull();
            }
            return builder.Build();
        }

        private static Int32Array Ints(params int?[] values)
        {
            var builder = new Int32Array.Builder();
            foreach (int? v in values)
            {
                if (v.HasValue) builder.Append(v.Value);
                else builder.AppendNull();
            }
            return builder.Build();
        }

        [Fact]
        public void Sum_Int32_NoNulls()
        {
            Assert.Equal(10, Ints(1, 2, 3, 4).Sum());
        }

        [Fact]
        public void Sum_Int32_WithNulls()
        {
            Assert.Equal(4, Ints(1, null, 3).Sum());
        }

        [Fact]
        public void Sum_Double_NoNulls()
        {
            Assert.Equal(6.5, Doubles(1.0, 2.0, 3.5).Sum(), 6);
        }

        [Fact]
        public void Min_Max_Double_NoNulls()
        {
            var a = Doubles(3.0, -1.0, 7.5, 2.0);
            Assert.Equal(-1.0, a.Min(), 6);
            Assert.Equal(7.5, a.Max(), 6);
        }

        [Fact]
        public void Min_Max_WithNulls_IgnoresNulls()
        {
            var a = Doubles(null, 5.0, null, 2.0, 9.0);
            Assert.Equal(2.0, a.Min(), 6);
            Assert.Equal(9.0, a.Max(), 6);
        }

        [Fact]
        public void Mean_Double_WithNulls_DividesByNonNullCount()
        {
            // (2 + 4) / 2 = 3, the null is excluded from both sum and count.
            Assert.Equal(3.0, Doubles(2.0, null, 4.0).Mean(), 6);
        }

        [Fact]
        public void Mean_Int32_ReturnsDouble()
        {
            Assert.Equal(2.5, Ints(1, 2, 3, 4).Mean(), 6);
        }

        [Fact]
        public void SingleElement()
        {
            Assert.Equal(42.0, Doubles(42.0).Sum(), 6);
            Assert.Equal(42.0, Doubles(42.0).Min(), 6);
            Assert.Equal(42.0, Doubles(42.0).Max(), 6);
            Assert.Equal(42.0, Doubles(42.0).Mean(), 6);
        }

        [Fact]
        public void Empty_Sum_IsZero_ButMinMaxMeanThrow()
        {
            var empty = Doubles();
            Assert.Equal(0.0, empty.Sum(), 6);
            Assert.Throws<InvalidOperationException>(() => empty.Min());
            Assert.Throws<InvalidOperationException>(() => empty.Max());
            Assert.Throws<InvalidOperationException>(() => empty.Mean());
        }

        [Fact]
        public void AllNull_MinMaxMeanThrow()
        {
            var allNull = Doubles(null, null, null);
            Assert.Throws<InvalidOperationException>(() => allNull.Min());
            Assert.Throws<InvalidOperationException>(() => allNull.Max());
            Assert.Throws<InvalidOperationException>(() => allNull.Mean());
        }

        [Fact]
        public void Large_FastPath_MatchesScalar()
        {
            const int n = 1_000_000;
            var rng = new Random(17);
            double[] data = Enumerable.Range(0, n).Select(_ => rng.NextDouble() * 100.0).ToArray();

            var builder = new DoubleArray.Builder();
            builder.Append(data.AsSpan());
            DoubleArray array = builder.Build();

            double scalar = 0.0;
            for (int i = 0; i < n; i++) scalar += data[i];

            // Fast (TensorPrimitives) path; allow small floating-point reorder tolerance.
            Assert.Equal(scalar, array.Sum(), 3);
            Assert.Equal(data.Min(), array.Min(), 9);
            Assert.Equal(data.Max(), array.Max(), 9);
        }
    }
}
