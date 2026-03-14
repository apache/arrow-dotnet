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
using System.Collections.Generic;
using Xunit;

namespace Apache.Arrow.Variant.Tests
{
    public class VariantRoundTripTests
    {
        private readonly VariantBuilder _builder = new VariantBuilder();

        private VariantValue RoundTrip(VariantValue original)
        {
            (byte[] metadata, byte[] value) = _builder.Encode(original);
            VariantReader reader = new VariantReader(metadata, value);
            return reader.ToVariantValue();
        }

        // ---------------------------------------------------------------
        // Primitives
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_Null() =>
            Assert.Equal(VariantValue.Null, RoundTrip(VariantValue.Null));

        [Fact]
        public void RoundTrip_True() =>
            Assert.Equal(VariantValue.True, RoundTrip(VariantValue.True));

        [Fact]
        public void RoundTrip_False() =>
            Assert.Equal(VariantValue.False, RoundTrip(VariantValue.False));

        [Fact]
        public void RoundTrip_Int8()
        {
            VariantValue v = VariantValue.FromInt8(-42);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Int16()
        {
            VariantValue v = VariantValue.FromInt16(short.MaxValue);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Int32()
        {
            VariantValue v = VariantValue.FromInt32(int.MinValue);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Int64()
        {
            VariantValue v = VariantValue.FromInt64(long.MaxValue);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Float()
        {
            VariantValue v = VariantValue.FromFloat(float.Epsilon);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Double()
        {
            VariantValue v = VariantValue.FromDouble(Math.PI);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Decimal4()
        {
            VariantValue v = VariantValue.FromDecimal4(99.99m);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Decimal8()
        {
            VariantValue v = VariantValue.FromDecimal8(123456789.12m);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Decimal16()
        {
            // Uses a value requiring all 96 bits (exceeds Decimal8 range)
            VariantValue v = VariantValue.FromDecimal16(79228162514264337593543950335m);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Decimal16_Negative()
        {
            VariantValue v = VariantValue.FromDecimal16(-79228162514264337593543950335m);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Decimal16_Small()
        {
            // A small value explicitly stored as Decimal16
            VariantValue v = VariantValue.FromDecimal16(42.5m);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Date()
        {
            VariantValue v = VariantValue.FromDate(19000);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Timestamp()
        {
            VariantValue v = VariantValue.FromTimestamp(1640995200000000L);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_TimestampNtz()
        {
            VariantValue v = VariantValue.FromTimestampNtz(1640995200000000L);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_String_Short()
        {
            VariantValue v = VariantValue.FromString("hello");
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_String_Long()
        {
            VariantValue v = VariantValue.FromString(new string('x', 100));
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Binary()
        {
            VariantValue v = VariantValue.FromBinary(new byte[] { 0, 1, 2, 255 });
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_Uuid()
        {
            VariantValue v = VariantValue.FromUuid(Guid.NewGuid());
            Assert.Equal(v, RoundTrip(v));
        }

        // ---------------------------------------------------------------
        // Containers
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_EmptyObject() =>
            Assert.Equal(
                VariantValue.FromObject(new Dictionary<string, VariantValue>()),
                RoundTrip(VariantValue.FromObject(new Dictionary<string, VariantValue>())));

        [Fact]
        public void RoundTrip_EmptyArray() =>
            Assert.Equal(
                VariantValue.FromArray(new List<VariantValue>()),
                RoundTrip(VariantValue.FromArray(new List<VariantValue>())));

        [Fact]
        public void RoundTrip_ObjectWithFields()
        {
            VariantValue v = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "age", VariantValue.FromInt32(30) },
                { "active", VariantValue.True },
            });
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_ArrayMixed()
        {
            VariantValue v = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"),
                VariantValue.Null,
                VariantValue.True);
            Assert.Equal(v, RoundTrip(v));
        }

        [Fact]
        public void RoundTrip_DeepNesting()
        {
            VariantValue v = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "users", VariantValue.FromArray(
                    VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "name", VariantValue.FromString("Alice") },
                        { "scores", VariantValue.FromArray(
                            VariantValue.FromInt32(95),
                            VariantValue.FromInt32(87),
                            VariantValue.FromInt32(92)) },
                    }),
                    VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "name", VariantValue.FromString("Bob") },
                        { "scores", VariantValue.FromArray(
                            VariantValue.FromInt32(88),
                            VariantValue.FromInt32(91)) },
                    }))
                },
                { "count", VariantValue.FromInt32(2) },
            });
            Assert.Equal(v, RoundTrip(v));
        }
    }
}
