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
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    /// <summary>
    /// Round-trip tests: VariantValue → Shred → Reconstruct → VariantValue, verifying equality.
    /// </summary>
    public class ShredRoundTripTests
    {
        private static VariantValue RoundTrip(VariantValue original, ShredSchema schema)
        {
            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(new[] { original }, schema);
            VariantValue? result = VariantUnshredder.Reconstruct(rows[0], schema, metadata);
            Assert.True(result.HasValue, "Round-trip should not produce a missing value.");
            return result.Value;
        }

        // ---------------------------------------------------------------
        // Primitives through typed columns
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_Boolean_True()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Boolean);
            Assert.Equal(VariantValue.True, RoundTrip(VariantValue.True, schema));
        }

        [Fact]
        public void RoundTrip_Boolean_False()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Boolean);
            Assert.Equal(VariantValue.False, RoundTrip(VariantValue.False, schema));
        }

        [Fact]
        public void RoundTrip_Int8()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int8);
            VariantValue v = VariantValue.FromInt8(-42);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Int16()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int16);
            VariantValue v = VariantValue.FromInt16(short.MaxValue);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Int32()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int32);
            VariantValue v = VariantValue.FromInt32(42);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Int64()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int64);
            VariantValue v = VariantValue.FromInt64(long.MaxValue);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Float()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Float);
            VariantValue v = VariantValue.FromFloat(3.14f);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Double()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Double);
            VariantValue v = VariantValue.FromDouble(Math.PI);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_String()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.String);
            VariantValue v = VariantValue.FromString("hello world");
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Binary()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Binary);
            VariantValue v = VariantValue.FromBinary(new byte[] { 0, 1, 2, 255 });
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Uuid()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Uuid);
            VariantValue v = VariantValue.FromUuid(Guid.NewGuid());
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Date()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Date);
            VariantValue v = VariantValue.FromDate(19000);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Timestamp()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Timestamp);
            VariantValue v = VariantValue.FromTimestamp(1640995200000000L);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Decimal4()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Decimal4);
            VariantValue v = VariantValue.FromDecimal4(99.99m);
            Assert.Equal(v, RoundTrip(v, schema));
        }

        // ---------------------------------------------------------------
        // Primitives through binary fallback (type mismatch)
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_TypeMismatch_FallsBackToBinary()
        {
            // Schema expects Int32, but value is a string — goes through binary.
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int32);
            VariantValue v = VariantValue.FromString("hello");
            Assert.Equal(v, RoundTrip(v, schema));
        }

        [Fact]
        public void RoundTrip_Null_FallsBackToBinary()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int32);
            Assert.Equal(VariantValue.Null, RoundTrip(VariantValue.Null, schema));
        }

        // ---------------------------------------------------------------
        // Objects
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_FullyShreddedObject()
        {
            VariantValue original = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "age", VariantValue.FromInt32(30) },
            });

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            Assert.Equal(original, RoundTrip(original, schema));
        }

        [Fact]
        public void RoundTrip_PartiallyShreddedObject()
        {
            VariantValue original = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "age", VariantValue.FromInt32(30) },
                { "active", VariantValue.True },
            });

            // Only shred "name" — "age" and "active" go to residual binary.
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
            });

            Assert.Equal(original, RoundTrip(original, schema));
        }

        [Fact]
        public void RoundTrip_ObjectWithMissingField()
        {
            // Only "name" present, schema expects "name" and "age".
            VariantValue original = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
            });

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            Assert.Equal(original, RoundTrip(original, schema));
        }

        [Fact]
        public void RoundTrip_EmptyObject()
        {
            VariantValue original = VariantValue.FromObject(new Dictionary<string, VariantValue>());
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "x", ShredSchema.Primitive(ShredType.Int32) },
            });

            Assert.Equal(original, RoundTrip(original, schema));
        }

        // ---------------------------------------------------------------
        // Arrays
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_Array_Homogeneous()
        {
            VariantValue original = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromInt32(2),
                VariantValue.FromInt32(3));

            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));
            Assert.Equal(original, RoundTrip(original, schema));
        }

        [Fact]
        public void RoundTrip_Array_Mixed()
        {
            VariantValue original = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"),
                VariantValue.Null,
                VariantValue.True);

            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));
            Assert.Equal(original, RoundTrip(original, schema));
        }

        [Fact]
        public void RoundTrip_EmptyArray()
        {
            VariantValue original = VariantValue.FromArray(new List<VariantValue>());
            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));
            Assert.Equal(original, RoundTrip(original, schema));
        }

        // ---------------------------------------------------------------
        // Nested structures
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_NestedObjectsAndArrays()
        {
            VariantValue original = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "users", VariantValue.FromArray(
                    VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "name", VariantValue.FromString("Alice") },
                        { "scores", VariantValue.FromArray(
                            VariantValue.FromInt32(95),
                            VariantValue.FromInt32(87))
                        },
                    }),
                    VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "name", VariantValue.FromString("Bob") },
                        { "scores", VariantValue.FromArray(
                            VariantValue.FromInt32(88))
                        },
                    }))
                },
                { "count", VariantValue.FromInt32(2) },
            });

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "users", ShredSchema.ForArray(
                    ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                    {
                        { "name", ShredSchema.Primitive(ShredType.String) },
                        { "scores", ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32)) },
                    }))
                },
                { "count", ShredSchema.Primitive(ShredType.Int32) },
            });

            Assert.Equal(original, RoundTrip(original, schema));
        }

        // ---------------------------------------------------------------
        // Unshredded fallback
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_Unshredded()
        {
            VariantValue original = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "age", VariantValue.FromInt32(30) },
            });

            ShredSchema schema = ShredSchema.Unshredded();
            Assert.Equal(original, RoundTrip(original, schema));
        }

        // ---------------------------------------------------------------
        // Cross-codec: JSON → shred → unshred → JSON
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_JsonThroughShredding()
        {
            string originalJson = "{\"name\":\"Alice\",\"age\":30,\"tags\":[\"a\",\"b\"]}";
            (byte[] metaIn, byte[] valueIn) = VariantJsonReader.Parse(originalJson);
            VariantValue parsed = new VariantReader(metaIn, valueIn).ToVariantValue();

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int8) },
            });

            VariantValue reconstructed = RoundTrip(parsed, schema);
            string resultJson = VariantJsonWriter.ToJson(reconstructed);

            // Field order in JSON output is not guaranteed to match; compare via parsed equality.
            (byte[] metaOut, byte[] valueOut) = VariantJsonReader.Parse(resultJson);
            VariantValue reparsed = new VariantReader(metaOut, valueOut).ToVariantValue();
            Assert.Equal(parsed, reparsed);
        }

        // ---------------------------------------------------------------
        // Column-level shared metadata: rows share a single dictionary
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_MultipleRows_SharedMetadata()
        {
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Alice") },
                    { "extra1", VariantValue.FromInt32(1) },
                }),
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Bob") },
                    { "extra2", VariantValue.FromString("hi") },
                }),
            };

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
            });

            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(values, schema);

            Assert.Equal(values.Count, rows.Count);
            for (int i = 0; i < values.Count; i++)
            {
                VariantValue? reconstructed = VariantUnshredder.Reconstruct(rows[i], schema, metadata);
                Assert.True(reconstructed.HasValue);
                Assert.Equal(values[i], reconstructed.Value);
            }
        }
    }
}
