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
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    /// <summary>
    /// Round-trip tests for the producer path: take <see cref="VariantValue"/>s,
    /// shred them, assemble into a shredded <see cref="VariantArray"/>, and then
    /// read each row back via <c>GetLogicalVariantValue</c>. The reader is the
    /// trusted oracle (validated against the Iceberg corpus), so equality here
    /// confirms the builder produces a correct Arrow structure.
    /// </summary>
    public class ShreddedVariantArrayBuilderTests
    {
        private static VariantArray ShredAndBuild(IReadOnlyList<VariantValue> values, ShredSchema schema)
        {
            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(values, schema);
            return ShreddedVariantArrayBuilder.Build(schema, metadata, rows);
        }

        private static void AssertRoundTrip(IReadOnlyList<VariantValue> values, ShredSchema schema)
        {
            VariantArray array = ShredAndBuild(values, schema);
            Assert.Equal(values.Count, array.Length);
            for (int i = 0; i < values.Count; i++)
            {
                VariantValue actual = array.GetLogicalVariantValue(i);
                Assert.Equal(values[i], actual);
            }
        }

        // ---------------------------------------------------------------
        // Unshredded (schema = None)
        // ---------------------------------------------------------------

        [Fact]
        public void Unshredded_Column_HasNoTypedValue()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromInt32(42),
                VariantValue.FromString("hello"),
            };
            VariantArray array = ShredAndBuild(values, ShredSchema.Unshredded());

            Assert.False(array.IsShredded);
            AssertRoundTrip(values, ShredSchema.Unshredded());
        }

        // ---------------------------------------------------------------
        // Primitive shredding
        // ---------------------------------------------------------------

        [Fact]
        public void Primitive_Int32()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromInt32(1),
                VariantValue.FromInt32(2),
                VariantValue.FromInt32(-42),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Int32));
        }

        [Fact]
        public void Primitive_Boolean()
        {
            var values = new List<VariantValue> { VariantValue.True, VariantValue.False, VariantValue.True };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Boolean));
        }

        [Fact]
        public void Primitive_String()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromString("alpha"),
                VariantValue.FromString("beta"),
                VariantValue.FromString(""),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.String));
        }

        [Fact]
        public void Primitive_Int64()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromInt64(long.MaxValue),
                VariantValue.FromInt64(long.MinValue),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Int64));
        }

        [Fact]
        public void Primitive_Double()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromDouble(Math.PI),
                VariantValue.FromDouble(-0.0),
                VariantValue.FromDouble(double.MaxValue),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Double));
        }

        [Fact]
        public void Primitive_Decimal4()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromDecimal4(123.45m),
                VariantValue.FromDecimal4(-99.99m),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Decimal4));
        }

        [Fact]
        public void Primitive_Date()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromDate(19000),
                VariantValue.FromDate(0),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Date));
        }

        [Fact]
        public void Primitive_Timestamp()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromTimestamp(1640995200000000L),
                VariantValue.FromTimestamp(0L),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Timestamp));
        }

        [Fact]
        public void Primitive_Uuid()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromUuid(Guid.NewGuid()),
                VariantValue.FromUuid(Guid.Empty),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Uuid));
        }

        [Fact]
        public void Primitive_Binary()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromBinary(new byte[] { 1, 2, 3 }),
                VariantValue.FromBinary(new byte[] { 0xff, 0x00 }),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Binary));
        }

        // ---------------------------------------------------------------
        // Primitive type mismatch — falls back to residual
        // ---------------------------------------------------------------

        [Fact]
        public void Primitive_TypeMismatch_FallsBackToBinary()
        {
            // Schema expects Int32, values include a string — the string goes to residual.
            var values = new List<VariantValue>
            {
                VariantValue.FromInt32(42),
                VariantValue.FromString("not an int"),
                VariantValue.FromInt32(99),
            };
            AssertRoundTrip(values, ShredSchema.Primitive(ShredType.Int32));
        }

        // ---------------------------------------------------------------
        // Object shredding
        // ---------------------------------------------------------------

        [Fact]
        public void Object_FullyShredded()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Alice") },
                    { "age", VariantValue.FromInt32(30) },
                }),
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Bob") },
                    { "age", VariantValue.FromInt32(25) },
                }),
            };

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            VariantArray array = ShredAndBuild(values, schema);
            Assert.True(array.IsShredded);
            AssertRoundTrip(values, schema);
        }

        [Fact]
        public void Object_PartiallyShredded_MergesResidualFields()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Alice") },
                    { "age", VariantValue.FromInt32(30) },
                    { "extra", VariantValue.True },
                }),
            };
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
            });

            AssertRoundTrip(values, schema);
        }

        [Fact]
        public void Object_MissingField_NotInReconstruction()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("only-name") },
                }),
            };
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            AssertRoundTrip(values, schema);
        }

        // ---------------------------------------------------------------
        // Array shredding
        // ---------------------------------------------------------------

        [Fact]
        public void Array_Homogeneous()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromArray(
                    VariantValue.FromInt32(1),
                    VariantValue.FromInt32(2),
                    VariantValue.FromInt32(3)),
                VariantValue.FromArray(VariantValue.FromInt32(4)),
                VariantValue.FromArray(new List<VariantValue>()),
            };
            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));
            AssertRoundTrip(values, schema);
        }

        [Fact]
        public void Array_MixedElements_FallbackToBinary()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromArray(
                    VariantValue.FromInt32(1),
                    VariantValue.FromString("two"),
                    VariantValue.FromInt32(3)),
            };
            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));
            AssertRoundTrip(values, schema);
        }

        // ---------------------------------------------------------------
        // Nested structures
        // ---------------------------------------------------------------

        [Fact]
        public void Nested_ObjectsAndArrays()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "users", VariantValue.FromArray(
                        VariantValue.FromObject(new Dictionary<string, VariantValue>
                        {
                            { "name", VariantValue.FromString("Alice") },
                            { "score", VariantValue.FromInt32(95) },
                        }),
                        VariantValue.FromObject(new Dictionary<string, VariantValue>
                        {
                            { "name", VariantValue.FromString("Bob") },
                            { "score", VariantValue.FromInt32(88) },
                        }))
                    },
                    { "count", VariantValue.FromInt32(2) },
                }),
            };

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "users", ShredSchema.ForArray(
                    ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                    {
                        { "name", ShredSchema.Primitive(ShredType.String) },
                        { "score", ShredSchema.Primitive(ShredType.Int32) },
                    }))
                },
                { "count", ShredSchema.Primitive(ShredType.Int32) },
            });

            AssertRoundTrip(values, schema);
        }

        // ---------------------------------------------------------------
        // Shape of the built Arrow array
        // ---------------------------------------------------------------

        [Fact]
        public void Build_ProducesExpectedArrowShape_PrimitiveInt32()
        {
            var values = new List<VariantValue> { VariantValue.FromInt32(42) };
            VariantArray array = ShredAndBuild(values, ShredSchema.Primitive(ShredType.Int32));
            Assert.True(array.IsShredded);
            Assert.NotNull(array.TypedValueArray);
            Assert.IsType<Int32Array>(array.TypedValueArray);
        }

        [Fact]
        public void Build_ProducesExpectedArrowShape_Object()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "x", VariantValue.FromInt32(1) },
                }),
            };
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "x", ShredSchema.Primitive(ShredType.Int32) },
            });
            VariantArray array = ShredAndBuild(values, schema);
            Assert.True(array.IsShredded);
            Assert.IsType<StructArray>(array.TypedValueArray);
        }

        [Fact]
        public void Build_ProducesExpectedArrowShape_Array()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromArray(VariantValue.FromInt32(1), VariantValue.FromInt32(2)),
            };
            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));
            VariantArray array = ShredAndBuild(values, schema);
            Assert.True(array.IsShredded);
            Assert.IsType<ListArray>(array.TypedValueArray);
        }

        // ---------------------------------------------------------------
        // Reader-side composition: built array is usable by the shredded reader.
        // ---------------------------------------------------------------

        [Fact]
        public void BuiltArray_SupportsShreddedReaderAccess()
        {
            var values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("alice") },
                    { "age", VariantValue.FromInt32(42) },
                }),
            };
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });
            VariantArray array = ShredAndBuild(values, schema);

            ShreddedVariant slot = array.GetShreddedVariant(0);
            ShreddedObject obj = slot.GetObject();

            Assert.True(obj.TryGetField("name", out ShreddedVariant nameField));
            Assert.Equal("alice", nameField.GetString());

            Assert.True(obj.TryGetField("age", out ShreddedVariant ageField));
            Assert.Equal(42, ageField.GetInt32());
        }
    }
}
