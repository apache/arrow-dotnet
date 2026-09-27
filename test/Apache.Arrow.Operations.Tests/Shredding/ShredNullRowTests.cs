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

using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    /// <summary>
    /// Tests for SQL-NULL rows (a null <see cref="VariantValue"/>?) flowing through
    /// inference, shredding and array assembly, and staying distinct from a present
    /// <see cref="VariantValue.Null"/>.
    /// </summary>
    public class ShredNullRowTests
    {
        private static VariantValue Obj(int a) => VariantValue.FromObject(
            new Dictionary<string, VariantValue> { ["a"] = VariantValue.FromInt32(a) });

        private static VariantArray ShredAndBuild(IReadOnlyList<VariantValue?> values, ShredSchema schema)
        {
            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(values, schema);
            return ShreddedVariantArrayBuilder.Build(schema, metadata, rows);
        }

        private static void AssertRows(IReadOnlyList<VariantValue?> expected, VariantArray array)
        {
            Assert.Equal(expected.Count, array.Length);
            for (int i = 0; i < expected.Count; i++)
            {
                if (expected[i].HasValue)
                {
                    Assert.False(array.IsNull(i), $"row {i} should be valid");
                    Assert.Equal(expected[i].Value, array.GetLogicalVariantValue(i));
                }
                else
                {
                    Assert.True(array.IsNull(i), $"row {i} should be null");
                }
            }
        }

        [Fact]
        public void SqlNullRow_IsDistinctFromVariantNull()
        {
            var values = new List<VariantValue?> { Obj(1), null, VariantValue.Null, Obj(3) };

            ShredSchema schema = new ShredSchemaInferer().Infer(values, ShredOptions.Default);
            VariantArray array = ShredAndBuild(values, schema);

            Assert.Equal(ShredType.Object, schema.TypedValueType);
            Assert.Equal(1, array.NullCount);
            Assert.Equal(1, array.StorageArray.NullCount);
            AssertRows(values, array);
            Assert.False(array.IsNull(2));
            Assert.True(array.GetLogicalVariantValue(2).IsNull);
        }

        [Fact]
        public void SqlNullRow_Unshredded()
        {
            var values = new List<VariantValue?> { VariantValue.FromString("x"), null, VariantValue.FromInt32(5) };
            AssertRows(values, ShredAndBuild(values, ShredSchema.Unshredded()));
        }

        [Fact]
        public void SqlNullRow_Primitive()
        {
            var values = new List<VariantValue?> { null, VariantValue.FromInt32(1), VariantValue.FromString("residual"), null };
            VariantArray array = ShredAndBuild(values, ShredSchema.Primitive(ShredType.Int32));
            Assert.Equal(2, array.NullCount);
            AssertRows(values, array);
        }

        [Fact]
        public void SqlNullRow_Array()
        {
            var values = new List<VariantValue?>
            {
                VariantValue.FromArray(VariantValue.FromInt32(1), VariantValue.FromInt32(2)),
                null,
                VariantValue.FromArray(VariantValue.FromInt32(3)),
            };
            AssertRows(values, ShredAndBuild(values, ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32))));
        }

        [Fact]
        public void AllRowsNull()
        {
            var values = new List<VariantValue?> { null, null };
            ShredSchema schema = new ShredSchemaInferer().Infer(values);
            Assert.Equal(ShredType.None, schema.TypedValueType);

            VariantArray array = ShredAndBuild(values, schema);
            Assert.Equal(2, array.NullCount);
            AssertRows(values, array);
        }

        [Fact]
        public void NoNullRows_ProducesNoValidityBuffer()
        {
            var values = new List<VariantValue?> { Obj(1), Obj(2) };
            VariantArray array = ShredAndBuild(values, new ShredSchemaInferer().Infer(values));
            Assert.Equal(0, array.NullCount);
            Assert.True(array.StorageArray.NullBitmapBuffer.IsEmpty);
        }

        [Fact]
        public void SlicedArray_KeepsValidityAligned()
        {
            var values = new List<VariantValue?> { Obj(1), null, Obj(3), null, Obj(5) };
            VariantArray array = ShredAndBuild(values, new ShredSchemaInferer().Infer(values));

            var sliced = (VariantArray)ArrowArrayFactory.Slice(array, 1, 3);
            AssertRows(new List<VariantValue?> { null, Obj(3), null }, sliced);
        }

        [Fact]
        public void Infer_IgnoresNullRows()
        {
            // With nulls counted, Int32 would appear in only a third of the rows.
            var values = new List<VariantValue?> { VariantValue.FromInt32(1), null, null, null, VariantValue.FromInt32(2), null };
            ShredSchema schema = new ShredSchemaInferer().Infer(values);
            Assert.Equal(ShredType.Int32, schema.TypedValueType);
        }

        [Fact]
        public void Shred_ReturnsNullEntryForNullRow()
        {
            var values = new List<VariantValue?> { Obj(1), null };
            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(values, ShredSchema.Unshredded());
            Assert.NotNull(metadata);
            Assert.NotNull(rows[0]);
            Assert.Null(rows[1]);
        }

        [Fact]
        public void Reconstruct_RoundTripsNullRows()
        {
            var values = new List<VariantValue?> { Obj(1), null, VariantValue.Null, VariantValue.FromString("s") };
            ShredSchema schema = new ShredSchemaInferer().Infer(values);
            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(values, schema);

            for (int i = 0; i < values.Count; i++)
            {
                Assert.Equal(values[i], VariantUnshredder.Reconstruct(rows[i], schema, metadata));
            }
        }
    }
}
