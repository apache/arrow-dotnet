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
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    /// <summary>
    /// Reader-style API tests: exercise <see cref="ShreddedVariant.GetShreddedVariant"/>,
    /// <see cref="ShreddedObject"/>, and <see cref="ShreddedArray"/> typed accessors
    /// without going through full variant materialization. These mirror what a
    /// query engine would do for push-down reads against typed Parquet columns.
    /// </summary>
    public class ShreddedVariantReaderTests
    {
        private static readonly string IpcDir = FindIpcDir();

        private static string FindIpcDir()
        {
            string dir = AppContext.BaseDirectory;
            for (int i = 0; i < 10; i++)
            {
                string candidate = Path.Combine(dir, "test", "shredded_variant_ipc");
                if (Directory.Exists(candidate) && Directory.GetFiles(candidate, "*.arrow").Length > 0)
                    return candidate;
                string parent = Path.GetDirectoryName(dir);
                if (parent == null || parent == dir) break;
                dir = parent;
            }
            return null;
        }

        private static VariantArray LoadCase(string caseStem)
        {
            Skip.If(IpcDir == null, "regen.py has not been run");
            string path = Path.Combine(IpcDir, caseStem + ".arrow");
            Skip.IfNot(File.Exists(path), $"Missing {path}");

            using Stream stream = File.OpenRead(path);
            using ArrowFileReader reader = new ArrowFileReader(stream);
            RecordBatch batch = reader.ReadNextRecordBatch();
            return new VariantArray(batch.Column(batch.Schema.GetFieldIndex("var")));
        }

        // ---------------------------------------------------------------
        // Schema + state introspection
        // ---------------------------------------------------------------

        [SkippableFact]
        public void GetShredSchema_ReflectsPrimitiveTypedValue()
        {
            VariantArray array = LoadCase("case-010"); // typed_value: int32
            ShredSchema schema = array.GetShredSchema();
            Assert.Equal(ShredType.Int32, schema.TypedValueType);
        }

        [SkippableFact]
        public void GetShreddedVariant_HasTypedValue_WhenColumnPopulated()
        {
            VariantArray array = LoadCase("case-010"); // Int32 = 12345
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.True(slot.HasTypedValue);
            Assert.False(slot.HasResidual);
            Assert.False(slot.IsMissing);
        }

        [SkippableFact]
        public void GetShreddedVariant_HasResidual_WhenUnshredded()
        {
            VariantArray array = LoadCase("case-048"); // testUnshreddedVariants (bool true)
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.False(slot.HasTypedValue);
            Assert.True(slot.HasResidual);
        }

        // ---------------------------------------------------------------
        // Typed primitive accessors
        // ---------------------------------------------------------------

        [SkippableFact]
        public void GetInt32_ReadsShreddedValue()
        {
            VariantArray array = LoadCase("case-010"); // Int32 = 12345
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.Equal(12345, slot.GetInt32());
        }

        [SkippableFact]
        public void GetInt8_ReadsShreddedValue()
        {
            VariantArray array = LoadCase("case-006"); // Int8 = 34
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.Equal((sbyte)34, slot.GetInt8());
        }

        [SkippableFact]
        public void GetInt64_ReadsShreddedValue()
        {
            VariantArray array = LoadCase("case-012"); // Int64 = 9876543210
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.Equal(9876543210L, slot.GetInt64());
        }

        [SkippableFact]
        public void GetBoolean_ReadsShreddedValue()
        {
            VariantArray array = LoadCase("case-004"); // Bool = true
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.True(slot.GetBoolean());
        }

        [SkippableFact]
        public void GetString_ReadsShreddedValue()
        {
            VariantArray array = LoadCase("case-031"); // String
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.Equal("iceberg", slot.GetString());
        }

        [SkippableFact]
        public void GetDouble_ReadsShreddedValue()
        {
            VariantArray array = LoadCase("case-016"); // Double = 14.3
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.Equal(14.3, slot.GetDouble());
        }

        [SkippableFact]
        public void GetDecimal_ReadsShreddedDecimal4()
        {
            VariantArray array = LoadCase("case-024"); // Decimal4 = 12345.6789 (scale 4)
            ShreddedVariant slot = array.GetShreddedVariant(0);
            decimal d = slot.GetDecimal();
            Assert.Equal(12345.6789m, d);
        }

        // ---------------------------------------------------------------
        // Type-mismatch errors
        // ---------------------------------------------------------------

        [SkippableFact]
        public void GetInt32_OnStringSchema_Throws()
        {
            VariantArray array = LoadCase("case-031"); // String
            Assert.Throws<InvalidOperationException>(() => array.GetShreddedVariant(0).GetInt32());
        }

        [SkippableFact]
        public void GetString_OnInt32Schema_Throws()
        {
            VariantArray array = LoadCase("case-010"); // Int32
            Assert.Throws<InvalidOperationException>(() => array.GetShreddedVariant(0).GetString());
        }

        [SkippableFact]
        public void GetInt32_WithResidualOnly_Throws()
        {
            // Case 48 is unshredded — typed column absent on this row.
            VariantArray array = LoadCase("case-048");
            Assert.False(array.GetShreddedVariant(0).HasTypedValue);
            Assert.Throws<InvalidOperationException>(() => array.GetShreddedVariant(0).GetInt32());
        }

        // ---------------------------------------------------------------
        // Residual reader access
        // ---------------------------------------------------------------

        [SkippableFact]
        public void TryGetResidualReader_ReturnsUnderlyingBytes()
        {
            VariantArray array = LoadCase("case-048"); // unshredded bool=true
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.True(slot.TryGetResidualReader(out VariantReader reader));
            Assert.True(reader.IsBoolean);
            Assert.True(reader.GetBoolean());
        }

        [SkippableFact]
        public void TryGetResidualReader_FalseWhenNoResidual()
        {
            VariantArray array = LoadCase("case-010"); // fully shredded Int32
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.False(slot.TryGetResidualReader(out VariantReader _));
        }

        // ---------------------------------------------------------------
        // Object traversal
        // ---------------------------------------------------------------

        [SkippableFact]
        public void GetObject_Nested_FullyTypedLeaves()
        {
            // case-044: outer object with field 'c' whose typed_value is itself a
            // shredded object {a: int32=34, b: string="iceberg"}.
            VariantArray array = LoadCase("case-044");
            ShreddedVariant slot = array.GetShreddedVariant(0);
            Assert.Equal(ShredType.Object, slot.Schema.TypedValueType);

            ShreddedObject outerObj = slot.GetObject();
            Assert.True(outerObj.TryGetField("c", out ShreddedVariant cField));
            Assert.True(cField.HasTypedValue);
            Assert.Equal(ShredType.Object, cField.Schema.TypedValueType);

            ShreddedObject innerObj = cField.GetObject();
            Assert.True(innerObj.TryGetField("a", out ShreddedVariant aField));
            Assert.True(aField.HasTypedValue);
            Assert.Equal(34, aField.GetInt32());

            Assert.True(innerObj.TryGetField("b", out ShreddedVariant bField));
            Assert.True(bField.HasTypedValue);
            Assert.Equal("iceberg", bField.GetString());
        }

        [SkippableFact]
        public void GetObject_MixedTypedAndResidualFields()
        {
            // case-138: top-level value column is missing (fully shredded at the top),
            // but individual field typed_values can still be null at this row —
            // their values live in the field-level residual ('value' sub-column).
            // Field 'a' has a shredded typed of int32 but the actual value is int16,
            // so 'a' falls to residual; field 'b' is typed string "iceberg".
            VariantArray array = LoadCase("case-138");
            ShreddedObject obj = array.GetShreddedVariant(0).GetObject();

            Assert.True(obj.TryGetField("a", out ShreddedVariant aField));
            Assert.False(aField.HasTypedValue);    // schema says int32, value is int16
            Assert.True(aField.HasResidual);       // residual holds the int16 bytes
            Assert.True(aField.TryGetResidualReader(out VariantReader aReader));
            Assert.Equal((short)1234, aReader.GetInt16());

            Assert.True(obj.TryGetField("b", out ShreddedVariant bField));
            Assert.True(bField.HasTypedValue);
            Assert.Equal("iceberg", bField.GetString());
        }

        [SkippableFact]
        public void TryGetField_ReturnsFalseForUnknownField()
        {
            VariantArray array = LoadCase("case-138");
            ShreddedObject obj = array.GetShreddedVariant(0).GetObject();
            Assert.False(obj.TryGetField("nonexistent", out _));
        }

        [SkippableFact]
        public void Object_PartialShred_ExposesResidual()
        {
            // case-134: partially shredded — typed fields a, b plus residual field d (date).
            VariantArray array = LoadCase("case-134");
            ShreddedVariant slot = array.GetShreddedVariant(0);
            ShreddedObject obj = slot.GetObject();

            Assert.True(obj.TryGetField("b", out ShreddedVariant bField));
            Assert.Equal("iceberg", bField.GetString());

            Assert.True(obj.TryGetResidualReader(out VariantReader residualReader));
            Assert.True(residualReader.IsObject);
            // Residual is a variant object holding the unshredded field(s).
            VariantValue residualValue = residualReader.ToVariantValue();
            Assert.True(residualValue.IsObject);
            Assert.Contains("d", residualValue.AsObject().Keys);
        }

        [SkippableFact]
        public void Object_MissingField_IsDetectable()
        {
            // case-132: typed struct<a, b> but typed_value for `a` is null at row 0.
            // Expected variant has only `b`; field `a` is missing.
            VariantArray array = LoadCase("case-132");
            ShreddedObject obj = array.GetShreddedVariant(0).GetObject();

            Assert.True(obj.TryGetField("a", out ShreddedVariant aField));
            Assert.True(aField.IsMissing);

            Assert.True(obj.TryGetField("b", out ShreddedVariant bField));
            Assert.False(bField.IsMissing);
            Assert.Equal("iceberg", bField.GetString());
        }

        // ---------------------------------------------------------------
        // Array traversal
        // ---------------------------------------------------------------

        [SkippableFact]
        public void GetArray_Shredded_IteratesElements()
        {
            // case-001: shredded array of strings [comedy, drama]
            VariantArray array = LoadCase("case-001");
            ShreddedVariant slot = array.GetShreddedVariant(0);

            Assert.Equal(ShredType.Array, slot.Schema.TypedValueType);
            ShreddedArray arr = slot.GetArray();
            Assert.True(arr.IsTypedList);
            Assert.Equal(2, arr.ElementCount);

            Assert.Equal("comedy", arr.GetElement(0).GetString());
            Assert.Equal("drama", arr.GetElement(1).GetString());
        }

        [SkippableFact]
        public void GetArray_Empty()
        {
            // case-002: empty array
            VariantArray array = LoadCase("case-002");
            ShreddedArray arr = array.GetShreddedVariant(0).GetArray();
            Assert.True(arr.IsTypedList);
            Assert.Equal(0, arr.ElementCount);
        }

        [SkippableFact]
        public void GetArray_ElementAccessMatches_Materialization()
        {
            VariantArray array = LoadCase("case-001");
            ShreddedArray arr = array.GetShreddedVariant(0).GetArray();
            // Cross-check that per-element typed access agrees with whole-array materialization.
            VariantValue materialized = array.GetLogicalVariantValue(0);
            Assert.True(materialized.IsArray);
            var elements = materialized.AsArray();
            Assert.Equal(elements.Count, arr.ElementCount);
            for (int i = 0; i < arr.ElementCount; i++)
            {
                Assert.Equal(elements[i].AsString(), arr.GetElement(i).GetString());
            }
        }

        [Fact]
        public void GetArray_BothNull_MaterializesAsVariantNull()
        {
            // A row where both the residual binary and the typed list are null
            // encodes a variant null — same convention as ShreddedObject and
            // ShreddedVariant. Direct GetArray().ToVariantValue() must agree.
            VariantArray array = BuildArrayShreddedColumnWithNullRow();
            ShreddedVariant slot = array.GetShreddedVariant(0);

            Assert.Equal(ShredType.Array, slot.Schema.TypedValueType);
            Assert.False(slot.HasResidual);
            Assert.False(slot.HasTypedValue);

            ShreddedArray arr = slot.GetArray();
            Assert.False(arr.IsTypedList);
            Assert.False(arr.TryGetResidualReader(out _));
            Assert.Equal(VariantValue.Null, arr.ToVariantValue());

            // The slot-level entry-point path (which short-circuits on IsMissing)
            // also returns variant null — keeping both APIs consistent.
            Assert.Equal(VariantValue.Null, slot.ToVariantValue());
        }

        /// <summary>
        /// Builds a one-row shredded VariantArray with schema Array&lt;Int32&gt;
        /// where row 0 has both <c>value</c> and <c>typed_value</c> set to null.
        /// </summary>
        private static VariantArray BuildArrayShreddedColumnWithNullRow()
        {
            byte[] emptyMetadata = new VariantMetadataBuilder().Build();
            BinaryArray metadataArr = new BinaryArray.Builder().Append(emptyMetadata.AsSpan()).Build();
            BinaryArray valueArr = new BinaryArray.Builder().AppendNull().Build();

            // typed_value is list<struct<value: binary, typed_value: int32>>.
            StructType elementGroupType = new StructType(new List<Field>
            {
                new Field("value", BinaryType.Default, true),
                new Field("typed_value", Int32Type.Default, true),
            });
            // Empty inner struct (length 0) — the list row is null so no elements are referenced.
            StructArray emptyElementGroup = new StructArray(
                elementGroupType, length: 0,
                new IArrowArray[]
                {
                    new BinaryArray.Builder().Build(),
                    new Int32Array.Builder().Build(),
                },
                ArrowBuffer.Empty, nullCount: 0);

            ListType listType = new ListType(new Field("element", elementGroupType, true));
            ArrowBuffer offsetsBuffer = new ArrowBuffer.Builder<int>().Append(0).Append(0).Build();
            ArrowBuffer listValidity = new ArrowBuffer.BitmapBuilder().Append(false).Build();
            ListArray typedValueList = new ListArray(
                listType, length: 1, offsetsBuffer, emptyElementGroup, listValidity, nullCount: 1);

            StructType storageType = new StructType(new List<Field>
            {
                new Field("metadata", BinaryType.Default, false),
                new Field("value", BinaryType.Default, true),
                new Field("typed_value", listType, true),
            });
            StructArray storage = new StructArray(
                storageType, length: 1,
                new IArrowArray[] { metadataArr, valueArr, typedValueList },
                ArrowBuffer.Empty, nullCount: 0);
            return new VariantArray(storage);
        }

        // ---------------------------------------------------------------
        // Decimal32 / Decimal64 typed_value: construct the Arrow struct
        // directly (the Iceberg corpus only exercises Decimal128Type).
        // ---------------------------------------------------------------

        [Fact]
        public void GetDecimal_BackedByDecimal32Array()
        {
            // struct<metadata: binary, value: binary, typed_value: decimal32(5, 2)>
            decimal expected = 123.45m;
            VariantArray array = BuildShreddedColumn(
                new Decimal32Type(5, 2),
                new Decimal32Array.Builder(new Decimal32Type(5, 2)).Append(expected).Build());

            Assert.Equal(ShredType.Decimal4, array.GetShredSchema().TypedValueType);
            Assert.Equal(expected, array.GetShreddedVariant(0).GetDecimal());
        }

        [Fact]
        public void GetDecimal_BackedByDecimal64Array()
        {
            // struct<metadata: binary, value: binary, typed_value: decimal64(18, 9)>
            decimal expected = 987654321.123456789m;
            VariantArray array = BuildShreddedColumn(
                new Decimal64Type(18, 9),
                new Decimal64Array.Builder(new Decimal64Type(18, 9)).Append(expected).Build());

            Assert.Equal(ShredType.Decimal8, array.GetShredSchema().TypedValueType);
            Assert.Equal(expected, array.GetShreddedVariant(0).GetDecimal());
        }

        [Fact]
        public void GetDecimal_BackedByDecimal32Array_MaterializesCorrectly()
        {
            // End-to-end: via GetLogicalVariantValue.
            decimal expected = 42.50m;
            VariantArray array = BuildShreddedColumn(
                new Decimal32Type(4, 2),
                new Decimal32Array.Builder(new Decimal32Type(4, 2)).Append(expected).Build());

            VariantValue v = array.GetLogicalVariantValue(0);
            Assert.Equal(expected, v.AsDecimal());
        }

        [Fact]
        public void GetSqlDecimal_BackedByDecimal128Array_ExceedingSystemDecimalRange()
        {
            // Decimal16 value larger than System.Decimal (max ~7.9228e28). Precision 38,
            // scale 0, value 10^38 - 1 fits in SqlDecimal/Decimal128 but overflows decimal.
            System.Data.SqlTypes.SqlDecimal expected =
                System.Data.SqlTypes.SqlDecimal.Parse("99999999999999999999999999999999999999");
            Decimal128Type type = new Decimal128Type(38, 0);
            VariantArray array = BuildShreddedColumn(
                type,
                new Decimal128Array.Builder(type).Append(expected).Build());

            Assert.Equal(ShredType.Decimal16, array.GetShredSchema().TypedValueType);

            // Typed accessor: SqlDecimal path preserves full precision.
            Assert.Equal(expected, array.GetShreddedVariant(0).GetSqlDecimal());

            // GetDecimal() overflows System.Decimal for this value. ShreddedVariant
            // is a ref struct, so it cannot be captured by the Throws lambda —
            // the call must be made on a fresh slot inside the delegate.
            Assert.Throws<OverflowException>(() => array.GetShreddedVariant(0).GetDecimal());

            // Materialization must not throw: ReadTypedPrimitive dispatches the
            // Decimal16 case through GetSqlDecimal / FromSqlDecimal, so the value
            // is retained with SqlDecimal storage inside the VariantValue.
            VariantValue v = array.GetLogicalVariantValue(0);
            Assert.Equal(expected, v.AsSqlDecimal());
            Assert.Throws<OverflowException>(() => v.AsDecimal());
        }

        /// <summary>
        /// Helper: builds a one-row shredded VariantArray whose typed_value column
        /// is <paramref name="typedArray"/> (of type <paramref name="typedType"/>),
        /// with empty metadata and null value.
        /// </summary>
        private static VariantArray BuildShreddedColumn(IArrowType typedType, IArrowArray typedArray)
        {
            byte[] emptyMetadata = new VariantMetadataBuilder().Build();
            BinaryArray metadataArr = new BinaryArray.Builder().Append(emptyMetadata.AsSpan()).Build();
            BinaryArray valueArr = new BinaryArray.Builder().AppendNull().Build();

            StructType storageType = new StructType(new List<Field>
            {
                new Field("metadata", BinaryType.Default, false),
                new Field("value", BinaryType.Default, true),
                new Field("typed_value", typedType, true),
            });
            StructArray storage = new StructArray(
                storageType, length: 1,
                new IArrowArray[] { metadataArr, valueArr, typedArray },
                ArrowBuffer.Empty, nullCount: 0);
            return new VariantArray(storage);
        }
    }
}
