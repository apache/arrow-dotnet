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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    /// <summary>
    /// The shredded readers must accept every binary representation a variant column
    /// may use for its <c>metadata</c> and <c>value</c> fields (binary, large_binary,
    /// binary_view), at every nesting level, as well as large_utf8 / large_binary
    /// <c>typed_value</c> columns.
    /// </summary>
    public class ShreddedVariantStorageTypeTests
    {
        public enum Storage { LargeBinary, BinaryView }

        private static readonly ShredSchema Schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
        {
            ["a"] = ShredSchema.Primitive(ShredType.Int32),
            ["s"] = ShredSchema.Primitive(ShredType.String),
            ["bin"] = ShredSchema.Primitive(ShredType.Binary),
            ["tags"] = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32)),
        });

        private static VariantValue Obj(params (string Name, VariantValue Value)[] fields)
        {
            var dict = new Dictionary<string, VariantValue>();
            foreach (var (name, value) in fields) dict[name] = value;
            return VariantValue.FromObject(dict);
        }

        // Exercises residuals at the top level, inside a partially shredded object,
        // inside an object field element group, and inside a list element group.
        private static readonly List<VariantValue?> Rows = new List<VariantValue?>
        {
            Obj(("a", VariantValue.FromInt32(1)),
                ("s", VariantValue.FromString("hello")),
                ("bin", VariantValue.FromBinary(new byte[] { 1, 2, 3 })),
                ("tags", VariantValue.FromArray(VariantValue.FromInt32(7), VariantValue.FromString("residual element"))),
                ("extra", VariantValue.FromBoolean(true))),
            null,
            VariantValue.FromString("not an object"),
            Obj(("a", VariantValue.FromString("residual field")),
                ("tags", VariantValue.FromString("not an array"))),
        };

        private static VariantArray BuildShredded()
        {
            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(Rows, Schema);
            return ShreddedVariantArrayBuilder.Build(Schema, metadata, rows);
        }

        [Theory]
        [InlineData(Storage.LargeBinary)]
        [InlineData(Storage.BinaryView)]
        public void GetLogicalVariantValue_ReadsAlternateStorage(Storage storage)
        {
            VariantArray array = Convert(BuildShredded(), storage);

            Assert.Equal(Rows.Count, array.Length);
            for (int i = 0; i < Rows.Count; i++)
            {
                if (Rows[i].HasValue)
                    Assert.Equal(Rows[i].Value, array.GetLogicalVariantValue(i));
                else
                    Assert.True(array.IsNull(i));
            }
        }

        [Theory]
        [InlineData(Storage.LargeBinary)]
        [InlineData(Storage.BinaryView)]
        public void GetLogicalVariantValue_ReadsAlternateStorage_Unshredded(Storage storage)
        {
            var builder = new VariantArray.Builder();
            builder.AppendRange(Rows);
            VariantArray array = Convert(builder.Build(), storage);

            Assert.Equal(Rows[0].Value, array.GetLogicalVariantValue(0));
        }

        [Fact]
        public void TypedAccessors_ReadLargeTypedColumns()
        {
            VariantArray array = Convert(BuildShredded(), Storage.LargeBinary);
            ShreddedObject obj = array.GetShreddedVariant(0).GetObject();

            Assert.True(obj.TryGetField("s", out ShreddedVariant s));
            Assert.Equal("hello", s.GetString());
            Assert.True(obj.TryGetField("bin", out ShreddedVariant bin));
            Assert.Equal(new byte[] { 1, 2, 3 }, bin.GetBinaryBytes().ToArray());
        }

        // ---------------------------------------------------------------
        // Storage conversion
        // ---------------------------------------------------------------

        private static VariantArray Convert(VariantArray array, Storage storage)
        {
            return new VariantArray((StructArray)ConvertArray(array.StorageArray, null, storage));
        }

        /// <summary>
        /// Recursively rewrites <c>metadata</c> / <c>value</c> binary fields to the target
        /// storage. For <see cref="Storage.LargeBinary"/> it also rewrites string and binary
        /// <c>typed_value</c> columns to large_utf8 / large_binary; there is no view
        /// counterpart because the shredding schema doesn't map view types for typed_value.
        /// </summary>
        private static IArrowArray ConvertArray(IArrowArray array, string fieldName, Storage storage)
        {
            bool isVariantBinary = fieldName == "metadata" || fieldName == "value";
            switch (array)
            {
                case StringArray str when !isVariantBinary && storage == Storage.LargeBinary:
                {
                    var b = new LargeStringArray.Builder();
                    for (int i = 0; i < str.Length; i++)
                    {
                        if (str.IsNull(i)) b.AppendNull(); else b.Append(str.GetString(i));
                    }
                    return b.Build();
                }
                case StringArray str:
                    return str;
                case BinaryArray bin when isVariantBinary || storage == Storage.LargeBinary:
                    return storage == Storage.LargeBinary ? ToLargeBinary(bin) : (IArrowArray)ToBinaryView(bin);
                case StructArray st:
                {
                    var type = (StructType)st.Data.DataType;
                    var fields = new List<Field>();
                    var children = new List<IArrowArray>();
                    for (int f = 0; f < type.Fields.Count; f++)
                    {
                        Field field = type.Fields[f];
                        IArrowArray child = ConvertArray(st.Fields[f], field.Name, storage);
                        children.Add(child);
                        fields.Add(new Field(field.Name, child.Data.DataType, field.IsNullable));
                    }
                    return new StructArray(new StructType(fields), st.Length, children, st.NullBitmapBuffer, st.NullCount);
                }
                case ListArray list:
                {
                    Field element = ((ListType)list.Data.DataType).ValueField;
                    IArrowArray values = ConvertArray(list.Values, element.Name, storage);
                    var listType = new ListType(new Field(element.Name, values.Data.DataType, element.IsNullable));
                    return new ListArray(listType, list.Length, list.ValueOffsetsBuffer, values, list.NullBitmapBuffer, list.NullCount);
                }
                default:
                    return array;
            }
        }

        private static LargeBinaryArray ToLargeBinary(BinaryArray bin)
        {
            var b = new LargeBinaryArray.Builder();
            for (int i = 0; i < bin.Length; i++)
            {
                if (bin.IsNull(i)) b.AppendNull(); else b.Append(bin.GetBytes(i));
            }
            return b.Build();
        }

        private static BinaryViewArray ToBinaryView(BinaryArray bin)
        {
            var b = new BinaryViewArray.Builder();
            for (int i = 0; i < bin.Length; i++)
            {
                if (bin.IsNull(i)) b.AppendNull(); else b.Append(bin.GetBytes(i));
            }
            return b.Build();
        }
    }
}
