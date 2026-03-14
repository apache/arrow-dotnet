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
    public class VariantBuilderTests
    {
        private readonly VariantBuilder _builder = new VariantBuilder();

        // ---------------------------------------------------------------
        // Metadata builder
        // ---------------------------------------------------------------

        [Fact]
        public void MetadataBuilder_AddReturnsUniqueIds()
        {
            VariantMetadataBuilder mb = new VariantMetadataBuilder();
            Assert.Equal(0, mb.Add("alpha"));
            Assert.Equal(1, mb.Add("beta"));
            Assert.Equal(0, mb.Add("alpha")); // duplicate returns existing
            Assert.Equal(2, mb.Count);
        }

        [Fact]
        public void MetadataBuilder_BuildSortedMetadata()
        {
            VariantMetadataBuilder mb = new VariantMetadataBuilder();
            mb.Add("gamma");
            mb.Add("alpha");
            mb.Add("beta");

            byte[] metadata = mb.Build(out int[] idRemap);

            // Verify the built metadata is sorted.
            VariantMetadata meta = new VariantMetadata(metadata);
            Assert.Equal(3, meta.DictionarySize);
            Assert.True(meta.IsSorted);
            Assert.Equal("alpha", meta.GetString(0));
            Assert.Equal("beta", meta.GetString(1));
            Assert.Equal("gamma", meta.GetString(2));

            // Verify remap: gamma(0)->2, alpha(1)->0, beta(2)->1
            Assert.Equal(2, idRemap[0]); // gamma was id 0, now sorted pos 2
            Assert.Equal(0, idRemap[1]); // alpha was id 1, now sorted pos 0
            Assert.Equal(1, idRemap[2]); // beta was id 2, now sorted pos 1
        }

        // ---------------------------------------------------------------
        // Encode primitives
        // ---------------------------------------------------------------

        [Fact]
        public void Encode_Null()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.Null);
            VariantReader reader = new VariantReader(metadata, value);
            Assert.True(reader.IsNull);
        }

        [Fact]
        public void Encode_BoolTrue()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.True);
            VariantReader reader = new VariantReader(metadata, value);
            Assert.True(reader.GetBoolean());
        }

        [Fact]
        public void Encode_BoolFalse()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.False);
            VariantReader reader = new VariantReader(metadata, value);
            Assert.False(reader.GetBoolean());
        }

        [Fact]
        public void Encode_Int32()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromInt32(42));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(42, reader.GetInt32());
        }

        [Fact]
        public void Encode_Double()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromDouble(3.14));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(3.14, reader.GetDouble());
        }

        [Fact]
        public void Encode_Float()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromFloat(1.5f));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(1.5f, reader.GetFloat());
        }

        [Fact]
        public void Encode_ShortString()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromString("Hi"));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(VariantBasicType.ShortString, reader.BasicType);
            Assert.Equal("Hi", reader.GetString());
        }

        [Fact]
        public void Encode_LongString()
        {
            string longStr = new string('x', 100);
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromString(longStr));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(VariantBasicType.Primitive, reader.BasicType);
            Assert.Equal(VariantPrimitiveType.String, reader.PrimitiveType);
            Assert.Equal(longStr, reader.GetString());
        }

        [Fact]
        public void Encode_Binary()
        {
            byte[] data = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromBinary(data));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.True(reader.GetBinary().SequenceEqual(data));
        }

        [Fact]
        public void Encode_Uuid()
        {
            Guid guid = new Guid("550e8400-e29b-41d4-a716-446655440000");
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromUuid(guid));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(guid, reader.GetUuid());
        }

        [Fact]
        public void Encode_Decimal4()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromDecimal4(123.45m));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(123.45m, reader.GetDecimal4());
        }

        [Fact]
        public void Encode_Decimal8()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromDecimal8(123456789.12m));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(VariantPrimitiveType.Decimal8, reader.PrimitiveType);
            Assert.Equal(123456789.12m, reader.GetDecimal8());
        }

        [Fact]
        public void Encode_Decimal16()
        {
            decimal d = 79228162514264337593543950335m;
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromDecimal16(d));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(VariantPrimitiveType.Decimal16, reader.PrimitiveType);
            Assert.Equal(d, reader.GetDecimal16());
        }

        [Fact]
        public void Encode_Decimal16_Negative()
        {
            decimal d = -123456789012345678.90m;
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromDecimal16(d));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(d, reader.GetDecimal16());
        }

        [Fact]
        public void Encode_Date()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromDate(19000));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(19000, reader.GetDateDays());
        }

        [Fact]
        public void Encode_Timestamp()
        {
            (byte[] metadata, byte[] value) = _builder.Encode(VariantValue.FromTimestamp(1640995200000000L));
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal(1640995200000000L, reader.GetTimestampMicros());
        }

        // ---------------------------------------------------------------
        // Encode objects
        // ---------------------------------------------------------------

        [Fact]
        public void Encode_EmptyObject()
        {
            VariantValue obj = VariantValue.FromObject(new Dictionary<string, VariantValue>());
            (byte[] metadata, byte[] value) = _builder.Encode(obj);

            VariantObjectReader reader = new VariantObjectReader(metadata, value);
            Assert.Equal(0, reader.FieldCount);
        }

        [Fact]
        public void Encode_Object_FieldsSortedByName()
        {
            VariantValue obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "zebra", VariantValue.FromInt32(1) },
                { "apple", VariantValue.FromInt32(2) },
            });
            (byte[] metadata, byte[] value) = _builder.Encode(obj);

            VariantObjectReader reader = new VariantObjectReader(metadata, value);
            Assert.Equal(2, reader.FieldCount);

            // Field IDs should be sorted by name, so "apple" comes first.
            Assert.Equal("apple", reader.GetFieldName(0));
            Assert.Equal("zebra", reader.GetFieldName(1));
            Assert.Equal(2, reader.GetFieldValue(0).GetInt32());
            Assert.Equal(1, reader.GetFieldValue(1).GetInt32());
        }

        // ---------------------------------------------------------------
        // Encode arrays
        // ---------------------------------------------------------------

        [Fact]
        public void Encode_Array()
        {
            VariantValue arr = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"),
                VariantValue.Null);
            (byte[] metadata, byte[] value) = _builder.Encode(arr);

            VariantArrayReader reader = new VariantArrayReader(metadata, value);
            Assert.Equal(3, reader.ElementCount);
            Assert.Equal(1, reader.GetElement(0).GetInt32());
            Assert.Equal("two", reader.GetElement(1).GetString());
            Assert.True(reader.GetElement(2).IsNull);
        }

        // ---------------------------------------------------------------
        // Encode nested structures
        // ---------------------------------------------------------------

        [Fact]
        public void Encode_NestedObjectInArray()
        {
            VariantValue nested = VariantValue.FromArray(
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "x", VariantValue.FromInt32(10) },
                }),
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "x", VariantValue.FromInt32(20) },
                }));
            (byte[] metadata, byte[] value) = _builder.Encode(nested);

            VariantArrayReader arr = new VariantArrayReader(metadata, value);
            Assert.Equal(2, arr.ElementCount);

            VariantObjectReader obj0 = new VariantObjectReader(metadata, arr.GetElement(0).Value);
            Assert.Equal(10, obj0.GetFieldValue(0).GetInt32());

            VariantObjectReader obj1 = new VariantObjectReader(metadata, arr.GetElement(1).Value);
            Assert.Equal(20, obj1.GetFieldValue(0).GetInt32());
        }
    }
}
