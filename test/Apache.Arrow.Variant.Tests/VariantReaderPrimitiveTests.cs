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
using Xunit;

namespace Apache.Arrow.Variant.Tests
{
    public class VariantReaderPrimitiveTests
    {
        // ---------------------------------------------------------------
        // Null
        // ---------------------------------------------------------------

        [Fact]
        public void Null_IsDetected()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveNull);

            Assert.Equal(VariantBasicType.Primitive, reader.BasicType);
            Assert.Equal(VariantPrimitiveType.NullType, reader.PrimitiveType);
            Assert.True(reader.IsNull);
            Assert.False(reader.IsBoolean);
            Assert.False(reader.IsString);
            Assert.False(reader.IsObject);
            Assert.False(reader.IsArray);
            Assert.False(reader.IsNumeric);
        }

        // ---------------------------------------------------------------
        // Boolean
        // ---------------------------------------------------------------

        [Fact]
        public void BoolTrue_ReturnsTrue()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveBoolTrue);

            Assert.Equal(VariantBasicType.Primitive, reader.BasicType);
            Assert.Equal(VariantPrimitiveType.BooleanTrue, reader.PrimitiveType);
            Assert.True(reader.IsBoolean);
            Assert.False(reader.IsNull);
            Assert.True(reader.GetBoolean());
        }

        [Fact]
        public void BoolFalse_ReturnsFalse()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveBoolFalse);

            Assert.Equal(VariantPrimitiveType.BooleanFalse, reader.PrimitiveType);
            Assert.True(reader.IsBoolean);
            Assert.False(reader.GetBoolean());
        }

        // ---------------------------------------------------------------
        // Int8
        // ---------------------------------------------------------------

        [Fact]
        public void Int8_Positive()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveInt8_42);

            Assert.Equal(VariantPrimitiveType.Int8, reader.PrimitiveType);
            Assert.True(reader.IsNumeric);
            Assert.Equal(42, reader.GetInt8());
        }

        [Fact]
        public void Int8_Negative()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveInt8_Neg1);

            Assert.Equal(VariantPrimitiveType.Int8, reader.PrimitiveType);
            Assert.Equal(-1, reader.GetInt8());
        }

        // ---------------------------------------------------------------
        // Int16
        // ---------------------------------------------------------------

        [Fact]
        public void Int16_Value()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveInt16_1000);

            Assert.Equal(VariantPrimitiveType.Int16, reader.PrimitiveType);
            Assert.True(reader.IsNumeric);
            Assert.Equal(1000, reader.GetInt16());
        }

        // ---------------------------------------------------------------
        // Int32
        // ---------------------------------------------------------------

        [Fact]
        public void Int32_Value()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveInt32_100000);

            Assert.Equal(VariantPrimitiveType.Int32, reader.PrimitiveType);
            Assert.Equal(100000, reader.GetInt32());
        }

        // ---------------------------------------------------------------
        // Int64
        // ---------------------------------------------------------------

        [Fact]
        public void Int64_Value()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveInt64_2Pow40);

            Assert.Equal(VariantPrimitiveType.Int64, reader.PrimitiveType);
            Assert.Equal(1099511627776L, reader.GetInt64());
        }

        // ---------------------------------------------------------------
        // Float
        // ---------------------------------------------------------------

        [Fact]
        public void Float_Value()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveFloat_1_5);

            Assert.Equal(VariantPrimitiveType.Float, reader.PrimitiveType);
            Assert.True(reader.IsNumeric);
            Assert.Equal(1.5f, reader.GetFloat());
        }

        // ---------------------------------------------------------------
        // Double
        // ---------------------------------------------------------------

        [Fact]
        public void Double_Value()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveDouble_3_14);

            Assert.Equal(VariantPrimitiveType.Double, reader.PrimitiveType);
            Assert.True(reader.IsNumeric);
            Assert.Equal(3.14, reader.GetDouble());
        }

        // ---------------------------------------------------------------
        // Decimal4
        // ---------------------------------------------------------------

        [Fact]
        public void Decimal4_Value()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveDecimal4_123_45);

            Assert.Equal(VariantPrimitiveType.Decimal4, reader.PrimitiveType);
            Assert.True(reader.IsNumeric);
            Assert.Equal(123.45m, reader.GetDecimal4());
        }

        // ---------------------------------------------------------------
        // String (long, primitive type 16)
        // ---------------------------------------------------------------

        [Fact]
        public void String_LongPrimitive()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveString_HelloWorld);

            Assert.Equal(VariantBasicType.Primitive, reader.BasicType);
            Assert.Equal(VariantPrimitiveType.String, reader.PrimitiveType);
            Assert.True(reader.IsString);
            Assert.False(reader.IsNull);
            Assert.Equal("Hello, World!", reader.GetString());
        }

        [Fact]
        public void String_LongPrimitive_GetStringBytes()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveString_HelloWorld);

            ReadOnlySpan<byte> bytes = reader.GetStringBytes();
            Assert.Equal(13, bytes.Length);
            Assert.Equal((byte)'H', bytes[0]);
            Assert.Equal((byte)'!', bytes[12]);
        }

        // ---------------------------------------------------------------
        // Short strings
        // ---------------------------------------------------------------

        [Fact]
        public void ShortString_Hi()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.ShortString_Hi);

            Assert.Equal(VariantBasicType.ShortString, reader.BasicType);
            Assert.True(reader.IsString);
            Assert.False(reader.IsNull);
            Assert.False(reader.IsNumeric);
            Assert.Equal("Hi", reader.GetString());
        }

        [Fact]
        public void ShortString_Empty()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.ShortString_Empty);

            Assert.Equal(VariantBasicType.ShortString, reader.BasicType);
            Assert.True(reader.IsString);
            Assert.Equal("", reader.GetString());
        }

        // ---------------------------------------------------------------
        // Binary
        // ---------------------------------------------------------------

        [Fact]
        public void Binary_DeadBeef()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveBinary_DeadBeef);

            Assert.Equal(VariantPrimitiveType.Binary, reader.PrimitiveType);
            Assert.False(reader.IsString);

            ReadOnlySpan<byte> binary = reader.GetBinary();
            Assert.Equal(4, binary.Length);
            Assert.Equal(0xDE, binary[0]);
            Assert.Equal(0xAD, binary[1]);
            Assert.Equal(0xBE, binary[2]);
            Assert.Equal(0xEF, binary[3]);
        }

        // ---------------------------------------------------------------
        // Date
        // ---------------------------------------------------------------

        [Fact]
        public void Date_DaysSinceEpoch()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveDate_19000);

            Assert.Equal(VariantPrimitiveType.Date, reader.PrimitiveType);
            Assert.Equal(19000, reader.GetDateDays());
        }

        [Fact]
        public void Date_AsDateTime()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveDate_19000);

            DateTime date = reader.GetDate();
            DateTime expected = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(19000);
            Assert.Equal(expected, date);
        }

        // ---------------------------------------------------------------
        // Timestamp (microseconds, with timezone)
        // ---------------------------------------------------------------

        [Fact]
        public void Timestamp_Micros()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveTimestamp_2022);

            Assert.Equal(VariantPrimitiveType.Timestamp, reader.PrimitiveType);
            Assert.Equal(1640995200000000L, reader.GetTimestampMicros());
        }

        [Fact]
        public void Timestamp_AsDateTimeOffset()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveTimestamp_2022);

            DateTimeOffset ts = reader.GetTimestamp();
            Assert.Equal(new DateTimeOffset(2022, 1, 1, 0, 0, 0, TimeSpan.Zero), ts);
        }

        // ---------------------------------------------------------------
        // UUID
        // ---------------------------------------------------------------

        [Fact]
        public void Uuid_Value()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveUuid);

            Assert.Equal(VariantPrimitiveType.Uuid, reader.PrimitiveType);

            Guid expected = new Guid("550e8400-e29b-41d4-a716-446655440000");
            Assert.Equal(expected, reader.GetUuid());
        }

        [Fact]
        public void Uuid_RawBytes()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveUuid);

            ReadOnlySpan<byte> raw = reader.GetUuidBytes();
            Assert.Equal(16, raw.Length);
            // First 4 bytes should be 0x550E8400 (big-endian)
            Assert.Equal(0x55, raw[0]);
            Assert.Equal(0x0E, raw[1]);
            Assert.Equal(0x84, raw[2]);
            Assert.Equal(0x00, raw[3]);
        }

        // ---------------------------------------------------------------
        // Type mismatch errors
        // ---------------------------------------------------------------

        [Fact]
        public void GetInt32_OnString_Throws()
        {
            Assert.Throws<InvalidOperationException>(() =>
                new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveString_HelloWorld).GetInt32());
        }

        [Fact]
        public void GetBoolean_OnNull_Throws()
        {
            Assert.Throws<InvalidOperationException>(() =>
                new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveNull).GetBoolean());
        }

        [Fact]
        public void GetString_OnInt_Throws()
        {
            Assert.Throws<InvalidOperationException>(() =>
                new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveInt32_100000).GetString());
        }

        [Fact]
        public void GetInt8_OnObject_Throws()
        {
            Assert.Throws<InvalidOperationException>(() =>
                new VariantReader(TestVectors.EmptyMetadata, TestVectors.ObjectEmpty).GetInt8());
        }

        // ---------------------------------------------------------------
        // Object/Array type inspection
        // ---------------------------------------------------------------

        [Fact]
        public void Object_TypeInspection()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.ObjectEmpty);

            Assert.Equal(VariantBasicType.Object, reader.BasicType);
            Assert.True(reader.IsObject);
            Assert.False(reader.IsArray);
            Assert.False(reader.IsNull);
            Assert.False(reader.IsString);
            Assert.False(reader.IsNumeric);
        }

        [Fact]
        public void Array_TypeInspection()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.ArrayEmpty);

            Assert.Equal(VariantBasicType.Array, reader.BasicType);
            Assert.True(reader.IsArray);
            Assert.False(reader.IsObject);
            Assert.False(reader.IsNull);
            Assert.False(reader.IsString);
            Assert.False(reader.IsNumeric);
        }
    }
}
