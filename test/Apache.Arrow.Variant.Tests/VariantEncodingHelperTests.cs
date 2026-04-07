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
    public class VariantEncodingHelperTests
    {
        // ---------------------------------------------------------------
        // Value header: basic type + value header round-trip
        // ---------------------------------------------------------------

        [Theory]
        [InlineData(VariantBasicType.Primitive, 0)]
        [InlineData(VariantBasicType.Primitive, 20)]
        [InlineData(VariantBasicType.ShortString, 0)]
        [InlineData(VariantBasicType.ShortString, 63)]
        [InlineData(VariantBasicType.Object, 0)]
        [InlineData(VariantBasicType.Array, 0)]
        public void MakeAndParseValueHeader(VariantBasicType basicType, int valueHeader)
        {
            byte header = VariantEncodingHelper.MakeValueHeader(basicType, valueHeader);
            Assert.Equal(basicType, VariantEncodingHelper.GetBasicType(header));
            Assert.Equal(valueHeader, VariantEncodingHelper.GetValueHeader(header));
        }

        // ---------------------------------------------------------------
        // Primitive headers
        // ---------------------------------------------------------------

        [Theory]
        [InlineData(VariantPrimitiveType.NullType)]
        [InlineData(VariantPrimitiveType.BooleanTrue)]
        [InlineData(VariantPrimitiveType.BooleanFalse)]
        [InlineData(VariantPrimitiveType.Int8)]
        [InlineData(VariantPrimitiveType.Int32)]
        [InlineData(VariantPrimitiveType.Int64)]
        [InlineData(VariantPrimitiveType.Double)]
        [InlineData(VariantPrimitiveType.String)]
        [InlineData(VariantPrimitiveType.Binary)]
        [InlineData(VariantPrimitiveType.Uuid)]
        public void MakeAndParsePrimitiveHeader(VariantPrimitiveType primitiveType)
        {
            byte header = VariantEncodingHelper.MakePrimitiveHeader(primitiveType);
            Assert.Equal(VariantBasicType.Primitive, VariantEncodingHelper.GetBasicType(header));
            Assert.Equal(primitiveType, VariantEncodingHelper.GetPrimitiveType(header));
        }

        // ---------------------------------------------------------------
        // Short string headers
        // ---------------------------------------------------------------

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(32)]
        [InlineData(63)]
        public void MakeAndParseShortStringHeader(int length)
        {
            byte header = VariantEncodingHelper.MakeShortStringHeader(length);
            Assert.Equal(VariantBasicType.ShortString, VariantEncodingHelper.GetBasicType(header));
            Assert.Equal(length, VariantEncodingHelper.GetShortStringLength(header));
        }

        // ---------------------------------------------------------------
        // Object headers
        // ---------------------------------------------------------------

        [Theory]
        [InlineData(1, 1, false)]
        [InlineData(1, 1, true)]
        [InlineData(2, 3, false)]
        [InlineData(4, 4, true)]
        public void MakeAndParseObjectHeader(int fieldIdSize, int offsetSize, bool isLarge)
        {
            byte header = VariantEncodingHelper.MakeObjectHeader(fieldIdSize, offsetSize, isLarge);
            Assert.Equal(VariantBasicType.Object, VariantEncodingHelper.GetBasicType(header));

            VariantEncodingHelper.ParseObjectHeader(header, out int parsedFieldIdSize, out int parsedOffsetSize, out bool parsedIsLarge);
            Assert.Equal(fieldIdSize, parsedFieldIdSize);
            Assert.Equal(offsetSize, parsedOffsetSize);
            Assert.Equal(isLarge, parsedIsLarge);
        }

        // ---------------------------------------------------------------
        // Array headers
        // ---------------------------------------------------------------

        [Theory]
        [InlineData(1, false)]
        [InlineData(1, true)]
        [InlineData(2, false)]
        [InlineData(4, true)]
        public void MakeAndParseArrayHeader(int offsetSize, bool isLarge)
        {
            byte header = VariantEncodingHelper.MakeArrayHeader(offsetSize, isLarge);
            Assert.Equal(VariantBasicType.Array, VariantEncodingHelper.GetBasicType(header));

            VariantEncodingHelper.ParseArrayHeader(header, out int parsedOffsetSize, out bool parsedIsLarge);
            Assert.Equal(offsetSize, parsedOffsetSize);
            Assert.Equal(isLarge, parsedIsLarge);
        }

        // ---------------------------------------------------------------
        // Metadata header
        // ---------------------------------------------------------------

        [Theory]
        [InlineData(false, 1)]
        [InlineData(true, 1)]
        [InlineData(false, 4)]
        [InlineData(true, 4)]
        public void MakeAndParseMetadataHeader(bool sortedStrings, int offsetSize)
        {
            byte header = VariantEncodingHelper.MakeMetadataHeader(sortedStrings, offsetSize);

            VariantEncodingHelper.ParseMetadataHeader(header, out int version, out bool parsedSorted, out int parsedOffsetSize);
            Assert.Equal(VariantEncodingHelper.MetadataVersion, version);
            Assert.Equal(sortedStrings, parsedSorted);
            Assert.Equal(offsetSize, parsedOffsetSize);
        }

        // ---------------------------------------------------------------
        // Little-endian integer read/write round-trips
        // ---------------------------------------------------------------

        [Theory]
        [InlineData(1, 0)]
        [InlineData(1, 255)]
        [InlineData(2, 0)]
        [InlineData(2, 12345)]
        [InlineData(2, 65535)]
        [InlineData(3, 0)]
        [InlineData(3, 1000000)]
        [InlineData(4, 0)]
        [InlineData(4, int.MaxValue)]
        public void ReadWriteLittleEndianInt(int byteWidth, int value)
        {
            byte[] buffer = new byte[4];
            Span<byte> span = buffer.AsSpan();

            VariantEncodingHelper.WriteLittleEndianInt(span, value, byteWidth);

            int result = VariantEncodingHelper.ReadLittleEndianInt(new ReadOnlySpan<byte>(buffer), byteWidth);
            Assert.Equal(value, result);
        }

        [Fact]
        public void ReadLittleEndianInt_3Byte_CorrectByteOrder()
        {
            // 0x030201 = 197121
            byte[] buffer = new byte[] { 0x01, 0x02, 0x03 };
            int result = VariantEncodingHelper.ReadLittleEndianInt(buffer, 3);
            Assert.Equal(0x030201, result);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(5)]
        [InlineData(-1)]
        public void ReadWriteLittleEndianInt_InvalidByteWidth_Throws(int byteWidth)
        {
            byte[] buffer = new byte[4];
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                VariantEncodingHelper.ReadLittleEndianInt(buffer, byteWidth));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                VariantEncodingHelper.WriteLittleEndianInt(buffer, 0, byteWidth));
        }

        // ---------------------------------------------------------------
        // ByteWidthForValue
        // ---------------------------------------------------------------

        [Theory]
        [InlineData(0, 1)]
        [InlineData(255, 1)]
        [InlineData(256, 2)]
        [InlineData(65535, 2)]
        [InlineData(65536, 3)]
        [InlineData(0xFFFFFF, 3)]
        [InlineData(0x1000000, 4)]
        [InlineData(int.MaxValue, 4)]
        public void ByteWidthForValue_ReturnsCorrectWidth(int value, int expectedWidth)
        {
            Assert.Equal(expectedWidth, VariantEncodingHelper.ByteWidthForValue(value));
        }
    }
}
