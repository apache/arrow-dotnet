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
using System.Buffers.Binary;

namespace Apache.Arrow.Variant
{
    /// <summary>
    /// Internal helpers for reading and writing variant binary encoding.
    /// </summary>
    internal static class VariantEncodingHelper
    {
        // ---------------------------------------------------------------
        // Unix epoch constants
        // ---------------------------------------------------------------

        internal static readonly DateTime UnixEpochUtc =
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        internal static readonly DateTime UnixEpochUnspecified =
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        internal static readonly DateTimeOffset UnixEpochOffset =
            new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // ---------------------------------------------------------------
        // Value header byte layout
        // ---------------------------------------------------------------
        //
        //  +------+------+------+------+------+------+------+------+
        //  | bit7 | bit6 | bit5 | bit4 | bit3 | bit2 | bit1 | bit0 |
        //  +------+------+------+------+------+------+------+------+
        //  |       value_header (6 bits)       | basic_type (2 bits)|
        //  +------+------+------+------+------+------+------+------+

        private const int BasicTypeMask = 0x03;
        private const int ValueHeaderShift = 2;

        /// <summary>
        /// Extracts the basic type (bits 0-1) from a value header byte.
        /// </summary>
        public static VariantBasicType GetBasicType(byte header) =>
            (VariantBasicType)(header & BasicTypeMask);

        /// <summary>
        /// Extracts the value header (bits 2-7) from a value header byte.
        /// For primitives this is the primitive type ID; for short strings this is the length.
        /// </summary>
        public static int GetValueHeader(byte header) =>
            header >> ValueHeaderShift;

        /// <summary>
        /// Builds a value header byte from a basic type and a 6-bit value header.
        /// </summary>
        public static byte MakeValueHeader(VariantBasicType basicType, int valueHeader) =>
            (byte)((valueHeader << ValueHeaderShift) | (int)basicType);

        // ---------------------------------------------------------------
        // Primitive value header
        // ---------------------------------------------------------------

        /// <summary>
        /// Extracts the primitive type from a value header byte whose basic type is Primitive.
        /// </summary>
        public static VariantPrimitiveType GetPrimitiveType(byte header) =>
            (VariantPrimitiveType)GetValueHeader(header);

        /// <summary>
        /// Builds a value header byte for a primitive type.
        /// </summary>
        public static byte MakePrimitiveHeader(VariantPrimitiveType primitiveType) =>
            MakeValueHeader(VariantBasicType.Primitive, (int)primitiveType);

        // ---------------------------------------------------------------
        // Short string value header
        // ---------------------------------------------------------------

        /// <summary>
        /// Extracts the short string length from a value header byte whose basic type is ShortString.
        /// </summary>
        public static int GetShortStringLength(byte header) =>
            GetValueHeader(header);

        /// <summary>
        /// Builds a value header byte for a short string of the given byte length (0-63).
        /// </summary>
        public static byte MakeShortStringHeader(int length) =>
            MakeValueHeader(VariantBasicType.ShortString, length);

        // ---------------------------------------------------------------
        // Object value header
        // ---------------------------------------------------------------
        //
        //  Bits 2-3: field_id_size - 1 (0-3 => 1-4 bytes)
        //  Bits 4-5: field_offset_size - 1 (0-3 => 1-4 bytes)
        //  Bit  6:   is_large (0 = 1-byte num_fields, 1 = 4-byte num_fields)
        //  Bit  7:   unused (must be 0)

        /// <summary>
        /// Builds a value header byte for an object.
        /// </summary>
        /// <param name="fieldIdSize">Number of bytes per field ID (1-4).</param>
        /// <param name="offsetSize">Number of bytes per offset (1-4).</param>
        /// <param name="isLarge">Whether the field count is stored as 4 bytes instead of 1.</param>
        public static byte MakeObjectHeader(int fieldIdSize, int offsetSize, bool isLarge)
        {
            int valueHeader =
                ((fieldIdSize - 1) & 0x03) |
                (((offsetSize - 1) & 0x03) << 2) |
                ((isLarge ? 1 : 0) << 4);
            return MakeValueHeader(VariantBasicType.Object, valueHeader);
        }

        /// <summary>
        /// Parses an object value header byte.
        /// </summary>
        public static void ParseObjectHeader(byte header, out int fieldIdSize, out int offsetSize, out bool isLarge)
        {
            int valueHeader = GetValueHeader(header);
            fieldIdSize = (valueHeader & 0x03) + 1;
            offsetSize = ((valueHeader >> 2) & 0x03) + 1;
            isLarge = ((valueHeader >> 4) & 0x01) != 0;
        }

        // ---------------------------------------------------------------
        // Array value header
        // ---------------------------------------------------------------
        //
        //  Bits 2-3: offset_size - 1 (0-3 => 1-4 bytes)
        //  Bit  4:   is_large (0 = 1-byte num_elements, 1 = 4-byte num_elements)
        //  Bits 5-7: unused (must be 0)

        /// <summary>
        /// Builds a value header byte for an array.
        /// </summary>
        /// <param name="offsetSize">Number of bytes per offset (1-4).</param>
        /// <param name="isLarge">Whether the element count is stored as 4 bytes instead of 1.</param>
        public static byte MakeArrayHeader(int offsetSize, bool isLarge)
        {
            int valueHeader =
                ((offsetSize - 1) & 0x03) |
                ((isLarge ? 1 : 0) << 2);
            return MakeValueHeader(VariantBasicType.Array, valueHeader);
        }

        /// <summary>
        /// Parses an array value header byte.
        /// </summary>
        public static void ParseArrayHeader(byte header, out int offsetSize, out bool isLarge)
        {
            int valueHeader = GetValueHeader(header);
            offsetSize = (valueHeader & 0x03) + 1;
            isLarge = ((valueHeader >> 2) & 0x01) != 0;
        }

        // ---------------------------------------------------------------
        // Metadata header byte
        // ---------------------------------------------------------------
        //
        //  Bits 0-3: version (must be 1)
        //  Bit  4:   reserved (must be 0)
        //  Bit  5:   sorted_strings (1 if dictionary strings are sorted)
        //  Bits 6-7: offset_size - 1 (0-3 => 1-4 bytes)

        private const int MetadataVersionMask = 0x0F;
        private const int MetadataSortedBit = 5;
        private const int MetadataOffsetSizeShift = 6;
        public const int MetadataVersion = 1;

        /// <summary>
        /// Parses the metadata header byte.
        /// </summary>
        public static void ParseMetadataHeader(byte header, out int version, out bool sortedStrings, out int offsetSize)
        {
            version = header & MetadataVersionMask;
            sortedStrings = ((header >> MetadataSortedBit) & 0x01) != 0;
            offsetSize = ((header >> MetadataOffsetSizeShift) & 0x03) + 1;
        }

        /// <summary>
        /// Builds a metadata header byte.
        /// </summary>
        public static byte MakeMetadataHeader(bool sortedStrings, int offsetSize) =>
            (byte)(
                MetadataVersion |
                ((sortedStrings ? 1 : 0) << MetadataSortedBit) |
                (((offsetSize - 1) & 0x03) << MetadataOffsetSizeShift));

        // ---------------------------------------------------------------
        // Variable-width little-endian integer reading/writing (1-4 bytes)
        // ---------------------------------------------------------------

        /// <summary>
        /// Reads a 1-, 2-, 3-, or 4-byte unsigned little-endian integer from a span.
        /// </summary>
        public static int ReadLittleEndianInt(ReadOnlySpan<byte> span, int byteWidth)
        {
            switch (byteWidth)
            {
                case 1:
                    return span[0];
                case 2:
                    return BinaryPrimitives.ReadUInt16LittleEndian(span);
                case 3:
                    return span[0] | (span[1] << 8) | (span[2] << 16);
                case 4:
                    return BinaryPrimitives.ReadInt32LittleEndian(span);
                default:
                    throw new ArgumentOutOfRangeException(nameof(byteWidth), byteWidth, "Byte width must be 1, 2, 3, or 4.");
            }
        }

        /// <summary>
        /// Writes a 1-, 2-, 3-, or 4-byte unsigned little-endian integer to a span.
        /// </summary>
        public static void WriteLittleEndianInt(Span<byte> span, int value, int byteWidth)
        {
            switch (byteWidth)
            {
                case 1:
                    span[0] = (byte)value;
                    break;
                case 2:
                    BinaryPrimitives.WriteUInt16LittleEndian(span, (ushort)value);
                    break;
                case 3:
                    span[0] = (byte)value;
                    span[1] = (byte)(value >> 8);
                    span[2] = (byte)(value >> 16);
                    break;
                case 4:
                    BinaryPrimitives.WriteInt32LittleEndian(span, value);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(byteWidth), byteWidth, "Byte width must be 1, 2, 3, or 4.");
            }
        }

        /// <summary>
        /// Returns the minimum number of bytes (1-4) needed to represent the given non-negative integer value.
        /// </summary>
        public static int ByteWidthForValue(int value)
        {
            if (value <= 0xFF) return 1;
            if (value <= 0xFFFF) return 2;
            if (value <= 0xFFFFFF) return 3;
            return 4;
        }
    }
}
