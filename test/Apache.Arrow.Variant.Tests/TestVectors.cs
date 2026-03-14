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

namespace Apache.Arrow.Variant.Tests
{
    /// <summary>
    /// Hand-crafted binary test vectors for the variant encoding.
    /// All multi-byte integers are little-endian per the spec.
    /// </summary>
    internal static class TestVectors
    {
        // =================================================================
        // Metadata test vectors
        // =================================================================

        /// <summary>
        /// Empty metadata: version 1, unsorted, 1-byte offsets, 0 dictionary entries.
        /// Layout: [header=0x01] [dict_size=0x00] [offset=0x00]
        /// </summary>
        public static ReadOnlySpan<byte> EmptyMetadata => new byte[]
        {
            0x01,       // header: version=1, sorted=0, offset_size=1
            0x00,       // dictionary_size = 0
            0x00,       // single offset (end marker) = 0
        };

        /// <summary>
        /// Metadata with 2 unsorted strings: "b", "a".
        /// 1-byte offsets, unsorted.
        /// Layout: [header] [dict_size=2] [offset0=0] [offset1=1] [offset2=2] "ba"
        /// </summary>
        public static ReadOnlySpan<byte> UnsortedMetadata_B_A => new byte[]
        {
            0x01,       // header: version=1, sorted=0, offset_size=1
            0x02,       // dictionary_size = 2
            0x00,       // offset[0] = 0
            0x01,       // offset[1] = 1
            0x02,       // offset[2] = 2
            (byte)'b',  // string 0: "b"
            (byte)'a',  // string 1: "a"
        };

        /// <summary>
        /// Metadata with 3 sorted strings: "alpha", "beta", "gamma".
        /// 1-byte offsets, sorted.
        /// </summary>
        public static ReadOnlySpan<byte> SortedMetadata_Alpha_Beta_Gamma => new byte[]
        {
            0x21,       // header: version=1, sorted=1, offset_size=1
                        //   binary: 0010_0001 => bits 0-3=0001 (ver 1), bit 5=1 (sorted), bits 6-7=00 (offset_size=1)
            0x03,       // dictionary_size = 3
            0x00,       // offset[0] = 0
            0x05,       // offset[1] = 5
            0x09,       // offset[2] = 9
            0x0E,       // offset[3] = 14
            // "alpha" (5 bytes)
            (byte)'a', (byte)'l', (byte)'p', (byte)'h', (byte)'a',
            // "beta" (4 bytes)
            (byte)'b', (byte)'e', (byte)'t', (byte)'a',
            // "gamma" (5 bytes)
            (byte)'g', (byte)'a', (byte)'m', (byte)'m', (byte)'a',
        };

        /// <summary>
        /// Metadata with 2-byte offsets, sorted, 2 strings: "hello", "world".
        /// </summary>
        public static ReadOnlySpan<byte> SortedMetadata2ByteOffsets_Hello_World => new byte[]
        {
            0x61,       // header: version=1, sorted=1, offset_size=2
                        //   binary: 0110_0001 => bits 0-3=0001 (ver 1), bit 5=1 (sorted), bits 6-7=01 (offset_size=2)
            0x02, 0x00, // dictionary_size = 2
            0x00, 0x00, // offset[0] = 0
            0x05, 0x00, // offset[1] = 5
            0x0A, 0x00, // offset[2] = 10
            // "hello" (5 bytes)
            (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o',
            // "world" (5 bytes)
            (byte)'w', (byte)'o', (byte)'r', (byte)'l', (byte)'d',
        };

        /// <summary>
        /// Metadata with a single string "name", 1-byte offsets, sorted.
        /// Used as metadata for object/field test vectors.
        /// </summary>
        public static ReadOnlySpan<byte> SortedMetadata_Name => new byte[]
        {
            0x21,       // header: version=1, sorted=1, offset_size=1
            0x01,       // dictionary_size = 1
            0x00,       // offset[0] = 0
            0x04,       // offset[1] = 4
            (byte)'n', (byte)'a', (byte)'m', (byte)'e',
        };

        /// <summary>
        /// Metadata with 2 sorted strings: "age", "name".
        /// 1-byte offsets, sorted.
        /// </summary>
        public static ReadOnlySpan<byte> SortedMetadata_Age_Name => new byte[]
        {
            0x21,       // header: version=1, sorted=1, offset_size=1
            0x02,       // dictionary_size = 2
            0x00,       // offset[0] = 0
            0x03,       // offset[1] = 3
            0x07,       // offset[2] = 7
            (byte)'a', (byte)'g', (byte)'e',
            (byte)'n', (byte)'a', (byte)'m', (byte)'e',
        };

        // =================================================================
        // Primitive value test vectors
        // =================================================================

        /// <summary>Null: header byte only (basic_type=0, primitive_type=0).</summary>
        public static ReadOnlySpan<byte> PrimitiveNull => new byte[]
        {
            0x00,       // basic_type=Primitive(0), primitive_type=NullType(0)
        };

        /// <summary>Boolean true: header byte only.</summary>
        public static ReadOnlySpan<byte> PrimitiveBoolTrue => new byte[]
        {
            0x04,       // basic_type=Primitive(0), primitive_type=BooleanTrue(1) => (1 << 2) | 0 = 4
        };

        /// <summary>Boolean false: header byte only.</summary>
        public static ReadOnlySpan<byte> PrimitiveBoolFalse => new byte[]
        {
            0x08,       // basic_type=Primitive(0), primitive_type=BooleanFalse(2) => (2 << 2) | 0 = 8
        };

        /// <summary>Int8 value = 42.</summary>
        public static ReadOnlySpan<byte> PrimitiveInt8_42 => new byte[]
        {
            0x0C,       // (3 << 2) | 0 = 12
            0x2A,       // 42
        };

        /// <summary>Int8 value = -1 (0xFF signed).</summary>
        public static ReadOnlySpan<byte> PrimitiveInt8_Neg1 => new byte[]
        {
            0x0C,       // (3 << 2) | 0 = 12
            0xFF,       // -1 as signed byte
        };

        /// <summary>Int16 value = 1000.</summary>
        public static ReadOnlySpan<byte> PrimitiveInt16_1000 => new byte[]
        {
            0x10,       // (4 << 2) | 0 = 16
            0xE8, 0x03, // 1000 LE
        };

        /// <summary>Int32 value = 100000.</summary>
        public static ReadOnlySpan<byte> PrimitiveInt32_100000 => new byte[]
        {
            0x14,       // (5 << 2) | 0 = 20
            0xA0, 0x86, 0x01, 0x00, // 100000 LE
        };

        /// <summary>Int64 value = 1099511627776 (2^40).</summary>
        public static ReadOnlySpan<byte> PrimitiveInt64_2Pow40 => new byte[]
        {
            0x18,       // (6 << 2) | 0 = 24
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, // 2^40 LE
        };

        /// <summary>Double value = 3.14.</summary>
        public static ReadOnlySpan<byte> PrimitiveDouble_3_14 => new byte[]
        {
            0x1C,       // (7 << 2) | 0 = 28
            0x1F, 0x85, 0xEB, 0x51, 0xB8, 0x1E, 0x09, 0x40, // 3.14 IEEE 754 LE
        };

        /// <summary>Float value = 1.5f.</summary>
        public static ReadOnlySpan<byte> PrimitiveFloat_1_5 => new byte[]
        {
            0x38,       // (14 << 2) | 0 = 56
            0x00, 0x00, 0xC0, 0x3F, // 1.5f IEEE 754 LE
        };

        /// <summary>Decimal4: scale=2, value=12345 => 123.45.</summary>
        public static ReadOnlySpan<byte> PrimitiveDecimal4_123_45 => new byte[]
        {
            0x20,       // (8 << 2) | 0 = 32
            0x02,       // scale = 2
            0x39, 0x30, 0x00, 0x00, // 12345 LE (4 bytes)
        };

        /// <summary>
        /// Long string (primitive type 16): "Hello, World!" with 4-byte length prefix.
        /// </summary>
        public static ReadOnlySpan<byte> PrimitiveString_HelloWorld => new byte[]
        {
            0x40,       // (16 << 2) | 0 = 64
            0x0D, 0x00, 0x00, 0x00, // length = 13
            (byte)'H', (byte)'e', (byte)'l', (byte)'l', (byte)'o',
            (byte)',', (byte)' ',
            (byte)'W', (byte)'o', (byte)'r', (byte)'l', (byte)'d', (byte)'!',
        };

        /// <summary>
        /// Binary (primitive type 15): 4 bytes [0xDE, 0xAD, 0xBE, 0xEF].
        /// </summary>
        public static ReadOnlySpan<byte> PrimitiveBinary_DeadBeef => new byte[]
        {
            0x3C,       // (15 << 2) | 0 = 60
            0x04, 0x00, 0x00, 0x00, // length = 4
            0xDE, 0xAD, 0xBE, 0xEF,
        };

        /// <summary>
        /// Date (primitive type 11): days since epoch = 19000 (2022-01-01 is ~18993).
        /// </summary>
        public static ReadOnlySpan<byte> PrimitiveDate_19000 => new byte[]
        {
            0x2C,       // (11 << 2) | 0 = 44
            0x38, 0x4A, 0x00, 0x00, // 19000 LE
        };

        /// <summary>
        /// Timestamp (type 12): microseconds since epoch = 1640995200000000 (2022-01-01T00:00:00Z).
        /// </summary>
        public static ReadOnlySpan<byte> PrimitiveTimestamp_2022 => new byte[]
        {
            0x30,       // (12 << 2) | 0 = 48
            0x00, 0x60, 0xF9, 0xF7, 0x79, 0xD4, 0x05, 0x00, // 1640995200000000 LE
        };

        /// <summary>
        /// UUID: 16 bytes big-endian. Value: 550e8400-e29b-41d4-a716-446655440000.
        /// </summary>
        public static ReadOnlySpan<byte> PrimitiveUuid => new byte[]
        {
            0x50,       // (20 << 2) | 0 = 80
            0x55, 0x0E, 0x84, 0x00,
            0xE2, 0x9B, 0x41, 0xD4,
            0xA7, 0x16, 0x44, 0x66,
            0x55, 0x44, 0x00, 0x00,
        };

        // =================================================================
        // Short string test vectors
        // =================================================================

        /// <summary>Short string "Hi" (length 2).</summary>
        public static ReadOnlySpan<byte> ShortString_Hi => new byte[]
        {
            0x09,       // basic_type=ShortString(1), length=2 => (2 << 2) | 1 = 9
            (byte)'H', (byte)'i',
        };

        /// <summary>Short string "" (length 0).</summary>
        public static ReadOnlySpan<byte> ShortString_Empty => new byte[]
        {
            0x01,       // basic_type=ShortString(1), length=0 => (0 << 2) | 1 = 1
        };

        // =================================================================
        // Object test vectors
        // =================================================================

        /// <summary>
        /// Empty object: 0 fields, field_id_size=1, offset_size=1, is_large=false.
        /// Layout: [header] [num_fields=0]
        /// </summary>
        public static ReadOnlySpan<byte> ObjectEmpty => new byte[]
        {
            0x02,       // basic_type=Object(2), field_id_size=1, offset_size=1, is_large=false
                        //   value_header bits: 00_00_00 => (0<<2)|2 = 2
            0x00,       // num_fields = 0 (1 byte since is_large=false)
        };

        /// <summary>
        /// Object with 1 field: {"name": "Alice"}
        /// Uses SortedMetadata_Name for metadata (field 0 = "name").
        /// field_id_size=1, offset_size=1, is_large=false.
        /// Layout: [header] [num_fields=1] [field_id=0] [offset=0] [end_offset=7]
        ///         [value: short string "Alice"]
        /// </summary>
        public static ReadOnlySpan<byte> Object_Name_Alice => new byte[]
        {
            0x02,       // header: basic_type=Object(2), fid_size=1, off_size=1, is_large=false
            0x01,       // num_fields = 1
            0x00,       // field_id[0] = 0 (=> "name" in SortedMetadata_Name)
            0x00,       // offset[0] = 0
            0x06,       // end_offset = 6
            // value: short string "Alice" (5 bytes)
            0x15,       // basic_type=ShortString(1), length=5 => (5 << 2) | 1 = 21
            (byte)'A', (byte)'l', (byte)'i', (byte)'c', (byte)'e',
        };

        /// <summary>
        /// Object with 2 fields: {"age": 30, "name": "Bob"}
        /// Uses SortedMetadata_Age_Name for metadata (field 0 = "age", field 1 = "name").
        /// field_id_size=1, offset_size=1, is_large=false.
        /// Field IDs must be sorted by field name order in metadata.
        /// </summary>
        public static ReadOnlySpan<byte> Object_Age30_Name_Bob => new byte[]
        {
            0x02,       // header: basic_type=Object(2), fid_size=1, off_size=1, is_large=false
            0x02,       // num_fields = 2
            0x00,       // field_id[0] = 0 (=> "age")
            0x01,       // field_id[1] = 1 (=> "name")
            0x00,       // offset[0] = 0
            0x02,       // offset[1] = 2
            0x06,       // end_offset = 6
            // value 0: Int8 = 30
            0x0C,       // primitive Int8 header
            0x1E,       // 30
            // value 1: short string "Bob"
            0x0D,       // basic_type=ShortString(1), length=3 => (3 << 2) | 1 = 13
            (byte)'B', (byte)'o', (byte)'b',
        };

        // =================================================================
        // Array test vectors
        // =================================================================

        /// <summary>
        /// Empty array: 0 elements, offset_size=1, is_large=false.
        /// Layout: [header] [num_elements=0]
        /// </summary>
        public static ReadOnlySpan<byte> ArrayEmpty => new byte[]
        {
            0x03,       // basic_type=Array(3), offset_size=1, is_large=false
                        //   value_header: 00_0_00 => (0<<2)|3 = 3
            0x00,       // num_elements = 0 (1 byte)
        };

        /// <summary>
        /// Array with 3 elements: [1, 2, 3] as Int8 values.
        /// offset_size=1, is_large=false.
        /// Layout: [header] [num_elements=3] [off0=0] [off1=2] [off2=4] [end=6]
        ///         [Int8(1)] [Int8(2)] [Int8(3)]
        /// </summary>
        public static ReadOnlySpan<byte> Array_Int8_1_2_3 => new byte[]
        {
            0x03,       // header: basic_type=Array(3), offset_size=1, is_large=false
            0x03,       // num_elements = 3
            0x00,       // offset[0] = 0
            0x02,       // offset[1] = 2
            0x04,       // offset[2] = 4
            0x06,       // end_offset = 6
            // element 0: Int8 = 1
            0x0C, 0x01,
            // element 1: Int8 = 2
            0x0C, 0x02,
            // element 2: Int8 = 3
            0x0C, 0x03,
        };

        /// <summary>
        /// Array with mixed types: [42, "hi", null].
        /// offset_size=1, is_large=false.
        /// </summary>
        public static ReadOnlySpan<byte> Array_Mixed => new byte[]
        {
            0x03,       // header: basic_type=Array(3), offset_size=1, is_large=false
            0x03,       // num_elements = 3
            0x00,       // offset[0] = 0
            0x02,       // offset[1] = 2
            0x05,       // offset[2] = 5
            0x06,       // end_offset = 6
            // element 0: Int8 = 42
            0x0C, 0x2A,
            // element 1: short string "hi" (2 bytes)
            0x09, (byte)'h', (byte)'i',
            // element 2: null
            0x00,
        };
    }
}
