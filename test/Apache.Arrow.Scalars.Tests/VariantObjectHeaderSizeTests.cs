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
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Scalars.Tests
{
    /// <summary>
    /// End-to-end coverage of the object value header's <c>field_id_size</c> and
    /// <c>offset_size</c> bits, over the matrix of widths the two can take.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="VariantEncodingHelperTests"/> pins <c>MakeObjectHeader</c> and
    /// <c>ParseObjectHeader</c> to literal header bytes, but only at the helper level: it says
    /// nothing about which widths <see cref="VariantValueWriter"/> actually asks for, or whether
    /// the body it emits is laid out at those widths. These tests build real objects — a padded
    /// dictionary to drive the field IDs up, a padded value to drive the data length up — assert
    /// the header byte that lands in the output, and then decode the object body by hand against
    /// the spec rather than through the library.
    /// </para>
    /// <para>
    /// The interesting cells are the asymmetric ones. When <c>field_id_size == offset_size</c>
    /// the two 2-bit fields are interchangeable and a transposition is invisible. The writer
    /// computes them independently — IDs from the metadata dictionary, offsets from the encoded
    /// data length — so in practice they diverge routinely.
    /// </para>
    /// <para>
    /// Per <c>apache/parquet-format</c> <c>VariantEncoding.md</c>, an object header byte is:
    /// </para>
    /// <code>
    ///   bits 0-1: basic_type = 2 (Object)
    ///   bits 2-3: field_offset_size_minus_one
    ///   bits 4-5: field_id_size_minus_one
    ///   bit  6:   is_large
    ///   bit  7:   unused
    /// </code>
    /// <para>
    /// Width 4 is not reachable from a real object in a unit test: a 4-byte field ID needs a
    /// metadata dictionary of more than 16,777,216 entries, and a 4-byte offset needs more than
    /// 16 MiB of field data. Those cells stay covered at the helper level only.
    /// </para>
    /// </remarks>
    public class VariantObjectHeaderSizeTests
    {
        // Smallest values that need 2- and 3-byte encoding, for both field IDs and offsets.
        private const int TwoByteThreshold = 0x100;
        private const int ThreeByteThreshold = 0x10000;

        // Padded value lengths that put the object's end offset in each width's band, with room
        // to spare for the remaining fields.
        private const int PadForTwoByteOffsets = 300;
        private const int PadForThreeByteOffsets = 70000;

        private const int SmallFieldCount = 2;

        // is_large is set for more than 255 fields.
        private const int LargeFieldCount = 300;

        // ---------------------------------------------------------------
        // The matrix
        // ---------------------------------------------------------------
        //
        // expected header = (field_id_size - 1) << 4 | (offset_size - 1) << 2 | Object(2)

        [Theory]
        [InlineData(1, 1, 0x02)]
        [InlineData(1, 2, 0x06)]
        [InlineData(1, 3, 0x0A)]
        [InlineData(2, 1, 0x12)]
        [InlineData(2, 2, 0x16)]
        [InlineData(2, 3, 0x1A)]
        [InlineData(3, 1, 0x22)]
        [InlineData(3, 2, 0x26)]
        [InlineData(3, 3, 0x2A)]
        public void ObjectHeaderUsesSpecBitLayout(int fieldIdSize, int offsetSize, int expectedHeader) =>
            AssertObjectHeader(fieldIdSize, offsetSize, SmallFieldCount, expectedHeader);

        // Objects with more than 255 fields also set is_large (bit 6), on top of the two size
        // fields. Such an object always carries at least 2-byte IDs (its own IDs run past 255)
        // and at least 2-byte offsets (300 values do not fit in 255 bytes).

        [Theory]
        [InlineData(2, 2, 0x56)]
        [InlineData(2, 3, 0x5A)]
        [InlineData(3, 2, 0x66)]
        [InlineData(3, 3, 0x6A)]
        public void LargeObjectHeaderUsesSpecBitLayout(int fieldIdSize, int offsetSize, int expectedHeader) =>
            AssertObjectHeader(fieldIdSize, offsetSize, LargeFieldCount, expectedHeader);

        private static void AssertObjectHeader(int fieldIdSize, int offsetSize, int fieldCount, int expectedHeader)
        {
            EncodedObject encoded = BuildObject(fieldIdSize, offsetSize, fieldCount);

            Assert.Equal(expectedHeader, (int)encoded.Value[0]);

            // Decoded here rather than through VariantEncodingHelper: the point is to check the
            // writer against the spec, not against the reader that shares its convention.
            ObjectLayout layout = DecodeObjectPerSpec(encoded.Value);
            Assert.Equal(fieldIdSize, layout.FieldIdSize);
            Assert.Equal(offsetSize, layout.OffsetSize);
            Assert.Equal(fieldCount > 255, layout.IsLarge);
            Assert.Equal(fieldCount, layout.FieldCount);

            // The IDs and offsets must make sense when read at the declared widths. Had the
            // writer laid the body out at the other width, these lists would be garbage even
            // though the header byte above is the one the spec asks for.
            for (int i = 0; i < fieldCount; i++)
            {
                Assert.Equal(encoded.FirstFieldId + i, layout.FieldIds[i]);
            }

            Assert.Equal(0, layout.Offsets[0]);
            for (int i = 0; i < fieldCount; i++)
            {
                Assert.True(layout.Offsets[i + 1] > layout.Offsets[i], "offsets are not increasing at index " + i);
            }
            Assert.Equal(encoded.Value.Length - layout.DataStart, layout.Offsets[fieldCount]);

            // Confirm the fixture forced each width for the reason it meant to, rather than
            // landing on it by accident: the largest ID and the end offset are the two values
            // the widths are computed from.
            Assert.Equal(fieldIdSize, ByteWidth(layout.FieldIds[fieldCount - 1]));
            Assert.Equal(offsetSize, ByteWidth(layout.Offsets[fieldCount]));

            AssertRoundTrips(encoded, fieldCount);
        }

        /// <summary>
        /// Reads the object back through the library, which has to honor the widths declared in
        /// the header to find anything at all.
        /// </summary>
        private static void AssertRoundTrips(EncodedObject encoded, int fieldCount)
        {
            VariantObjectReader obj = new VariantObjectReader(encoded.Metadata, encoded.Value);
            Assert.Equal(fieldCount, obj.FieldCount);

            for (int i = 0; i < fieldCount; i++)
            {
                string name = FieldName(i);
                Assert.Equal(name, obj.GetFieldName(i));
                Assert.True(obj.TryGetField(name, out VariantReader value), "field " + name + " not found");

                if (i == 0 && encoded.PaddedValue != null)
                {
                    Assert.Equal(encoded.PaddedValue, value.GetString());
                }
                else
                {
                    Assert.Equal(FieldValue(i), value.GetInt8());
                }
            }
        }

        // ---------------------------------------------------------------
        // Building objects with the widths we want
        // ---------------------------------------------------------------

        /// <summary>
        /// Encodes <c>{ "z000": ..., "z001": ..., ... }</c> such that the writer picks
        /// <paramref name="fieldIdSize"/> and <paramref name="offsetSize"/> on its own.
        /// </summary>
        /// <remarks>
        /// The field ID width comes from the largest ID in the object, so the dictionary is
        /// padded with names that sort ahead of the object's own ("p..." before "z...") until the
        /// object's IDs reach the band. The offset width comes from the object's total encoded
        /// field data, so the first field's value is padded to reach that band.
        /// </remarks>
        private static EncodedObject BuildObject(int fieldIdSize, int offsetSize, int fieldCount)
        {
            MetadataFixture fixture = GetMetadata(fieldIdSize, fieldCount);

            string paddedValue = offsetSize == 1 ? null
                : new string('x', offsetSize == 2 ? PadForTwoByteOffsets : PadForThreeByteOffsets);

            byte[] value;
            using (VariantValueWriter writer = new VariantValueWriter(fixture.Builder, fixture.IdRemap))
            {
                writer.BeginObject();
                for (int i = 0; i < fieldCount; i++)
                {
                    writer.WriteFieldName(FieldName(i));
                    if (i == 0 && paddedValue != null)
                    {
                        writer.WriteString(paddedValue);
                    }
                    else
                    {
                        writer.WriteInt8(FieldValue(i));
                    }
                }
                writer.EndObject();
                value = writer.ToArray();
            }

            return new EncodedObject
            {
                Metadata = fixture.MetadataBytes,
                Value = value,
                FirstFieldId = fixture.PadCount,
                PaddedValue = paddedValue,
            };
        }

        private static string FieldName(int index) => "z" + index.ToString("D3");

        private static sbyte FieldValue(int index) => (sbyte)(index & 0x7F);

        /// <summary>
        /// Builds — and caches — a metadata dictionary whose last <paramref name="fieldCount"/>
        /// IDs need exactly <paramref name="fieldIdSize"/> bytes. The 3-byte case is a 65,537
        /// entry dictionary, worth building once rather than once per offset width.
        /// </summary>
        private static MetadataFixture GetMetadata(int fieldIdSize, int fieldCount)
        {
            int key = (fieldIdSize << 16) | fieldCount;
            lock (_metadataCache)
            {
                if (_metadataCache.TryGetValue(key, out MetadataFixture cached))
                {
                    return cached;
                }

                // The largest ID in the object is the dictionary size minus one, so size the
                // dictionary to the smallest value that needs this many bytes — or to the object
                // itself, whose IDs are consecutive and may already run past that.
                int smallestIdOfWidth = fieldIdSize == 1 ? 0
                    : fieldIdSize == 2 ? TwoByteThreshold : ThreeByteThreshold;
                int maxFieldId = Math.Max(smallestIdOfWidth, fieldCount - 1);
                Assert.Equal(fieldIdSize, ByteWidth(maxFieldId));

                int padCount = maxFieldId + 1 - fieldCount;
                Assert.True(padCount >= 0, fieldCount + " fields cannot fit in " + fieldIdSize + "-byte IDs");

                VariantMetadataBuilder builder = new VariantMetadataBuilder();
                for (int i = 0; i < padCount; i++)
                {
                    builder.Add("p" + i.ToString("D7"));
                }
                for (int i = 0; i < fieldCount; i++)
                {
                    builder.Add(FieldName(i));
                }

                MetadataFixture fixture = new MetadataFixture
                {
                    Builder = builder,
                    PadCount = padCount,
                };
                fixture.MetadataBytes = builder.Build(out int[] idRemap);
                fixture.IdRemap = idRemap;

                _metadataCache.Add(key, fixture);
                return fixture;
            }
        }

        private static readonly Dictionary<int, MetadataFixture> _metadataCache =
            new Dictionary<int, MetadataFixture>();

        private sealed class MetadataFixture
        {
            public VariantMetadataBuilder Builder;
            public byte[] MetadataBytes;
            public int[] IdRemap;

            /// <summary>The number of padding names, which is also the object's smallest field ID.</summary>
            public int PadCount;
        }

        private sealed class EncodedObject
        {
            public byte[] Metadata;
            public byte[] Value;
            public int FirstFieldId;

            /// <summary>The first field's string value, or null when every field is an Int8.</summary>
            public string PaddedValue;
        }

        // ---------------------------------------------------------------
        // A decoder written from the spec, independent of the library
        // ---------------------------------------------------------------

        private sealed class ObjectLayout
        {
            public int FieldIdSize;
            public int OffsetSize;
            public bool IsLarge;
            public int FieldCount;
            public int[] FieldIds;
            public int[] Offsets;

            /// <summary>Start of the field data area, which the offsets are relative to.</summary>
            public int DataStart;
        }

        private static ObjectLayout DecodeObjectPerSpec(byte[] value)
        {
            byte header = value[0];
            Assert.Equal(2, header & 0x03); // basic_type = Object
            Assert.Equal(0, header & 0x80); // bit 7 unused

            ObjectLayout layout = new ObjectLayout
            {
                OffsetSize = ((header >> 2) & 0x03) + 1,
                FieldIdSize = ((header >> 4) & 0x03) + 1,
                IsLarge = ((header >> 6) & 0x01) != 0,
            };

            int pos = 1;
            layout.FieldCount = layout.IsLarge ? ReadLittleEndian(value, pos, 4) : value[pos];
            pos += layout.IsLarge ? 4 : 1;

            layout.FieldIds = new int[layout.FieldCount];
            for (int i = 0; i < layout.FieldCount; i++)
            {
                layout.FieldIds[i] = ReadLittleEndian(value, pos, layout.FieldIdSize);
                pos += layout.FieldIdSize;
            }

            layout.Offsets = new int[layout.FieldCount + 1];
            for (int i = 0; i <= layout.FieldCount; i++)
            {
                layout.Offsets[i] = ReadLittleEndian(value, pos, layout.OffsetSize);
                pos += layout.OffsetSize;
            }

            layout.DataStart = pos;
            return layout;
        }

        private static int ReadLittleEndian(byte[] buffer, int start, int width)
        {
            int result = 0;
            for (int i = 0; i < width; i++)
            {
                result |= buffer[start + i] << (8 * i);
            }
            return result;
        }

        private static int ByteWidth(int value) =>
            value <= 0xFF ? 1 : value <= 0xFFFF ? 2 : value <= 0xFFFFFF ? 3 : 4;
    }
}
