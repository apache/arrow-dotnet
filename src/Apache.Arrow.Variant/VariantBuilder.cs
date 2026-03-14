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
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using System.Text.Json;

namespace Apache.Arrow.Variant
{
    /// <summary>
    /// Encodes a <see cref="VariantValue"/> into the binary variant format,
    /// producing metadata and value byte arrays.
    /// </summary>
    public sealed class VariantBuilder
    {
        /// <summary>
        /// Maximum number of offset entries (count + 1) to allocate on the stack.
        /// 256 ints = 1024 bytes, well within safe stack limits.
        /// </summary>
        private const int StackAllocThreshold = 256;

#if !NET8_0_OR_GREATER
        private byte[] _buffer;
#endif
        private readonly Stack<MemoryStream> _streamPool = new Stack<MemoryStream>();

        /// <summary>
        /// Encodes a <see cref="VariantValue"/> to the variant binary format.
        /// </summary>
        /// <returns>A tuple of (metadata bytes, value bytes).</returns>
        public (byte[] Metadata, byte[] Value) Encode(VariantValue variant)
        {
            EnsureBuffer();

            // Phase 1: collect all field names recursively.
            VariantMetadataBuilder metadataBuilder = new VariantMetadataBuilder();
            CollectFieldNames(variant, metadataBuilder);

            // Phase 2: build sorted metadata + get the ID remap.
            byte[] metadata = metadataBuilder.Build(out int[] idRemap);

            // Phase 3: encode the value.
            using (MemoryStream ms = new MemoryStream())
            {
                WriteValue(variant, metadataBuilder, idRemap, ms);
                return (metadata, ms.ToArray());
            }
        }

        private static void CollectFieldNames(VariantValue variant, VariantMetadataBuilder builder)
        {
            if (variant.IsObject)
            {
                foreach (KeyValuePair<string, VariantValue> field in variant.AsObject())
                {
                    builder.Add(field.Key);
                    CollectFieldNames(field.Value, builder);
                }
            }
            else if (variant.IsArray)
            {
                foreach (VariantValue element in variant.AsArray())
                {
                    CollectFieldNames(element, builder);
                }
            }
        }

        private void WriteValue(VariantValue variant, VariantMetadataBuilder metadataBuilder, int[] idRemap, MemoryStream ms)
        {
            if (variant.IsNull)
            {
                ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.NullType));
                return;
            }

            if (variant.IsBoolean)
            {
                VariantPrimitiveType pt = variant.AsBoolean()
                    ? VariantPrimitiveType.BooleanTrue
                    : VariantPrimitiveType.BooleanFalse;
                ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(pt));
                return;
            }

            if (variant.IsObject)
            {
                WriteObject(variant, metadataBuilder, idRemap, ms);
                return;
            }

            if (variant.IsArray)
            {
                WriteArray(variant, metadataBuilder, idRemap, ms);
                return;
            }

            // Primitive types
            switch (variant.PrimitiveType)
            {
                case VariantPrimitiveType.Int8:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int8));
                    ms.WriteByte((byte)variant.AsInt8());
                    break;

                case VariantPrimitiveType.Int16:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int16));
                    WriteInt16(ms, variant.AsInt16());
                    break;

                case VariantPrimitiveType.Int32:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int32));
                    WriteInt32(ms, variant.AsInt32());
                    break;

                case VariantPrimitiveType.Int64:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int64));
                    WriteInt64(ms, variant.AsInt64());
                    break;

                case VariantPrimitiveType.Float:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Float));
                    WriteFloat(ms, variant.AsFloat());
                    break;

                case VariantPrimitiveType.Double:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Double));
                    WriteDouble(ms, variant.AsDouble());
                    break;

                case VariantPrimitiveType.Decimal4:
                    WriteDecimal4(ms, variant.AsDecimal());
                    break;

                case VariantPrimitiveType.Decimal8:
                    WriteDecimal8(ms, variant.AsDecimal());
                    break;

                case VariantPrimitiveType.Decimal16:
                    SqlDecimal sd = variant.AsSqlDecimal();
                    WriteSqlDecimal16(ms, sd);
                    break;

                case VariantPrimitiveType.Date:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Date));
                    WriteInt32(ms, variant.AsDateDays());
                    break;

                case VariantPrimitiveType.Timestamp:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Timestamp));
                    WriteInt64(ms, variant.AsTimestampMicros());
                    break;

                case VariantPrimitiveType.TimestampNtz:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampNtz));
                    WriteInt64(ms, variant.AsTimestampNtzMicros());
                    break;

                case VariantPrimitiveType.TimeNtz:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimeNtz));
                    WriteInt64(ms, variant.AsTimeNtzMicros());
                    break;

                case VariantPrimitiveType.TimestampTzNanos:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampTzNanos));
                    WriteInt64(ms, variant.AsTimestampTzNanos());
                    break;

                case VariantPrimitiveType.TimestampNtzNanos:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampNtzNanos));
                    WriteInt64(ms, variant.AsTimestampNtzNanos());
                    break;

                case VariantPrimitiveType.String:
                    WriteString(ms, variant.AsString());
                    break;

                case VariantPrimitiveType.Binary:
                    WriteBinary(ms, variant.AsBinary());
                    break;

                case VariantPrimitiveType.Uuid:
                    WriteUuid(ms, variant.AsUuid());
                    break;

                default:
                    throw new NotSupportedException($"Unsupported primitive type: {variant.PrimitiveType}");
            }
        }

        private void WriteString(MemoryStream ms, string value)
        {
            int byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount <= 63)
            {
                // Short string
                ms.WriteByte(VariantEncodingHelper.MakeShortStringHeader(byteCount));
            }
            else
            {
                // Long string (primitive type 16)
                ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.String));
                WriteInt32(ms, byteCount);
            }

            // Encode UTF-8 directly into the MemoryStream's buffer to avoid a temporary byte[] allocation.
            int dataPos = (int)ms.Position;
            int needed = dataPos + byteCount;
            if (needed > ms.Length)
                ms.SetLength(needed);
            Encoding.UTF8.GetBytes(value, 0, value.Length, ms.GetBuffer(), dataPos);
            ms.Position = needed;
        }

        private void WriteBinary(MemoryStream ms, byte[] data)
        {
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Binary));
            WriteInt32(ms, data.Length);
            ms.Write(data, 0, data.Length);
        }

        private void WriteUuid(MemoryStream ms, Guid guid)
        {
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Uuid));
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[16];
            guid.TryWriteBytes(buf, bigEndian: true, out int _);
            ms.Write(buf);
#else
            byte[] native = guid.ToByteArray();
            // Convert from .NET mixed-endian to big-endian (RFC 4122)
            _buffer[0] = native[3]; _buffer[1] = native[2]; _buffer[2] = native[1]; _buffer[3] = native[0];
            _buffer[4] = native[5]; _buffer[5] = native[4];
            _buffer[6] = native[7]; _buffer[7] = native[6];
            Buffer.BlockCopy(native, 8, _buffer, 8, 8);
            ms.Write(_buffer, 0, 16);
#endif
        }

        private void WriteDecimal4(MemoryStream ms, decimal value)
        {
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Decimal4));
#if NET8_0_OR_GREATER
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);
#else
            int[] bits = decimal.GetBits(value);
#endif
            byte scale = (byte)((bits[3] >> 16) & 0x7F);
            bool negative = (bits[3] & unchecked((int)0x80000000)) != 0;
            int unscaled = bits[0];
            if (negative) unscaled = -unscaled;
            ms.WriteByte(scale);
            WriteInt32(ms, unscaled);
        }

        private void WriteDecimal8(MemoryStream ms, decimal value)
        {
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Decimal8));
#if NET8_0_OR_GREATER
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);
#else
            int[] bits = decimal.GetBits(value);
#endif
            byte scale = (byte)((bits[3] >> 16) & 0x7F);
            bool negative = (bits[3] & unchecked((int)0x80000000)) != 0;
            long unscaled = ((long)bits[1] << 32) | (uint)bits[0];
            if (negative) unscaled = -unscaled;
            ms.WriteByte(scale);
            WriteInt64(ms, unscaled);
        }

        private void WriteSqlDecimal16(MemoryStream ms, SqlDecimal value)
        {
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Decimal16));

            bool positive = value.IsPositive;
            byte scale = (byte)value.Scale;
            int[] data = value.Data;

            // SqlDecimal.Data: [0]=least-significant, [3]=most-significant
            // Convert to lo/hi longs (little-endian)
            long lo = ((long)(uint)data[1] << 32) | (uint)data[0];
            long hi = ((long)(uint)data[3] << 32) | (uint)data[2];

            if (!positive)
            {
                // Two's complement negate 128-bit
                lo = ~lo;
                hi = ~hi;
                ulong uLo = (ulong)lo + 1;
                if (uLo == 0) hi++;
                lo = (long)uLo;
            }

            ms.WriteByte(scale);
            WriteInt64(ms, lo);
            WriteInt64(ms, hi);
        }

        private void WriteObject(VariantValue variant, VariantMetadataBuilder metadataBuilder, int[] idRemap, MemoryStream ms)
        {
            IReadOnlyDictionary<string, VariantValue> fields = variant.AsObject();
            int fieldCount = fields.Count;

            // Build sorted array of (sortedFieldId, value) pairs.
            KeyValuePair<int, VariantValue>[] sortedFields = new KeyValuePair<int, VariantValue>[fieldCount];
            int fieldIndex = 0;
            foreach (KeyValuePair<string, VariantValue> field in fields)
            {
                int originalId = metadataBuilder.GetId(field.Key);
                int sortedId = idRemap[originalId];
                sortedFields[fieldIndex++] = new KeyValuePair<int, VariantValue>(sortedId, field.Value);
            }
            Array.Sort(sortedFields, static (a, b) => a.Key.CompareTo(b.Key));

            // Encode all field values into a single stream, recording offsets.
            MemoryStream valuesMs = RentStream();
            int offsetCount = fieldCount + 1;
            Span<int> offsets = offsetCount <= StackAllocThreshold
                ? stackalloc int[offsetCount]
                : new int[offsetCount];
            offsets[0] = 0;
            for (int i = 0; i < fieldCount; i++)
            {
                WriteValue(sortedFields[i].Value, metadataBuilder, idRemap, valuesMs);
                offsets[i + 1] = (int)valuesMs.Position;
            }

            // Determine sizes. sortedFields is sorted by Key, so max is the last element.
            int maxFieldId = fieldCount > 0 ? sortedFields[fieldCount - 1].Key : 0;

            int fieldIdSize = fieldCount > 0 ? VariantEncodingHelper.ByteWidthForValue(maxFieldId) : 1;
            int offsetSize = VariantEncodingHelper.ByteWidthForValue(Math.Max(1, offsets[fieldCount]));
            bool isLarge = fieldCount > 255;

            // Write header.
            ms.WriteByte(VariantEncodingHelper.MakeObjectHeader(fieldIdSize, offsetSize, isLarge));

            // Write field count.
            if (isLarge)
            {
                WriteInt32(ms, fieldCount);
            }
            else
            {
                ms.WriteByte((byte)fieldCount);
            }

            // Write field IDs.
            for (int i = 0; i < fieldCount; i++)
            {
                WriteSmallInt(ms, sortedFields[i].Key, fieldIdSize);
            }

            // Write offsets.
            for (int i = 0; i <= fieldCount; i++)
            {
                WriteSmallInt(ms, offsets[i], offsetSize);
            }

            // Write field values from the shared buffer.
            ms.Write(valuesMs.GetBuffer(), 0, (int)valuesMs.Position);
            ReturnStream(valuesMs);
        }

        private void WriteArray(VariantValue variant, VariantMetadataBuilder metadataBuilder, int[] idRemap, MemoryStream ms)
        {
            IReadOnlyList<VariantValue> elements = variant.AsArray();
            int elementCount = elements.Count;

            // Encode all elements into a single stream, recording offsets.
            MemoryStream valuesMs = RentStream();
            int offsetCount = elementCount + 1;
            Span<int> offsets = offsetCount <= StackAllocThreshold
                ? stackalloc int[offsetCount]
                : new int[offsetCount];
            offsets[0] = 0;
            for (int i = 0; i < elementCount; i++)
            {
                WriteValue(elements[i], metadataBuilder, idRemap, valuesMs);
                offsets[i + 1] = (int)valuesMs.Position;
            }

            int offsetSize = VariantEncodingHelper.ByteWidthForValue(Math.Max(1, offsets[elementCount]));
            bool isLarge = elementCount > 255;

            // Write header.
            ms.WriteByte(VariantEncodingHelper.MakeArrayHeader(offsetSize, isLarge));

            // Write element count.
            if (isLarge)
            {
                WriteInt32(ms, elementCount);
            }
            else
            {
                ms.WriteByte((byte)elementCount);
            }

            // Write offsets.
            for (int i = 0; i <= elementCount; i++)
            {
                WriteSmallInt(ms, offsets[i], offsetSize);
            }

            // Write element values from the shared buffer.
            ms.Write(valuesMs.GetBuffer(), 0, (int)valuesMs.Position);
            ReturnStream(valuesMs);
        }

        // ---------------------------------------------------------------
        // Primitive write helpers
        // ---------------------------------------------------------------

        private void EnsureBuffer()
        {
#if !NET8_0_OR_GREATER
            if (_buffer == null) _buffer = new byte[16];
#endif
        }

        private MemoryStream RentStream()
        {
            if (_streamPool.Count > 0)
            {
                MemoryStream ms = _streamPool.Pop();
                ms.SetLength(0);
                return ms;
            }
            return new MemoryStream();
        }

        private void ReturnStream(MemoryStream ms)
        {
            _streamPool.Push(ms);
        }

        private void WriteSmallInt(MemoryStream ms, int value, int byteWidth)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[4];
            VariantEncodingHelper.WriteLittleEndianInt(buf, value, byteWidth);
            ms.Write(buf.Slice(0, byteWidth));
#else
            VariantEncodingHelper.WriteLittleEndianInt(_buffer, value, byteWidth);
            ms.Write(_buffer, 0, byteWidth);
#endif
        }

        private void WriteInt16(MemoryStream ms, short value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[2];
            BinaryPrimitives.WriteInt16LittleEndian(buf, value);
            ms.Write(buf);
#else
            BinaryPrimitives.WriteInt16LittleEndian(_buffer, value);
            ms.Write(_buffer, 0, 2);
#endif
        }

        private void WriteInt32(MemoryStream ms, int value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(buf, value);
            ms.Write(buf);
#else
            BinaryPrimitives.WriteInt32LittleEndian(_buffer, value);
            ms.Write(_buffer, 0, 4);
#endif
        }

        private void WriteInt64(MemoryStream ms, long value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(buf, value);
            ms.Write(buf);
#else
            BinaryPrimitives.WriteInt64LittleEndian(_buffer, value);
            ms.Write(_buffer, 0, 8);
#endif
        }

        private void WriteFloat(MemoryStream ms, float value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[4];
            BinaryPrimitives.WriteSingleLittleEndian(buf, value);
            ms.Write(buf);
#else
            int bits = System.Runtime.CompilerServices.Unsafe.As<float, int>(ref value);
            BinaryPrimitives.WriteInt32LittleEndian(_buffer, bits);
            ms.Write(_buffer, 0, 4);
#endif
        }

        private void WriteDouble(MemoryStream ms, double value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[8];
            BinaryPrimitives.WriteDoubleLittleEndian(buf, value);
            ms.Write(buf);
#else
            long bits = BitConverter.DoubleToInt64Bits(value);
            BinaryPrimitives.WriteInt64LittleEndian(_buffer, bits);
            ms.Write(_buffer, 0, 8);
#endif
        }

        // ---------------------------------------------------------------
        // Streaming JSON encoding
        // ---------------------------------------------------------------

        /// <summary>
        /// Encodes UTF-8 JSON bytes directly to the variant binary format
        /// without creating intermediate <see cref="VariantValue"/> objects.
        /// </summary>
        /// <param name="utf8Json">The UTF-8 encoded JSON bytes.</param>
        /// <returns>A tuple of (metadata bytes, value bytes).</returns>
        public (byte[] Metadata, byte[] Value) EncodeFromJson(ReadOnlySpan<byte> utf8Json)
        {
            EnsureBuffer();

            // Pass 1: collect all field names.
            VariantMetadataBuilder metadataBuilder = new VariantMetadataBuilder();
            Utf8JsonReader reader1 = new Utf8JsonReader(utf8Json);
            reader1.Read();
            CollectFieldNamesFromJson(ref reader1, metadataBuilder);

            // Build sorted metadata + get the ID remap.
            byte[] metadata = metadataBuilder.Build(out int[] idRemap);

            // Pass 2: encode the value.
            Utf8JsonReader reader2 = new Utf8JsonReader(utf8Json);
            reader2.Read();
            using (MemoryStream ms = new MemoryStream())
            {
                WriteJsonValue(ref reader2, metadataBuilder, idRemap, ms);
                return (metadata, ms.ToArray());
            }
        }

        private static void CollectFieldNamesFromJson(ref Utf8JsonReader reader, VariantMetadataBuilder builder)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.EndObject)
                            return;
                        // PropertyName token
                        builder.Add(reader.GetString());
                        reader.Read();
                        CollectFieldNamesFromJson(ref reader, builder);
                    }
                    throw new JsonException("Unterminated JSON object.");

                case JsonTokenType.StartArray:
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.EndArray)
                            return;
                        CollectFieldNamesFromJson(ref reader, builder);
                    }
                    throw new JsonException("Unterminated JSON array.");

                default:
                    // Primitive value (null, bool, number, string) — no field names.
                    return;
            }
        }

        private void WriteJsonValue(ref Utf8JsonReader reader, VariantMetadataBuilder metadataBuilder, int[] idRemap, MemoryStream ms)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.Null:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.NullType));
                    return;

                case JsonTokenType.True:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.BooleanTrue));
                    return;

                case JsonTokenType.False:
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.BooleanFalse));
                    return;

                case JsonTokenType.Number:
                    WriteJsonNumber(ref reader, ms);
                    return;

                case JsonTokenType.String:
                    WriteString(ms, reader.GetString());
                    return;

                case JsonTokenType.StartObject:
                    WriteJsonObject(ref reader, metadataBuilder, idRemap, ms);
                    return;

                case JsonTokenType.StartArray:
                    WriteJsonArray(ref reader, metadataBuilder, idRemap, ms);
                    return;

                default:
                    throw new JsonException($"Unexpected JSON token type {reader.TokenType}.");
            }
        }

        private void WriteJsonNumber(ref Utf8JsonReader reader, MemoryStream ms)
        {
            if (reader.TryGetInt64(out long longValue))
            {
                if (longValue >= sbyte.MinValue && longValue <= sbyte.MaxValue)
                {
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int8));
                    ms.WriteByte((byte)(sbyte)longValue);
                }
                else if (longValue >= short.MinValue && longValue <= short.MaxValue)
                {
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int16));
                    WriteInt16(ms, (short)longValue);
                }
                else if (longValue >= int.MinValue && longValue <= int.MaxValue)
                {
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int32));
                    WriteInt32(ms, (int)longValue);
                }
                else
                {
                    ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int64));
                    WriteInt64(ms, longValue);
                }
            }
            else
            {
                ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Double));
                WriteDouble(ms, reader.GetDouble());
            }
        }

        private void WriteJsonObject(ref Utf8JsonReader reader, VariantMetadataBuilder metadataBuilder, int[] idRemap, MemoryStream ms)
        {
            // Encode all field values into a shared stream, tracking positions.
            MemoryStream valuesMs = RentStream();
            List<int> fieldIds = new List<int>(16);
            List<int> valueStarts = new List<int>(16);

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    break;

                string name = reader.GetString();
                fieldIds.Add(idRemap[metadataBuilder.GetId(name)]);

                reader.Read();
                valueStarts.Add((int)valuesMs.Position);
                WriteJsonValue(ref reader, metadataBuilder, idRemap, valuesMs);
            }

            int fieldCount = fieldIds.Count;

            // Sentinel marks end of last value.
            valueStarts.Add((int)valuesMs.Position);

            // Build sort indices so we can write fields in sorted ID order.
#if NET8_0_OR_GREATER
            Span<int> sortOrder = fieldCount <= StackAllocThreshold
                ? stackalloc int[fieldCount]
                : new int[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                sortOrder[i] = i;
            }
            sortOrder.Sort((a, b) => fieldIds[a].CompareTo(fieldIds[b]));
#else
            int[] sortOrder = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                sortOrder[i] = i;
            }
            Array.Sort(sortOrder, (a, b) => fieldIds[a].CompareTo(fieldIds[b]));
#endif

            // Build offsets in sorted order.
            int offsetCount = fieldCount + 1;
            Span<int> offsets = offsetCount <= StackAllocThreshold
                ? stackalloc int[offsetCount]
                : new int[offsetCount];
            offsets[0] = 0;
            for (int i = 0; i < fieldCount; i++)
            {
                int idx = sortOrder[i];
                offsets[i + 1] = offsets[i] + (valueStarts[idx + 1] - valueStarts[idx]);
            }

            // Determine sizes.
            int maxFieldId = fieldCount > 0 ? fieldIds[sortOrder[fieldCount - 1]] : 0;
            int fieldIdSize = fieldCount > 0 ? VariantEncodingHelper.ByteWidthForValue(maxFieldId) : 1;
            int offsetSize = VariantEncodingHelper.ByteWidthForValue(Math.Max(1, offsets[fieldCount]));
            bool isLarge = fieldCount > 255;

            // Write header.
            ms.WriteByte(VariantEncodingHelper.MakeObjectHeader(fieldIdSize, offsetSize, isLarge));

            // Write field count.
            if (isLarge)
            {
                WriteInt32(ms, fieldCount);
            }
            else
            {
                ms.WriteByte((byte)fieldCount);
            }

            // Write field IDs in sorted order.
            for (int i = 0; i < fieldCount; i++)
            {
                WriteSmallInt(ms, fieldIds[sortOrder[i]], fieldIdSize);
            }

            // Write offsets.
            for (int i = 0; i <= fieldCount; i++)
            {
                WriteSmallInt(ms, offsets[i], offsetSize);
            }

            // Write field values in sorted order.
            byte[] valueBuffer = valuesMs.GetBuffer();
            for (int i = 0; i < fieldCount; i++)
            {
                int idx = sortOrder[i];
                int start = valueStarts[idx];
                int length = valueStarts[idx + 1] - start;
                ms.Write(valueBuffer, start, length);
            }
            ReturnStream(valuesMs);
        }

        private void WriteJsonArray(ref Utf8JsonReader reader, VariantMetadataBuilder metadataBuilder, int[] idRemap, MemoryStream ms)
        {
            MemoryStream valuesMs = RentStream();
            List<int> offsets = new List<int>(16);
            offsets.Add(0);

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndArray)
                    break;

                WriteJsonValue(ref reader, metadataBuilder, idRemap, valuesMs);
                offsets.Add((int)valuesMs.Position);
            }

            int elementCount = offsets.Count - 1;
            int offsetSize = VariantEncodingHelper.ByteWidthForValue(Math.Max(1, offsets[elementCount]));
            bool isLarge = elementCount > 255;

            // Write header.
            ms.WriteByte(VariantEncodingHelper.MakeArrayHeader(offsetSize, isLarge));

            // Write element count.
            if (isLarge)
            {
                WriteInt32(ms, elementCount);
            }
            else
            {
                ms.WriteByte((byte)elementCount);
            }

            // Write offsets.
            for (int i = 0; i <= elementCount; i++)
            {
                WriteSmallInt(ms, offsets[i], offsetSize);
            }

            // Write element values.
            ms.Write(valuesMs.GetBuffer(), 0, (int)valuesMs.Position);
            ReturnStream(valuesMs);
        }
    }
}
