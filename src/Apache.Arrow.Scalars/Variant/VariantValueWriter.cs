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
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Text;

namespace Apache.Arrow.Scalars.Variant
{
    /// <summary>
    /// Streams variant value bytes directly from primitive calls, without
    /// requiring an intermediate <see cref="VariantValue"/>. Use this to
    /// implement encoders from arbitrary input formats (JSON, CBOR, etc.).
    /// </summary>
    /// <remarks>
    /// Usage pattern:
    /// <list type="number">
    /// <item>Create a <see cref="VariantMetadataBuilder"/> and <see cref="VariantMetadataBuilder.Add(string)"/> every field name that will appear.</item>
    /// <item>Call <see cref="VariantMetadataBuilder.Build(out int[])"/> to produce the metadata bytes and the ID remap.</item>
    /// <item>Create a <see cref="VariantValueWriter"/> with the metadata builder and remap, emit the value via the <c>Write*</c> / <c>Begin*</c> / <c>End*</c> methods, then call <see cref="ToArray"/>.</item>
    /// </list>
    /// </remarks>
    public sealed class VariantValueWriter
    {
        private const int StackAllocThreshold = 256;

        private readonly VariantMetadataBuilder _metadata;
        private readonly int[] _idRemap;
        private readonly MemoryStream _root = new MemoryStream();
        private readonly Stack<Frame> _frameStack = new Stack<Frame>();
        private readonly Stack<MemoryStream> _streamPool = new Stack<MemoryStream>();
        private Frame _frame;

#if !NET8_0_OR_GREATER
        private readonly byte[] _scratch = new byte[16];
#endif

        /// <summary>
        /// Creates a writer that produces value bytes referencing the given metadata.
        /// </summary>
        /// <param name="metadata">The metadata builder used to resolve field names to IDs.</param>
        /// <param name="idRemap">The remap returned by <see cref="VariantMetadataBuilder.Build(out int[])"/>.</param>
        public VariantValueWriter(VariantMetadataBuilder metadata, int[] idRemap)
        {
            _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            _idRemap = idRemap ?? throw new ArgumentNullException(nameof(idRemap));

            if (_metadata.Count != _idRemap.Length)
            {
                throw new ArgumentException(
                    "the idRemap array length must match the metadata builder count used to create it.",
                    nameof(idRemap));
            }
        }

        /// <summary>
        /// Returns the encoded value bytes. All opened objects and arrays must be closed.
        /// </summary>
        public byte[] ToArray()
        {
            if (_frame != null)
            {
                throw new InvalidOperationException("Unclosed object or array at the top of the writer.");
            }
            return _root.ToArray();
        }

        // ---------------------------------------------------------------
        // Object / array scope
        // ---------------------------------------------------------------

        /// <summary>Begins writing an object. Pair with <see cref="EndObject"/>.</summary>
        public void BeginObject()
        {
            BeforeWriteValue();
            _frameStack.Push(_frame);
            ObjectFrame frame = new ObjectFrame { Buffer = RentStream() };
            frame.FieldIds = ArrayPool<int>.Shared.Rent(InitialFrameCapacity);
            frame.ValueStarts = ArrayPool<int>.Shared.Rent(InitialFrameCapacity);
            _frame = frame;
        }

        /// <summary>
        /// Writes the name of the next field in the current object. Must be called
        /// before every field value (a primitive, nested object, or nested array).
        /// The name must already exist in the <see cref="VariantMetadataBuilder"/>.
        /// </summary>
        public void WriteFieldName(string name)
        {
            if (!(_frame is ObjectFrame objFrame))
            {
                throw new InvalidOperationException("WriteFieldName may only be called inside an object scope.");
            }
            if (objFrame.PendingValue)
            {
                throw new InvalidOperationException("A value must be written for the previous field before writing the next field name.");
            }
            int fieldId = _idRemap[_metadata.GetId(name)];
            AppendInt(ref objFrame.FieldIds, ref objFrame.FieldIdCount, fieldId);
            objFrame.PendingValue = true;
        }

        /// <summary>Ends the current object scope.</summary>
        public void EndObject()
        {
            if (!(_frame is ObjectFrame objFrame))
            {
                throw new InvalidOperationException("EndObject called without matching BeginObject.");
            }
            if (objFrame.PendingValue)
            {
                throw new InvalidOperationException("Missing value for the last field name before EndObject.");
            }

            _frame = _frameStack.Pop();
            MemoryStream output = _frame != null ? _frame.Buffer : _root;
            WriteObjectBody(output, objFrame);
            ArrayPool<int>.Shared.Return(objFrame.FieldIds);
            ArrayPool<int>.Shared.Return(objFrame.ValueStarts);
            ReturnStream(objFrame.Buffer);
        }

        /// <summary>Begins writing an array. Pair with <see cref="EndArray"/>.</summary>
        public void BeginArray()
        {
            BeforeWriteValue();
            _frameStack.Push(_frame);
            ArrayFrame frame = new ArrayFrame { Buffer = RentStream() };
            frame.ValueStarts = ArrayPool<int>.Shared.Rent(InitialFrameCapacity);
            _frame = frame;
        }

        /// <summary>Ends the current array scope.</summary>
        public void EndArray()
        {
            if (!(_frame is ArrayFrame arrFrame))
            {
                throw new InvalidOperationException("EndArray called without matching BeginArray.");
            }

            _frame = _frameStack.Pop();
            MemoryStream output = _frame != null ? _frame.Buffer : _root;
            WriteArrayBody(output, arrFrame);
            ArrayPool<int>.Shared.Return(arrFrame.ValueStarts);
            ReturnStream(arrFrame.Buffer);
        }

        // ---------------------------------------------------------------
        // Primitive writes
        // ---------------------------------------------------------------

        /// <summary>Writes a null value.</summary>
        public void WriteNull()
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.NullType));
        }

        /// <summary>Writes a boolean value.</summary>
        public void WriteBoolean(bool value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(
                value ? VariantPrimitiveType.BooleanTrue : VariantPrimitiveType.BooleanFalse));
        }

        /// <summary>Writes an 8-bit signed integer.</summary>
        public void WriteInt8(sbyte value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int8));
            ms.WriteByte((byte)value);
        }

        /// <summary>Writes a 16-bit signed integer.</summary>
        public void WriteInt16(short value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int16));
            WriteInt16LE(ms, value);
        }

        /// <summary>Writes a 32-bit signed integer.</summary>
        public void WriteInt32(int value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int32));
            WriteInt32LE(ms, value);
        }

        /// <summary>Writes a 64-bit signed integer.</summary>
        public void WriteInt64(long value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int64));
            WriteInt64LE(ms, value);
        }

        /// <summary>
        /// Writes an integer using the narrowest of Int8/Int16/Int32/Int64 that fits.
        /// Useful for size-minimising encoders such as JSON.
        /// </summary>
        public void WriteIntegerCompact(long value)
        {
            if (value >= sbyte.MinValue && value <= sbyte.MaxValue)
            {
                WriteInt8((sbyte)value);
            }
            else if (value >= short.MinValue && value <= short.MaxValue)
            {
                WriteInt16((short)value);
            }
            else if (value >= int.MinValue && value <= int.MaxValue)
            {
                WriteInt32((int)value);
            }
            else
            {
                WriteInt64(value);
            }
        }

        /// <summary>Writes a 32-bit IEEE 754 float.</summary>
        public void WriteFloat(float value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Float));
            WriteFloatLE(ms, value);
        }

        /// <summary>Writes a 64-bit IEEE 754 double.</summary>
        public void WriteDouble(double value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Double));
            WriteDoubleLE(ms, value);
        }

        /// <summary>Writes a Decimal4 (precision ≤ 9) value.</summary>
        public void WriteDecimal4(decimal value)
        {
            MemoryStream ms = BeforeWriteValue();
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
            WriteInt32LE(ms, unscaled);
        }

        /// <summary>Writes a Decimal8 (precision ≤ 18) value.</summary>
        public void WriteDecimal8(decimal value)
        {
            MemoryStream ms = BeforeWriteValue();
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
            WriteInt64LE(ms, unscaled);
        }

        /// <summary>Writes a Decimal16 (precision ≤ 38) value stored as <see cref="SqlDecimal"/>.</summary>
        public void WriteDecimal16(SqlDecimal value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Decimal16));

            bool positive = value.IsPositive;
            byte scale = (byte)value.Scale;
            int[] data = value.Data;

            // SqlDecimal.Data: [0]=least-significant, [3]=most-significant
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
            WriteInt64LE(ms, lo);
            WriteInt64LE(ms, hi);
        }

        /// <summary>Writes a string. Uses the short-string encoding when the UTF-8 byte length is ≤ 63.</summary>
        public void WriteString(string value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            MemoryStream ms = BeforeWriteValue();
            int byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount <= 63)
            {
                ms.WriteByte(VariantEncodingHelper.MakeShortStringHeader(byteCount));
            }
            else
            {
                ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.String));
                WriteInt32LE(ms, byteCount);
            }

            // Encode UTF-8 directly into the MemoryStream's buffer.
            int dataPos = (int)ms.Position;
            int needed = dataPos + byteCount;
            if (needed > ms.Length)
            {
                ms.SetLength(needed);
            }
            Encoding.UTF8.GetBytes(value, 0, value.Length, ms.GetBuffer(), dataPos);
            ms.Position = needed;
        }

        /// <summary>Writes a binary blob.</summary>
        public void WriteBinary(byte[] data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Binary));
            WriteInt32LE(ms, data.Length);
            ms.Write(data, 0, data.Length);
        }

        /// <summary>Writes a UUID.</summary>
        public void WriteUuid(Guid value)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Uuid));
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[16];
            value.TryWriteBytes(buf, bigEndian: true, out int _);
            ms.Write(buf);
#else
            byte[] native = value.ToByteArray();
            // Convert from .NET mixed-endian to big-endian (RFC 4122).
            _scratch[0] = native[3]; _scratch[1] = native[2]; _scratch[2] = native[1]; _scratch[3] = native[0];
            _scratch[4] = native[5]; _scratch[5] = native[4];
            _scratch[6] = native[7]; _scratch[7] = native[6];
            Buffer.BlockCopy(native, 8, _scratch, 8, 8);
            ms.Write(_scratch, 0, 16);
#endif
        }

        /// <summary>Writes a date as days since the Unix epoch.</summary>
        public void WriteDateDays(int days)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Date));
            WriteInt32LE(ms, days);
        }

        /// <summary>Writes a timestamp (tz-adjusted microseconds since the Unix epoch).</summary>
        public void WriteTimestampMicros(long micros)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Timestamp));
            WriteInt64LE(ms, micros);
        }

        /// <summary>Writes a timestamp-without-timezone (microseconds since the Unix epoch).</summary>
        public void WriteTimestampNtzMicros(long micros)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampNtz));
            WriteInt64LE(ms, micros);
        }

        /// <summary>Writes a time-without-timezone value (microseconds since midnight).</summary>
        public void WriteTimeNtzMicros(long micros)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimeNtz));
            WriteInt64LE(ms, micros);
        }

        /// <summary>Writes a timestamp with timezone (nanoseconds since the Unix epoch).</summary>
        public void WriteTimestampTzNanos(long nanos)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampTzNanos));
            WriteInt64LE(ms, nanos);
        }

        /// <summary>Writes a timestamp without timezone (nanoseconds since the Unix epoch).</summary>
        public void WriteTimestampNtzNanos(long nanos)
        {
            MemoryStream ms = BeforeWriteValue();
            ms.WriteByte(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampNtzNanos));
            WriteInt64LE(ms, nanos);
        }

        // ---------------------------------------------------------------
        // Transcode from a VariantReader
        // ---------------------------------------------------------------

        /// <summary>
        /// Copies the variant value pointed to by <paramref name="source"/> into this
        /// writer. Useful when copying between metadata dictionaries: field IDs in the
        /// source are re-looked-up against this writer's <see cref="VariantMetadataBuilder"/>
        /// on the fly, via <see cref="WriteFieldName"/>.
        /// </summary>
        /// <remarks>
        /// All field names referenced anywhere in <paramref name="source"/> must already
        /// exist in the metadata builder used to construct this writer. Use
        /// <see cref="VariantMetadataBuilder.CollectFieldNames(VariantReader)"/> during
        /// the metadata-collection phase of a two-pass encode to accumulate them.
        /// </remarks>
        public void CopyValue(VariantReader source)
        {
            switch (source.BasicType)
            {
                case VariantBasicType.Primitive:
                    CopyPrimitive(source);
                    return;

                case VariantBasicType.ShortString:
                    WriteString(source.GetString());
                    return;

                case VariantBasicType.Object:
                    VariantObjectReader obj = new VariantObjectReader(source.Metadata, source.Value);
                    BeginObject();
                    for (int i = 0; i < obj.FieldCount; i++)
                    {
                        WriteFieldName(obj.GetFieldName(i));
                        CopyValue(obj.GetFieldValue(i));
                    }
                    EndObject();
                    return;

                case VariantBasicType.Array:
                    VariantArrayReader arr = new VariantArrayReader(source.Metadata, source.Value);
                    BeginArray();
                    for (int i = 0; i < arr.ElementCount; i++)
                    {
                        CopyValue(arr.GetElement(i));
                    }
                    EndArray();
                    return;

                default:
                    throw new NotSupportedException($"Unsupported basic type: {source.BasicType}");
            }
        }

        private void CopyPrimitive(VariantReader source)
        {
            VariantPrimitiveType? pt = source.PrimitiveType;
            switch (pt)
            {
                case VariantPrimitiveType.NullType: WriteNull(); return;
                case VariantPrimitiveType.BooleanTrue: WriteBoolean(true); return;
                case VariantPrimitiveType.BooleanFalse: WriteBoolean(false); return;
                case VariantPrimitiveType.Int8: WriteInt8(source.GetInt8()); return;
                case VariantPrimitiveType.Int16: WriteInt16(source.GetInt16()); return;
                case VariantPrimitiveType.Int32: WriteInt32(source.GetInt32()); return;
                case VariantPrimitiveType.Int64: WriteInt64(source.GetInt64()); return;
                case VariantPrimitiveType.Float: WriteFloat(source.GetFloat()); return;
                case VariantPrimitiveType.Double: WriteDouble(source.GetDouble()); return;
                case VariantPrimitiveType.Decimal4: WriteDecimal4(source.GetDecimal4()); return;
                case VariantPrimitiveType.Decimal8: WriteDecimal8(source.GetDecimal8()); return;
                // Decimal16 may exceed System.Decimal's range, so route through SqlDecimal.
                case VariantPrimitiveType.Decimal16: WriteDecimal16(source.GetSqlDecimal()); return;
                case VariantPrimitiveType.Date: WriteDateDays(source.GetDateDays()); return;
                case VariantPrimitiveType.Timestamp: WriteTimestampMicros(source.GetTimestampMicros()); return;
                case VariantPrimitiveType.TimestampNtz: WriteTimestampNtzMicros(source.GetTimestampNtzMicros()); return;
                case VariantPrimitiveType.TimeNtz: WriteTimeNtzMicros(source.GetTimeNtzMicros()); return;
                case VariantPrimitiveType.TimestampTzNanos: WriteTimestampTzNanos(source.GetTimestampTzNanos()); return;
                case VariantPrimitiveType.TimestampNtzNanos: WriteTimestampNtzNanos(source.GetTimestampNtzNanos()); return;
                case VariantPrimitiveType.String: WriteString(source.GetString()); return;
                case VariantPrimitiveType.Binary: WriteBinary(source.GetBinary().ToArray()); return;
                case VariantPrimitiveType.Uuid: WriteUuid(source.GetUuid()); return;
                default:
                    throw new NotSupportedException($"Unsupported primitive type: {pt}");
            }
        }

        // ---------------------------------------------------------------
        // Internal bookkeeping
        // ---------------------------------------------------------------

        private MemoryStream BeforeWriteValue()
        {
            if (_frame is ObjectFrame objFrame)
            {
                if (!objFrame.PendingValue)
                {
                    throw new InvalidOperationException("A field name is required before writing an object field value. Call WriteFieldName first.");
                }
                AppendInt(ref objFrame.ValueStarts, ref objFrame.ValueStartCount, (int)objFrame.Buffer.Position);
                objFrame.PendingValue = false;
                return objFrame.Buffer;
            }
            if (_frame is ArrayFrame arrFrame)
            {
                AppendInt(ref arrFrame.ValueStarts, ref arrFrame.ValueStartCount, (int)arrFrame.Buffer.Position);
                return arrFrame.Buffer;
            }
            return _root;
        }

        private static void AppendInt(ref int[] array, ref int count, int value)
        {
            if (count == array.Length)
            {
                int[] grown = ArrayPool<int>.Shared.Rent(array.Length * 2);
                Array.Copy(array, 0, grown, 0, count);
                ArrayPool<int>.Shared.Return(array);
                array = grown;
            }
            array[count++] = value;
        }

        private void WriteObjectBody(MemoryStream output, ObjectFrame frame)
        {
            int fieldCount = frame.FieldIdCount;
            // Sentinel marks the end of the last value in the frame buffer.
            AppendInt(ref frame.ValueStarts, ref frame.ValueStartCount, (int)frame.Buffer.Position);

            int[] fieldIds = frame.FieldIds;
            int[] valueStarts = frame.ValueStarts;

            // Sort indices so fields are emitted in sorted-field-id order.
#if NET8_0_OR_GREATER
            Span<int> sortOrder = fieldCount <= StackAllocThreshold
                ? stackalloc int[fieldCount]
                : new int[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                sortOrder[i] = i;
            }
            int[] fieldIdsLocal = fieldIds;
            sortOrder.Sort((a, b) => fieldIdsLocal[a].CompareTo(fieldIdsLocal[b]));
#else
            int[] sortOrder = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                sortOrder[i] = i;
            }
            int[] fieldIdsLocal = fieldIds;
            Array.Sort(sortOrder, (a, b) => fieldIdsLocal[a].CompareTo(fieldIdsLocal[b]));
#endif

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

            int maxFieldId = fieldCount > 0 ? fieldIds[sortOrder[fieldCount - 1]] : 0;
            int fieldIdSize = fieldCount > 0 ? VariantEncodingHelper.ByteWidthForValue(maxFieldId) : 1;
            int offsetSize = VariantEncodingHelper.ByteWidthForValue(Math.Max(1, offsets[fieldCount]));
            bool isLarge = fieldCount > 255;

            output.WriteByte(VariantEncodingHelper.MakeObjectHeader(fieldIdSize, offsetSize, isLarge));

            if (isLarge)
            {
                WriteInt32LE(output, fieldCount);
            }
            else
            {
                output.WriteByte((byte)fieldCount);
            }

            for (int i = 0; i < fieldCount; i++)
            {
                WriteSmallInt(output, fieldIds[sortOrder[i]], fieldIdSize);
            }

            for (int i = 0; i <= fieldCount; i++)
            {
                WriteSmallInt(output, offsets[i], offsetSize);
            }

            byte[] valueBuffer = frame.Buffer.GetBuffer();
            for (int i = 0; i < fieldCount; i++)
            {
                int idx = sortOrder[i];
                int start = valueStarts[idx];
                int length = valueStarts[idx + 1] - start;
                output.Write(valueBuffer, start, length);
            }
        }

        private void WriteArrayBody(MemoryStream output, ArrayFrame frame)
        {
            int elementCount = frame.ValueStartCount;
            // Sentinel marks the end of the last element.
            AppendInt(ref frame.ValueStarts, ref frame.ValueStartCount, (int)frame.Buffer.Position);
            int[] valueStarts = frame.ValueStarts;

            int offsetSize = VariantEncodingHelper.ByteWidthForValue(Math.Max(1, valueStarts[elementCount]));
            bool isLarge = elementCount > 255;

            output.WriteByte(VariantEncodingHelper.MakeArrayHeader(offsetSize, isLarge));

            if (isLarge)
            {
                WriteInt32LE(output, elementCount);
            }
            else
            {
                output.WriteByte((byte)elementCount);
            }

            for (int i = 0; i <= elementCount; i++)
            {
                WriteSmallInt(output, valueStarts[i], offsetSize);
            }

            output.Write(frame.Buffer.GetBuffer(), 0, (int)frame.Buffer.Position);
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
            VariantEncodingHelper.WriteLittleEndianInt(_scratch, value, byteWidth);
            ms.Write(_scratch, 0, byteWidth);
#endif
        }

        private void WriteInt16LE(MemoryStream ms, short value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[2];
            BinaryPrimitives.WriteInt16LittleEndian(buf, value);
            ms.Write(buf);
#else
            BinaryPrimitives.WriteInt16LittleEndian(_scratch, value);
            ms.Write(_scratch, 0, 2);
#endif
        }

        private void WriteInt32LE(MemoryStream ms, int value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(buf, value);
            ms.Write(buf);
#else
            BinaryPrimitives.WriteInt32LittleEndian(_scratch, value);
            ms.Write(_scratch, 0, 4);
#endif
        }

        private void WriteInt64LE(MemoryStream ms, long value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(buf, value);
            ms.Write(buf);
#else
            BinaryPrimitives.WriteInt64LittleEndian(_scratch, value);
            ms.Write(_scratch, 0, 8);
#endif
        }

        private void WriteFloatLE(MemoryStream ms, float value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[4];
            BinaryPrimitives.WriteSingleLittleEndian(buf, value);
            ms.Write(buf);
#else
            int bits = System.Runtime.CompilerServices.Unsafe.As<float, int>(ref value);
            BinaryPrimitives.WriteInt32LittleEndian(_scratch, bits);
            ms.Write(_scratch, 0, 4);
#endif
        }

        private void WriteDoubleLE(MemoryStream ms, double value)
        {
#if NET8_0_OR_GREATER
            Span<byte> buf = stackalloc byte[8];
            BinaryPrimitives.WriteDoubleLittleEndian(buf, value);
            ms.Write(buf);
#else
            long bits = BitConverter.DoubleToInt64Bits(value);
            BinaryPrimitives.WriteInt64LittleEndian(_scratch, bits);
            ms.Write(_scratch, 0, 8);
#endif
        }

        private const int InitialFrameCapacity = 16;

        private abstract class Frame
        {
            public MemoryStream Buffer;
            public int[] ValueStarts;
            public int ValueStartCount;
        }

        private sealed class ObjectFrame : Frame
        {
            public int[] FieldIds;
            public int FieldIdCount;
            public bool PendingValue;
        }

        private sealed class ArrayFrame : Frame
        {
        }
    }
}
