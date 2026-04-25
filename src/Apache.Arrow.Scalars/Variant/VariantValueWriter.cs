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
using System.Collections.Generic;
using System.Data.SqlTypes;
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
    /// <item><see cref="Dispose"/> the writer to return its cached backing arrays to <see cref="ArrayPool{T}.Shared"/>. Skipping <c>Dispose</c> leaks those arrays to the GC.</item>
    /// </list>
    /// </remarks>
    public sealed class VariantValueWriter : IDisposable
    {
        private const int StackAllocThreshold = 256;

        private readonly VariantMetadataBuilder _metadata;
        private readonly int[] _idRemap;

        // Per-writer stacks of cached backing arrays, separate from
        // ArrayPool<T>.Shared so that capacity grown on one frame's buffer
        // carries over to the next frame through the same writer without
        // being redistributed by size class.
        private readonly Stack<byte[]> _bytePool = new Stack<byte[]>();
        private readonly Stack<int[]> _intPool = new Stack<int[]>();

        private Buffer<byte> _root;
        private readonly Stack<Frame> _frameStack = new Stack<Frame>();
        private Frame _frame;
        private bool _disposed;

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

            _root.Acquire(_bytePool);
        }

        /// <summary>
        /// Returns the encoded value bytes. All opened objects and arrays must be closed.
        /// </summary>
        public byte[] ToArray()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(VariantValueWriter));
            if (_frame != null)
            {
                throw new InvalidOperationException("Unclosed object or array at the top of the writer.");
            }
            return _root.ToArray();
        }

        /// <summary>
        /// Returns all cached backing arrays (the root buffer, any still-open
        /// frame buffers, and the per-writer array pools) to
        /// <see cref="ArrayPool{T}.Shared"/>. The writer must not be used after
        /// <see cref="Dispose"/>; calls to <see cref="ToArray"/> or any
        /// <c>Write*</c> / <c>Begin*</c> method will throw
        /// <see cref="ObjectDisposedException"/>. Idempotent.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            // Release still-owned frame arrays into the per-writer pools.
            if (_frame != null) ReleaseFrameArrays(_frame);
            while (_frameStack.Count > 0)
            {
                Frame f = _frameStack.Pop();
                if (f != null) ReleaseFrameArrays(f);
            }
            _root.Release(_bytePool);

            // Drain the per-writer pools back to the process-wide shared pool.
            while (_bytePool.Count > 0) ArrayPool<byte>.Shared.Return(_bytePool.Pop());
            while (_intPool.Count > 0) ArrayPool<int>.Shared.Return(_intPool.Pop());
        }

        private void ReleaseFrameArrays(Frame f)
        {
            f.Buffer.Release(_bytePool);
            f.ValueStarts.Release(_intPool);
            if (f is ObjectFrame obj) obj.FieldIds.Release(_intPool);
        }

        // ---------------------------------------------------------------
        // Object / array scope
        // ---------------------------------------------------------------

        /// <summary>Begins writing an object. Pair with <see cref="EndObject"/>.</summary>
        public void BeginObject()
        {
            BeforeWriteValue();
            ObjectFrame frame = new ObjectFrame();
            // Keep _frame / _frameStack untouched until every Acquire + the Push
            // have succeeded. If any step throws, the catch releases whatever
            // was acquired so far (Release is a no-op on un-Acquired buffers),
            // and the writer's visible state is as if BeginObject was never
            // called — Dispose sees no orphaned arrays.
            try
            {
                frame.Buffer.Acquire(_bytePool);
                frame.ValueStarts.Acquire(_intPool);
                frame.FieldIds.Acquire(_intPool);
                _frameStack.Push(_frame);
            }
            catch
            {
                frame.FieldIds.Release(_intPool);
                frame.ValueStarts.Release(_intPool);
                frame.Buffer.Release(_bytePool);
                throw;
            }
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
            objFrame.FieldIds.Append(fieldId);
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
            // Once objFrame is popped it's no longer visible to Dispose, so
            // WriteObjectBody must not leave its buffers unreleased on throw.
            try
            {
                if (_frame != null)
                {
                    WriteObjectBody(ref _frame.Buffer, objFrame);
                }
                else
                {
                    WriteObjectBody(ref _root, objFrame);
                }
            }
            finally
            {
                objFrame.FieldIds.Release(_intPool);
                objFrame.ValueStarts.Release(_intPool);
                objFrame.Buffer.Release(_bytePool);
            }
        }

        /// <summary>Begins writing an array. Pair with <see cref="EndArray"/>.</summary>
        public void BeginArray()
        {
            BeforeWriteValue();
            ArrayFrame frame = new ArrayFrame();
            // See BeginObject: defer any visible state change until all the
            // rent-and-push steps have succeeded; on throw, release whatever
            // was acquired so nothing escapes Dispose's reach.
            try
            {
                frame.Buffer.Acquire(_bytePool);
                frame.ValueStarts.Acquire(_intPool);
                _frameStack.Push(_frame);
            }
            catch
            {
                frame.ValueStarts.Release(_intPool);
                frame.Buffer.Release(_bytePool);
                throw;
            }
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
            // Popped frame is no longer visible to Dispose; the finally makes
            // sure its buffers are released even if WriteArrayBody throws.
            try
            {
                if (_frame != null)
                {
                    WriteArrayBody(ref _frame.Buffer, arrFrame);
                }
                else
                {
                    WriteArrayBody(ref _root, arrFrame);
                }
            }
            finally
            {
                arrFrame.ValueStarts.Release(_intPool);
                arrFrame.Buffer.Release(_bytePool);
            }
        }

        // ---------------------------------------------------------------
        // Primitive writes
        // ---------------------------------------------------------------

        /// <summary>Writes a null value.</summary>
        public void WriteNull()
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.NullType));
        }

        /// <summary>Writes a boolean value.</summary>
        public void WriteBoolean(bool value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(
                value ? VariantPrimitiveType.BooleanTrue : VariantPrimitiveType.BooleanFalse));
        }

        /// <summary>Writes an 8-bit signed integer.</summary>
        public void WriteInt8(sbyte value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int8));
            buf.Append((byte)value);
        }

        /// <summary>Writes a 16-bit signed integer.</summary>
        public void WriteInt16(short value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int16));
            buf.WriteInt16LE(value);
        }

        /// <summary>Writes a 32-bit signed integer.</summary>
        public void WriteInt32(int value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int32));
            buf.WriteInt32LE(value);
        }

        /// <summary>Writes a 64-bit signed integer.</summary>
        public void WriteInt64(long value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Int64));
            buf.WriteInt64LE(value);
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
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Float));
            buf.WriteFloatLE(value);
        }

        /// <summary>Writes a 64-bit IEEE 754 double.</summary>
        public void WriteDouble(double value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Double));
            buf.WriteDoubleLE(value);
        }

        /// <summary>Writes a Decimal4 (precision ≤ 9) value.</summary>
        public void WriteDecimal4(decimal value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Decimal4));
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
            buf.Append(scale);
            buf.WriteInt32LE(unscaled);
        }

        /// <summary>Writes a Decimal8 (precision ≤ 18) value.</summary>
        public void WriteDecimal8(decimal value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Decimal8));
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
            buf.Append(scale);
            buf.WriteInt64LE(unscaled);
        }

        /// <summary>Writes a Decimal16 (precision ≤ 38) value stored as <see cref="SqlDecimal"/>.</summary>
        public void WriteDecimal16(SqlDecimal value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Decimal16));

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

            buf.Append(scale);
            buf.WriteInt64LE(lo);
            buf.WriteInt64LE(hi);
        }

        /// <summary>Writes a string. Uses the short-string encoding when the UTF-8 byte length is ≤ 63.</summary>
        public void WriteString(string value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            ref Buffer<byte> buf = ref BeforeWriteValue();
            int byteCount = Encoding.UTF8.GetByteCount(value);
            if (byteCount <= 63)
            {
                buf.Append(VariantEncodingHelper.MakeShortStringHeader(byteCount));
            }
            else
            {
                buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.String));
                buf.WriteInt32LE(byteCount);
            }

            // Encode UTF-8 directly into the buffer.
            Span<byte> dest = buf.GetSpan(byteCount);
#if NET8_0_OR_GREATER
            Encoding.UTF8.GetBytes(value, dest);
#else
            Encoding.UTF8.GetBytes(value, 0, value.Length, buf.RawBuffer, buf.Length);
#endif
            buf.Advance(byteCount);
        }

        /// <summary>Writes a binary blob.</summary>
        public void WriteBinary(ReadOnlySpan<byte> data)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Binary));
            buf.WriteInt32LE(data.Length);
            buf.Append(data);
        }

        /// <summary>Writes a UUID.</summary>
        public void WriteUuid(Guid value)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Uuid));
            Span<byte> raw = stackalloc byte[16];
#if NET8_0_OR_GREATER
            value.TryWriteBytes(raw, bigEndian: true, out _);
#else
            // Convert from .NET mixed-endian to big-endian (RFC 4122).
            byte[] native = value.ToByteArray();
            raw[0] = native[3]; raw[1] = native[2]; raw[2] = native[1]; raw[3] = native[0];
            raw[4] = native[5]; raw[5] = native[4];
            raw[6] = native[7]; raw[7] = native[6];
            native.AsSpan(8, 8).CopyTo(raw.Slice(8));
#endif
            buf.Append(raw);
        }

        /// <summary>Writes a date as days since the Unix epoch.</summary>
        public void WriteDateDays(int days)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Date));
            buf.WriteInt32LE(days);
        }

        /// <summary>Writes a timestamp (tz-adjusted microseconds since the Unix epoch).</summary>
        public void WriteTimestampMicros(long micros)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.Timestamp));
            buf.WriteInt64LE(micros);
        }

        /// <summary>Writes a timestamp-without-timezone (microseconds since the Unix epoch).</summary>
        public void WriteTimestampNtzMicros(long micros)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampNtz));
            buf.WriteInt64LE(micros);
        }

        /// <summary>Writes a time-without-timezone value (microseconds since midnight).</summary>
        public void WriteTimeNtzMicros(long micros)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimeNtz));
            buf.WriteInt64LE(micros);
        }

        /// <summary>Writes a timestamp with timezone (nanoseconds since the Unix epoch).</summary>
        public void WriteTimestampTzNanos(long nanos)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampTzNanos));
            buf.WriteInt64LE(nanos);
        }

        /// <summary>Writes a timestamp without timezone (nanoseconds since the Unix epoch).</summary>
        public void WriteTimestampNtzNanos(long nanos)
        {
            ref Buffer<byte> buf = ref BeforeWriteValue();
            buf.Append(VariantEncodingHelper.MakePrimitiveHeader(VariantPrimitiveType.TimestampNtzNanos));
            buf.WriteInt64LE(nanos);
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
                case VariantPrimitiveType.Binary: WriteBinary(source.GetBinary()); return;
                case VariantPrimitiveType.Uuid: WriteUuid(source.GetUuid()); return;
                default:
                    throw new NotSupportedException($"Unsupported primitive type: {pt}");
            }
        }

        // ---------------------------------------------------------------
        // Internal bookkeeping
        // ---------------------------------------------------------------

        private ref Buffer<byte> BeforeWriteValue()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(VariantValueWriter));
            if (_frame is ObjectFrame objFrame)
            {
                if (!objFrame.PendingValue)
                {
                    throw new InvalidOperationException("A field name is required before writing an object field value. Call WriteFieldName first.");
                }
                objFrame.ValueStarts.Append(objFrame.Buffer.Length);
                objFrame.PendingValue = false;
                return ref objFrame.Buffer;
            }
            if (_frame is ArrayFrame arrFrame)
            {
                arrFrame.ValueStarts.Append(arrFrame.Buffer.Length);
                return ref arrFrame.Buffer;
            }
            return ref _root;
        }

        private static void WriteObjectBody(ref Buffer<byte> output, ObjectFrame frame)
        {
            int fieldCount = frame.FieldIds.Length;
            // Sentinel marks the end of the last value in the frame buffer.
            frame.ValueStarts.Append(frame.Buffer.Length);

            int[] fieldIds = frame.FieldIds.RawBuffer;
            int[] valueStarts = frame.ValueStarts.RawBuffer;

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

            output.Append(VariantEncodingHelper.MakeObjectHeader(fieldIdSize, offsetSize, isLarge));

            if (isLarge)
            {
                output.WriteInt32LE(fieldCount);
            }
            else
            {
                output.Append((byte)fieldCount);
            }

            for (int i = 0; i < fieldCount; i++)
            {
                output.WriteSmallInt(fieldIds[sortOrder[i]], fieldIdSize);
            }

            for (int i = 0; i <= fieldCount; i++)
            {
                output.WriteSmallInt(offsets[i], offsetSize);
            }

            byte[] valueBuffer = frame.Buffer.RawBuffer;
            for (int i = 0; i < fieldCount; i++)
            {
                int idx = sortOrder[i];
                int start = valueStarts[idx];
                int length = valueStarts[idx + 1] - start;
                output.Append(valueBuffer, start, length);
            }
        }

        private static void WriteArrayBody(ref Buffer<byte> output, ArrayFrame frame)
        {
            int elementCount = frame.ValueStarts.Length;
            // Sentinel marks the end of the last element.
            frame.ValueStarts.Append(frame.Buffer.Length);
            int[] valueStarts = frame.ValueStarts.RawBuffer;

            int offsetSize = VariantEncodingHelper.ByteWidthForValue(Math.Max(1, valueStarts[elementCount]));
            bool isLarge = elementCount > 255;

            output.Append(VariantEncodingHelper.MakeArrayHeader(offsetSize, isLarge));

            if (isLarge)
            {
                output.WriteInt32LE(elementCount);
            }
            else
            {
                output.Append((byte)elementCount);
            }

            for (int i = 0; i <= elementCount; i++)
            {
                output.WriteSmallInt(valueStarts[i], offsetSize);
            }

            output.Append(frame.Buffer.RawBuffer, 0, frame.Buffer.Length);
        }

        private abstract class Frame
        {
            public Buffer<byte> Buffer;
            public Buffer<int> ValueStarts;
        }

        private sealed class ObjectFrame : Frame
        {
            public Buffer<int> FieldIds;
            public bool PendingValue;
        }

        private sealed class ArrayFrame : Frame
        {
        }
    }
}
