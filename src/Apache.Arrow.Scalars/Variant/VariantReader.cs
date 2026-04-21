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
using System.Runtime.CompilerServices;
using System.Text;

namespace Apache.Arrow.Scalars.Variant
{
    /// <summary>
    /// Zero-copy reader for a single variant value. Provides type inspection
    /// and typed accessors for primitives, short strings, objects, and arrays.
    /// </summary>
    public ref struct VariantReader
    {
        private readonly ReadOnlySpan<byte> _metadata;
        private readonly ReadOnlySpan<byte> _value;

        /// <summary>
        /// Creates a reader over a variant value.
        /// </summary>
        /// <param name="metadata">The variant metadata buffer (shared across all values in a column).</param>
        /// <param name="value">The variant value buffer for this specific value.</param>
        public VariantReader(ReadOnlySpan<byte> metadata, ReadOnlySpan<byte> value)
        {
            _metadata = metadata;
            _value = value;
        }

        /// <summary>Gets the full metadata span.</summary>
        public ReadOnlySpan<byte> Metadata => _metadata;

        /// <summary>Gets the full value span.</summary>
        public ReadOnlySpan<byte> Value => _value;

        // ---------------------------------------------------------------
        // Type inspection
        // ---------------------------------------------------------------

        /// <summary>Gets the header byte of this value.</summary>
        public byte Header => _value[0];

        /// <summary>Gets the basic type of this value.</summary>
        public VariantBasicType BasicType => VariantEncodingHelper.GetBasicType(Header);

        /// <summary>
        /// Gets the primitive type when <see cref="BasicType"/> is <see cref="VariantBasicType.Primitive"/>.
        /// </summary>
        public VariantPrimitiveType? PrimitiveType =>
            BasicType == VariantBasicType.Primitive ? VariantEncodingHelper.GetPrimitiveType(Header) : null;

        /// <summary>Returns true if this value is null.</summary>
        public bool IsNull =>
            BasicType == VariantBasicType.Primitive &&
            PrimitiveType == VariantPrimitiveType.NullType;

        /// <summary>Returns true if this value is a boolean (true or false).</summary>
        public bool IsBoolean =>
            BasicType == VariantBasicType.Primitive &&
            (PrimitiveType == VariantPrimitiveType.BooleanTrue ||
             PrimitiveType == VariantPrimitiveType.BooleanFalse);

        /// <summary>
        /// Returns true if this value is a string (either short string or long string primitive).
        /// </summary>
        public bool IsString =>
            BasicType == VariantBasicType.ShortString ||
            (BasicType == VariantBasicType.Primitive && PrimitiveType == VariantPrimitiveType.String);

        /// <summary>Returns true if this value is an object.</summary>
        public bool IsObject => BasicType == VariantBasicType.Object;

        /// <summary>Returns true if this value is an array.</summary>
        public bool IsArray => BasicType == VariantBasicType.Array;

        /// <summary>
        /// Returns true if this value is a numeric type (any integer, float, double, or decimal).
        /// </summary>
        public bool IsNumeric
        {
            get
            {
                if (BasicType != VariantBasicType.Primitive)
                {
                    return false;
                }

                switch (PrimitiveType)
                {
                    case VariantPrimitiveType.Int8:
                    case VariantPrimitiveType.Int16:
                    case VariantPrimitiveType.Int32:
                    case VariantPrimitiveType.Int64:
                    case VariantPrimitiveType.Float:
                    case VariantPrimitiveType.Double:
                    case VariantPrimitiveType.Decimal4:
                    case VariantPrimitiveType.Decimal8:
                    case VariantPrimitiveType.Decimal16:
                        return true;
                    default:
                        return false;
                }
            }
        }

        // ---------------------------------------------------------------
        // The data portion of the value (everything after the header byte).
        // ---------------------------------------------------------------

        private ReadOnlySpan<byte> Data => _value.Slice(1);

        // ---------------------------------------------------------------
        // Boolean
        // ---------------------------------------------------------------

        /// <summary>Gets the boolean value.</summary>
        public bool GetBoolean()
        {
            EnsurePrimitive();
            switch (PrimitiveType)
            {
                case VariantPrimitiveType.BooleanTrue:
                    return true;
                case VariantPrimitiveType.BooleanFalse:
                    return false;
                default:
                    throw new InvalidOperationException($"Cannot read boolean from primitive type {PrimitiveType}.");
            }
        }

        // ---------------------------------------------------------------
        // Integer types
        // ---------------------------------------------------------------

        /// <summary>Gets an Int8 value.</summary>
        public sbyte GetInt8()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Int8);
            return (sbyte)Data[0];
        }

        /// <summary>Gets an Int16 value.</summary>
        public short GetInt16()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Int16);
            return BinaryPrimitives.ReadInt16LittleEndian(Data);
        }

        /// <summary>Gets an Int32 value.</summary>
        public int GetInt32()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Int32);
            return BinaryPrimitives.ReadInt32LittleEndian(Data);
        }

        /// <summary>Gets an Int64 value.</summary>
        public long GetInt64()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Int64);
            return BinaryPrimitives.ReadInt64LittleEndian(Data);
        }

        // ---------------------------------------------------------------
        // Floating point types
        // ---------------------------------------------------------------

        /// <summary>Gets a float (Float) value.</summary>
        public float GetFloat()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Float);
#if NET8_0_OR_GREATER
            return BinaryPrimitives.ReadSingleLittleEndian(Data);
#else
            int bits = BinaryPrimitives.ReadInt32LittleEndian(Data);
            return Unsafe.As<int, float>(ref bits);
#endif
        }

        /// <summary>Gets a double value.</summary>
        public double GetDouble()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Double);
#if NET8_0_OR_GREATER
            return BinaryPrimitives.ReadDoubleLittleEndian(Data);
#else
            return BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(Data));
#endif
        }

        // ---------------------------------------------------------------
        // Decimal types
        // ---------------------------------------------------------------

        /// <summary>
        /// Gets a Decimal4 value (1-byte scale + 4-byte unscaled integer).
        /// </summary>
        public decimal GetDecimal4()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Decimal4);
            byte scale = Data[0];
            int unscaled = BinaryPrimitives.ReadInt32LittleEndian(Data.Slice(1));
            return MakeDecimal(unscaled, scale);
        }

        /// <summary>
        /// Gets a Decimal8 value (1-byte scale + 8-byte unscaled integer).
        /// </summary>
        public decimal GetDecimal8()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Decimal8);
            byte scale = Data[0];
            long unscaled = BinaryPrimitives.ReadInt64LittleEndian(Data.Slice(1));
            return MakeDecimal(unscaled, scale);
        }

        /// <summary>
        /// Gets the raw scale and 16-byte unscaled value for a Decimal16.
        /// The 16-byte integer may exceed <see cref="decimal"/> range.
        /// </summary>
        /// <param name="scale">The scale (number of decimal digits after the point).</param>
        /// <returns>The 16-byte unscaled value in little-endian byte order.</returns>
        public ReadOnlySpan<byte> GetDecimal16Raw(out byte scale)
        {
            EnsurePrimitiveType(VariantPrimitiveType.Decimal16);
            scale = Data[0];
            return Data.Slice(1, 16);
        }

        /// <summary>
        /// Attempts to get a Decimal16 value as a <see cref="decimal"/>.
        /// This may throw <see cref="OverflowException"/> if the value exceeds 96 bits.
        /// </summary>
        public decimal GetDecimal16()
        {
            ReadOnlySpan<byte> raw = GetDecimal16Raw(out byte scale);

            // Read as two 64-bit halves (little-endian).
            long lo = BinaryPrimitives.ReadInt64LittleEndian(raw);
            long hi = BinaryPrimitives.ReadInt64LittleEndian(raw.Slice(8));

            bool negative = hi < 0;
            if (negative)
            {
                // Two's-complement negate the 128-bit value.
                lo = ~lo;
                hi = ~hi;
                ulong uLo = (ulong)lo + 1;
                if (uLo == 0) // overflow carry
                {
                    hi++;
                }
                lo = (long)uLo;
            }

            // Decimal supports 96 bits. hi must fit in 32 bits.
            if ((ulong)hi > uint.MaxValue)
            {
                throw new OverflowException("Decimal16 unscaled value exceeds the range of System.Decimal.");
            }

            int lo32 = (int)(lo & 0xFFFFFFFF);
            int mid32 = (int)((ulong)lo >> 32);
            int hi32 = (int)hi;

            return new decimal(lo32, mid32, hi32, negative, scale);
        }

        /// <summary>
        /// Gets a decimal value as a <see cref="SqlDecimal"/>.
        /// Works for Decimal4, Decimal8, and Decimal16 types.
        /// Unlike <see cref="GetDecimal16"/>, this method does not throw
        /// <see cref="OverflowException"/> for values exceeding 96 bits.
        /// </summary>
        public SqlDecimal GetSqlDecimal()
        {
            EnsurePrimitive();
            switch (PrimitiveType)
            {
                case VariantPrimitiveType.Decimal4:
                    return new SqlDecimal(GetDecimal4());
                case VariantPrimitiveType.Decimal8:
                    return new SqlDecimal(GetDecimal8());
                case VariantPrimitiveType.Decimal16:
                    return GetSqlDecimal16();
                default:
                    throw new InvalidOperationException($"Cannot read decimal from primitive type {PrimitiveType}.");
            }
        }

        /// <summary>
        /// Attempts to get a Decimal16 value as a <see cref="decimal"/>.
        /// Returns false if the value exceeds 96 bits instead of throwing.
        /// </summary>
        public bool TryGetDecimal16(out decimal value)
        {
            ReadOnlySpan<byte> raw = GetDecimal16Raw(out byte scale);

            long lo = BinaryPrimitives.ReadInt64LittleEndian(raw);
            long hi = BinaryPrimitives.ReadInt64LittleEndian(raw.Slice(8));

            bool negative = hi < 0;
            if (negative)
            {
                lo = ~lo;
                hi = ~hi;
                ulong uLo = (ulong)lo + 1;
                if (uLo == 0)
                {
                    hi++;
                }
                lo = (long)uLo;
            }

            if ((ulong)hi > uint.MaxValue)
            {
                value = default;
                return false;
            }

            int lo32 = (int)(lo & 0xFFFFFFFF);
            int mid32 = (int)((ulong)lo >> 32);
            int hi32 = (int)hi;

            value = new decimal(lo32, mid32, hi32, negative, scale);
            return true;
        }

        private SqlDecimal GetSqlDecimal16()
        {
            ReadOnlySpan<byte> raw = GetDecimal16Raw(out byte scale);

            long lo = BinaryPrimitives.ReadInt64LittleEndian(raw);
            long hi = BinaryPrimitives.ReadInt64LittleEndian(raw.Slice(8));

            bool positive = hi >= 0;
            if (!positive)
            {
                lo = ~lo;
                hi = ~hi;
                ulong uLo = (ulong)lo + 1;
                if (uLo == 0)
                {
                    hi++;
                }
                lo = (long)uLo;
            }

            // SqlDecimal.Data ordering: [0]=least-significant, [3]=most-significant
            int d1 = (int)((ulong)lo & 0xFFFFFFFF);
            int d2 = (int)((ulong)lo >> 32);
            int d3 = (int)((ulong)hi & 0xFFFFFFFF);
            int d4 = (int)((ulong)hi >> 32);

            return new SqlDecimal(38, scale, positive, new int[] { d1, d2, d3, d4 });
        }

        // ---------------------------------------------------------------
        // Date and time types
        // ---------------------------------------------------------------

        /// <summary>Gets the date as days since Unix epoch (1970-01-01).</summary>
        public int GetDateDays()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Date);
            return BinaryPrimitives.ReadInt32LittleEndian(Data);
        }

        /// <summary>Gets the date as a <see cref="DateTime"/>.</summary>
        public DateTime GetDate()
        {
            int days = GetDateDays();
            return VariantEncodingHelper.UnixEpochUtc.AddDays(days);
        }

        /// <summary>Gets a Timestamp value as microseconds since Unix epoch (UTC).</summary>
        public long GetTimestampMicros()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Timestamp);
            return BinaryPrimitives.ReadInt64LittleEndian(Data);
        }

        /// <summary>Gets a Timestamp as a <see cref="DateTimeOffset"/> (UTC).</summary>
        public DateTimeOffset GetTimestamp()
        {
            long micros = GetTimestampMicros();
            return VariantEncodingHelper.UnixEpochOffset.AddTicks(micros * 10);
        }

        /// <summary>Gets a TimestampNTZ value as microseconds since epoch (no timezone).</summary>
        public long GetTimestampNtzMicros()
        {
            EnsurePrimitiveType(VariantPrimitiveType.TimestampNtz);
            return BinaryPrimitives.ReadInt64LittleEndian(Data);
        }

        /// <summary>Gets a TimestampNTZ as a <see cref="DateTime"/> (Unspecified kind).</summary>
        public DateTime GetTimestampNtz()
        {
            long micros = GetTimestampNtzMicros();
            return VariantEncodingHelper.UnixEpochUnspecified.AddTicks(micros * 10);
        }

        /// <summary>Gets a TimeNTZ value as microseconds since midnight.</summary>
        public long GetTimeNtzMicros()
        {
            EnsurePrimitiveType(VariantPrimitiveType.TimeNtz);
            return BinaryPrimitives.ReadInt64LittleEndian(Data);
        }

        /// <summary>Gets a TimestampTzNanos value as nanoseconds since Unix epoch (UTC).</summary>
        public long GetTimestampTzNanos()
        {
            EnsurePrimitiveType(VariantPrimitiveType.TimestampTzNanos);
            return BinaryPrimitives.ReadInt64LittleEndian(Data);
        }

        /// <summary>Gets a TimestampNtzNanos value as nanoseconds since epoch (no timezone).</summary>
        public long GetTimestampNtzNanos()
        {
            EnsurePrimitiveType(VariantPrimitiveType.TimestampNtzNanos);
            return BinaryPrimitives.ReadInt64LittleEndian(Data);
        }

        // ---------------------------------------------------------------
        // Binary
        // ---------------------------------------------------------------

        /// <summary>Gets the binary value as a byte span.</summary>
        public ReadOnlySpan<byte> GetBinary()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Binary);
            int length = BinaryPrimitives.ReadInt32LittleEndian(Data);
            return Data.Slice(4, length);
        }

        // ---------------------------------------------------------------
        // String (short string or long string primitive)
        // ---------------------------------------------------------------

        /// <summary>
        /// Gets the raw UTF-8 bytes of the string value.
        /// Works for both short strings and long string primitives (type 16).
        /// </summary>
        public ReadOnlySpan<byte> GetStringBytes()
        {
            VariantBasicType bt = BasicType;
            if (bt == VariantBasicType.ShortString)
            {
                int length = VariantEncodingHelper.GetShortStringLength(Header);
                return _value.Slice(1, length);
            }

            if (bt == VariantBasicType.Primitive && PrimitiveType == VariantPrimitiveType.String)
            {
                int length = BinaryPrimitives.ReadInt32LittleEndian(Data);
                return Data.Slice(4, length);
            }

            throw new InvalidOperationException($"Cannot read string from variant with basic type {bt}.");
        }

        /// <summary>
        /// Gets the string value decoded from UTF-8.
        /// Works for both short strings and long string primitives (type 16).
        /// </summary>
        public string GetString()
        {
            ReadOnlySpan<byte> bytes = GetStringBytes();
#if NET8_0_OR_GREATER
            return Encoding.UTF8.GetString(bytes);
#else
            return Encoding.UTF8.GetString(bytes.ToArray());
#endif
        }

        // ---------------------------------------------------------------
        // UUID
        // ---------------------------------------------------------------

        /// <summary>
        /// Gets the UUID value. The variant encoding stores UUIDs as 16 bytes
        /// in big-endian (RFC 4122) byte order.
        /// </summary>
        public Guid GetUuid()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Uuid);
            ReadOnlySpan<byte> raw = Data.Slice(0, 16);

            // .NET's Guid constructor from byte[] uses mixed-endian format:
            //   bytes 0-3: little-endian (Data1)
            //   bytes 4-5: little-endian (Data2)
            //   bytes 6-7: little-endian (Data3)
            //   bytes 8-15: big-endian (Data4)
            // The variant spec stores all 16 bytes in network (big-endian) order,
            // so we need to byte-swap the first 3 groups.
#if NET8_0_OR_GREATER
            return new Guid(raw, bigEndian: true);
#else
            byte[] bytes = new byte[16];
            // Data1: swap bytes 0-3
            bytes[0] = raw[3];
            bytes[1] = raw[2];
            bytes[2] = raw[1];
            bytes[3] = raw[0];
            // Data2: swap bytes 4-5
            bytes[4] = raw[5];
            bytes[5] = raw[4];
            // Data3: swap bytes 6-7
            bytes[6] = raw[7];
            bytes[7] = raw[6];
            // Data4: bytes 8-15 remain big-endian
            raw.Slice(8, 8).CopyTo(bytes.AsSpan(8));
            return new Guid(bytes);
#endif
        }

        /// <summary>Gets the raw 16 UUID bytes in big-endian (RFC 4122) order.</summary>
        public ReadOnlySpan<byte> GetUuidBytes()
        {
            EnsurePrimitiveType(VariantPrimitiveType.Uuid);
            return Data.Slice(0, 16);
        }

        // ---------------------------------------------------------------
        // Helpers
        // ---------------------------------------------------------------

        private static decimal MakeDecimal(long unscaled, byte scale)
        {
            bool negative = unscaled < 0;
            ulong abs = negative ? (ulong)(-unscaled) : (ulong)unscaled;
            int lo = (int)(abs & 0xFFFFFFFF);
            int mid = (int)(abs >> 32);
            return new decimal(lo, mid, 0, negative, scale);
        }

        private void EnsurePrimitive()
        {
            if (BasicType != VariantBasicType.Primitive)
            {
                throw new InvalidOperationException(
                    $"Expected primitive variant but got basic type {BasicType}.");
            }
        }

        private void EnsurePrimitiveType(VariantPrimitiveType expected)
        {
            EnsurePrimitive();
            VariantPrimitiveType actual = PrimitiveType.Value;
            if (actual != expected)
            {
                throw new InvalidOperationException(
                    $"Expected primitive type {expected} but got {actual}.");
            }
        }

        // ---------------------------------------------------------------
        // Materialization
        // ---------------------------------------------------------------

        /// <summary>
        /// Materializes this variant value into a <see cref="VariantValue"/> object.
        /// Recursively materializes objects and arrays.
        /// </summary>
        public VariantValue ToVariantValue()
        {
            switch (BasicType)
            {
                case VariantBasicType.ShortString:
                    return VariantValue.FromString(GetString());

                case VariantBasicType.Object:
                    VariantObjectReader obj = new VariantObjectReader(_metadata, _value);
                    Dictionary<string, VariantValue> fields = new Dictionary<string, VariantValue>(obj.FieldCount);
                    for (int i = 0; i < obj.FieldCount; i++)
                    {
                        string name = obj.GetFieldName(i);
                        VariantReader fieldValue = obj.GetFieldValue(i);
                        fields[name] = fieldValue.ToVariantValue();
                    }
                    return VariantValue.FromObject(fields);

                case VariantBasicType.Array:
                    VariantArrayReader arr = new VariantArrayReader(_metadata, _value);
                    List<VariantValue> elements = new List<VariantValue>(arr.ElementCount);
                    for (int i = 0; i < arr.ElementCount; i++)
                    {
                        VariantReader elem = arr.GetElement(i);
                        elements.Add(elem.ToVariantValue());
                    }
                    return VariantValue.FromArray(elements);

                case VariantBasicType.Primitive:
                    return MaterializePrimitive();

                default:
                    throw new NotSupportedException($"Unsupported basic type {BasicType}.");
            }
        }

        /// <summary>
        /// Materializes a primitive value without redundant type validation.
        /// The caller (<see cref="ToVariantValue"/>) has already verified
        /// <see cref="BasicType"/> is <see cref="VariantBasicType.Primitive"/>,
        /// so we read <see cref="Data"/> directly instead of going through
        /// the public getters which would re-check the type.
        /// </summary>
        private VariantValue MaterializePrimitive()
        {
            ReadOnlySpan<byte> data = Data;
            switch (PrimitiveType)
            {
                case VariantPrimitiveType.NullType:
                    return VariantValue.Null;
                case VariantPrimitiveType.BooleanTrue:
                    return VariantValue.True;
                case VariantPrimitiveType.BooleanFalse:
                    return VariantValue.False;
                case VariantPrimitiveType.Int8:
                    return VariantValue.FromInt8((sbyte)data[0]);
                case VariantPrimitiveType.Int16:
                    return VariantValue.FromInt16(BinaryPrimitives.ReadInt16LittleEndian(data));
                case VariantPrimitiveType.Int32:
                    return VariantValue.FromInt32(BinaryPrimitives.ReadInt32LittleEndian(data));
                case VariantPrimitiveType.Int64:
                    return VariantValue.FromInt64(BinaryPrimitives.ReadInt64LittleEndian(data));
                case VariantPrimitiveType.Float:
#if NET8_0_OR_GREATER
                    return VariantValue.FromFloat(BinaryPrimitives.ReadSingleLittleEndian(data));
#else
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(data);
                    return VariantValue.FromFloat(Unsafe.As<int, float>(ref bits));
#endif
                case VariantPrimitiveType.Double:
#if NET8_0_OR_GREATER
                    return VariantValue.FromDouble(BinaryPrimitives.ReadDoubleLittleEndian(data));
#else
                    return VariantValue.FromDouble(BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(data)));
#endif
                case VariantPrimitiveType.Decimal4:
                    return VariantValue.FromDecimal4(MakeDecimal(BinaryPrimitives.ReadInt32LittleEndian(data.Slice(1)), data[0]));
                case VariantPrimitiveType.Decimal8:
                    return VariantValue.FromDecimal8(MakeDecimal(BinaryPrimitives.ReadInt64LittleEndian(data.Slice(1)), data[0]));
                case VariantPrimitiveType.Decimal16:
                    if (TryGetDecimal16(out decimal d16))
                        return VariantValue.FromDecimal16(d16);
                    return VariantValue.FromSqlDecimal(GetSqlDecimal16());
                case VariantPrimitiveType.Date:
                    return VariantValue.FromDate(BinaryPrimitives.ReadInt32LittleEndian(data));
                case VariantPrimitiveType.Timestamp:
                    return VariantValue.FromTimestamp(BinaryPrimitives.ReadInt64LittleEndian(data));
                case VariantPrimitiveType.TimestampNtz:
                    return VariantValue.FromTimestampNtz(BinaryPrimitives.ReadInt64LittleEndian(data));
                case VariantPrimitiveType.TimeNtz:
                    return VariantValue.FromTimeNtz(BinaryPrimitives.ReadInt64LittleEndian(data));
                case VariantPrimitiveType.TimestampTzNanos:
                    return VariantValue.FromTimestampTzNanos(BinaryPrimitives.ReadInt64LittleEndian(data));
                case VariantPrimitiveType.TimestampNtzNanos:
                    return VariantValue.FromTimestampNtzNanos(BinaryPrimitives.ReadInt64LittleEndian(data));
                case VariantPrimitiveType.Binary:
                    int length = BinaryPrimitives.ReadInt32LittleEndian(data);
                    return VariantValue.FromBinary(data.Slice(4, length).ToArray());
                case VariantPrimitiveType.String:
                    return VariantValue.FromString(GetString());
                case VariantPrimitiveType.Uuid:
                    return VariantValue.FromUuid(GetUuid());
                default:
                    throw new NotSupportedException($"Unsupported primitive type {PrimitiveType}.");
            }
        }
    }
}
