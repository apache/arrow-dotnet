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
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Apache.Arrow.Variant
{
    /// <summary>
    /// Represents a materialized variant value as a discriminated union.
    /// Preserves exact type information from the variant encoding.
    /// </summary>
    public readonly struct VariantValue : IEquatable<VariantValue>
    {
        // ---------------------------------------------------------------
        // Internal storage
        // ---------------------------------------------------------------

        private readonly VariantPrimitiveType _primitiveType;
        private readonly long _inlineValue;
        private readonly object _objectValue;

        private VariantValue(VariantPrimitiveType primitiveType, long inlineValue)
        {
            _primitiveType = primitiveType;
            _inlineValue = inlineValue;
            _objectValue = null;
        }

        private VariantValue(VariantPrimitiveType primitiveType, object objectValue)
        {
            _primitiveType = primitiveType;
            _inlineValue = 0;
            _objectValue = objectValue;
        }

        // Object and Array use sentinel primitive types that don't collide with real ones.
        private const VariantPrimitiveType ObjectSentinel = (VariantPrimitiveType)254;
        private const VariantPrimitiveType ArraySentinel = (VariantPrimitiveType)255;

        // ---------------------------------------------------------------
        // Singletons
        // ---------------------------------------------------------------

        /// <summary>The null variant value.</summary>
        public static readonly VariantValue Null = default;

        /// <summary>The boolean true variant value.</summary>
        public static readonly VariantValue True = new VariantValue(VariantPrimitiveType.BooleanTrue, 0L);

        /// <summary>The boolean false variant value.</summary>
        public static readonly VariantValue False = new VariantValue(VariantPrimitiveType.BooleanFalse, 0L);

        // ---------------------------------------------------------------
        // Factory methods — primitives
        // ---------------------------------------------------------------

        /// <summary>Creates a boolean variant value.</summary>
        public static VariantValue FromBoolean(bool value) => value ? True : False;

        /// <summary>Creates an Int8 variant value.</summary>
        public static VariantValue FromInt8(sbyte value) =>
            new VariantValue(VariantPrimitiveType.Int8, (long)value);

        /// <summary>Creates an Int16 variant value.</summary>
        public static VariantValue FromInt16(short value) =>
            new VariantValue(VariantPrimitiveType.Int16, (long)value);

        /// <summary>Creates an Int32 variant value.</summary>
        public static VariantValue FromInt32(int value) =>
            new VariantValue(VariantPrimitiveType.Int32, (long)value);

        /// <summary>Creates an Int64 variant value.</summary>
        public static VariantValue FromInt64(long value) =>
            new VariantValue(VariantPrimitiveType.Int64, value);

        /// <summary>Creates a Float variant value.</summary>
        public static VariantValue FromFloat(float value)
        {
            int bits = Unsafe.As<float, int>(ref value);
            return new VariantValue(VariantPrimitiveType.Float, (long)bits);
        }

        /// <summary>Creates a Double variant value.</summary>
        public static VariantValue FromDouble(double value) =>
            new VariantValue(VariantPrimitiveType.Double, BitConverter.DoubleToInt64Bits(value));

        /// <summary>Creates a Decimal4 variant value.</summary>
        public static VariantValue FromDecimal4(decimal value) =>
            new VariantValue(VariantPrimitiveType.Decimal4, (object)value);

        /// <summary>Creates a Decimal8 variant value.</summary>
        public static VariantValue FromDecimal8(decimal value) =>
            new VariantValue(VariantPrimitiveType.Decimal8, (object)value);

        /// <summary>Creates a Decimal16 variant value.</summary>
        public static VariantValue FromDecimal16(decimal value) =>
            new VariantValue(VariantPrimitiveType.Decimal16, (object)value);

        /// <summary>
        /// Creates a decimal variant value, choosing the smallest decimal type
        /// that can represent the value (Decimal4, Decimal8, or Decimal16).
        /// </summary>
        public static VariantValue FromDecimal(decimal value)
        {
#if NET8_0_OR_GREATER
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);
#else
            int[] bits = decimal.GetBits(value);
#endif
            // bits[2] = high 32 bits, bits[1] = mid 32 bits, bits[0] = lo 32 bits
            if (bits[2] == 0 && bits[1] == 0)
            {
                return FromDecimal4(value);
            }
            if (bits[2] == 0)
            {
                return FromDecimal8(value);
            }
            return FromDecimal16(value);
        }

        /// <summary>
        /// Creates a decimal variant value from a <see cref="SqlDecimal"/>.
        /// Values fitting in <see cref="decimal"/> are stored as Decimal4/8/16 using
        /// the smallest type. Values exceeding <see cref="decimal"/> range are stored
        /// as Decimal16 with <see cref="SqlDecimal"/> storage.
        /// </summary>
        public static VariantValue FromSqlDecimal(SqlDecimal value)
        {
            int[] data = value.Data;
            // SqlDecimal.Data: [0]=least-significant, [3]=most-significant
            if (data[3] != 0)
            {
                // Exceeds 96 bits — must store as SqlDecimal
                SqlDecimal normalized = SqlDecimal.ConvertToPrecScale(value, 38, value.Scale);
                return new VariantValue(VariantPrimitiveType.Decimal16, (object)normalized);
            }

            // Fits in decimal — convert and dispatch
            decimal d = value.Value;
            if (data[2] == 0 && data[1] == 0)
            {
                return FromDecimal4(d);
            }
            if (data[2] == 0)
            {
                return FromDecimal8(d);
            }
            return FromDecimal16(d);
        }

        /// <summary>Creates a Date variant value from days since epoch.</summary>
        public static VariantValue FromDate(int daysSinceEpoch) =>
            new VariantValue(VariantPrimitiveType.Date, (long)daysSinceEpoch);

        /// <summary>Creates a Date variant value from a DateTime.</summary>
        public static VariantValue FromDate(DateTime date)
        {
            int days = (int)(date.Date - VariantEncodingHelper.UnixEpochUtc).TotalDays;
            return FromDate(days);
        }

        /// <summary>Creates a Timestamp variant value from microseconds since epoch (UTC).</summary>
        public static VariantValue FromTimestamp(long microseconds) =>
            new VariantValue(VariantPrimitiveType.Timestamp, microseconds);

        /// <summary>Creates a Timestamp variant value from a DateTimeOffset.</summary>
        public static VariantValue FromTimestamp(DateTimeOffset value)
        {
            long ticks = value.UtcTicks - VariantEncodingHelper.UnixEpochUtc.Ticks;
            return FromTimestamp(ticks / 10);
        }

        /// <summary>Creates a TimestampNtz variant value from microseconds since epoch.</summary>
        public static VariantValue FromTimestampNtz(long microseconds) =>
            new VariantValue(VariantPrimitiveType.TimestampNtz, microseconds);

        /// <summary>Creates a TimestampNtz variant value from a DateTime.</summary>
        public static VariantValue FromTimestampNtz(DateTime value)
        {
            long ticks = value.Ticks - VariantEncodingHelper.UnixEpochUnspecified.Ticks;
            return FromTimestampNtz(ticks / 10);
        }

        /// <summary>Creates a TimeNtz variant value from microseconds since midnight.</summary>
        public static VariantValue FromTimeNtz(long microseconds) =>
            new VariantValue(VariantPrimitiveType.TimeNtz, microseconds);

        /// <summary>Creates a TimestampTzNanos variant value from nanoseconds since epoch (UTC).</summary>
        public static VariantValue FromTimestampTzNanos(long nanoseconds) =>
            new VariantValue(VariantPrimitiveType.TimestampTzNanos, nanoseconds);

        /// <summary>Creates a TimestampNtzNanos variant value from nanoseconds since epoch.</summary>
        public static VariantValue FromTimestampNtzNanos(long nanoseconds) =>
            new VariantValue(VariantPrimitiveType.TimestampNtzNanos, nanoseconds);

        /// <summary>Creates a String variant value.</summary>
        public static VariantValue FromString(string value) =>
            new VariantValue(VariantPrimitiveType.String, value ?? throw new ArgumentNullException(nameof(value)));

        /// <summary>Creates a Binary variant value.</summary>
        public static VariantValue FromBinary(byte[] value) =>
            new VariantValue(VariantPrimitiveType.Binary, value ?? throw new ArgumentNullException(nameof(value)));

        /// <summary>Creates a UUID variant value.</summary>
        public static VariantValue FromUuid(Guid value) =>
            new VariantValue(VariantPrimitiveType.Uuid, (object)value);

        // ---------------------------------------------------------------
        // Factory methods — containers
        // ---------------------------------------------------------------

        /// <summary>
        /// Creates an object variant value from a dictionary of field name to value.
        /// </summary>
        public static VariantValue FromObject(IDictionary<string, VariantValue> fields)
        {
            if (fields == null) throw new ArgumentNullException(nameof(fields));
            Dictionary<string, VariantValue> copy = new Dictionary<string, VariantValue>(fields);
            return new VariantValue(ObjectSentinel, copy);
        }

        /// <summary>
        /// Creates an array variant value from a list of elements.
        /// </summary>
        public static VariantValue FromArray(IList<VariantValue> elements)
        {
            if (elements == null) throw new ArgumentNullException(nameof(elements));
            List<VariantValue> copy = new List<VariantValue>(elements);
            return new VariantValue(ArraySentinel, copy);
        }

        /// <summary>Creates an array variant value from params.</summary>
        public static VariantValue FromArray(params VariantValue[] elements) =>
            FromArray((IList<VariantValue>)elements);

        // ---------------------------------------------------------------
        // Type inspection
        // ---------------------------------------------------------------

        /// <summary>Gets the primitive type. For objects/arrays this is a sentinel value.</summary>
        public VariantPrimitiveType PrimitiveType => _primitiveType;

        /// <summary>Returns true if this is a null value.</summary>
        public bool IsNull => _primitiveType == VariantPrimitiveType.NullType;

        /// <summary>Returns true if this is a boolean value.</summary>
        public bool IsBoolean =>
            _primitiveType == VariantPrimitiveType.BooleanTrue ||
            _primitiveType == VariantPrimitiveType.BooleanFalse;

        /// <summary>Returns true if this is a string value.</summary>
        public bool IsString => _primitiveType == VariantPrimitiveType.String;

        /// <summary>Returns true if this is an object value.</summary>
        public bool IsObject => _primitiveType == ObjectSentinel;

        /// <summary>Returns true if this is an array value.</summary>
        public bool IsArray => _primitiveType == ArraySentinel;

        // ---------------------------------------------------------------
        // Typed accessors
        // ---------------------------------------------------------------

        /// <summary>Gets the boolean value.</summary>
        public bool AsBoolean()
        {
            if (_primitiveType == VariantPrimitiveType.BooleanTrue) return true;
            if (_primitiveType == VariantPrimitiveType.BooleanFalse) return false;
            throw new InvalidOperationException($"Cannot read boolean from variant type {_primitiveType}.");
        }

        /// <summary>Gets the Int8 value.</summary>
        public sbyte AsInt8()
        {
            if (_primitiveType != VariantPrimitiveType.Int8)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Int8} but got {_primitiveType}.");
            return (sbyte)_inlineValue;
        }

        /// <summary>Gets the Int16 value.</summary>
        public short AsInt16()
        {
            if (_primitiveType != VariantPrimitiveType.Int16)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Int16} but got {_primitiveType}.");
            return (short)_inlineValue;
        }

        /// <summary>Gets the Int32 value.</summary>
        public int AsInt32()
        {
            if (_primitiveType != VariantPrimitiveType.Int32)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Int32} but got {_primitiveType}.");
            return (int)_inlineValue;
        }

        /// <summary>Gets the Int64 value.</summary>
        public long AsInt64()
        {
            if (_primitiveType != VariantPrimitiveType.Int64)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Int64} but got {_primitiveType}.");
            return _inlineValue;
        }

        /// <summary>Gets the Float value.</summary>
        public float AsFloat()
        {
            if (_primitiveType != VariantPrimitiveType.Float)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Float} but got {_primitiveType}.");
            int bits = (int)_inlineValue;
            return Unsafe.As<int, float>(ref bits);
        }

        /// <summary>Gets the Double value.</summary>
        public double AsDouble()
        {
            if (_primitiveType != VariantPrimitiveType.Double)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Double} but got {_primitiveType}.");
            return BitConverter.Int64BitsToDouble(_inlineValue);
        }

        /// <summary>Gets a decimal value (works for Decimal4, Decimal8, and Decimal16).</summary>
        /// <remarks>
        /// For Decimal16 values stored as <see cref="SqlDecimal"/> (exceeding 96 bits),
        /// this will throw <see cref="OverflowException"/>. Use <see cref="AsSqlDecimal()"/> instead.
        /// </remarks>
        public decimal AsDecimal()
        {
            if (_primitiveType == VariantPrimitiveType.Decimal4 ||
                _primitiveType == VariantPrimitiveType.Decimal8 ||
                _primitiveType == VariantPrimitiveType.Decimal16)
            {
                if (_objectValue is SqlDecimal sd)
                    return sd.Value;
                return (decimal)_objectValue;
            }
            throw new InvalidOperationException($"Cannot read decimal from variant type {_primitiveType}.");
        }

        /// <summary>
        /// Gets a decimal value as a <see cref="SqlDecimal"/> (works for Decimal4, Decimal8, and Decimal16).
        /// Unlike <see cref="AsDecimal()"/>, this method does not throw for large Decimal16 values.
        /// </summary>
        public SqlDecimal AsSqlDecimal()
        {
            if (_primitiveType == VariantPrimitiveType.Decimal4 ||
                _primitiveType == VariantPrimitiveType.Decimal8 ||
                _primitiveType == VariantPrimitiveType.Decimal16)
            {
                if (_objectValue is SqlDecimal sd)
                    return sd;
                return new SqlDecimal((decimal)_objectValue);
            }
            throw new InvalidOperationException($"Cannot read decimal from variant type {_primitiveType}.");
        }

        /// <summary>
        /// Returns true when the decimal value is stored internally as <see cref="SqlDecimal"/>
        /// (i.e., it exceeds the range of <see cref="decimal"/>).
        /// </summary>
        internal bool IsSqlDecimalStorage =>
            (_primitiveType == VariantPrimitiveType.Decimal4 ||
             _primitiveType == VariantPrimitiveType.Decimal8 ||
             _primitiveType == VariantPrimitiveType.Decimal16) &&
            _objectValue is SqlDecimal;

        /// <summary>Gets the Date value as days since epoch.</summary>
        public int AsDateDays()
        {
            if (_primitiveType != VariantPrimitiveType.Date)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Date} but got {_primitiveType}.");
            return (int)_inlineValue;
        }

        /// <summary>Gets the Date as a DateTime.</summary>
        public DateTime AsDate()
        {
            int days = AsDateDays();
            return VariantEncodingHelper.UnixEpochUtc.AddDays(days);
        }

        /// <summary>Gets a Timestamp value as microseconds since epoch.</summary>
        public long AsTimestampMicros()
        {
            if (_primitiveType != VariantPrimitiveType.Timestamp)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Timestamp} but got {_primitiveType}.");
            return _inlineValue;
        }

        /// <summary>Gets a Timestamp as a DateTimeOffset (UTC).</summary>
        public DateTimeOffset AsTimestamp()
        {
            long micros = AsTimestampMicros();
            return VariantEncodingHelper.UnixEpochOffset.AddTicks(micros * 10);
        }

        /// <summary>Gets a TimestampNtz value as microseconds since epoch.</summary>
        public long AsTimestampNtzMicros()
        {
            if (_primitiveType != VariantPrimitiveType.TimestampNtz)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.TimestampNtz} but got {_primitiveType}.");
            return _inlineValue;
        }

        /// <summary>Gets a TimestampNtz as a DateTime.</summary>
        public DateTime AsTimestampNtz()
        {
            long micros = AsTimestampNtzMicros();
            return VariantEncodingHelper.UnixEpochUnspecified.AddTicks(micros * 10);
        }

        /// <summary>Gets a TimeNtz value as microseconds since midnight.</summary>
        public long AsTimeNtzMicros()
        {
            if (_primitiveType != VariantPrimitiveType.TimeNtz)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.TimeNtz} but got {_primitiveType}.");
            return _inlineValue;
        }

        /// <summary>Gets a TimestampTzNanos value as nanoseconds since epoch.</summary>
        public long AsTimestampTzNanos()
        {
            if (_primitiveType != VariantPrimitiveType.TimestampTzNanos)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.TimestampTzNanos} but got {_primitiveType}.");
            return _inlineValue;
        }

        /// <summary>Gets a TimestampNtzNanos value as nanoseconds since epoch.</summary>
        public long AsTimestampNtzNanos()
        {
            if (_primitiveType != VariantPrimitiveType.TimestampNtzNanos)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.TimestampNtzNanos} but got {_primitiveType}.");
            return _inlineValue;
        }

        /// <summary>Gets the string value.</summary>
        public string AsString()
        {
            if (_primitiveType != VariantPrimitiveType.String)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.String} but got {_primitiveType}.");
            return (string)_objectValue;
        }

        /// <summary>Gets the binary value.</summary>
        public byte[] AsBinary()
        {
            if (_primitiveType != VariantPrimitiveType.Binary)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Binary} but got {_primitiveType}.");
            return (byte[])_objectValue;
        }

        /// <summary>Gets the UUID value.</summary>
        public Guid AsUuid()
        {
            if (_primitiveType != VariantPrimitiveType.Uuid)
                throw new InvalidOperationException($"Expected variant type {VariantPrimitiveType.Uuid} but got {_primitiveType}.");
            return (Guid)_objectValue;
        }

        /// <summary>Gets the object fields as a read-only dictionary.</summary>
        public IReadOnlyDictionary<string, VariantValue> AsObject()
        {
            if (_primitiveType != ObjectSentinel)
            {
                throw new InvalidOperationException($"Cannot read object from variant type {_primitiveType}.");
            }
            return (Dictionary<string, VariantValue>)_objectValue;
        }

        /// <summary>Gets the array elements as a read-only list.</summary>
        public IReadOnlyList<VariantValue> AsArray()
        {
            if (_primitiveType != ArraySentinel)
            {
                throw new InvalidOperationException($"Cannot read array from variant type {_primitiveType}.");
            }
            return (List<VariantValue>)_objectValue;
        }

        // ---------------------------------------------------------------
        // Equality
        // ---------------------------------------------------------------

        /// <inheritdoc />
        public bool Equals(VariantValue other)
        {
            if (_primitiveType != other._primitiveType) return false;

            // Null and booleans: no payload to compare
            if (_objectValue == null && other._objectValue == null) return _inlineValue == other._inlineValue;
            if (_objectValue == null || other._objectValue == null) return false;

            // Binary needs byte-level comparison
            if (_primitiveType == VariantPrimitiveType.Binary)
            {
                byte[] a = (byte[])_objectValue;
                byte[] b = (byte[])other._objectValue;
                if (ReferenceEquals(a, b)) return true;
                if (a.Length != b.Length) return false;
                return a.AsSpan().SequenceEqual(b);
            }

            // Object: compare dictionaries
            if (_primitiveType == ObjectSentinel)
            {
                Dictionary<string, VariantValue> a = (Dictionary<string, VariantValue>)_objectValue;
                Dictionary<string, VariantValue> b = (Dictionary<string, VariantValue>)other._objectValue;
                if (a.Count != b.Count) return false;
                foreach (KeyValuePair<string, VariantValue> kv in a)
                {
                    if (!b.TryGetValue(kv.Key, out VariantValue bVal) || !kv.Value.Equals(bVal))
                    {
                        return false;
                    }
                }
                return true;
            }

            // Array: compare element lists
            if (_primitiveType == ArraySentinel)
            {
                List<VariantValue> a = (List<VariantValue>)_objectValue;
                List<VariantValue> b = (List<VariantValue>)other._objectValue;
                if (a.Count != b.Count) return false;
                for (int i = 0; i < a.Count; i++)
                {
                    if (!a[i].Equals(b[i])) return false;
                }
                return true;
            }

            return _objectValue.Equals(other._objectValue);
        }

        /// <inheritdoc />
        public override bool Equals(object obj) => obj is VariantValue other && Equals(other);

        /// <inheritdoc />
        public override int GetHashCode()
        {
            if (_objectValue == null) return _primitiveType.GetHashCode() ^ _inlineValue.GetHashCode();
            if (_primitiveType == VariantPrimitiveType.Binary)
            {
                byte[] bytes = (byte[])_objectValue;
#if NET8_0_OR_GREATER
                HashCode hc = new HashCode();
                hc.Add(_primitiveType);
                hc.AddBytes(bytes);
                return hc.ToHashCode();
#else
                int hash = _primitiveType.GetHashCode();
                ReadOnlySpan<byte> span = bytes;
                while (span.Length >= 4)
                {
                    hash = hash * 397 ^ System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(span);
                    span = span.Slice(4);
                }
                for (int i = 0; i < span.Length; i++)
                {
                    hash = hash * 31 ^ span[i];
                }
                return hash;
#endif
            }
            // Object: order-independent hash of key/value pairs (consistent with Equals)
            if (_primitiveType == ObjectSentinel)
            {
                Dictionary<string, VariantValue> dict = (Dictionary<string, VariantValue>)_objectValue;
                int hash = ObjectSentinel.GetHashCode() ^ dict.Count;
                foreach (KeyValuePair<string, VariantValue> kv in dict)
                {
                    // XOR is order-independent, matching unordered dictionary equality
                    hash ^= kv.Key.GetHashCode() ^ kv.Value.GetHashCode();
                }
                return hash;
            }

            // Array: order-dependent hash of elements (consistent with Equals)
            if (_primitiveType == ArraySentinel)
            {
                List<VariantValue> list = (List<VariantValue>)_objectValue;
                int hash = ArraySentinel.GetHashCode() ^ list.Count;
                for (int i = 0; i < list.Count; i++)
                {
                    hash = hash * 31 + list[i].GetHashCode();
                }
                return hash;
            }

            return _primitiveType.GetHashCode() ^ _objectValue.GetHashCode();
        }

        /// <summary>Equality operator.</summary>
        public static bool operator ==(VariantValue left, VariantValue right) => left.Equals(right);

        /// <summary>Inequality operator.</summary>
        public static bool operator !=(VariantValue left, VariantValue right) => !(left == right);

        /// <inheritdoc />
        public override string ToString() => _primitiveType switch
        {
            VariantPrimitiveType.NullType => "null",
            VariantPrimitiveType.BooleanTrue => "true",
            VariantPrimitiveType.BooleanFalse => "false",
            VariantPrimitiveType.String => $"\"{AsString()}\"",
            VariantPrimitiveType.Float => AsFloat().ToString(CultureInfo.InvariantCulture),
            VariantPrimitiveType.Double => AsDouble().ToString(CultureInfo.InvariantCulture),
            VariantPrimitiveType.Decimal4 or VariantPrimitiveType.Decimal8 => AsDecimal().ToString(CultureInfo.InvariantCulture),
            VariantPrimitiveType.Decimal16 => AsSqlDecimal().ToString(),
            ObjectSentinel => $"{{object with {AsObject().Count} fields}}",
            ArraySentinel => $"[array with {AsArray().Count} elements]",
            _ => _inlineValue.ToString(CultureInfo.InvariantCulture),
        };
    }
}
