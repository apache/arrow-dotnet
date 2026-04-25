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
using System.Data.SqlTypes;
using Apache.Arrow.Arrays;
using Apache.Arrow.Scalars.Variant;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Zero-copy reader for a single row of a (possibly shredded) variant column.
    /// Composes with the <see cref="ShredSchema"/> for this position to expose the
    /// typed columns and residual bytes side-by-side, or to materialize the logical
    /// value on demand.
    /// <para>
    /// A <see cref="ShreddedVariant"/> does not own any Arrow buffers; it is only
    /// valid while the underlying Arrow arrays are alive.
    /// </para>
    /// </summary>
    public ref struct ShreddedVariant
    {
        private readonly ShredSchema _schema;
        private readonly ReadOnlySpan<byte> _metadata;
        // _valueArray is the residual binary column at this level (may be null).
        private readonly IArrowArray _valueArray;
        // _typedValueArray is the typed column at this level (may be null if no shredding here).
        private readonly IArrowArray _typedValueArray;
        private readonly int _index;

        internal ShreddedVariant(
            ShredSchema schema,
            ReadOnlySpan<byte> metadata,
            IArrowArray valueArray,
            IArrowArray typedValueArray,
            int index)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _metadata = metadata;
            _valueArray = valueArray;
            _typedValueArray = typedValueArray;
            _index = index;
        }

        /// <summary>The schema describing how this slot is shredded.</summary>
        public ShredSchema Schema => _schema;

        /// <summary>The column-level variant metadata.</summary>
        public ReadOnlySpan<byte> Metadata => _metadata;

        /// <summary>True when the residual <c>value</c> column has a value at this index.</summary>
        public bool HasResidual => _valueArray != null && !_valueArray.IsNull(_index);

        /// <summary>True when the <c>typed_value</c> column has a value at this index.</summary>
        public bool HasTypedValue => _typedValueArray != null && !_typedValueArray.IsNull(_index);

        /// <summary>
        /// True when neither the residual nor the typed column is populated at this index
        /// — valid only for sub-fields of shredded objects.
        /// </summary>
        public bool IsMissing => !HasResidual && !HasTypedValue;

        /// <summary>
        /// Materializes this slot into a logical <see cref="VariantValue"/>, merging
        /// typed-column values with residual bytes per the shredding spec.
        /// </summary>
        /// <exception cref="InvalidOperationException">If the slot is missing.</exception>
        public VariantValue ToVariantValue()
        {
            // Both-null at this slot means the logical value is variant null. (The
            // "missing" encoding — omitting the field entirely from the output — is
            // a choice made by the container: see ShreddedObject, which uses
            // IsMissing to decide whether to drop a sub-field.)
            if (IsMissing)
            {
                return VariantValue.Null;
            }

            switch (_schema.TypedValueType)
            {
                case ShredType.None:
                    return ReadResidual();

                case ShredType.Object:
                    return GetObject().ToVariantValue();

                case ShredType.Array:
                    return GetArray().ToVariantValue();

                default:
                    // Primitive shredding. Per the Parquet variant shredding spec, a
                    // primitive slot may have at most one of value / typed_value set.
                    // If both are populated at the same row, the shredded data is
                    // invalid and implementations should reject it.
                    if (HasTypedValue)
                    {
                        if (HasResidual)
                        {
                            throw new InvalidOperationException(
                                "Invalid shredded variant: primitive slot has both 'value' and 'typed_value' populated.");
                        }
                        return ReadTypedPrimitive();
                    }
                    return ReadResidual();
            }
        }

        /// <summary>
        /// If the residual column has a value at this index, returns a
        /// <see cref="VariantReader"/> over its bytes.
        /// </summary>
        public bool TryGetResidualReader(out VariantReader reader)
        {
            if (HasResidual)
            {
                BinaryArray binary = (BinaryArray)_valueArray;
                ReadOnlySpan<byte> bytes = binary.GetBytes(_index, out _);
                reader = new VariantReader(_metadata, bytes);
                return true;
            }
            reader = default;
            return false;
        }

        /// <summary>
        /// Reader for a shredded object at this slot. Valid only when the schema's
        /// <see cref="ShredSchema.TypedValueType"/> is <see cref="ShredType.Object"/>.
        /// </summary>
        public ShreddedObject GetObject()
        {
            if (_schema.TypedValueType != ShredType.Object)
            {
                throw new InvalidOperationException(
                    $"Slot is not shredded as an object (schema type {_schema.TypedValueType}).");
            }
            return new ShreddedObject(_schema, _metadata, _typedValueArray as StructArray, _valueArray, _index);
        }

        /// <summary>
        /// Reader for a shredded array at this slot. Valid only when the schema's
        /// <see cref="ShredSchema.TypedValueType"/> is <see cref="ShredType.Array"/>.
        /// </summary>
        public ShreddedArray GetArray()
        {
            if (_schema.TypedValueType != ShredType.Array)
            {
                throw new InvalidOperationException(
                    $"Slot is not shredded as an array (schema type {_schema.TypedValueType}).");
            }
            return new ShreddedArray(_schema, _metadata, _typedValueArray as ListArray, _valueArray, _index);
        }

        private VariantValue ReadResidual()
        {
            if (!HasResidual)
            {
                throw new InvalidOperationException("No residual value to read.");
            }
            BinaryArray binary = (BinaryArray)_valueArray;
            ReadOnlySpan<byte> bytes = binary.GetBytes(_index, out _);
            return new VariantReader(_metadata, bytes).ToVariantValue();
        }

        // ---------------------------------------------------------------
        // Typed-column accessors — zero-copy access to the shredded value
        // without materializing a VariantValue.
        //
        // Each getter requires:
        //   (a) the slot's schema to match the requested type, and
        //   (b) the typed_value column to be populated at this index.
        // Otherwise it throws. Callers that want automatic residual fallback
        // should use ToVariantValue instead.
        // ---------------------------------------------------------------

        /// <summary>Reads the shredded boolean value at this slot.</summary>
        public bool GetBoolean() => ((BooleanArray)RequireTyped(ShredType.Boolean)).GetValue(_index).Value;

        /// <summary>Reads the shredded 8-bit signed integer at this slot.</summary>
        public sbyte GetInt8() => ((Int8Array)RequireTyped(ShredType.Int8)).GetValue(_index).Value;

        /// <summary>Reads the shredded 16-bit signed integer at this slot.</summary>
        public short GetInt16() => ((Int16Array)RequireTyped(ShredType.Int16)).GetValue(_index).Value;

        /// <summary>Reads the shredded 32-bit signed integer at this slot.</summary>
        public int GetInt32() => ((Int32Array)RequireTyped(ShredType.Int32)).GetValue(_index).Value;

        /// <summary>Reads the shredded 64-bit signed integer at this slot.</summary>
        public long GetInt64() => ((Int64Array)RequireTyped(ShredType.Int64)).GetValue(_index).Value;

        /// <summary>Reads the shredded 32-bit float at this slot.</summary>
        public float GetFloat() => ((FloatArray)RequireTyped(ShredType.Float)).GetValue(_index).Value;

        /// <summary>Reads the shredded 64-bit double at this slot.</summary>
        public double GetDouble() => ((DoubleArray)RequireTyped(ShredType.Double)).GetValue(_index).Value;

        /// <summary>
        /// Reads the shredded decimal value at this slot. Works for Decimal4, Decimal8,
        /// and Decimal16 shred types, regardless of whether the Arrow column is backed
        /// by Decimal32Array, Decimal64Array, or Decimal128Array.
        /// </summary>
        public decimal GetDecimal()
        {
            RequireDecimalSchema();
            if (!HasTypedValue) ThrowNoTyped();
            IArrowArray arr = UnwrapExtension(_typedValueArray);
            switch (arr)
            {
                case Decimal32Array d32: return d32.GetValue(_index).Value;
                case Decimal64Array d64: return d64.GetValue(_index).Value;
                case Decimal128Array d128: return d128.GetValue(_index).Value;
                default:
                    throw new InvalidOperationException(
                        $"Shredded decimal column is backed by {arr.GetType().Name}, which is not a supported decimal array type.");
            }
        }

        /// <summary>
        /// Reads the shredded decimal value at this slot. Works for Decimal4, Decimal8,
        /// and Decimal16 shred types, regardless of whether the Arrow column is backed
        /// by Decimal32Array, Decimal64Array, or Decimal128Array.
        /// </summary>
        public SqlDecimal GetSqlDecimal()
        {
            RequireDecimalSchema();
            if (!HasTypedValue) ThrowNoTyped();
            IArrowArray arr = UnwrapExtension(_typedValueArray);
            switch (arr)
            {
                case Decimal32Array d32: return d32.GetValue(_index).Value;
                case Decimal64Array d64: return d64.GetValue(_index).Value;
                case Decimal128Array d128: return d128.GetSqlDecimal(_index).Value;
                default:
                    throw new InvalidOperationException(
                        $"Shredded decimal column is backed by {arr.GetType().Name}, which is not a supported decimal array type.");
            }
        }

        /// <summary>Reads the shredded date (days since epoch) at this slot.</summary>
        public int GetDateDays() => ((Date32Array)RequireTyped(ShredType.Date)).GetValue(_index).Value;

        /// <summary>Reads the shredded timestamp (microseconds since epoch, UTC) at this slot.</summary>
        public long GetTimestampMicros() => ((TimestampArray)RequireTyped(ShredType.Timestamp)).GetValue(_index).Value;

        /// <summary>Reads the shredded timestamp-without-tz (microseconds since epoch) at this slot.</summary>
        public long GetTimestampNtzMicros() => ((TimestampArray)RequireTyped(ShredType.TimestampNtz)).GetValue(_index).Value;

        /// <summary>Reads the shredded time-without-tz (microseconds since midnight) at this slot.</summary>
        public long GetTimeNtzMicros() => ((Time64Array)RequireTyped(ShredType.TimeNtz)).GetValue(_index).Value;

        /// <summary>Reads the shredded timestamp-with-tz (nanoseconds since epoch) at this slot.</summary>
        public long GetTimestampTzNanos() => ((TimestampArray)RequireTyped(ShredType.TimestampTzNanos)).GetValue(_index).Value;

        /// <summary>Reads the shredded timestamp-without-tz (nanoseconds since epoch) at this slot.</summary>
        public long GetTimestampNtzNanos() => ((TimestampArray)RequireTyped(ShredType.TimestampNtzNanos)).GetValue(_index).Value;

        /// <summary>Reads the shredded string value at this slot.</summary>
        public string GetString() => ((StringArray)RequireTyped(ShredType.String)).GetString(_index);

        /// <summary>Reads the shredded binary value at this slot as a byte span.</summary>
        public ReadOnlySpan<byte> GetBinaryBytes() => ((BinaryArray)RequireTyped(ShredType.Binary)).GetBytes(_index);

        /// <summary>Reads the shredded UUID at this slot.</summary>
        public Guid GetUuid()
        {
            FixedSizeBinaryArray arr = (FixedSizeBinaryArray)RequireTyped(ShredType.Uuid);
            ReadOnlySpan<byte> raw = arr.GetBytes(_index);
#if NET8_0_OR_GREATER
            return new Guid(raw, bigEndian: true);
#else
            byte[] bytes = new byte[16];
            bytes[0] = raw[3]; bytes[1] = raw[2]; bytes[2] = raw[1]; bytes[3] = raw[0];
            bytes[4] = raw[5]; bytes[5] = raw[4];
            bytes[6] = raw[7]; bytes[7] = raw[6];
            raw.Slice(8, 8).CopyTo(bytes.AsSpan(8));
            return new Guid(bytes);
#endif
        }

        /// <summary>Reads the shredded UUID at this slot as raw big-endian (RFC 4122) bytes.</summary>
        public ReadOnlySpan<byte> GetUuidBytes()
            => ((FixedSizeBinaryArray)RequireTyped(ShredType.Uuid)).GetBytes(_index);

        // ---------------------------------------------------------------
        // Primitive dispatch for internal materialization. Delegates to the
        // typed getters so the two paths stay in sync.
        // ---------------------------------------------------------------

        private VariantValue ReadTypedPrimitive()
        {
            switch (_schema.TypedValueType)
            {
                case ShredType.Boolean:      return VariantValue.FromBoolean(GetBoolean());
                case ShredType.Int8:         return VariantValue.FromInt8(GetInt8());
                case ShredType.Int16:        return VariantValue.FromInt16(GetInt16());
                case ShredType.Int32:        return VariantValue.FromInt32(GetInt32());
                case ShredType.Int64:        return VariantValue.FromInt64(GetInt64());
                case ShredType.Float:        return VariantValue.FromFloat(GetFloat());
                case ShredType.Double:       return VariantValue.FromDouble(GetDouble());
                case ShredType.Decimal4:     return VariantValue.FromDecimal4(GetDecimal());
                case ShredType.Decimal8:     return VariantValue.FromDecimal8(GetDecimal());
                case ShredType.Decimal16:    return VariantValue.FromSqlDecimal(GetSqlDecimal());
                case ShredType.Date:         return VariantValue.FromDate(GetDateDays());
                case ShredType.Timestamp:    return VariantValue.FromTimestamp(GetTimestampMicros());
                case ShredType.TimestampNtz: return VariantValue.FromTimestampNtz(GetTimestampNtzMicros());
                case ShredType.TimeNtz:      return VariantValue.FromTimeNtz(GetTimeNtzMicros());
                case ShredType.TimestampTzNanos:    return VariantValue.FromTimestampTzNanos(GetTimestampTzNanos());
                case ShredType.TimestampNtzNanos:   return VariantValue.FromTimestampNtzNanos(GetTimestampNtzNanos());
                case ShredType.String:       return VariantValue.FromString(GetString());
                case ShredType.Binary:       return VariantValue.FromBinary(GetBinaryBytes().ToArray());
                case ShredType.Uuid:         return VariantValue.FromUuid(GetUuid());
                default:
                    throw new InvalidOperationException(
                        $"Unexpected primitive shred type {_schema.TypedValueType}.");
            }
        }

        private IArrowArray RequireTyped(ShredType expected)
        {
            if (_schema.TypedValueType != expected)
            {
                throw new InvalidOperationException(
                    $"Slot schema is {_schema.TypedValueType}, not {expected}.");
            }
            if (!HasTypedValue) ThrowNoTyped();
            return UnwrapExtension(_typedValueArray);
        }

        private void RequireDecimalSchema()
        {
            if (_schema.TypedValueType != ShredType.Decimal4 &&
                _schema.TypedValueType != ShredType.Decimal8 &&
                _schema.TypedValueType != ShredType.Decimal16)
            {
                throw new InvalidOperationException(
                    $"Slot schema is {_schema.TypedValueType}, not a decimal type.");
            }
        }

        private void ThrowNoTyped() =>
            throw new InvalidOperationException(
                "No typed_value at this index (check HasTypedValue first, or use ToVariantValue for residual fallback).");

        private static IArrowArray UnwrapExtension(IArrowArray arr) =>
            arr is ExtensionArray ext ? ext.Storage : arr;
    }
}
