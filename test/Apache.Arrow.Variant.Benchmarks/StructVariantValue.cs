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

namespace Apache.Arrow.Variant.Benchmarks
{
    /// <summary>
    /// Struct variant value with inline storage for numeric primitives.
    /// Numeric types are stored in _inlineValue (no boxing), reference types
    /// in _objectValue.
    /// Size on x64: 24 bytes inline (VariantPrimitiveType + long + object ref).
    /// </summary>
    public readonly struct StructVariantValue : IEquatable<StructVariantValue>
    {
        private readonly VariantPrimitiveType _primitiveType;
        private readonly long _inlineValue;
        private readonly object _objectValue;

        private StructVariantValue(VariantPrimitiveType primitiveType, long inlineValue)
        {
            _primitiveType = primitiveType;
            _inlineValue = inlineValue;
            _objectValue = null;
        }

        private StructVariantValue(VariantPrimitiveType primitiveType, object objectValue)
        {
            _primitiveType = primitiveType;
            _inlineValue = 0;
            _objectValue = objectValue;
        }

        private const VariantPrimitiveType ObjectSentinel = (VariantPrimitiveType)254;
        private const VariantPrimitiveType ArraySentinel = (VariantPrimitiveType)255;

        // default(StructVariantValue) has _primitiveType = NullType = 0
        public static readonly StructVariantValue Null = default;

        // ---------------------------------------------------------------
        // Factory methods — inline storage (no boxing)
        // ---------------------------------------------------------------

        public static StructVariantValue FromBoolean(bool value) =>
            new StructVariantValue(
                value ? VariantPrimitiveType.BooleanTrue : VariantPrimitiveType.BooleanFalse,
                0L);

        public static StructVariantValue FromInt32(int value) =>
            new StructVariantValue(VariantPrimitiveType.Int32, (long)value);

        public static StructVariantValue FromInt64(long value) =>
            new StructVariantValue(VariantPrimitiveType.Int64, value);

        public static StructVariantValue FromFloat(float value) =>
            new StructVariantValue(VariantPrimitiveType.Float, (long)BitConverter.SingleToInt32Bits(value));

        public static StructVariantValue FromDouble(double value) =>
            new StructVariantValue(VariantPrimitiveType.Double, BitConverter.DoubleToInt64Bits(value));

        // ---------------------------------------------------------------
        // Factory methods — object storage
        // ---------------------------------------------------------------

        public static StructVariantValue FromString(string value) =>
            new StructVariantValue(VariantPrimitiveType.String, value);

        public static StructVariantValue FromObject(Dictionary<string, StructVariantValue> fields) =>
            new StructVariantValue(ObjectSentinel, fields);

        public static StructVariantValue FromArray(List<StructVariantValue> elements) =>
            new StructVariantValue(ArraySentinel, elements);

        // ---------------------------------------------------------------
        // Accessors (minimal — no type guards for benchmark hot paths)
        // ---------------------------------------------------------------

        public VariantPrimitiveType PrimitiveType => _primitiveType;
        public bool IsNull => _primitiveType == VariantPrimitiveType.NullType;

        public bool AsBoolean() => _primitiveType == VariantPrimitiveType.BooleanTrue;
        public int AsInt32() => (int)_inlineValue;
        public long AsInt64() => _inlineValue;
        public float AsFloat() => BitConverter.Int32BitsToSingle((int)_inlineValue);
        public double AsDouble() => BitConverter.Int64BitsToDouble(_inlineValue);
        public string AsString() => (string)_objectValue;

        public Dictionary<string, StructVariantValue> AsObject() =>
            (Dictionary<string, StructVariantValue>)_objectValue;

        public List<StructVariantValue> AsArray() =>
            (List<StructVariantValue>)_objectValue;

        // ---------------------------------------------------------------
        // Equality
        // ---------------------------------------------------------------

        public bool Equals(StructVariantValue other)
        {
            if (_primitiveType != other._primitiveType) return false;
            // For inline types (_objectValue is null), compare the long directly
            if (_objectValue == null && other._objectValue == null)
                return _inlineValue == other._inlineValue;
            if (_objectValue == null || other._objectValue == null) return false;
            return _objectValue.Equals(other._objectValue);
        }

        public override bool Equals(object obj) =>
            obj is StructVariantValue other && Equals(other);

        public override int GetHashCode() =>
            _objectValue == null
                ? _primitiveType.GetHashCode() ^ _inlineValue.GetHashCode()
                : _primitiveType.GetHashCode() ^ _objectValue.GetHashCode();

        public static bool operator ==(StructVariantValue left, StructVariantValue right) =>
            left.Equals(right);

        public static bool operator !=(StructVariantValue left, StructVariantValue right) =>
            !left.Equals(right);
    }
}
