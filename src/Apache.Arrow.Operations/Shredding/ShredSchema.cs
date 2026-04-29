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
using Apache.Arrow;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Describes the shredding schema for a variant column — which fields
    /// to extract into typed Parquet columns and at what types.
    /// </summary>
    public sealed class ShredSchema
    {
        /// <summary>
        /// The type of the typed_value column. For primitives, this is the
        /// expected scalar type. For objects, use <see cref="ShredType.Object"/>
        /// and populate <see cref="ObjectFields"/>. For arrays, use
        /// <see cref="ShredType.Array"/> and populate <see cref="ArrayElement"/>.
        /// <see cref="ShredType.None"/> means no typed_value — everything goes to binary value.
        /// </summary>
        public ShredType TypedValueType { get; }

        /// <summary>
        /// For <see cref="ShredType.Object"/>: the shredding schemas for each named sub-field.
        /// Null for non-object types.
        /// </summary>
        public IReadOnlyDictionary<string, ShredSchema> ObjectFields { get; }

        /// <summary>
        /// For <see cref="ShredType.Array"/>: the shredding schema applied to each element.
        /// Null for non-array types.
        /// </summary>
        public ShredSchema ArrayElement { get; }

        private ShredSchema(ShredType typedValueType, IReadOnlyDictionary<string, ShredSchema> objectFields, ShredSchema arrayElement)
        {
            TypedValueType = typedValueType;
            ObjectFields = objectFields;
            ArrayElement = arrayElement;
        }

        /// <summary>Creates a schema that does no shredding (all values go to binary).</summary>
        public static ShredSchema Unshredded() => new ShredSchema(ShredType.None, null, null);

        /// <summary>
        /// Creates a schema that shreds values into a typed primitive column.
        /// Values not matching this type fall back to the binary value column.
        /// </summary>
        public static ShredSchema Primitive(ShredType type)
        {
            if (type == ShredType.None || type == ShredType.Object || type == ShredType.Array)
            {
                throw new ArgumentException($"Use the appropriate factory method for {type}.", nameof(type));
            }
            return new ShredSchema(type, null, null);
        }

        /// <summary>
        /// Creates a schema that shreds object values by extracting named fields
        /// into typed sub-columns.
        /// </summary>
        public static ShredSchema ForObject(IDictionary<string, ShredSchema> fields)
        {
            if (fields == null) throw new ArgumentNullException(nameof(fields));
            Dictionary<string, ShredSchema> copy = new Dictionary<string, ShredSchema>(fields);
            return new ShredSchema(ShredType.Object, copy, null);
        }

        /// <summary>
        /// Creates a schema that shreds array values by applying the element
        /// schema to each element.
        /// </summary>
        public static ShredSchema ForArray(ShredSchema elementSchema)
        {
            if (elementSchema == null) throw new ArgumentNullException(nameof(elementSchema));
            return new ShredSchema(ShredType.Array, null, elementSchema);
        }

        /// <summary>
        /// Maps a <see cref="VariantPrimitiveType"/> to the corresponding <see cref="ShredType"/>.
        /// </summary>
        public static ShredType ShredTypeFromPrimitive(VariantPrimitiveType primitiveType)
        {
            switch (primitiveType)
            {
                case VariantPrimitiveType.BooleanTrue:
                case VariantPrimitiveType.BooleanFalse:
                    return ShredType.Boolean;
                case VariantPrimitiveType.Int8: return ShredType.Int8;
                case VariantPrimitiveType.Int16: return ShredType.Int16;
                case VariantPrimitiveType.Int32: return ShredType.Int32;
                case VariantPrimitiveType.Int64: return ShredType.Int64;
                case VariantPrimitiveType.Float: return ShredType.Float;
                case VariantPrimitiveType.Double: return ShredType.Double;
                case VariantPrimitiveType.Decimal4: return ShredType.Decimal4;
                case VariantPrimitiveType.Decimal8: return ShredType.Decimal8;
                case VariantPrimitiveType.Decimal16: return ShredType.Decimal16;
                case VariantPrimitiveType.Date: return ShredType.Date;
                case VariantPrimitiveType.Timestamp: return ShredType.Timestamp;
                case VariantPrimitiveType.TimestampNtz: return ShredType.TimestampNtz;
                case VariantPrimitiveType.TimeNtz: return ShredType.TimeNtz;
                case VariantPrimitiveType.TimestampTzNanos: return ShredType.TimestampTzNanos;
                case VariantPrimitiveType.TimestampNtzNanos: return ShredType.TimestampNtzNanos;
                case VariantPrimitiveType.String: return ShredType.String;
                case VariantPrimitiveType.Binary: return ShredType.Binary;
                case VariantPrimitiveType.Uuid: return ShredType.Uuid;
                default: return ShredType.None;
            }
        }

        /// <summary>
        /// Derives a <see cref="ShredSchema"/> from the Arrow type of a <c>typed_value</c> column.
        /// </summary>
        /// <param name="typedValueType">
        /// The <c>typed_value</c> Arrow type, or null for a fully unshredded column.
        /// </param>
        /// <returns>A <see cref="ShredSchema"/> describing the shredding.</returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="typedValueType"/> is not a valid shredded type
        /// per the Parquet variant shredding spec (for example, an unsigned integer or
        /// a fixed-size binary that isn't UUID).
        /// </exception>
        public static ShredSchema FromArrowType(IArrowType typedValueType)
        {
            if (typedValueType == null) return Unshredded();
            return MapArrowType(typedValueType);
        }

        private static ShredSchema MapArrowType(IArrowType type)
        {
            switch (type)
            {
                case BooleanType _: return Primitive(ShredType.Boolean);
                case Int8Type _: return Primitive(ShredType.Int8);
                case Int16Type _: return Primitive(ShredType.Int16);
                case Int32Type _: return Primitive(ShredType.Int32);
                case Int64Type _: return Primitive(ShredType.Int64);
                case FloatType _: return Primitive(ShredType.Float);
                case DoubleType _: return Primitive(ShredType.Double);
                case StringType _: return Primitive(ShredType.String);
                case BinaryType _: return Primitive(ShredType.Binary);
                case LargeBinaryType _: return Primitive(ShredType.Binary);
                case LargeStringType _: return Primitive(ShredType.String);
                case Date32Type _: return Primitive(ShredType.Date);

                case Time64Type t when t.Unit == TimeUnit.Microsecond:
                    return Primitive(ShredType.TimeNtz);

                case TimestampType ts when ts.Unit == TimeUnit.Microsecond && ts.IsTimeZoneAware:
                    return Primitive(ShredType.Timestamp);
                case TimestampType ts when ts.Unit == TimeUnit.Microsecond && !ts.IsTimeZoneAware:
                    return Primitive(ShredType.TimestampNtz);
                case TimestampType ts when ts.Unit == TimeUnit.Nanosecond && ts.IsTimeZoneAware:
                    return Primitive(ShredType.TimestampTzNanos);
                case TimestampType ts when ts.Unit == TimeUnit.Nanosecond && !ts.IsTimeZoneAware:
                    return Primitive(ShredType.TimestampNtzNanos);

                // The Parquet variant spec allows any Arrow decimal representation
                // whose precision fits in one of the variant's decimal widths
                // (≤9 digits → 4-byte unscaled, ≤18 → 8-byte, ≤38 → 16-byte).
                // Decimal128Type extends FixedSizeBinaryType with byte_width=16, so
                // we MUST match the decimal cases before the UUID fallback below,
                // and dispatch by precision inside the cases rather than via `when`
                // guards that can fall through into the FSB(16) branch.
                case Decimal32Type d32: return MapDecimalByPrecision(d32.Precision, type);
                case Decimal64Type d64: return MapDecimalByPrecision(d64.Precision, type);
                case Decimal128Type d128: return MapDecimalByPrecision(d128.Precision, type);

                case ExtensionType ext when ext.Name == "arrow.uuid":
                    return Primitive(ShredType.Uuid);

                // When the Arrow IPC reader has no UUID extension registered, the
                // column comes through as its storage type (16-byte fixed binary).
                // Per the Parquet variant shredding spec, fixed_size_binary(16) is
                // the only valid fixed-size binary type and represents UUID.
                case FixedSizeBinaryType fsb when fsb.ByteWidth == 16:
                    return Primitive(ShredType.Uuid);

                case ListType list:
                    return MapArrayType(list);

                case StructType structType:
                    return MapObjectType(structType);

                default:
                    throw new ArgumentException(
                        $"Unsupported shredded value type: {type}",
                        nameof(type));
            }
        }

        private static ShredSchema MapDecimalByPrecision(int precision, IArrowType type)
        {
            if (precision <= 9) return Primitive(ShredType.Decimal4);
            if (precision <= 18) return Primitive(ShredType.Decimal8);
            if (precision <= 38) return Primitive(ShredType.Decimal16);
            throw new ArgumentException(
                $"Unsupported decimal precision {precision} (max 38): {type}",
                nameof(type));
        }

        private static ShredSchema MapArrayType(ListType list)
        {
            if (!(list.ValueDataType is StructType elementStruct) || !IsElementGroupStruct(elementStruct))
            {
                throw new ArgumentException(
                    "Shredded array element must be a struct with 'value' and/or 'typed_value' fields.",
                    nameof(list));
            }
            return ForArray(ParseElementGroup(elementStruct));
        }

        private static ShredSchema MapObjectType(StructType structType)
        {
            Dictionary<string, ShredSchema> fields = new Dictionary<string, ShredSchema>(structType.Fields.Count);
            foreach (Field field in structType.Fields)
            {
                if (!(field.DataType is StructType elementGroup) || !IsElementGroupStruct(elementGroup))
                {
                    throw new ArgumentException(
                        $"Shredded object field '{field.Name}' must be a struct with 'value' and/or 'typed_value' fields.",
                        nameof(structType));
                }
                fields[field.Name] = ParseElementGroup(elementGroup);
            }
            return ForObject(fields);
        }

        /// <summary>
        /// Tests whether a struct type is a valid shredded "element group":
        /// a struct with at least one of <c>value</c> (binary) or <c>typed_value</c>,
        /// and no other fields.
        /// </summary>
        private static bool IsElementGroupStruct(StructType st)
        {
            int valueIdx = st.GetFieldIndex("value");
            int typedIdx = st.GetFieldIndex("typed_value");

            if (valueIdx < 0 && typedIdx < 0)
            {
                return false;
            }

            if (valueIdx >= 0)
            {
                IArrowType valueFieldType = st.Fields[valueIdx].DataType;
                if (!(valueFieldType is BinaryType ||
                      valueFieldType is LargeBinaryType ||
                      valueFieldType is BinaryViewType))
                {
                    return false;
                }
            }

            // Reject structs with unexpected extra fields.
            foreach (Field f in st.Fields)
            {
                if (f.Name != "value" && f.Name != "typed_value")
                {
                    return false;
                }
            }

            return true;
        }

        private static ShredSchema ParseElementGroup(StructType elementStruct)
        {
            int typedIdx = elementStruct.GetFieldIndex("typed_value");
            if (typedIdx < 0)
            {
                return Unshredded();
            }
            return MapArrowType(elementStruct.Fields[typedIdx].DataType);
        }
    }
}
