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

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Decomposes <see cref="VariantValue"/>s into shredded (value, typed_value) pairs
    /// according to a <see cref="ShredSchema"/>.
    /// <para>
    /// Per the Parquet variant shredding spec, the variant metadata dictionary is shared
    /// across an entire column. The <see cref="Shred(IEnumerable{VariantValue}, ShredSchema)"/>
    /// batch entrypoint builds that shared metadata and emits per-row value bytes that
    /// reference it — ready to drop into a Parquet <c>value</c> column.
    /// </para>
    /// </summary>
    public static class VariantShredder
    {
        /// <summary>
        /// Shreds a column of variant values into a shared metadata dictionary and
        /// per-row <see cref="ShredResult"/>s. The residual <see cref="ShredResult.Value"/>
        /// bytes for each row reference the returned metadata.
        /// </summary>
        public static (byte[] Metadata, IReadOnlyList<ShredResult> Rows) Shred(
            IEnumerable<VariantValue> values,
            ShredSchema schema)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            // Materialize so we can make two passes (metadata collection, then shredding).
            List<VariantValue> rows = values as List<VariantValue> ?? new List<VariantValue>(values);

            // Pass 1: collect every field name into the shared metadata dictionary.
            // A superset is fine per-spec; the Parquet value column just needs to resolve
            // any field ID it references.
            VariantMetadataBuilder metadata = new VariantMetadataBuilder();
            foreach (VariantValue row in rows)
            {
                CollectFieldNames(row, metadata);
            }
            byte[] metadataBytes = metadata.Build(out int[] idRemap);

            // Pass 2: shred each row against the finalized metadata.
            ShredResult[] results = new ShredResult[rows.Count];
            for (int i = 0; i < rows.Count; i++)
            {
                results[i] = Shred(rows[i], schema, metadata, idRemap);
            }

            return (metadataBytes, results);
        }

        /// <summary>
        /// Shreds a single variant value against a caller-managed metadata dictionary.
        /// Use this when combining shredded columns with external metadata, or when
        /// streaming rows one at a time. The caller is responsible for ensuring
        /// <paramref name="metadata"/> already contains every field name the residual
        /// may reference.
        /// </summary>
        public static ShredResult Shred(
            VariantValue value,
            ShredSchema schema,
            VariantMetadataBuilder metadata,
            int[] idRemap)
        {
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (metadata == null) throw new ArgumentNullException(nameof(metadata));
            if (idRemap == null) throw new ArgumentNullException(nameof(idRemap));

            switch (schema.TypedValueType)
            {
                case ShredType.None:
                    return ShredAsUntyped(value, metadata, idRemap);
                case ShredType.Object:
                    return ShredAsObject(value, schema, metadata, idRemap);
                case ShredType.Array:
                    return ShredAsArray(value, schema, metadata, idRemap);
                default:
                    return ShredAsPrimitive(value, schema, metadata, idRemap);
            }
        }

        private static ShredResult ShredAsUntyped(VariantValue value, VariantMetadataBuilder metadata, int[] idRemap)
        {
            return new ShredResult(EncodeValue(value, metadata, idRemap), null);
        }

        private static ShredResult ShredAsPrimitive(VariantValue value, ShredSchema schema, VariantMetadataBuilder metadata, int[] idRemap)
        {
            ShredType actualType = GetShredType(value);
            if (actualType == schema.TypedValueType)
            {
                return new ShredResult(null, ExtractTypedValue(value, schema.TypedValueType));
            }
            return new ShredResult(EncodeValue(value, metadata, idRemap), null);
        }

        private static ShredResult ShredAsObject(VariantValue value, ShredSchema schema, VariantMetadataBuilder metadata, int[] idRemap)
        {
            if (!value.IsObject)
            {
                return new ShredResult(EncodeValue(value, metadata, idRemap), null);
            }

            IReadOnlyDictionary<string, VariantValue> fields = value.AsObject();
            Dictionary<string, ShredResult> shreddedFields = new Dictionary<string, ShredResult>(schema.ObjectFields.Count);
            List<KeyValuePair<string, VariantValue>> residualFields = null;

            foreach (KeyValuePair<string, ShredSchema> schemaField in schema.ObjectFields)
            {
                if (fields.TryGetValue(schemaField.Key, out VariantValue fieldValue))
                {
                    shreddedFields[schemaField.Key] = Shred(fieldValue, schemaField.Value, metadata, idRemap);
                }
                else
                {
                    shreddedFields[schemaField.Key] = ShredResult.Missing;
                }
            }

            foreach (KeyValuePair<string, VariantValue> field in fields)
            {
                if (!schema.ObjectFields.ContainsKey(field.Key))
                {
                    if (residualFields == null)
                    {
                        residualFields = new List<KeyValuePair<string, VariantValue>>();
                    }
                    residualFields.Add(field);
                }
            }

            ShredObjectResult typedValue = new ShredObjectResult(shreddedFields);
            if (residualFields != null)
            {
                return new ShredResult(EncodeResidualObject(residualFields, metadata, idRemap), typedValue);
            }
            return new ShredResult(null, typedValue);
        }

        private static ShredResult ShredAsArray(VariantValue value, ShredSchema schema, VariantMetadataBuilder metadata, int[] idRemap)
        {
            if (!value.IsArray)
            {
                return new ShredResult(EncodeValue(value, metadata, idRemap), null);
            }

            IReadOnlyList<VariantValue> elements = value.AsArray();
            List<ShredResult> shreddedElements = new List<ShredResult>(elements.Count);
            for (int i = 0; i < elements.Count; i++)
            {
                shreddedElements.Add(Shred(elements[i], schema.ArrayElement, metadata, idRemap));
            }
            return new ShredResult(null, new ShredArrayResult(shreddedElements));
        }

        // ---------------------------------------------------------------
        // Encoding helpers — write value bytes referencing the shared metadata
        // ---------------------------------------------------------------

        private static byte[] EncodeValue(VariantValue value, VariantMetadataBuilder metadata, int[] idRemap)
        {
            using VariantValueWriter writer = new VariantValueWriter(metadata, idRemap);
            WriteVariantValue(writer, value);
            return writer.ToArray();
        }

        private static byte[] EncodeResidualObject(List<KeyValuePair<string, VariantValue>> fields, VariantMetadataBuilder metadata, int[] idRemap)
        {
            using VariantValueWriter writer = new VariantValueWriter(metadata, idRemap);
            writer.BeginObject();
            foreach (KeyValuePair<string, VariantValue> field in fields)
            {
                writer.WriteFieldName(field.Key);
                WriteVariantValue(writer, field.Value);
            }
            writer.EndObject();
            return writer.ToArray();
        }

        private static void WriteVariantValue(VariantValueWriter writer, VariantValue variant)
        {
            if (variant.IsNull) { writer.WriteNull(); return; }
            if (variant.IsBoolean) { writer.WriteBoolean(variant.AsBoolean()); return; }
            if (variant.IsObject)
            {
                writer.BeginObject();
                foreach (KeyValuePair<string, VariantValue> field in variant.AsObject())
                {
                    writer.WriteFieldName(field.Key);
                    WriteVariantValue(writer, field.Value);
                }
                writer.EndObject();
                return;
            }
            if (variant.IsArray)
            {
                writer.BeginArray();
                foreach (VariantValue element in variant.AsArray())
                {
                    WriteVariantValue(writer, element);
                }
                writer.EndArray();
                return;
            }

            switch (variant.PrimitiveType)
            {
                case VariantPrimitiveType.Int8: writer.WriteInt8(variant.AsInt8()); break;
                case VariantPrimitiveType.Int16: writer.WriteInt16(variant.AsInt16()); break;
                case VariantPrimitiveType.Int32: writer.WriteInt32(variant.AsInt32()); break;
                case VariantPrimitiveType.Int64: writer.WriteInt64(variant.AsInt64()); break;
                case VariantPrimitiveType.Float: writer.WriteFloat(variant.AsFloat()); break;
                case VariantPrimitiveType.Double: writer.WriteDouble(variant.AsDouble()); break;
                case VariantPrimitiveType.Decimal4: writer.WriteDecimal4(variant.AsDecimal()); break;
                case VariantPrimitiveType.Decimal8: writer.WriteDecimal8(variant.AsDecimal()); break;
                case VariantPrimitiveType.Decimal16: writer.WriteDecimal16(variant.AsSqlDecimal()); break;
                case VariantPrimitiveType.Date: writer.WriteDateDays(variant.AsDateDays()); break;
                case VariantPrimitiveType.Timestamp: writer.WriteTimestampMicros(variant.AsTimestampMicros()); break;
                case VariantPrimitiveType.TimestampNtz: writer.WriteTimestampNtzMicros(variant.AsTimestampNtzMicros()); break;
                case VariantPrimitiveType.TimeNtz: writer.WriteTimeNtzMicros(variant.AsTimeNtzMicros()); break;
                case VariantPrimitiveType.TimestampTzNanos: writer.WriteTimestampTzNanos(variant.AsTimestampTzNanos()); break;
                case VariantPrimitiveType.TimestampNtzNanos: writer.WriteTimestampNtzNanos(variant.AsTimestampNtzNanos()); break;
                case VariantPrimitiveType.String: writer.WriteString(variant.AsString()); break;
                case VariantPrimitiveType.Binary: writer.WriteBinary(variant.AsBinary()); break;
                case VariantPrimitiveType.Uuid: writer.WriteUuid(variant.AsUuid()); break;
                default: throw new NotSupportedException($"Unsupported primitive type: {variant.PrimitiveType}");
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

        // ---------------------------------------------------------------
        // Type extraction
        // ---------------------------------------------------------------

        /// <summary>
        /// Extracts the native CLR value from a <see cref="VariantValue"/> for
        /// storage in a typed Parquet column.
        /// </summary>
        internal static object ExtractTypedValue(VariantValue value, ShredType shredType)
        {
            switch (shredType)
            {
                case ShredType.Boolean: return value.AsBoolean();
                case ShredType.Int8: return value.AsInt8();
                case ShredType.Int16: return value.AsInt16();
                case ShredType.Int32: return value.AsInt32();
                case ShredType.Int64: return value.AsInt64();
                case ShredType.Float: return value.AsFloat();
                case ShredType.Double: return value.AsDouble();
                case ShredType.Decimal4:
                case ShredType.Decimal8:
                case ShredType.Decimal16: return value.AsDecimal();
                case ShredType.Date: return value.AsDateDays();
                case ShredType.Timestamp: return value.AsTimestampMicros();
                case ShredType.TimestampNtz: return value.AsTimestampNtzMicros();
                case ShredType.TimeNtz: return value.AsTimeNtzMicros();
                case ShredType.TimestampTzNanos: return value.AsTimestampTzNanos();
                case ShredType.TimestampNtzNanos: return value.AsTimestampNtzNanos();
                case ShredType.String: return value.AsString();
                case ShredType.Binary: return value.AsBinary();
                case ShredType.Uuid: return value.AsUuid();
                default:
                    throw new InvalidOperationException($"Cannot extract typed value for ShredType.{shredType}.");
            }
        }

        /// <summary>
        /// Determines the <see cref="ShredType"/> of a <see cref="VariantValue"/>.
        /// </summary>
        internal static ShredType GetShredType(VariantValue value)
        {
            if (value.IsObject) return ShredType.Object;
            if (value.IsArray) return ShredType.Array;
            return ShredSchema.ShredTypeFromPrimitive(value.PrimitiveType);
        }
    }
}
