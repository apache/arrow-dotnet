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

namespace Apache.Arrow.Variant
{
    /// <summary>
    /// Encodes a <see cref="VariantValue"/> into the binary variant format,
    /// producing metadata and value byte arrays.
    /// </summary>
    public sealed class VariantBuilder
    {
        /// <summary>
        /// Encodes a <see cref="VariantValue"/> to the variant binary format.
        /// </summary>
        /// <returns>A tuple of (metadata bytes, value bytes).</returns>
        public (byte[] Metadata, byte[] Value) Encode(VariantValue variant)
        {
            VariantMetadataBuilder metadataBuilder = new VariantMetadataBuilder();
            CollectFieldNames(variant, metadataBuilder);
            byte[] metadata = metadataBuilder.Build(out int[] idRemap);

            VariantValueWriter writer = new VariantValueWriter(metadataBuilder, idRemap);
            WriteValue(writer, variant);
            return (metadata, writer.ToArray());
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

        private static void WriteValue(VariantValueWriter writer, VariantValue variant)
        {
            if (variant.IsNull)
            {
                writer.WriteNull();
                return;
            }

            if (variant.IsBoolean)
            {
                writer.WriteBoolean(variant.AsBoolean());
                return;
            }

            if (variant.IsObject)
            {
                writer.BeginObject();
                foreach (KeyValuePair<string, VariantValue> field in variant.AsObject())
                {
                    writer.WriteFieldName(field.Key);
                    WriteValue(writer, field.Value);
                }
                writer.EndObject();
                return;
            }

            if (variant.IsArray)
            {
                writer.BeginArray();
                foreach (VariantValue element in variant.AsArray())
                {
                    WriteValue(writer, element);
                }
                writer.EndArray();
                return;
            }

            switch (variant.PrimitiveType)
            {
                case VariantPrimitiveType.Int8:
                    writer.WriteInt8(variant.AsInt8());
                    break;
                case VariantPrimitiveType.Int16:
                    writer.WriteInt16(variant.AsInt16());
                    break;
                case VariantPrimitiveType.Int32:
                    writer.WriteInt32(variant.AsInt32());
                    break;
                case VariantPrimitiveType.Int64:
                    writer.WriteInt64(variant.AsInt64());
                    break;
                case VariantPrimitiveType.Float:
                    writer.WriteFloat(variant.AsFloat());
                    break;
                case VariantPrimitiveType.Double:
                    writer.WriteDouble(variant.AsDouble());
                    break;
                case VariantPrimitiveType.Decimal4:
                    writer.WriteDecimal4(variant.AsDecimal());
                    break;
                case VariantPrimitiveType.Decimal8:
                    writer.WriteDecimal8(variant.AsDecimal());
                    break;
                case VariantPrimitiveType.Decimal16:
                    writer.WriteDecimal16(variant.AsSqlDecimal());
                    break;
                case VariantPrimitiveType.Date:
                    writer.WriteDateDays(variant.AsDateDays());
                    break;
                case VariantPrimitiveType.Timestamp:
                    writer.WriteTimestampMicros(variant.AsTimestampMicros());
                    break;
                case VariantPrimitiveType.TimestampNtz:
                    writer.WriteTimestampNtzMicros(variant.AsTimestampNtzMicros());
                    break;
                case VariantPrimitiveType.TimeNtz:
                    writer.WriteTimeNtzMicros(variant.AsTimeNtzMicros());
                    break;
                case VariantPrimitiveType.TimestampTzNanos:
                    writer.WriteTimestampTzNanos(variant.AsTimestampTzNanos());
                    break;
                case VariantPrimitiveType.TimestampNtzNanos:
                    writer.WriteTimestampNtzNanos(variant.AsTimestampNtzNanos());
                    break;
                case VariantPrimitiveType.String:
                    writer.WriteString(variant.AsString());
                    break;
                case VariantPrimitiveType.Binary:
                    writer.WriteBinary(variant.AsBinary());
                    break;
                case VariantPrimitiveType.Uuid:
                    writer.WriteUuid(variant.AsUuid());
                    break;
                default:
                    throw new NotSupportedException($"Unsupported primitive type: {variant.PrimitiveType}");
            }
        }
    }
}
