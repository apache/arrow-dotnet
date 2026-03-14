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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Variant.Json
{
    /// <summary>
    /// A <see cref="JsonConverter{T}"/> for <see cref="VariantValue"/>.
    /// Serializes variant values to their JSON equivalents and deserializes
    /// JSON values to <see cref="VariantValue"/> with integer-size inference.
    /// </summary>
    /// <remarks>
    /// Serialization mapping:
    /// - null → JSON null
    /// - boolean → JSON true/false
    /// - int8/16/32/64 → JSON number
    /// - float/double → JSON number (NaN/Infinity throw)
    /// - decimal4/8/16 → JSON number
    /// - date → ISO 8601 string (yyyy-MM-dd)
    /// - timestamp/timestampNtz → ISO 8601 string
    /// - binary → base64 string
    /// - uuid → string (lowercase with hyphens)
    /// - string → JSON string
    /// - object → JSON object
    /// - array → JSON array
    ///
    /// Deserialization mapping:
    /// - JSON null → VariantValue.Null
    /// - JSON true/false → VariantValue.True/False
    /// - JSON number: integer if no decimal point, choosing smallest type (int8/16/32/64);
    ///   decimal point present → double
    /// - JSON string → VariantValue.FromString
    /// - JSON object → VariantValue.FromObject
    /// - JSON array → VariantValue.FromArray
    /// </remarks>
    public sealed class VariantJsonConverter : JsonConverter<VariantValue>
    {
        /// <summary>A shared default instance.</summary>
        public static readonly VariantJsonConverter Instance = new VariantJsonConverter();

        /// <summary>
        /// Ensures the converter is called for JSON null tokens instead of
        /// returning C# null by default.
        /// </summary>
        public override bool HandleNull => true;

        /// <inheritdoc />
        public override VariantValue Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return ReadValue(ref reader);
        }

        /// <inheritdoc />
        public override void Write(Utf8JsonWriter writer, VariantValue value, JsonSerializerOptions options)
        {
            WriteValue(writer, value);
        }

        private static VariantValue ReadValue(ref Utf8JsonReader reader)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.Null:
                    return VariantValue.Null;

                case JsonTokenType.True:
                    return VariantValue.True;

                case JsonTokenType.False:
                    return VariantValue.False;

                case JsonTokenType.Number:
                    return ReadNumber(ref reader);

                case JsonTokenType.String:
                    return VariantValue.FromString(reader.GetString());

                case JsonTokenType.StartObject:
                    return ReadObject(ref reader);

                case JsonTokenType.StartArray:
                    return ReadArray(ref reader);

                default:
                    throw new JsonException($"Unexpected token type {reader.TokenType}.");
            }
        }

        private static VariantValue ReadNumber(ref Utf8JsonReader reader)
        {
            // Try integer first. If GetInt64 succeeds and there's no decimal point,
            // choose the smallest integer type.
            if (reader.TryGetInt64(out long longValue))
            {
                if (longValue >= sbyte.MinValue && longValue <= sbyte.MaxValue)
                {
                    return VariantValue.FromInt8((sbyte)longValue);
                }
                if (longValue >= short.MinValue && longValue <= short.MaxValue)
                {
                    return VariantValue.FromInt16((short)longValue);
                }
                if (longValue >= int.MinValue && longValue <= int.MaxValue)
                {
                    return VariantValue.FromInt32((int)longValue);
                }
                return VariantValue.FromInt64(longValue);
            }

            // Fall back to double for floating-point numbers.
            return VariantValue.FromDouble(reader.GetDouble());
        }

        private static VariantValue ReadObject(ref Utf8JsonReader reader)
        {
            Dictionary<string, VariantValue> fields = new Dictionary<string, VariantValue>();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    return VariantValue.FromObject(fields);
                }

                string key = reader.GetString();
                reader.Read();
                fields[key] = ReadValue(ref reader);
            }

            throw new JsonException("Unterminated JSON object.");
        }

        private static VariantValue ReadArray(ref Utf8JsonReader reader)
        {
            List<VariantValue> elements = new List<VariantValue>();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndArray)
                {
                    return VariantValue.FromArray(elements);
                }

                elements.Add(ReadValue(ref reader));
            }

            throw new JsonException("Unterminated JSON array.");
        }

        internal static void WriteValue(Utf8JsonWriter writer, VariantValue value)
        {
            if (value.IsNull)
            {
                writer.WriteNullValue();
                return;
            }

            if (value.IsBoolean)
            {
                writer.WriteBooleanValue(value.AsBoolean());
                return;
            }

            if (value.IsString)
            {
                writer.WriteStringValue(value.AsString());
                return;
            }

            if (value.IsObject)
            {
                writer.WriteStartObject();
                foreach (KeyValuePair<string, VariantValue> field in value.AsObject())
                {
                    writer.WritePropertyName(field.Key);
                    WriteValue(writer, field.Value);
                }
                writer.WriteEndObject();
                return;
            }

            if (value.IsArray)
            {
                writer.WriteStartArray();
                foreach (VariantValue element in value.AsArray())
                {
                    WriteValue(writer, element);
                }
                writer.WriteEndArray();
                return;
            }

            // Primitive types
            switch (value.PrimitiveType)
            {
                case VariantPrimitiveType.Int8:
                    writer.WriteNumberValue(value.AsInt8());
                    break;
                case VariantPrimitiveType.Int16:
                    writer.WriteNumberValue(value.AsInt16());
                    break;
                case VariantPrimitiveType.Int32:
                    writer.WriteNumberValue(value.AsInt32());
                    break;
                case VariantPrimitiveType.Int64:
                    writer.WriteNumberValue(value.AsInt64());
                    break;
                case VariantPrimitiveType.Float:
                    float f = value.AsFloat();
                    if (float.IsNaN(f) || float.IsInfinity(f))
                    {
                        throw new InvalidOperationException(
                            $"Cannot serialize {f} to JSON. NaN and Infinity are not valid JSON numbers.");
                    }
                    writer.WriteNumberValue(f);
                    break;
                case VariantPrimitiveType.Double:
                    double d = value.AsDouble();
                    if (double.IsNaN(d) || double.IsInfinity(d))
                    {
                        throw new InvalidOperationException(
                            $"Cannot serialize {d} to JSON. NaN and Infinity are not valid JSON numbers.");
                    }
                    writer.WriteNumberValue(d);
                    break;
                case VariantPrimitiveType.Decimal4:
                case VariantPrimitiveType.Decimal8:
                    writer.WriteNumberValue(value.AsDecimal());
                    break;
                case VariantPrimitiveType.Decimal16:
                    if (value.IsSqlDecimalStorage)
                        writer.WriteRawValue(value.AsSqlDecimal().ToString());
                    else
                        writer.WriteNumberValue(value.AsDecimal());
                    break;
                case VariantPrimitiveType.Date:
                    DateTime date = value.AsDate();
#if NET8_0_OR_GREATER
                    Span<char> buf = stackalloc char[10];
                    date.TryFormat(buf, out int written, "yyyy-MM-dd");
                    writer.WriteStringValue(buf.Slice(0, written));
#else
                    writer.WriteStringValue(date.ToString("yyyy-MM-dd"));
#endif
                    break;
                case VariantPrimitiveType.Timestamp:
                    DateTimeOffset ts = value.AsTimestamp();
#if NET8_0_OR_GREATER
                    buf = stackalloc char[64];
                    ts.TryFormat(buf, out written, "O");
                    writer.WriteStringValue(buf.Slice(0, written));
#else
                    writer.WriteStringValue(ts.ToString("O"));
#endif
                    break;
                case VariantPrimitiveType.TimestampNtz:
                    DateTime ntz = value.AsTimestampNtz();
#if NET8_0_OR_GREATER
                    buf = stackalloc char[64];
                    ntz.TryFormat(buf, out written, "O");
                    writer.WriteStringValue(buf.Slice(0, written));
#else
                    writer.WriteStringValue(ntz.ToString("O"));
#endif
                    break;
                case VariantPrimitiveType.TimeNtz:
                    writer.WriteNumberValue(value.AsTimeNtzMicros());
                    break;
                case VariantPrimitiveType.TimestampTzNanos:
                    writer.WriteNumberValue(value.AsTimestampTzNanos());
                    break;
                case VariantPrimitiveType.TimestampNtzNanos:
                    writer.WriteNumberValue(value.AsTimestampNtzNanos());
                    break;
                case VariantPrimitiveType.Binary:
                    writer.WriteBase64StringValue(value.AsBinary());
                    break;
                case VariantPrimitiveType.Uuid:
#if NET8_0_OR_GREATER
                    Guid uuid = value.AsUuid();
                    buf = stackalloc char[36];
                    uuid.TryFormat(buf, out written, "D");
                    writer.WriteStringValue(buf.Slice(0, written));
#else
                    writer.WriteStringValue(value.AsUuid().ToString("D"));
#endif
                    break;
                default:
                    throw new NotSupportedException($"Cannot serialize variant type {value.PrimitiveType} to JSON.");
            }
        }
    }
}
