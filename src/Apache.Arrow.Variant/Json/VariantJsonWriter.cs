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
#if NET8_0_OR_GREATER
using System.Buffers;
#else
using System.IO;
#endif
using System.Data.SqlTypes;
using System.Text;
using System.Text.Json;

namespace Apache.Arrow.Variant.Json
{
    /// <summary>
    /// Writes variant binary data directly to JSON without creating intermediate
    /// <see cref="VariantValue"/> objects.
    /// </summary>
    public static class VariantJsonWriter
    {
        /// <summary>
        /// Converts variant binary data to a JSON string.
        /// </summary>
        /// <param name="metadata">The variant metadata bytes.</param>
        /// <param name="value">The variant value bytes.</param>
        /// <param name="indented">Whether to produce indented (pretty-printed) JSON.</param>
        /// <returns>The JSON string representation.</returns>
        public static string ToJson(byte[] metadata, byte[] value, bool indented = false)
        {
            JsonWriterOptions writerOptions = new JsonWriterOptions { Indented = indented };
#if NET8_0_OR_GREATER
            ArrayBufferWriter<byte> buffer = new ArrayBufferWriter<byte>();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(buffer, writerOptions))
            {
                VariantReader reader = new VariantReader(metadata, value);
                WriteReader(writer, reader);
            }
            return Encoding.UTF8.GetString(buffer.WrittenSpan);
#else
            using (MemoryStream ms = new MemoryStream())
            {
                using (Utf8JsonWriter writer = new Utf8JsonWriter(ms, writerOptions))
                {
                    VariantReader reader = new VariantReader(metadata, value);
                    WriteReader(writer, reader);
                }
                return Encoding.UTF8.GetString(ms.GetBuffer(), 0, checked((int)ms.Position));
            }
#endif
        }

        /// <summary>
        /// Converts a <see cref="VariantValue"/> to a JSON string.
        /// </summary>
        /// <param name="value">The variant value to serialize.</param>
        /// <param name="indented">Whether to produce indented (pretty-printed) JSON.</param>
        /// <returns>The JSON string representation.</returns>
        public static string ToJson(VariantValue value, bool indented = false)
        {
            JsonWriterOptions writerOptions = new JsonWriterOptions { Indented = indented };
#if NET8_0_OR_GREATER
            ArrayBufferWriter<byte> buffer = new ArrayBufferWriter<byte>();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(buffer, writerOptions))
            {
                VariantJsonConverter.WriteValue(writer, value);
            }
            return Encoding.UTF8.GetString(buffer.WrittenSpan);
#else
            using (MemoryStream ms = new MemoryStream())
            {
                using (Utf8JsonWriter writer = new Utf8JsonWriter(ms, writerOptions))
                {
                    VariantJsonConverter.WriteValue(writer, value);
                }
                return Encoding.UTF8.GetString(ms.GetBuffer(), 0, checked((int)ms.Position));
            }
#endif
        }

        /// <summary>
        /// Writes variant binary data to a <see cref="Utf8JsonWriter"/>.
        /// </summary>
        /// <param name="writer">The JSON writer to write to.</param>
        /// <param name="metadata">The variant metadata bytes.</param>
        /// <param name="value">The variant value bytes.</param>
        public static void WriteTo(Utf8JsonWriter writer, byte[] metadata, byte[] value)
        {
            VariantReader reader = new VariantReader(metadata, value);
            WriteReader(writer, reader);
        }

        private static void WriteReader(Utf8JsonWriter writer, VariantReader reader)
        {
            switch (reader.BasicType)
            {
                case VariantBasicType.ShortString:
                    writer.WriteStringValue(reader.GetStringBytes());
                    return;

                case VariantBasicType.Object:
                    VariantObjectReader obj = new VariantObjectReader(reader.Metadata, reader.Value);
                    writer.WriteStartObject();
                    for (int i = 0; i < obj.FieldCount; i++)
                    {
                        writer.WritePropertyName(obj.GetFieldNameBytes(i));
                        VariantReader fieldValue = obj.GetFieldValue(i);
                        WriteReader(writer, fieldValue);
                    }
                    writer.WriteEndObject();
                    return;

                case VariantBasicType.Array:
                    VariantArrayReader arr = new VariantArrayReader(reader.Metadata, reader.Value);
                    writer.WriteStartArray();
                    for (int i = 0; i < arr.ElementCount; i++)
                    {
                        VariantReader elem = arr.GetElement(i);
                        WriteReader(writer, elem);
                    }
                    writer.WriteEndArray();
                    return;

                case VariantBasicType.Primitive:
                    WritePrimitive(writer, reader);
                    return;

                default:
                    throw new NotSupportedException($"Unsupported basic type {reader.BasicType}.");
            }
        }

        private static void WritePrimitive(Utf8JsonWriter writer, VariantReader reader)
        {
            switch (reader.PrimitiveType)
            {
                case VariantPrimitiveType.NullType:
                    writer.WriteNullValue();
                    break;
                case VariantPrimitiveType.BooleanTrue:
                    writer.WriteBooleanValue(true);
                    break;
                case VariantPrimitiveType.BooleanFalse:
                    writer.WriteBooleanValue(false);
                    break;
                case VariantPrimitiveType.Int8:
                    writer.WriteNumberValue(reader.GetInt8());
                    break;
                case VariantPrimitiveType.Int16:
                    writer.WriteNumberValue(reader.GetInt16());
                    break;
                case VariantPrimitiveType.Int32:
                    writer.WriteNumberValue(reader.GetInt32());
                    break;
                case VariantPrimitiveType.Int64:
                    writer.WriteNumberValue(reader.GetInt64());
                    break;
                case VariantPrimitiveType.Float:
                    float f = reader.GetFloat();
                    if (float.IsNaN(f) || float.IsInfinity(f))
                    {
                        throw new InvalidOperationException(
                            $"Cannot serialize {f} to JSON. NaN and Infinity are not valid JSON numbers.");
                    }
                    writer.WriteNumberValue(f);
                    break;
                case VariantPrimitiveType.Double:
                    double d = reader.GetDouble();
                    if (double.IsNaN(d) || double.IsInfinity(d))
                    {
                        throw new InvalidOperationException(
                            $"Cannot serialize {d} to JSON. NaN and Infinity are not valid JSON numbers.");
                    }
                    writer.WriteNumberValue(d);
                    break;
                case VariantPrimitiveType.Decimal4:
                    writer.WriteNumberValue(reader.GetDecimal4());
                    break;
                case VariantPrimitiveType.Decimal8:
                    writer.WriteNumberValue(reader.GetDecimal8());
                    break;
                case VariantPrimitiveType.Decimal16:
                    if (reader.TryGetDecimal16(out decimal dec))
                        writer.WriteNumberValue(dec);
                    else
                        writer.WriteRawValue(reader.GetSqlDecimal().ToString());
                    break;
                case VariantPrimitiveType.Date:
                    DateTime date = reader.GetDate();
#if NET8_0_OR_GREATER
                    Span<char> buf = stackalloc char[10]; // "yyyy-MM-dd"
                    date.TryFormat(buf, out int written, "yyyy-MM-dd");
                    writer.WriteStringValue(buf.Slice(0, written));
#else
                    writer.WriteStringValue(date.ToString("yyyy-MM-dd"));
#endif
                    break;
                case VariantPrimitiveType.Timestamp:
                    DateTimeOffset ts = reader.GetTimestamp();
#if NET8_0_OR_GREATER
                    buf = stackalloc char[64];
                    ts.TryFormat(buf, out written, "O");
                    writer.WriteStringValue(buf.Slice(0, written));
#else
                    writer.WriteStringValue(ts.ToString("O"));
#endif
                    break;
                case VariantPrimitiveType.TimestampNtz:
                    DateTime ntz = reader.GetTimestampNtz();
#if NET8_0_OR_GREATER
                    buf = stackalloc char[64];
                    ntz.TryFormat(buf, out written, "O");
                    writer.WriteStringValue(buf.Slice(0, written));
#else
                    writer.WriteStringValue(ntz.ToString("O"));
#endif
                    break;
                case VariantPrimitiveType.TimeNtz:
                    writer.WriteNumberValue(reader.GetTimeNtzMicros());
                    break;
                case VariantPrimitiveType.TimestampTzNanos:
                    writer.WriteNumberValue(reader.GetTimestampTzNanos());
                    break;
                case VariantPrimitiveType.TimestampNtzNanos:
                    writer.WriteNumberValue(reader.GetTimestampNtzNanos());
                    break;
                case VariantPrimitiveType.Binary:
                    writer.WriteBase64StringValue(reader.GetBinary());
                    break;
                case VariantPrimitiveType.String:
                    writer.WriteStringValue(reader.GetStringBytes());
                    break;
                case VariantPrimitiveType.Uuid:
#if NET8_0_OR_GREATER
                    Guid uuid = reader.GetUuid();
                    buf = stackalloc char[36]; // "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                    uuid.TryFormat(buf, out written, "D");
                    writer.WriteStringValue(buf.Slice(0, written));
#else
                    writer.WriteStringValue(reader.GetUuid().ToString("D"));
#endif
                    break;
                default:
                    throw new NotSupportedException($"Cannot serialize variant type {reader.PrimitiveType} to JSON.");
            }
        }
    }
}
