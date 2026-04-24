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
using System.Text;
using System.Text.Json;
using Apache.Arrow.Scalars.Variant;

namespace Apache.Arrow.Operations.VariantJson
{
    /// <summary>
    /// Parses a JSON string or UTF-8 bytes directly into variant binary format
    /// (metadata + value byte arrays) without creating intermediate <see cref="VariantValue"/> objects.
    /// </summary>
    public static class VariantJsonReader
    {
        /// <summary>
        /// Parses a JSON string into variant binary format.
        /// </summary>
        /// <param name="json">The JSON string to parse.</param>
        /// <returns>A tuple of (metadata bytes, value bytes).</returns>
        public static (byte[] Metadata, byte[] Value) Parse(string json)
        {
            byte[] utf8 = Encoding.UTF8.GetBytes(json);
            return Parse(new ReadOnlySpan<byte>(utf8));
        }

        /// <summary>
        /// Parses UTF-8 encoded JSON bytes into variant binary format.
        /// </summary>
        /// <param name="utf8Json">The UTF-8 encoded JSON bytes.</param>
        /// <returns>A tuple of (metadata bytes, value bytes).</returns>
        public static (byte[] Metadata, byte[] Value) Parse(ReadOnlySpan<byte> utf8Json)
        {
            // Pass 1: collect all field names into the metadata dictionary.
            VariantMetadataBuilder metadataBuilder = new VariantMetadataBuilder();
            Utf8JsonReader collector = new Utf8JsonReader(utf8Json);
            collector.Read();
            CollectFieldNames(ref collector, metadataBuilder);

            byte[] metadata = metadataBuilder.Build(out int[] idRemap);

            // Pass 2: stream values into a VariantValueWriter using the sorted field IDs.
            Utf8JsonReader emitter = new Utf8JsonReader(utf8Json);
            emitter.Read();
            VariantValueWriter writer = new VariantValueWriter(metadataBuilder, idRemap);
            WriteValue(ref emitter, writer);
            return (metadata, writer.ToArray());
        }

        private static void CollectFieldNames(ref Utf8JsonReader reader, VariantMetadataBuilder builder)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.EndObject)
                            return;
                        builder.Add(reader.GetString());
                        reader.Read();
                        CollectFieldNames(ref reader, builder);
                    }
                    throw new JsonException("Unterminated JSON object.");

                case JsonTokenType.StartArray:
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.EndArray)
                            return;
                        CollectFieldNames(ref reader, builder);
                    }
                    throw new JsonException("Unterminated JSON array.");

                default:
                    return;
            }
        }

        private static void WriteValue(ref Utf8JsonReader reader, VariantValueWriter writer)
        {
            switch (reader.TokenType)
            {
                case JsonTokenType.Null:
                    writer.WriteNull();
                    return;

                case JsonTokenType.True:
                    writer.WriteBoolean(true);
                    return;

                case JsonTokenType.False:
                    writer.WriteBoolean(false);
                    return;

                case JsonTokenType.Number:
                    if (reader.TryGetInt64(out long longValue))
                    {
                        writer.WriteIntegerCompact(longValue);
                    }
                    else
                    {
                        writer.WriteDouble(reader.GetDouble());
                    }
                    return;

                case JsonTokenType.String:
                    writer.WriteString(reader.GetString());
                    return;

                case JsonTokenType.StartObject:
                    writer.BeginObject();
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.EndObject)
                            break;
                        writer.WriteFieldName(reader.GetString());
                        reader.Read();
                        WriteValue(ref reader, writer);
                    }
                    writer.EndObject();
                    return;

                case JsonTokenType.StartArray:
                    writer.BeginArray();
                    while (reader.Read())
                    {
                        if (reader.TokenType == JsonTokenType.EndArray)
                            break;
                        WriteValue(ref reader, writer);
                    }
                    writer.EndArray();
                    return;

                default:
                    throw new JsonException($"Unexpected JSON token type {reader.TokenType}.");
            }
        }
    }
}
