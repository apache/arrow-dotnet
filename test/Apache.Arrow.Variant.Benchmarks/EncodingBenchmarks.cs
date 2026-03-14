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

using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Apache.Arrow.Variant.Json;
using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Variant.Benchmarks
{
    /// <summary>
    /// Measures encoding (VariantValue → binary, JSON → binary),
    /// decoding (binary → VariantValue), and round-trip performance.
    /// <see cref="MemoryDiagnoserAttribute"/> reports allocations per operation
    /// to quantify MemoryStream / intermediate object overhead.
    /// </summary>
    [MemoryDiagnoser]
    public class EncodingBenchmarks
    {
        // Pre-built test data, set up once before benchmarks run.
        private VariantValue _flatObject;
        private VariantValue _nestedObject;
        private VariantValue _largeArray;

        private byte[] _flatJson;
        private byte[] _nestedJson;
        private byte[] _arrayJson;

        private byte[] _flatMetadata;
        private byte[] _flatValue;
        private byte[] _nestedMetadata;
        private byte[] _nestedValue;
        private byte[] _arrayMetadata;
        private byte[] _arrayValue;

        [GlobalSetup]
        public void Setup()
        {
            // --- Flat object: 20 primitive fields ---
            Dictionary<string, VariantValue> flatFields = new Dictionary<string, VariantValue>();
            for (int i = 0; i < 20; i++)
            {
                flatFields["field_" + i] = i % 3 == 0
                    ? VariantValue.FromInt32(i * 100)
                    : i % 3 == 1
                        ? VariantValue.FromDouble(i * 1.5)
                        : VariantValue.FromString("value_" + i);
            }
            _flatObject = VariantValue.FromObject(flatFields);

            // --- Nested object: 5 levels deep, 4 fields per level ---
            _nestedObject = BuildNestedObject(5, 4);

            // --- Large array: 100 mixed elements ---
            List<VariantValue> elements = new List<VariantValue>();
            for (int i = 0; i < 100; i++)
            {
                elements.Add(i % 2 == 0
                    ? VariantValue.FromInt32(i)
                    : VariantValue.FromString("item_" + i));
            }
            _largeArray = VariantValue.FromArray(elements);

            // --- JSON versions ---
            _flatJson = Encoding.UTF8.GetBytes(VariantJsonWriter.ToJson(_flatObject));
            _nestedJson = Encoding.UTF8.GetBytes(VariantJsonWriter.ToJson(_nestedObject));
            _arrayJson = Encoding.UTF8.GetBytes(VariantJsonWriter.ToJson(_largeArray));

            // --- Binary versions (for decode benchmarks) ---
            VariantBuilder builder = new VariantBuilder();
            (_flatMetadata, _flatValue) = builder.Encode(_flatObject);
            (_nestedMetadata, _nestedValue) = builder.Encode(_nestedObject);
            (_arrayMetadata, _arrayValue) = builder.Encode(_largeArray);
        }

        private static VariantValue BuildNestedObject(int depth, int fieldsPerLevel)
        {
            if (depth == 0)
            {
                return VariantValue.FromString("leaf");
            }

            Dictionary<string, VariantValue> fields = new Dictionary<string, VariantValue>();
            for (int i = 0; i < fieldsPerLevel; i++)
            {
                fields["key_" + i] = i == 0
                    ? BuildNestedObject(depth - 1, fieldsPerLevel)
                    : VariantValue.FromInt32(i * depth);
            }
            return VariantValue.FromObject(fields);
        }

        // ---------------------------------------------------------------
        // Encode: VariantValue → binary
        // ---------------------------------------------------------------

        [Benchmark]
        public byte[] Encode_FlatObject()
        {
            VariantBuilder builder = new VariantBuilder();
            return builder.Encode(_flatObject).Value;
        }

        [Benchmark]
        public byte[] Encode_NestedObject()
        {
            VariantBuilder builder = new VariantBuilder();
            return builder.Encode(_nestedObject).Value;
        }

        [Benchmark]
        public byte[] Encode_LargeArray()
        {
            VariantBuilder builder = new VariantBuilder();
            return builder.Encode(_largeArray).Value;
        }

        // ---------------------------------------------------------------
        // EncodeFromJson: UTF-8 JSON → binary (streaming, no VariantValue)
        // ---------------------------------------------------------------

        [Benchmark]
        public byte[] EncodeFromJson_FlatObject()
        {
            VariantBuilder builder = new VariantBuilder();
            return builder.EncodeFromJson(_flatJson).Value;
        }

        [Benchmark]
        public byte[] EncodeFromJson_NestedObject()
        {
            VariantBuilder builder = new VariantBuilder();
            return builder.EncodeFromJson(_nestedJson).Value;
        }

        [Benchmark]
        public byte[] EncodeFromJson_LargeArray()
        {
            VariantBuilder builder = new VariantBuilder();
            return builder.EncodeFromJson(_arrayJson).Value;
        }

        // ---------------------------------------------------------------
        // Decode: binary → VariantValue
        // ---------------------------------------------------------------

        [Benchmark]
        public VariantValue Decode_FlatObject()
        {
            VariantReader reader = new VariantReader(_flatMetadata, _flatValue);
            return reader.ToVariantValue();
        }

        [Benchmark]
        public VariantValue Decode_NestedObject()
        {
            VariantReader reader = new VariantReader(_nestedMetadata, _nestedValue);
            return reader.ToVariantValue();
        }

        [Benchmark]
        public VariantValue Decode_LargeArray()
        {
            VariantReader reader = new VariantReader(_arrayMetadata, _arrayValue);
            return reader.ToVariantValue();
        }

        // ---------------------------------------------------------------
        // JSON write: binary → JSON string (ToJson: MemoryStream + StreamReader)
        // ---------------------------------------------------------------

        [Benchmark]
        public string WriteJson_FlatObject()
        {
            return VariantJsonWriter.ToJson(_flatMetadata, _flatValue);
        }

        [Benchmark]
        public string WriteJson_NestedObject()
        {
            return VariantJsonWriter.ToJson(_nestedMetadata, _nestedValue);
        }

        // ---------------------------------------------------------------
        // JSON write: binary → Utf8JsonWriter (WriteTo: no string materialization)
        // ---------------------------------------------------------------

        [Benchmark]
        public void WriteTo_FlatObject()
        {
            ArrayBufferWriter<byte> buffer = new ArrayBufferWriter<byte>();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(buffer))
            {
                VariantJsonWriter.WriteTo(writer, _flatMetadata, _flatValue);
            }
        }

        [Benchmark]
        public void WriteTo_NestedObject()
        {
            ArrayBufferWriter<byte> buffer = new ArrayBufferWriter<byte>();
            using (Utf8JsonWriter writer = new Utf8JsonWriter(buffer))
            {
                VariantJsonWriter.WriteTo(writer, _nestedMetadata, _nestedValue);
            }
        }

        // ---------------------------------------------------------------
        // Round-trip: JSON → binary → JSON
        // ---------------------------------------------------------------

        [Benchmark]
        public string RoundTrip_FlatObject()
        {
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse(_flatJson);
            return VariantJsonWriter.ToJson(metadata, value);
        }

        [Benchmark]
        public string RoundTrip_NestedObject()
        {
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse(_nestedJson);
            return VariantJsonWriter.ToJson(metadata, value);
        }
    }
}
