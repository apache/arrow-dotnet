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

using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text.Json;
using Apache.Arrow.Operations.Json;
using Apache.Arrow.Scalars;
using Xunit;

namespace Apache.Arrow.Operations.Tests
{
    public class VariantDecimalJsonTests
    {
        // Minimal metadata: version 1, 0 dictionary entries.
        private static readonly byte[] MinimalMetadata = new byte[] { 0x01, 0x00 };

        // Header bytes: (primitiveType << 2) | basicType, basicType=0 for Primitive
        // Decimal4=8 -> 0x20, Decimal8=9 -> 0x24, Decimal16=10 -> 0x28
        private const byte Decimal16Header = 0x28;

        // ---------------------------------------------------------------
        // JSON writer: large Decimal16 produces valid JSON number
        // ---------------------------------------------------------------

        [Fact]
        public void JsonWriter_LargeDecimal16_WritesValidJson()
        {
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00, // scale = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
            };

            string json = VariantJsonWriter.ToJson(MinimalMetadata, value);
            Assert.Equal("79228162514264337593543950336", json);
        }

        // ---------------------------------------------------------------
        // JSON converter: VariantValue with SqlDecimal storage writes valid JSON
        // ---------------------------------------------------------------

        [Fact]
        public void JsonConverter_SqlDecimalStorage_WritesValidJson()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue vv = VariantValue.FromSqlDecimal(sd);

            string json = VariantJsonWriter.ToJson(vv);
            Assert.Equal("99999999999999999999999999999999999999", json);
        }

        // ---------------------------------------------------------------
        // JSON writer for negative large Decimal16
        // ---------------------------------------------------------------

        [Fact]
        public void JsonWriter_NegativeLargeDecimal16_WritesValidJson()
        {
            SqlDecimal sd = SqlDecimal.Parse("-79228162514264337593543950336");
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            string json = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal("-79228162514264337593543950336", json);
        }

        // ---------------------------------------------------------------
        // JSON writer for small Decimal16 (decimal path)
        // ---------------------------------------------------------------

        [Fact]
        public void JsonWriter_SmallDecimal16_WritesNumberValue()
        {
            VariantValue vv = VariantValue.FromDecimal16(42.5m);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            string json = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal("42.5", json);
        }

        // ---------------------------------------------------------------
        // JSON converter serialization of nested SqlDecimal
        // ---------------------------------------------------------------

        [Fact]
        public void JsonConverter_ObjectWithSqlDecimal_WritesValidJson()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "val", VariantValue.FromSqlDecimal(sd) },
            });

            JsonSerializerOptions options = new JsonSerializerOptions();
            options.Converters.Add(VariantJsonConverter.Instance);
            string json = JsonSerializer.Serialize(obj, options);
            Assert.Equal("{\"val\":99999999999999999999999999999999999999}", json);
        }
    }
}
