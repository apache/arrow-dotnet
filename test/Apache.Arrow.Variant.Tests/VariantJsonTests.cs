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
using Apache.Arrow.Variant.Json;
using Xunit;

namespace Apache.Arrow.Variant.Tests
{
    public class VariantJsonTests
    {
        private static readonly JsonSerializerOptions s_options = CreateOptions();

        private static JsonSerializerOptions CreateOptions()
        {
            JsonSerializerOptions options = new JsonSerializerOptions();
            options.Converters.Add(VariantJsonConverter.Instance);
            return options;
        }

        // ---------------------------------------------------------------
        // Converter: JSON -> VariantValue (deserialization)
        // ---------------------------------------------------------------

        [Fact]
        public void Deserialize_Null()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("null", s_options);
            Assert.Equal(VariantValue.Null, v);
        }

        [Fact]
        public void Deserialize_True()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("true", s_options);
            Assert.Equal(VariantValue.True, v);
        }

        [Fact]
        public void Deserialize_False()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("false", s_options);
            Assert.Equal(VariantValue.False, v);
        }

        [Fact]
        public void Deserialize_Int8()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("42", s_options);
            Assert.Equal(VariantPrimitiveType.Int8, v.PrimitiveType);
            Assert.Equal((sbyte)42, v.AsInt8());
        }

        [Fact]
        public void Deserialize_Int16()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("1000", s_options);
            Assert.Equal(VariantPrimitiveType.Int16, v.PrimitiveType);
            Assert.Equal((short)1000, v.AsInt16());
        }

        [Fact]
        public void Deserialize_Int32()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("100000", s_options);
            Assert.Equal(VariantPrimitiveType.Int32, v.PrimitiveType);
            Assert.Equal(100000, v.AsInt32());
        }

        [Fact]
        public void Deserialize_Int64()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("9999999999", s_options);
            Assert.Equal(VariantPrimitiveType.Int64, v.PrimitiveType);
            Assert.Equal(9999999999L, v.AsInt64());
        }

        [Fact]
        public void Deserialize_Double()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("3.14", s_options);
            Assert.Equal(VariantPrimitiveType.Double, v.PrimitiveType);
            Assert.Equal(3.14, v.AsDouble());
        }

        [Fact]
        public void Deserialize_NegativeInt()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("-100", s_options);
            Assert.Equal(VariantPrimitiveType.Int8, v.PrimitiveType);
            Assert.Equal((sbyte)-100, v.AsInt8());
        }

        [Fact]
        public void Deserialize_String()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("\"hello\"", s_options);
            Assert.Equal("hello", v.AsString());
        }

        [Fact]
        public void Deserialize_EmptyObject()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("{}", s_options);
            Assert.True(v.IsObject);
            Assert.Empty(v.AsObject());
        }

        [Fact]
        public void Deserialize_Object()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>(
                "{\"name\":\"Alice\",\"age\":30}", s_options);
            Assert.True(v.IsObject);
            IReadOnlyDictionary<string, VariantValue> obj = v.AsObject();
            Assert.Equal(2, obj.Count);
            Assert.Equal("Alice", obj["name"].AsString());
            Assert.Equal((sbyte)30, obj["age"].AsInt8());
        }

        [Fact]
        public void Deserialize_EmptyArray()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("[]", s_options);
            Assert.True(v.IsArray);
            Assert.Empty(v.AsArray());
        }

        [Fact]
        public void Deserialize_Array()
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>("[1,\"two\",null,true]", s_options);
            Assert.True(v.IsArray);
            IReadOnlyList<VariantValue> arr = v.AsArray();
            Assert.Equal(4, arr.Count);
            Assert.Equal((sbyte)1, arr[0].AsInt8());
            Assert.Equal("two", arr[1].AsString());
            Assert.True(arr[2].IsNull);
            Assert.True(arr[3].AsBoolean());
        }

        [Fact]
        public void Deserialize_DeepNesting()
        {
            string json = "{\"users\":[{\"name\":\"Alice\",\"scores\":[95,87]},{\"name\":\"Bob\"}]}";
            VariantValue v = JsonSerializer.Deserialize<VariantValue>(json, s_options);
            Assert.True(v.IsObject);
            IReadOnlyList<VariantValue> users = v.AsObject()["users"].AsArray();
            Assert.Equal(2, users.Count);
            Assert.Equal("Alice", users[0].AsObject()["name"].AsString());
        }

        // ---------------------------------------------------------------
        // Converter: VariantValue -> JSON (serialization)
        // ---------------------------------------------------------------

        [Fact]
        public void Serialize_Null()
        {
            string json = JsonSerializer.Serialize(VariantValue.Null, s_options);
            Assert.Equal("null", json);
        }

        [Fact]
        public void Serialize_True()
        {
            string json = JsonSerializer.Serialize(VariantValue.True, s_options);
            Assert.Equal("true", json);
        }

        [Fact]
        public void Serialize_False()
        {
            string json = JsonSerializer.Serialize(VariantValue.False, s_options);
            Assert.Equal("false", json);
        }

        [Fact]
        public void Serialize_Int32()
        {
            string json = JsonSerializer.Serialize(VariantValue.FromInt32(42), s_options);
            Assert.Equal("42", json);
        }

        [Fact]
        public void Serialize_Double()
        {
            string json = JsonSerializer.Serialize(VariantValue.FromDouble(3.5), s_options);
            Assert.Equal("3.5", json);
        }

        [Fact]
        public void Serialize_String()
        {
            string json = JsonSerializer.Serialize(VariantValue.FromString("hello"), s_options);
            Assert.Equal("\"hello\"", json);
        }

        [Fact]
        public void Serialize_Decimal4()
        {
            string json = JsonSerializer.Serialize(VariantValue.FromDecimal4(123.45m), s_options);
            Assert.Equal("123.45", json);
        }

        [Fact]
        public void Serialize_Date()
        {
            VariantValue v = VariantValue.FromDate(new DateTime(2022, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            string json = JsonSerializer.Serialize(v, s_options);
            Assert.Equal("\"2022-01-01\"", json);
        }

        [Fact]
        public void Serialize_Timestamp()
        {
            DateTimeOffset dto = new DateTimeOffset(2022, 1, 1, 0, 0, 0, TimeSpan.Zero);
            VariantValue v = VariantValue.FromTimestamp(dto);
            string json = JsonSerializer.Serialize(v, s_options);
            Assert.Contains("2022-01-01", json);
        }

        [Fact]
        public void Serialize_Binary()
        {
            VariantValue v = VariantValue.FromBinary(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });
            string json = JsonSerializer.Serialize(v, s_options);
            Assert.Equal("\"3q2+7w==\"", json);
        }

        [Fact]
        public void Serialize_Uuid()
        {
            Guid guid = new Guid("550e8400-e29b-41d4-a716-446655440000");
            VariantValue v = VariantValue.FromUuid(guid);
            string json = JsonSerializer.Serialize(v, s_options);
            Assert.Equal("\"550e8400-e29b-41d4-a716-446655440000\"", json);
        }

        [Fact]
        public void Serialize_Object()
        {
            VariantValue v = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "x", VariantValue.FromInt32(1) },
            });
            string json = JsonSerializer.Serialize(v, s_options);
            Assert.Equal("{\"x\":1}", json);
        }

        [Fact]
        public void Serialize_Array()
        {
            VariantValue v = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"));
            string json = JsonSerializer.Serialize(v, s_options);
            Assert.Equal("[1,\"two\"]", json);
        }

        [Fact]
        public void Serialize_Decimal16_DecimalStorage()
        {
            string json = JsonSerializer.Serialize(VariantValue.FromDecimal16(12345.67m), s_options);
            Assert.Equal("12345.67", json);
        }

        [Fact]
        public void Serialize_Decimal16_SqlDecimalStorage()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            string json = JsonSerializer.Serialize(VariantValue.FromSqlDecimal(sd), s_options);
            Assert.Equal("99999999999999999999999999999999999999", json);
        }

        [Fact]
        public void Serialize_Decimal16_SqlDecimalStorage_Negative()
        {
            SqlDecimal sd = SqlDecimal.Parse("-12345678901234567890123456789012345678");
            string json = JsonSerializer.Serialize(VariantValue.FromSqlDecimal(sd), s_options);
            Assert.Equal("-12345678901234567890123456789012345678", json);
        }

        [Fact]
        public void Serialize_Float_NaN_Throws()
        {
            VariantValue v = VariantValue.FromFloat(float.NaN);
            Assert.Throws<InvalidOperationException>(() =>
                JsonSerializer.Serialize(v, s_options));
        }

        [Fact]
        public void Serialize_Double_Infinity_Throws()
        {
            VariantValue v = VariantValue.FromDouble(double.PositiveInfinity);
            Assert.Throws<InvalidOperationException>(() =>
                JsonSerializer.Serialize(v, s_options));
        }

        // ---------------------------------------------------------------
        // JSON <-> VariantValue round-trips
        // ---------------------------------------------------------------

        [Theory]
        [InlineData("null")]
        [InlineData("true")]
        [InlineData("false")]
        [InlineData("42")]
        [InlineData("3.5")]
        [InlineData("\"hello\"")]
        [InlineData("[]")]
        [InlineData("{}")]
        public void RoundTrip_JsonToVariantToJson(string json)
        {
            VariantValue v = JsonSerializer.Deserialize<VariantValue>(json, s_options);
            string result = JsonSerializer.Serialize(v, s_options);
            Assert.Equal(json, result);
        }

        [Fact]
        public void RoundTrip_ComplexObject()
        {
            string json = "{\"name\":\"Alice\",\"age\":30,\"active\":true,\"scores\":[95,87,92]}";
            VariantValue v = JsonSerializer.Deserialize<VariantValue>(json, s_options);
            string result = JsonSerializer.Serialize(v, s_options);
            // Deserialize both to compare structurally (key order may differ)
            VariantValue original = JsonSerializer.Deserialize<VariantValue>(json, s_options);
            VariantValue roundTripped = JsonSerializer.Deserialize<VariantValue>(result, s_options);
            Assert.Equal(original, roundTripped);
        }

        // ---------------------------------------------------------------
        // VariantJsonReader: JSON -> binary
        // ---------------------------------------------------------------

        [Fact]
        public void JsonReader_Parse_Null()
        {
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse("null");
            VariantReader reader = new VariantReader(metadata, value);
            Assert.True(reader.IsNull);
        }

        [Fact]
        public void JsonReader_Parse_Int()
        {
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse("42");
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal((sbyte)42, reader.GetInt8());
        }

        [Fact]
        public void JsonReader_Parse_String()
        {
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse("\"hello\"");
            VariantReader reader = new VariantReader(metadata, value);
            Assert.Equal("hello", reader.GetString());
        }

        [Fact]
        public void JsonReader_Parse_Object()
        {
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse("{\"x\":10}");
            VariantObjectReader obj = new VariantObjectReader(metadata, value);
            Assert.Equal(1, obj.FieldCount);
            Assert.Equal("x", obj.GetFieldName(0));
            Assert.Equal((sbyte)10, obj.GetFieldValue(0).GetInt8());
        }

        [Fact]
        public void JsonReader_Parse_Array()
        {
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse("[1,2,3]");
            VariantArrayReader arr = new VariantArrayReader(metadata, value);
            Assert.Equal(3, arr.ElementCount);
            Assert.Equal((sbyte)1, arr.GetElement(0).GetInt8());
            Assert.Equal((sbyte)2, arr.GetElement(1).GetInt8());
            Assert.Equal((sbyte)3, arr.GetElement(2).GetInt8());
        }

        // ---------------------------------------------------------------
        // VariantJsonWriter: binary -> JSON
        // ---------------------------------------------------------------

        [Fact]
        public void JsonWriter_ToJson_Null()
        {
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(VariantValue.Null);
            string json = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal("null", json);
        }

        [Fact]
        public void JsonWriter_ToJson_Int32()
        {
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(VariantValue.FromInt32(42));
            string json = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal("42", json);
        }

        [Fact]
        public void JsonWriter_ToJson_String()
        {
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(VariantValue.FromString("hello"));
            string json = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal("\"hello\"", json);
        }

        [Fact]
        public void JsonWriter_ToJson_Object()
        {
            VariantBuilder builder = new VariantBuilder();
            VariantValue obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "x", VariantValue.FromInt32(10) },
            });
            (byte[] metadata, byte[] value) = builder.Encode(obj);
            string json = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal("{\"x\":10}", json);
        }

        [Fact]
        public void JsonWriter_ToJson_Indented()
        {
            VariantBuilder builder = new VariantBuilder();
            VariantValue arr = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromInt32(2));
            (byte[] metadata, byte[] value) = builder.Encode(arr);
            string json = VariantJsonWriter.ToJson(metadata, value, indented: true);
            Assert.Contains("\n", json);
            Assert.Contains("1", json);
            Assert.Contains("2", json);
        }

        [Fact]
        public void JsonWriter_ToJson_VariantValue()
        {
            VariantValue v = VariantValue.FromString("test");
            string json = VariantJsonWriter.ToJson(v);
            Assert.Equal("\"test\"", json);
        }

        // ---------------------------------------------------------------
        // Full round-trip: JSON -> binary -> JSON
        // ---------------------------------------------------------------

        [Theory]
        [InlineData("null")]
        [InlineData("true")]
        [InlineData("false")]
        [InlineData("42")]
        [InlineData("\"hello\"")]
        [InlineData("[]")]
        [InlineData("{}")]
        public void FullRoundTrip_JsonToBinaryToJson(string json)
        {
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse(json);
            string result = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal(json, result);
        }

        [Fact]
        public void FullRoundTrip_ComplexStructure()
        {
            string json = "{\"users\":[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}],\"count\":2}";
            (byte[] metadata, byte[] value) = VariantJsonReader.Parse(json);
            string result = VariantJsonWriter.ToJson(metadata, value);

            // Compare structurally since key order may change
            VariantValue original = JsonSerializer.Deserialize<VariantValue>(json, s_options);
            VariantValue roundTripped = JsonSerializer.Deserialize<VariantValue>(result, s_options);
            Assert.Equal(original, roundTripped);
        }
    }
}
