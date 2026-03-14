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
using Xunit;

namespace Apache.Arrow.Variant.Tests
{
    public class VariantValueTests
    {
        // ---------------------------------------------------------------
        // Null
        // ---------------------------------------------------------------

        [Fact]
        public void Null_Properties()
        {
            Assert.True(VariantValue.Null.IsNull);
            Assert.False(VariantValue.Null.IsBoolean);
            Assert.False(VariantValue.Null.IsString);
            Assert.False(VariantValue.Null.IsObject);
            Assert.False(VariantValue.Null.IsArray);
        }

        [Fact]
        public void Null_Singleton()
        {
            Assert.Equal(VariantValue.Null, VariantValue.Null);
        }

        // ---------------------------------------------------------------
        // Boolean
        // ---------------------------------------------------------------

        [Fact]
        public void Boolean_True()
        {
            VariantValue v = VariantValue.FromBoolean(true);
            Assert.Equal(VariantValue.True, v);
            Assert.True(v.IsBoolean);
            Assert.True(v.AsBoolean());
        }

        [Fact]
        public void Boolean_False()
        {
            VariantValue v = VariantValue.FromBoolean(false);
            Assert.Equal(VariantValue.False, v);
            Assert.False(v.AsBoolean());
        }

        // ---------------------------------------------------------------
        // Integer types
        // ---------------------------------------------------------------

        [Fact]
        public void Int8()
        {
            VariantValue v = VariantValue.FromInt8(42);
            Assert.Equal(VariantPrimitiveType.Int8, v.PrimitiveType);
            Assert.Equal((sbyte)42, v.AsInt8());
        }

        [Fact]
        public void Int16()
        {
            VariantValue v = VariantValue.FromInt16(1000);
            Assert.Equal((short)1000, v.AsInt16());
        }

        [Fact]
        public void Int32()
        {
            VariantValue v = VariantValue.FromInt32(100000);
            Assert.Equal(100000, v.AsInt32());
        }

        [Fact]
        public void Int64()
        {
            VariantValue v = VariantValue.FromInt64(1099511627776L);
            Assert.Equal(1099511627776L, v.AsInt64());
        }

        // ---------------------------------------------------------------
        // Float / Double
        // ---------------------------------------------------------------

        [Fact]
        public void Float()
        {
            VariantValue v = VariantValue.FromFloat(1.5f);
            Assert.Equal(1.5f, v.AsFloat());
        }

        [Fact]
        public void Double()
        {
            VariantValue v = VariantValue.FromDouble(3.14);
            Assert.Equal(3.14, v.AsDouble());
        }

        // ---------------------------------------------------------------
        // Decimal
        // ---------------------------------------------------------------

        [Fact]
        public void Decimal4()
        {
            VariantValue v = VariantValue.FromDecimal4(123.45m);
            Assert.Equal(VariantPrimitiveType.Decimal4, v.PrimitiveType);
            Assert.Equal(123.45m, v.AsDecimal());
        }

        [Fact]
        public void Decimal_AutoSize_Small()
        {
            VariantValue v = VariantValue.FromDecimal(99.99m);
            Assert.Equal(VariantPrimitiveType.Decimal4, v.PrimitiveType);
        }

        [Fact]
        public void Decimal_AutoSize_Large()
        {
            VariantValue v = VariantValue.FromDecimal(99999999999.99m);
            Assert.Equal(VariantPrimitiveType.Decimal8, v.PrimitiveType);
        }

        [Fact]
        public void Decimal8()
        {
            VariantValue v = VariantValue.FromDecimal8(123456789.12m);
            Assert.Equal(VariantPrimitiveType.Decimal8, v.PrimitiveType);
            Assert.Equal(123456789.12m, v.AsDecimal());
        }

        [Fact]
        public void Decimal16()
        {
            decimal d = 79228162514264337593543950335m;
            VariantValue v = VariantValue.FromDecimal16(d);
            Assert.Equal(VariantPrimitiveType.Decimal16, v.PrimitiveType);
            Assert.Equal(d, v.AsDecimal());
        }

        [Fact]
        public void Decimal_AutoSize_VeryLarge()
        {
            // A value requiring all 96 bits — auto-sizes to Decimal16
            VariantValue v = VariantValue.FromDecimal(79228162514264337593543950335m);
            Assert.Equal(VariantPrimitiveType.Decimal16, v.PrimitiveType);
        }

        // ---------------------------------------------------------------
        // String
        // ---------------------------------------------------------------

        [Fact]
        public void String()
        {
            VariantValue v = VariantValue.FromString("hello");
            Assert.True(v.IsString);
            Assert.Equal("hello", v.AsString());
        }

        // ---------------------------------------------------------------
        // Binary
        // ---------------------------------------------------------------

        [Fact]
        public void Binary()
        {
            byte[] data = new byte[] { 0xDE, 0xAD };
            VariantValue v = VariantValue.FromBinary(data);
            Assert.Equal(data, v.AsBinary());
        }

        // ---------------------------------------------------------------
        // UUID
        // ---------------------------------------------------------------

        [Fact]
        public void Uuid()
        {
            Guid g = Guid.NewGuid();
            VariantValue v = VariantValue.FromUuid(g);
            Assert.Equal(g, v.AsUuid());
        }

        // ---------------------------------------------------------------
        // Date / Timestamp
        // ---------------------------------------------------------------

        [Fact]
        public void Date_DaysSinceEpoch()
        {
            VariantValue v = VariantValue.FromDate(19000);
            Assert.Equal(19000, v.AsDateDays());
        }

        [Fact]
        public void Timestamp_Micros()
        {
            VariantValue v = VariantValue.FromTimestamp(1640995200000000L);
            Assert.Equal(1640995200000000L, v.AsTimestampMicros());
        }

        [Fact]
        public void Timestamp_DateTimeOffset()
        {
            DateTimeOffset dto = new DateTimeOffset(2022, 1, 1, 0, 0, 0, TimeSpan.Zero);
            VariantValue v = VariantValue.FromTimestamp(dto);
            Assert.Equal(dto, v.AsTimestamp());
        }

        // ---------------------------------------------------------------
        // Object
        // ---------------------------------------------------------------

        [Fact]
        public void Object_Empty()
        {
            VariantValue v = VariantValue.FromObject(new Dictionary<string, VariantValue>());
            Assert.True(v.IsObject);
            Assert.Empty(v.AsObject());
        }

        [Fact]
        public void Object_WithFields()
        {
            VariantValue v = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "age", VariantValue.FromInt32(30) },
            });

            IReadOnlyDictionary<string, VariantValue> obj = v.AsObject();
            Assert.Equal(2, obj.Count);
            Assert.Equal("Alice", obj["name"].AsString());
            Assert.Equal(30, obj["age"].AsInt32());
        }

        // ---------------------------------------------------------------
        // Array
        // ---------------------------------------------------------------

        [Fact]
        public void Array_Empty()
        {
            VariantValue v = VariantValue.FromArray(new List<VariantValue>());
            Assert.True(v.IsArray);
            Assert.Empty(v.AsArray());
        }

        [Fact]
        public void Array_WithElements()
        {
            VariantValue v = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"),
                VariantValue.Null);

            IReadOnlyList<VariantValue> arr = v.AsArray();
            Assert.Equal(3, arr.Count);
            Assert.Equal(1, arr[0].AsInt32());
            Assert.Equal("two", arr[1].AsString());
            Assert.True(arr[2].IsNull);
        }

        // ---------------------------------------------------------------
        // Equality
        // ---------------------------------------------------------------

        [Fact]
        public void Equality_Null()
        {
            Assert.Equal(VariantValue.Null, VariantValue.Null);
        }

        [Fact]
        public void Equality_Primitives()
        {
            Assert.Equal(VariantValue.FromInt32(42), VariantValue.FromInt32(42));
            Assert.NotEqual(VariantValue.FromInt32(42), VariantValue.FromInt32(43));
            Assert.NotEqual(VariantValue.FromInt32(42), VariantValue.FromInt64(42));
        }

        [Fact]
        public void Equality_String()
        {
            Assert.Equal(VariantValue.FromString("hello"), VariantValue.FromString("hello"));
            Assert.NotEqual(VariantValue.FromString("hello"), VariantValue.FromString("world"));
        }

        [Fact]
        public void Equality_Binary()
        {
            Assert.Equal(
                VariantValue.FromBinary(new byte[] { 1, 2, 3 }),
                VariantValue.FromBinary(new byte[] { 1, 2, 3 }));
            Assert.NotEqual(
                VariantValue.FromBinary(new byte[] { 1, 2, 3 }),
                VariantValue.FromBinary(new byte[] { 1, 2, 4 }));
        }

        [Fact]
        public void Equality_Object()
        {
            VariantValue a = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "x", VariantValue.FromInt32(1) },
            });
            VariantValue b = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "x", VariantValue.FromInt32(1) },
            });
            Assert.Equal(a, b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void Equality_Object_Nested()
        {
            VariantValue a = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "scores", VariantValue.FromArray(VariantValue.FromInt32(1), VariantValue.FromInt32(2)) },
            });
            VariantValue b = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "scores", VariantValue.FromArray(VariantValue.FromInt32(1), VariantValue.FromInt32(2)) },
            });
            Assert.Equal(a, b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void Equality_Object_Empty()
        {
            VariantValue a = VariantValue.FromObject(new Dictionary<string, VariantValue>());
            VariantValue b = VariantValue.FromObject(new Dictionary<string, VariantValue>());
            Assert.Equal(a, b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void Equality_Array()
        {
            VariantValue a = VariantValue.FromArray(VariantValue.FromInt32(1), VariantValue.FromInt32(2));
            VariantValue b = VariantValue.FromArray(VariantValue.FromInt32(1), VariantValue.FromInt32(2));
            Assert.Equal(a, b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void Equality_Array_Empty()
        {
            VariantValue a = VariantValue.FromArray(new List<VariantValue>());
            VariantValue b = VariantValue.FromArray(new List<VariantValue>());
            Assert.Equal(a, b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void HashCode_Object_UsableInHashSet()
        {
            VariantValue a = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "key", VariantValue.FromString("value") },
            });
            VariantValue b = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "key", VariantValue.FromString("value") },
            });

            HashSet<VariantValue> set = new HashSet<VariantValue> { a };
            Assert.Contains(b, set);
        }

        [Fact]
        public void HashCode_Array_UsableInHashSet()
        {
            VariantValue a = VariantValue.FromArray(VariantValue.FromInt32(1), VariantValue.Null);
            VariantValue b = VariantValue.FromArray(VariantValue.FromInt32(1), VariantValue.Null);

            HashSet<VariantValue> set = new HashSet<VariantValue> { a };
            Assert.Contains(b, set);
        }

        // ---------------------------------------------------------------
        // Type mismatch errors
        // ---------------------------------------------------------------

        [Fact]
        public void AsInt32_OnString_Throws()
        {
            Assert.Throws<InvalidOperationException>(() => VariantValue.FromString("x").AsInt32());
        }

        [Fact]
        public void AsString_OnNull_Throws()
        {
            Assert.Throws<InvalidOperationException>(() => VariantValue.Null.AsString());
        }

        [Fact]
        public void AsObject_OnArray_Throws()
        {
            Assert.Throws<InvalidOperationException>(() =>
                VariantValue.FromArray(VariantValue.Null).AsObject());
        }

        [Fact]
        public void AsDecimal_OnInt_Throws()
        {
            Assert.Throws<InvalidOperationException>(() => VariantValue.FromInt32(42).AsDecimal());
        }

        // ---------------------------------------------------------------
        // SqlDecimal equality invariant
        // ---------------------------------------------------------------

        [Fact]
        public void Equality_SqlDecimal_SameStorageType()
        {
            // Two SqlDecimal-stored values with same numeric value
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue a = VariantValue.FromSqlDecimal(sd);
            VariantValue b = VariantValue.FromSqlDecimal(sd);
            Assert.Equal(a, b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void Equality_SqlDecimal_FittingInDecimal_UsesDecimalStorage()
        {
            // FromSqlDecimal with small value stores as decimal, matching FromDecimal4
            SqlDecimal sd = new SqlDecimal(42.5m);
            VariantValue fromSqlDecimal = VariantValue.FromSqlDecimal(sd);
            VariantValue fromDecimal4 = VariantValue.FromDecimal4(42.5m);
            Assert.Equal(fromDecimal4, fromSqlDecimal);
            Assert.Equal(fromDecimal4.GetHashCode(), fromSqlDecimal.GetHashCode());
        }

        [Fact]
        public void IsSqlDecimalStorage_FalseForNonDecimalTypes()
        {
            Assert.False(VariantValue.FromInt32(42).IsSqlDecimalStorage);
            Assert.False(VariantValue.FromString("hello").IsSqlDecimalStorage);
            Assert.False(VariantValue.Null.IsSqlDecimalStorage);
            Assert.False(VariantValue.FromDouble(3.14).IsSqlDecimalStorage);
        }

        [Fact]
        public void IsSqlDecimalStorage_FalseForDecimalStoredValues()
        {
            Assert.False(VariantValue.FromDecimal4(42.5m).IsSqlDecimalStorage);
            Assert.False(VariantValue.FromDecimal8(123456789.12m).IsSqlDecimalStorage);
            Assert.False(VariantValue.FromDecimal16(42.5m).IsSqlDecimalStorage);
        }

        // ---------------------------------------------------------------
        // ToString with SqlDecimal storage
        // ---------------------------------------------------------------

        [Fact]
        public void ToString_Decimal4()
        {
            VariantValue v = VariantValue.FromDecimal4(123.45m);
            Assert.Equal("123.45", v.ToString());
        }

        [Fact]
        public void ToString_SqlDecimalStored()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue v = VariantValue.FromSqlDecimal(sd);
            Assert.Contains("99999999999999999999999999999999999999", v.ToString());
        }

        // ---------------------------------------------------------------
        // ToVariantValue materialization from binary
        // ---------------------------------------------------------------

        [Fact]
        public void ToVariantValue_Null()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveNull);
            Assert.Equal(VariantValue.Null, reader.ToVariantValue());
        }

        [Fact]
        public void ToVariantValue_Int32()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveInt32_100000);
            VariantValue v = reader.ToVariantValue();
            Assert.Equal(VariantValue.FromInt32(100000), v);
        }

        [Fact]
        public void ToVariantValue_ShortString()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.ShortString_Hi);
            VariantValue v = reader.ToVariantValue();
            Assert.Equal(VariantValue.FromString("Hi"), v);
        }

        [Fact]
        public void ToVariantValue_Object()
        {
            VariantReader reader = new VariantReader(
                TestVectors.SortedMetadata_Age_Name, TestVectors.Object_Age30_Name_Bob);
            VariantValue v = reader.ToVariantValue();

            Assert.True(v.IsObject);
            IReadOnlyDictionary<string, VariantValue> obj = v.AsObject();
            Assert.Equal(2, obj.Count);
            Assert.Equal((sbyte)30, obj["age"].AsInt8());
            Assert.Equal("Bob", obj["name"].AsString());
        }

        [Fact]
        public void ToVariantValue_Array()
        {
            VariantReader reader = new VariantReader(TestVectors.EmptyMetadata, TestVectors.Array_Mixed);
            VariantValue v = reader.ToVariantValue();

            Assert.True(v.IsArray);
            IReadOnlyList<VariantValue> arr = v.AsArray();
            Assert.Equal(3, arr.Count);
            Assert.Equal((sbyte)42, arr[0].AsInt8());
            Assert.Equal("hi", arr[1].AsString());
            Assert.True(arr[2].IsNull);
        }
    }
}

