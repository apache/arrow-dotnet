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

using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization;
using Xunit;

namespace Apache.Arrow.Serialization.Tests;

public class SerializationTests
{
    [Fact]
    public void Primitives_RoundTrip()
    {
        var obj = new WithPrimitives
        {
            Name = "hello",
            Count = 42,
            BigCount = 123456789L,
            Score = 3.14f,
            Precise = 2.718281828,
            Flag = true,
            Small = 7,
        };

        var batch = WithPrimitives.ToRecordBatch(obj);
        Assert.Equal(1, batch.Length);

        var restored = WithPrimitives.FromRecordBatch(batch);
        Assert.Equal(obj.Name, restored.Name);
        Assert.Equal(obj.Count, restored.Count);
        Assert.Equal(obj.BigCount, restored.BigCount);
        Assert.Equal(obj.Score, restored.Score);
        Assert.Equal(obj.Precise, restored.Precise);
        Assert.Equal(obj.Flag, restored.Flag);
        Assert.Equal(obj.Small, restored.Small);
    }

    [Fact]
    public void String_And_Int_RoundTrip()
    {
        var obj = new Inner { X = 42, Label = "hello" };
        var batch = Inner.ToRecordBatch(obj);
        var restored = Inner.FromRecordBatch(batch);
        Assert.Equal(obj, restored);
    }

    [Fact]
    public void Bool_RoundTrip()
    {
        foreach (var val in new[] { true, false })
        {
            var obj = new WithBool { Flag = val };
            var restored = WithBool.FromRecordBatch(WithBool.ToRecordBatch(obj));
            Assert.Equal(val, restored.Flag);
        }
    }

    [Fact]
    public void Bytes_RoundTrip()
    {
        var obj = new WithBytes { Data = new byte[] { 0x00, 0x01, 0xFF } };
        var restored = WithBytes.FromRecordBatch(WithBytes.ToRecordBatch(obj));
        Assert.Equal(obj.Data, restored.Data);
    }

    [Fact]
    public void ReadOnlyMemoryBytes_RoundTrip()
    {
        var obj = new WithReadOnlyMemoryBytes { Data = new byte[] { 0x00, 0x01, 0xFF } };
        var batch = WithReadOnlyMemoryBytes.ToRecordBatch(obj);
        var restored = WithReadOnlyMemoryBytes.FromRecordBatch(batch);
        Assert.Equal(obj.Data.ToArray(), restored.Data.ToArray());
    }

    [Fact]
    public void ReadOnlyMemoryBytes_MultiRow_RoundTrip()
    {
        var items = new List<WithReadOnlyMemoryBytes>
        {
            new() { Data = new byte[] { 1, 2, 3 } },
            new() { Data = new byte[] { 4, 5 } },
        };
        var batch = WithReadOnlyMemoryBytes.ToRecordBatch(items);
        var restored = WithReadOnlyMemoryBytes.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Data.ToArray(), restored[0].Data.ToArray());
        Assert.Equal(items[1].Data.ToArray(), restored[1].Data.ToArray());
    }

    [Fact]
    public void Enum_RoundTrip()
    {
        var obj = new WithEnum { Color = Color.Red };
        var restored = WithEnum.FromRecordBatch(WithEnum.ToRecordBatch(obj));
        Assert.Equal(Color.Red, restored.Color);
    }

    [Fact]
    public void NullableEnum_None_RoundTrip()
    {
        var obj = new WithNullableEnum { Color = null };
        var restored = WithNullableEnum.FromRecordBatch(WithNullableEnum.ToRecordBatch(obj));
        Assert.Null(restored.Color);
    }

    [Fact]
    public void NullableEnum_Present_RoundTrip()
    {
        var obj = new WithNullableEnum { Color = Color.Green };
        var restored = WithNullableEnum.FromRecordBatch(WithNullableEnum.ToRecordBatch(obj));
        Assert.Equal(Color.Green, restored.Color);
    }

    [Fact]
    public void NestedRecord_RoundTrip()
    {
        var obj = new Outer { Inner = new Inner { X = 42, Label = "hi" }, Name = "test" };
        var restored = Outer.FromRecordBatch(Outer.ToRecordBatch(obj));
        Assert.Equal(obj, restored);
    }

    [Fact]
    public void DeeplyNested_RoundTrip()
    {
        var obj = new Level3
        {
            Level2 = new Level2
            {
                Inner = new Inner { X = 1, Label = "deep" },
                Tag = "t",
            },
            Score = 3.14,
        };
        var restored = Level3.FromRecordBatch(Level3.ToRecordBatch(obj));
        Assert.Equal(obj, restored);
    }

    [Fact]
    public void OptionalString_None_RoundTrip()
    {
        var obj = new WithOptional { Value = "hello", Extra = null };
        var restored = WithOptional.FromRecordBatch(WithOptional.ToRecordBatch(obj));
        Assert.Null(restored.Extra);
    }

    [Fact]
    public void OptionalString_Present_RoundTrip()
    {
        var obj = new WithOptional { Value = "hello", Extra = "world" };
        var restored = WithOptional.FromRecordBatch(WithOptional.ToRecordBatch(obj));
        Assert.Equal("world", restored.Extra);
    }

    [Fact]
    public void OptionalInt_None_RoundTrip()
    {
        var obj = new WithOptionalInt { Value = 5, Extra = null };
        var restored = WithOptionalInt.FromRecordBatch(WithOptionalInt.ToRecordBatch(obj));
        Assert.Null(restored.Extra);
    }

    [Fact]
    public void OptionalInt_Present_RoundTrip()
    {
        var obj = new WithOptionalInt { Value = 5, Extra = 42 };
        var restored = WithOptionalInt.FromRecordBatch(WithOptionalInt.ToRecordBatch(obj));
        Assert.Equal(42, restored.Extra);
    }

    [Fact]
    public void List_RoundTrip()
    {
        var obj = new WithList { Items = new List<int> { 1, 2, 3 } };
        var restored = WithList.FromRecordBatch(WithList.ToRecordBatch(obj));
        Assert.Equal(obj.Items, restored.Items);
    }

    [Fact]
    public void List_Empty_RoundTrip()
    {
        var obj = new WithList { Items = new List<int>() };
        var restored = WithList.FromRecordBatch(WithList.ToRecordBatch(obj));
        Assert.Empty(restored.Items);
    }

    [Fact]
    public void StringList_RoundTrip()
    {
        var obj = new WithStringList { Tags = new List<string> { "a", "b", "c" } };
        var restored = WithStringList.FromRecordBatch(WithStringList.ToRecordBatch(obj));
        Assert.Equal(obj.Tags, restored.Tags);
    }

    [Fact]
    public void Dict_RoundTrip()
    {
        var obj = new WithDict { Mapping = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 } };
        var restored = WithDict.FromRecordBatch(WithDict.ToRecordBatch(obj));
        Assert.Equal(obj.Mapping, restored.Mapping);
    }

    [Fact]
    public void Dict_Empty_RoundTrip()
    {
        var obj = new WithDict { Mapping = new Dictionary<string, int>() };
        var restored = WithDict.FromRecordBatch(WithDict.ToRecordBatch(obj));
        Assert.Empty(restored.Mapping);
    }

    [Fact]
    public void Set_RoundTrip()
    {
        var obj = new WithSet { Tags = new HashSet<int> { 10, 20 } };
        var restored = WithSet.FromRecordBatch(WithSet.ToRecordBatch(obj));
        Assert.Equal(obj.Tags, restored.Tags);
    }

    [Fact]
    public void OptionalNested_None_RoundTrip()
    {
        var obj = new WithOptionalNested { Inner = null };
        var restored = WithOptionalNested.FromRecordBatch(WithOptionalNested.ToRecordBatch(obj));
        Assert.Null(restored.Inner);
    }

    [Fact]
    public void OptionalNested_Present_RoundTrip()
    {
        var obj = new WithOptionalNested { Inner = new Inner { X = 7, Label = "nested" } };
        var restored = WithOptionalNested.FromRecordBatch(WithOptionalNested.ToRecordBatch(obj));
        Assert.Equal(obj, restored);
    }
}

public class MultiRowTests
{
    [Fact]
    public void Primitives_MultiRow_RoundTrip()
    {
        var items = new List<WithPrimitives>
        {
            new() { Name = "a", Count = 1, BigCount = 10L, Score = 1.1f, Precise = 1.11, Flag = true, Small = 1 },
            new() { Name = "b", Count = 2, BigCount = 20L, Score = 2.2f, Precise = 2.22, Flag = false, Small = 2 },
            new() { Name = "c", Count = 3, BigCount = 30L, Score = 3.3f, Precise = 3.33, Flag = true, Small = 3 },
        };

        var batch = WithPrimitives.ToRecordBatch(items);
        Assert.Equal(3, batch.Length);

        var restored = WithPrimitives.ListFromRecordBatch(batch);
        Assert.Equal(3, restored.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(items[i].Name, restored[i].Name);
            Assert.Equal(items[i].Count, restored[i].Count);
            Assert.Equal(items[i].BigCount, restored[i].BigCount);
            Assert.Equal(items[i].Score, restored[i].Score);
            Assert.Equal(items[i].Precise, restored[i].Precise);
            Assert.Equal(items[i].Flag, restored[i].Flag);
            Assert.Equal(items[i].Small, restored[i].Small);
        }
    }

    [Fact]
    public void Inner_MultiRow_RoundTrip()
    {
        var items = new List<Inner>
        {
            new() { X = 1, Label = "one" },
            new() { X = 2, Label = "two" },
            new() { X = 3, Label = "three" },
        };

        var batch = Inner.ToRecordBatch(items);
        Assert.Equal(3, batch.Length);

        var restored = Inner.ListFromRecordBatch(batch);
        Assert.Equal(items, restored);
    }

    [Fact]
    public void Enum_MultiRow_RoundTrip()
    {
        var items = new List<WithEnum>
        {
            new() { Color = Color.Red },
            new() { Color = Color.Green },
            new() { Color = Color.Blue },
            new() { Color = Color.Red },
        };

        var batch = WithEnum.ToRecordBatch(items);
        Assert.Equal(4, batch.Length);

        var restored = WithEnum.ListFromRecordBatch(batch);
        for (int i = 0; i < items.Count; i++)
            Assert.Equal(items[i].Color, restored[i].Color);
    }

    [Fact]
    public void NullableEnum_MultiRow_RoundTrip()
    {
        var items = new List<WithNullableEnum>
        {
            new() { Color = Color.Red },
            new() { Color = null },
            new() { Color = Color.Blue },
        };

        var batch = WithNullableEnum.ToRecordBatch(items);
        var restored = WithNullableEnum.ListFromRecordBatch(batch);
        Assert.Equal(Color.Red, restored[0].Color);
        Assert.Null(restored[1].Color);
        Assert.Equal(Color.Blue, restored[2].Color);
    }

    [Fact]
    public void NestedRecord_MultiRow_RoundTrip()
    {
        var items = new List<Outer>
        {
            new() { Inner = new Inner { X = 1, Label = "a" }, Name = "first" },
            new() { Inner = new Inner { X = 2, Label = "b" }, Name = "second" },
        };

        var batch = Outer.ToRecordBatch(items);
        Assert.Equal(2, batch.Length);

        var restored = Outer.ListFromRecordBatch(batch);
        Assert.Equal(items, restored);
    }

    [Fact]
    public void OptionalNested_MultiRow_RoundTrip()
    {
        var items = new List<WithOptionalNested>
        {
            new() { Inner = new Inner { X = 1, Label = "a" } },
            new() { Inner = null },
            new() { Inner = new Inner { X = 3, Label = "c" } },
        };

        var batch = WithOptionalNested.ToRecordBatch(items);
        var restored = WithOptionalNested.ListFromRecordBatch(batch);
        Assert.Equal(items[0], restored[0]);
        Assert.Null(restored[1].Inner);
        Assert.Equal(items[2], restored[2]);
    }

    [Fact]
    public void List_MultiRow_RoundTrip()
    {
        var items = new List<WithList>
        {
            new() { Items = new List<int> { 1, 2, 3 } },
            new() { Items = new List<int> { 4, 5 } },
            new() { Items = new List<int>() },
        };

        var batch = WithList.ToRecordBatch(items);
        var restored = WithList.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Items, restored[0].Items);
        Assert.Equal(items[1].Items, restored[1].Items);
        Assert.Empty(restored[2].Items);
    }

    [Fact]
    public void Dict_MultiRow_RoundTrip()
    {
        var items = new List<WithDict>
        {
            new() { Mapping = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 } },
            new() { Mapping = new Dictionary<string, int> { ["c"] = 3 } },
        };

        var batch = WithDict.ToRecordBatch(items);
        var restored = WithDict.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Mapping, restored[0].Mapping);
        Assert.Equal(items[1].Mapping, restored[1].Mapping);
    }

    [Fact]
    public void EnumList_SingleRow_RoundTrip()
    {
        var original = new WithColorEnumList { Colors = [Color.Red, Color.Green, Color.Blue] };
        var batch = WithColorEnumList.ToRecordBatch(original);
        var restored = WithColorEnumList.FromRecordBatch(batch);
        Assert.Equal(original.Colors, restored.Colors);
    }

    [Fact]
    public void EnumList_MultiRow_RoundTrip()
    {
        var items = new List<WithColorEnumList>
        {
            new() { Colors = [Color.Red, Color.Green] },
            new() { Colors = [Color.Blue] },
            new() { Colors = [] },
        };
        var batch = WithColorEnumList.ToRecordBatch(items);
        var restored = WithColorEnumList.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Colors, restored[0].Colors);
        Assert.Equal(items[1].Colors, restored[1].Colors);
        Assert.Empty(restored[2].Colors);
    }

    [Fact]
    public void NestedList_SingleRow_RoundTrip()
    {
        var original = new WithNestedList
        {
            Entries = [new Inner { X = 1, Label = "a" }, new Inner { X = 2, Label = "b" }],
        };
        var batch = WithNestedList.ToRecordBatch(original);
        var restored = WithNestedList.FromRecordBatch(batch);
        Assert.Equal(2, restored.Entries.Count);
        Assert.Equal(1, restored.Entries[0].X);
        Assert.Equal("a", restored.Entries[0].Label);
        Assert.Equal(2, restored.Entries[1].X);
        Assert.Equal("b", restored.Entries[1].Label);
    }

    [Fact]
    public void NestedList_MultiRow_RoundTrip()
    {
        var items = new List<WithNestedList>
        {
            new() { Entries = [new Inner { X = 1, Label = "a" }, new Inner { X = 2, Label = "b" }] },
            new() { Entries = [new Inner { X = 3, Label = "c" }] },
            new() { Entries = [] },
        };
        var batch = WithNestedList.ToRecordBatch(items);
        var restored = WithNestedList.ListFromRecordBatch(batch);
        Assert.Equal(2, restored[0].Entries.Count);
        Assert.Equal(1, restored[0].Entries[0].X);
        Assert.Single(restored[1].Entries);
        Assert.Equal(3, restored[1].Entries[0].X);
        Assert.Empty(restored[2].Entries);
    }

    [Fact]
    public void EnumList_IPC_RoundTrip()
    {
        var original = new WithColorEnumList { Colors = [Color.Red, Color.Blue] };
        var bytes = original.SerializeToBytes();
        var restored = ArrowSerializerExtensions.DeserializeFromBytes<WithColorEnumList>(bytes);
        Assert.Equal(original.Colors, restored.Colors);
    }

    [Fact]
    public void NestedList_IPC_RoundTrip()
    {
        var original = new WithNestedList
        {
            Entries = [new Inner { X = 42, Label = "hello" }],
        };
        var bytes = original.SerializeToBytes();
        var restored = ArrowSerializerExtensions.DeserializeFromBytes<WithNestedList>(bytes);
        Assert.Single(restored.Entries);
        Assert.Equal(42, restored.Entries[0].X);
        Assert.Equal("hello", restored.Entries[0].Label);
    }

    [Fact]
    public void Empty_MultiRow_RoundTrip()
    {
        var items = new List<Inner>();
        var batch = Inner.ToRecordBatch(items);
        Assert.Equal(0, batch.Length);

        var restored = Inner.ListFromRecordBatch(batch);
        Assert.Empty(restored);
    }

    [Fact]
    public void ExtensionMethod_ToRecordBatch_RoundTrip()
    {
        var items = new[] { new Inner { X = 1, Label = "one" }, new Inner { X = 2, Label = "two" } };
        var batch = items.ToRecordBatch();
        Assert.Equal(2, batch.Length);

        var restored = batch.ToList<Inner>();
        Assert.Equal(items, restored);
    }

    [Fact]
    public void IpcMultiRow_RoundTrip()
    {
        var items = new List<Inner>
        {
            new() { X = 1, Label = "one" },
            new() { X = 2, Label = "two" },
        };

        var bytes = items.SerializeListToBytes();
        var restored = ArrowSerializerExtensions.DeserializeListFromBytes<Inner>(bytes);
        Assert.Equal(items, restored);
    }

    [Fact]
    public void OptionalInt_MultiRow_RoundTrip()
    {
        var items = new List<WithOptionalInt>
        {
            new() { Value = 1, Extra = 10 },
            new() { Value = 2, Extra = null },
            new() { Value = 3, Extra = 30 },
        };

        var batch = WithOptionalInt.ToRecordBatch(items);
        var restored = WithOptionalInt.ListFromRecordBatch(batch);
        Assert.Equal(10, restored[0].Extra);
        Assert.Null(restored[1].Extra);
        Assert.Equal(30, restored[2].Extra);
    }
}

public class DateTimeTypeTests
{
    [Fact]
    public void DateTime_RoundTrip()
    {
        var obj = new WithDateTimeTypes
        {
            Timestamp = new DateTime(2025, 6, 15, 12, 30, 0, DateTimeKind.Utc),
            TimestampOffset = new DateTimeOffset(2025, 6, 15, 12, 30, 0, TimeSpan.Zero),
            Date = new DateOnly(2025, 6, 15),
            Time = new TimeOnly(12, 30, 45),
            Duration = TimeSpan.FromHours(2.5),
        };

        var batch = WithDateTimeTypes.ToRecordBatch(obj);
        var restored = WithDateTimeTypes.FromRecordBatch(batch);
        Assert.Equal(obj.Timestamp, restored.Timestamp);
        Assert.Equal(obj.TimestampOffset, restored.TimestampOffset);
        Assert.Equal(obj.Date, restored.Date);
        Assert.Equal(obj.Time, restored.Time);
        Assert.Equal(obj.Duration, restored.Duration);
    }

    [Fact]
    public void DateTime_LocalNormalizesToUtc()
    {
        // A local DateTime should be converted to UTC on serialize, and come back as UTC
        var localDt = new DateTime(2025, 6, 15, 14, 30, 0, DateTimeKind.Local);
        var expectedUtc = localDt.ToUniversalTime();

        var obj = new WithDateTimeTypes
        {
            Timestamp = localDt,
            TimestampOffset = new DateTimeOffset(2025, 6, 15, 14, 30, 0, TimeSpan.FromHours(2)),
            Date = new DateOnly(2025, 6, 15),
            Time = new TimeOnly(14, 30, 0),
            Duration = TimeSpan.Zero,
        };

        var batch = WithDateTimeTypes.ToRecordBatch(obj);
        var restored = WithDateTimeTypes.FromRecordBatch(batch);

        // DateTime comes back as UTC-normalized value
        Assert.Equal(expectedUtc, restored.Timestamp);
        Assert.Equal(DateTimeKind.Utc, restored.Timestamp.Kind);

        // DateTimeOffset with +02:00 offset normalizes to UTC
        Assert.Equal(new DateTimeOffset(2025, 6, 15, 12, 30, 0, TimeSpan.Zero), restored.TimestampOffset);
    }

    [Fact]
    public void DateTime_MultiRow_RoundTrip()
    {
        var items = new List<WithDateTimeTypes>
        {
            new() { Timestamp = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), TimestampOffset = DateTimeOffset.UtcNow,
                     Date = new DateOnly(2025, 1, 1), Time = new TimeOnly(8, 0), Duration = TimeSpan.FromMinutes(30) },
            new() { Timestamp = new DateTime(2025, 12, 31, 23, 59, 59, DateTimeKind.Utc), TimestampOffset = DateTimeOffset.UtcNow,
                     Date = new DateOnly(2025, 12, 31), Time = new TimeOnly(23, 59, 59), Duration = TimeSpan.FromDays(1) },
        };

        var batch = WithDateTimeTypes.ToRecordBatch(items);
        var restored = WithDateTimeTypes.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Date, restored[0].Date);
        Assert.Equal(items[0].Time, restored[0].Time);
        Assert.Equal(items[0].Duration, restored[0].Duration);
        Assert.Equal(items[1].Date, restored[1].Date);
        Assert.Equal(items[1].Time, restored[1].Time);
        Assert.Equal(items[1].Duration, restored[1].Duration);
    }

    [Fact]
    public void DecimalAndGuid_RoundTrip()
    {
        var obj = new WithDecimalAndGuid
        {
            Amount = 123456.789m,
            Id = Guid.Parse("12345678-1234-1234-1234-123456789abc"),
        };

        var batch = WithDecimalAndGuid.ToRecordBatch(obj);
        var restored = WithDecimalAndGuid.FromRecordBatch(batch);
        Assert.Equal(obj.Amount, restored.Amount);
        Assert.Equal(obj.Id, restored.Id);
    }

    [Fact]
    public void DecimalAndGuid_MultiRow_RoundTrip()
    {
        var items = new List<WithDecimalAndGuid>
        {
            new() { Amount = 100.50m, Id = Guid.NewGuid() },
            new() { Amount = 0.001m, Id = Guid.NewGuid() },
            new() { Amount = -999.99m, Id = Guid.Empty },
        };

        var batch = WithDecimalAndGuid.ToRecordBatch(items);
        var restored = WithDecimalAndGuid.ListFromRecordBatch(batch);
        for (int i = 0; i < items.Count; i++)
        {
            Assert.Equal(items[i].Amount, restored[i].Amount);
            Assert.Equal(items[i].Id, restored[i].Id);
        }
    }

    [Fact]
    public void Half_RoundTrip()
    {
        var obj = new WithHalf { Value = (Half)3.14 };
        var batch = WithHalf.ToRecordBatch(obj);
        var restored = WithHalf.FromRecordBatch(batch);
        Assert.Equal(obj.Value, restored.Value);
    }

    [Fact]
    public void NullableDateTimeTypes_RoundTrip_WithValues()
    {
        var obj = new WithNullableDateTimeTypes
        {
            Timestamp = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc),
            Date = new DateOnly(2025, 6, 15),
            Time = new TimeOnly(12, 30),
            Duration = TimeSpan.FromSeconds(90),
            Amount = 42.5m,
            Id = Guid.NewGuid(),
            HalfVal = (Half)1.5,
        };

        var batch = WithNullableDateTimeTypes.ToRecordBatch(obj);
        var restored = WithNullableDateTimeTypes.FromRecordBatch(batch);
        Assert.Equal(obj.Timestamp, restored.Timestamp);
        Assert.Equal(obj.Date, restored.Date);
        Assert.Equal(obj.Time, restored.Time);
        Assert.Equal(obj.Duration, restored.Duration);
        Assert.Equal(obj.Amount, restored.Amount);
        Assert.Equal(obj.Id, restored.Id);
        Assert.Equal(obj.HalfVal, restored.HalfVal);
    }

    [Fact]
    public void NullableDateTimeTypes_RoundTrip_WithNulls()
    {
        var obj = new WithNullableDateTimeTypes();

        var batch = WithNullableDateTimeTypes.ToRecordBatch(obj);
        var restored = WithNullableDateTimeTypes.FromRecordBatch(batch);
        Assert.Null(restored.Timestamp);
        Assert.Null(restored.Date);
        Assert.Null(restored.Time);
        Assert.Null(restored.Duration);
        Assert.Null(restored.Amount);
        Assert.Null(restored.Id);
        Assert.Null(restored.HalfVal);
    }

    [Fact]
    public void NullableDateTimeTypes_MultiRow_RoundTrip()
    {
        var items = new List<WithNullableDateTimeTypes>
        {
            new() { Timestamp = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), Date = new DateOnly(2025, 1, 1),
                     Time = new TimeOnly(8, 0), Duration = TimeSpan.FromMinutes(30),
                     Amount = 10.5m, Id = Guid.NewGuid(), HalfVal = (Half)1.0 },
            new(), // all nulls
            new() { Timestamp = new DateTime(2025, 12, 31, 0, 0, 0, DateTimeKind.Utc), Date = new DateOnly(2025, 12, 31),
                     Time = new TimeOnly(23, 59), Duration = TimeSpan.FromHours(1),
                     Amount = 99.99m, Id = Guid.NewGuid(), HalfVal = (Half)2.0 },
        };

        var batch = WithNullableDateTimeTypes.ToRecordBatch(items);
        var restored = WithNullableDateTimeTypes.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Date, restored[0].Date);
        Assert.Equal(items[0].Time, restored[0].Time);
        Assert.Equal(items[0].Duration, restored[0].Duration);
        Assert.Equal(items[0].Amount, restored[0].Amount);
        Assert.Equal(items[0].Id, restored[0].Id);
        Assert.Null(restored[1].Timestamp);
        Assert.Null(restored[1].Date);
        Assert.Null(restored[1].Time);
        Assert.Null(restored[1].Duration);
        Assert.Null(restored[1].Amount);
        Assert.Null(restored[1].Id);
        Assert.Equal(items[2].Date, restored[2].Date);
    }
}

public class ArrowTypeOverrideTests
{
    [Fact]
    public void TimestampOverride_UsesNanoseconds()
    {
        var schema = WithTimestampOverride.ArrowSchema;
        var tsType = Assert.IsType<TimestampType>(schema.FieldsList[0].DataType);
        Assert.Equal(TimeUnit.Nanosecond, tsType.Unit);
        Assert.Equal("UTC", tsType.Timezone);
    }

    [Fact]
    public void DecimalOverride_UsesCustomPrecisionScale()
    {
        var schema = WithDecimalOverride.ArrowSchema;
        var decType = Assert.IsType<Decimal128Type>(schema.FieldsList[0].DataType);
        Assert.Equal(28, decType.Precision);
        Assert.Equal(10, decType.Scale);
    }

    [Fact]
    public void TimestampOverride_DateTimeMilliseconds()
    {
        var schema = WithTimestampOverride.ArrowSchema;
        var tsType = Assert.IsType<TimestampType>(schema.FieldsList[1].DataType);
        Assert.Equal(TimeUnit.Millisecond, tsType.Unit);
        Assert.Equal("UTC", tsType.Timezone);

        var obj = new WithTimestampOverride
        {
            Value = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero),
            DateTimeValue = new DateTime(2025, 3, 1, 8, 30, 0, DateTimeKind.Utc),
        };
        var batch = WithTimestampOverride.ToRecordBatch(obj);
        var restored = WithTimestampOverride.FromRecordBatch(batch);
        Assert.Equal(obj.Value, restored.Value);
        Assert.Equal(obj.DateTimeValue, restored.DateTimeValue);
    }

    [Fact]
    public void TimestampOverride_RoundTrip()
    {
        var obj = new WithTimestampOverride { Value = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero) };
        var batch = WithTimestampOverride.ToRecordBatch(obj);
        var restored = WithTimestampOverride.FromRecordBatch(batch);
        Assert.Equal(obj.Value, restored.Value);
    }

    [Fact]
    public void DecimalOverride_RoundTrip()
    {
        var obj = new WithDecimalOverride { Value = 12345.6789m };
        var batch = WithDecimalOverride.ToRecordBatch(obj);
        var restored = WithDecimalOverride.FromRecordBatch(batch);
        Assert.Equal(obj.Value, restored.Value);
    }

    [Fact]
    public void TimeOverride_Time32MillisAndTime64Nanos()
    {
        var schema = WithTimeOverride.ArrowSchema;
        var t32 = Assert.IsType<Time32Type>(schema.FieldsList[0].DataType);
        Assert.Equal(TimeUnit.Millisecond, t32.Unit);
        var t64 = Assert.IsType<Time64Type>(schema.FieldsList[1].DataType);
        Assert.Equal(TimeUnit.Nanosecond, t64.Unit);

        var obj = new WithTimeOverride
        {
            Millis = new TimeOnly(14, 30, 45),
            Nanos = new TimeOnly(8, 15, 30, 500),
        };
        var batch = WithTimeOverride.ToRecordBatch(obj);
        var restored = WithTimeOverride.FromRecordBatch(batch);
        // time32[ms] has millisecond precision — truncates sub-ms
        Assert.Equal(obj.Millis, restored.Millis);
        Assert.Equal(obj.Nanos, restored.Nanos);
    }

    [Fact]
    public void TimeOverride_MultiRow_RoundTrip()
    {
        var items = new List<WithTimeOverride>
        {
            new() { Millis = new TimeOnly(0, 0, 0), Nanos = new TimeOnly(12, 0, 0) },
            new() { Millis = new TimeOnly(23, 59, 59), Nanos = new TimeOnly(6, 30, 15) },
        };
        var batch = WithTimeOverride.ToRecordBatch(items);
        var restored = WithTimeOverride.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Millis, restored[0].Millis);
        Assert.Equal(items[0].Nanos, restored[0].Nanos);
        Assert.Equal(items[1].Millis, restored[1].Millis);
        Assert.Equal(items[1].Nanos, restored[1].Nanos);
    }

    [Fact]
    public void DateOverride_Schema_Date64AndDate32()
    {
        var schema = WithDateOverride.ArrowSchema;
        Assert.IsType<Date32Type>(schema.FieldsList[0].DataType); // default
        Assert.IsType<Date64Type>(schema.FieldsList[1].DataType); // date64 override
        Assert.IsType<Date32Type>(schema.FieldsList[2].DataType); // explicit date32
    }

    [Fact]
    public void DateOverride_SingleRow_RoundTrip()
    {
        var obj = new WithDateOverride
        {
            Default = new DateOnly(2025, 3, 15),
            Date64Value = new DateOnly(2024, 12, 25),
            Date32Value = new DateOnly(2000, 1, 1),
        };
        var batch = WithDateOverride.ToRecordBatch(obj);
        var restored = WithDateOverride.FromRecordBatch(batch);
        Assert.Equal(obj.Default, restored.Default);
        Assert.Equal(obj.Date64Value, restored.Date64Value);
        Assert.Equal(obj.Date32Value, restored.Date32Value);
    }

    [Fact]
    public void DateOverride_MultiRow_RoundTrip()
    {
        var items = new List<WithDateOverride>
        {
            new() { Default = new DateOnly(2020, 1, 1), Date64Value = new DateOnly(2020, 6, 15), Date32Value = new DateOnly(1999, 12, 31) },
            new() { Default = new DateOnly(2025, 12, 31), Date64Value = new DateOnly(2025, 7, 4), Date32Value = new DateOnly(2030, 3, 1) },
        };
        var batch = WithDateOverride.ToRecordBatch(items);
        var restored = WithDateOverride.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.Equal(items[0].Default, restored[0].Default);
        Assert.Equal(items[0].Date64Value, restored[0].Date64Value);
        Assert.Equal(items[0].Date32Value, restored[0].Date32Value);
        Assert.Equal(items[1].Default, restored[1].Default);
        Assert.Equal(items[1].Date64Value, restored[1].Date64Value);
        Assert.Equal(items[1].Date32Value, restored[1].Date32Value);
    }

    [Fact]
    public void ViewTypes_Schema_UsesViewTypes()
    {
        var schema = WithViewTypes.ArrowSchema;
        Assert.IsType<StringViewType>(schema.FieldsList[0].DataType);
        Assert.IsType<BinaryViewType>(schema.FieldsList[1].DataType);
        Assert.IsType<StringType>(schema.FieldsList[2].DataType);
        Assert.IsType<BinaryType>(schema.FieldsList[3].DataType);
    }

    [Fact]
    public void ViewTypes_SingleRow_RoundTrip()
    {
        var obj = new WithViewTypes
        {
            ViewString = "hello view",
            ViewBinary = new byte[] { 1, 2, 3 },
            NormalString = "hello normal",
            NormalBinary = new byte[] { 4, 5, 6 },
        };
        var batch = WithViewTypes.ToRecordBatch(obj);
        var restored = WithViewTypes.FromRecordBatch(batch);
        Assert.Equal(obj.ViewString, restored.ViewString);
        Assert.Equal(obj.ViewBinary, restored.ViewBinary);
        Assert.Equal(obj.NormalString, restored.NormalString);
        Assert.Equal(obj.NormalBinary, restored.NormalBinary);
    }

    [Fact]
    public void ViewTypes_MultiRow_RoundTrip()
    {
        var items = new List<WithViewTypes>
        {
            new() { ViewString = "a", ViewBinary = new byte[] { 10 }, NormalString = "x", NormalBinary = new byte[] { 20 } },
            new() { ViewString = "b", ViewBinary = new byte[] { 30, 40 }, NormalString = "y", NormalBinary = new byte[] { 50 } },
        };
        var batch = WithViewTypes.ToRecordBatch(items);
        var restored = WithViewTypes.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.Equal("a", restored[0].ViewString);
        Assert.Equal(new byte[] { 10 }, restored[0].ViewBinary);
        Assert.Equal("b", restored[1].ViewString);
        Assert.Equal(new byte[] { 30, 40 }, restored[1].ViewBinary);
    }

    // --- Collection element type override tests ---

    [Fact]
    public void ListStringView_Schema_UsesStringViewElementType()
    {
        var schema = WithListStringView.ArrowSchema;
        // Items: List(StringView)
        var itemsField = schema.FieldsList[0];
        var listType = Assert.IsType<ListType>(itemsField.DataType);
        Assert.IsType<StringViewType>(listType.ValueDataType);
        // NormalItems: List(Utf8)
        var normalField = schema.FieldsList[1];
        var normalListType = Assert.IsType<ListType>(normalField.DataType);
        Assert.IsType<StringType>(normalListType.ValueDataType);
    }

    [Fact]
    public void ListStringView_SingleRow_RoundTrip()
    {
        var obj = new WithListStringView
        {
            Items = new List<string> { "hello", "world" },
            NormalItems = new List<string> { "foo", "bar" },
        };
        var batch = WithListStringView.ToRecordBatch(obj);
        var restored = WithListStringView.FromRecordBatch(batch);
        Assert.Equal(obj.Items, restored.Items);
        Assert.Equal(obj.NormalItems, restored.NormalItems);
    }

    [Fact]
    public void ListStringView_MultiRow_RoundTrip()
    {
        var items = new List<WithListStringView>
        {
            new() { Items = new List<string> { "a", "b" }, NormalItems = new List<string> { "x" } },
            new() { Items = new List<string> { "c" }, NormalItems = new List<string> { "y", "z" } },
        };
        var batch = WithListStringView.ToRecordBatch(items);
        var restored = WithListStringView.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.Equal(new[] { "a", "b" }, restored[0].Items);
        Assert.Equal(new[] { "c" }, restored[1].Items);
    }

    [Fact]
    public void DictStringViewKey_Schema_UsesStringViewKeyType()
    {
        var schema = WithDictStringViewKey.ArrowSchema;
        var mapType = Assert.IsType<MapType>(schema.FieldsList[0].DataType);
        Assert.IsType<StringViewType>(mapType.KeyField.DataType);
    }

    [Fact]
    public void DictStringViewKey_SingleRow_RoundTrip()
    {
        var obj = new WithDictStringViewKey
        {
            Mapping = new Dictionary<string, int> { ["alpha"] = 1, ["beta"] = 2 },
        };
        var batch = WithDictStringViewKey.ToRecordBatch(obj);
        var restored = WithDictStringViewKey.FromRecordBatch(batch);
        Assert.Equal(obj.Mapping, restored.Mapping);
    }

    [Fact]
    public void DictValueOverride_Schema_UsesStringViewValueType()
    {
        var schema = WithDictValueOverride.ArrowSchema;
        var mapType = Assert.IsType<MapType>(schema.FieldsList[0].DataType);
        Assert.IsType<StringType>(mapType.KeyField.DataType); // key stays normal
        Assert.IsType<StringViewType>(mapType.ValueField.DataType); // value overridden
    }

    [Fact]
    public void DictValueOverride_SingleRow_RoundTrip()
    {
        var obj = new WithDictValueOverride
        {
            Labels = new Dictionary<string, string> { ["key1"] = "val1", ["key2"] = "val2" },
        };
        var batch = WithDictValueOverride.ToRecordBatch(obj);
        var restored = WithDictValueOverride.FromRecordBatch(batch);
        Assert.Equal(obj.Labels, restored.Labels);
    }

    [Fact]
    public void Bool8_Schema_UsesBool8Type()
    {
        var schema = WithBool8.ArrowSchema;
        Assert.IsType<Bool8Type>(schema.FieldsList[0].DataType); // Bool8Value
        Assert.IsType<BooleanType>(schema.FieldsList[1].DataType); // NormalBool
        Assert.IsType<Bool8Type>(schema.FieldsList[2].DataType); // NullableBool8
    }

    [Fact]
    public void Bool8_SingleRow_RoundTrip()
    {
        var original = new WithBool8 { Bool8Value = true, NormalBool = false, NullableBool8 = true };
        var batch = WithBool8.ToRecordBatch(original);
        var restored = WithBool8.FromRecordBatch(batch);
        Assert.Equal(original, restored);
    }

    [Fact]
    public void Bool8_MultiRow_RoundTrip()
    {
        var items = new List<WithBool8>
        {
            new() { Bool8Value = true, NormalBool = false, NullableBool8 = null },
            new() { Bool8Value = false, NormalBool = true, NullableBool8 = true },
        };
        var batch = WithBool8.ToRecordBatch(items);
        var restored = WithBool8.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.True(restored[0].Bool8Value);
        Assert.False(restored[0].NormalBool);
        Assert.Null(restored[0].NullableBool8);
        Assert.False(restored[1].Bool8Value);
        Assert.True(restored[1].NormalBool);
        Assert.True(restored[1].NullableBool8);
    }

    [Fact]
    public void CustomConverter_Schema_UsesConverterType()
    {
        var schema = WithCustomConverter.ArrowSchema;
        Assert.IsType<StringType>(schema.FieldsList[1].DataType); // Origin — converter uses StringType
    }

    [Fact]
    public void CustomConverter_SingleRow_RoundTrip()
    {
        var original = new WithCustomConverter
        {
            Name = "route1",
            Origin = new Point2D { X = 1.5, Y = 2.5 },
            Destination = new Point2D { X = 3.0, Y = 4.0 },
        };
        var batch = WithCustomConverter.ToRecordBatch(original);
        var restored = WithCustomConverter.FromRecordBatch(batch);
        Assert.Equal("route1", restored.Name);
        Assert.Equal(1.5, restored.Origin.X);
        Assert.Equal(2.5, restored.Origin.Y);
        Assert.Equal(3.0, restored.Destination.X);
        Assert.Equal(4.0, restored.Destination.Y);
    }

    [Fact]
    public void CustomConverter_MultiRow_RoundTrip()
    {
        var items = new List<WithCustomConverter>
        {
            new() { Name = "a", Origin = new Point2D { X = 1, Y = 2 }, Destination = new Point2D { X = 3, Y = 4 } },
            new() { Name = "b", Origin = new Point2D { X = 5, Y = 6 }, Destination = new Point2D { X = 7, Y = 8 } },
        };
        var batch = WithCustomConverter.ToRecordBatch(items);
        var restored = WithCustomConverter.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.Equal(1, restored[0].Origin.X);
        Assert.Equal(6, restored[1].Origin.Y);
        Assert.Equal(7, restored[1].Destination.X);
    }
}

public class SchemaTests
{
    [Fact]
    public void Inner_Schema_HasCorrectFields()
    {
        var schema = Inner.ArrowSchema;
        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equal("X", schema.FieldsList[0].Name);
        Assert.Equal(Int32Type.Default, schema.FieldsList[0].DataType);
        Assert.Equal("Label", schema.FieldsList[1].Name);
        Assert.Equal(StringType.Default, schema.FieldsList[1].DataType);
    }

    [Fact]
    public void OptionalField_IsNullable()
    {
        var schema = WithOptional.ArrowSchema;
        Assert.False(schema.FieldsList[0].IsNullable); // Value
        Assert.True(schema.FieldsList[1].IsNullable);  // Extra
    }

    [Fact]
    public void Enum_IsDictionaryEncoded()
    {
        var schema = WithEnum.ArrowSchema;
        Assert.IsType<DictionaryType>(schema.FieldsList[0].DataType);
    }

    [Fact]
    public void NestedRecord_IsStruct()
    {
        var schema = Outer.ArrowSchema;
        Assert.IsType<StructType>(schema.FieldsList[0].DataType);
    }

    [Fact]
    public void List_IsListType()
    {
        var schema = WithList.ArrowSchema;
        Assert.IsType<ListType>(schema.FieldsList[0].DataType);
    }

    [Fact]
    public void Dict_IsMapType()
    {
        var schema = WithDict.ArrowSchema;
        Assert.IsType<MapType>(schema.FieldsList[0].DataType);
    }

    [Fact]
    public void Metadata_SchemaHasTypeMetadata()
    {
        var schema = WithMetadata.ArrowSchema;
        Assert.True(schema.HasMetadata);
        Assert.Equal("vgi-rpc-cs", schema.Metadata["source"]);
        Assert.Equal("1.0", schema.Metadata["version"]);
    }

    [Fact]
    public void Metadata_FieldHasMetadata()
    {
        var schema = WithMetadata.ArrowSchema;
        var distanceField = schema.FieldsList[0];
        Assert.Equal("Distance", distanceField.Name);
        Assert.True(distanceField.HasMetadata);
        Assert.Equal("meters", distanceField.Metadata["unit"]);
        Assert.Equal("Distance traveled", distanceField.Metadata["description"]);
    }

    [Fact]
    public void Metadata_FieldWithoutMetadata_HasNoMetadata()
    {
        var schema = WithMetadata.ArrowSchema;
        var labelField = schema.FieldsList[1];
        Assert.Equal("Label", labelField.Name);
        Assert.False(labelField.HasMetadata);
    }

    [Fact]
    public void Metadata_PreservedThroughIpc()
    {
        var obj = new WithMetadata { Distance = 42.5, Label = "test" };
        var bytes = obj.SerializeToBytes();
        var restored = ArrowSerializerExtensions.DeserializeFromBytes<WithMetadata>(bytes);
        Assert.Equal(obj.Distance, restored.Distance);
        Assert.Equal(obj.Label, restored.Label);
    }

    [Fact]
    public void Transient_ExcludedFromSchema()
    {
        var schema = WithTransient.ArrowSchema;
        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equal("Id", schema.FieldsList[0].Name);
        Assert.Equal("Name", schema.FieldsList[1].Name);
    }

    [Fact]
    public void Transient_RoundTrip_PreservesDefault()
    {
        var obj = new WithTransient { Id = 1, Computed = "custom", Name = "test" };
        var batch = WithTransient.ToRecordBatch(obj);
        var restored = WithTransient.FromRecordBatch(batch);
        Assert.Equal(1, restored.Id);
        Assert.Equal("test", restored.Name);
        Assert.Equal("default", restored.Computed); // transient field gets default, not "custom"
    }

    [Fact]
    public void Transient_MultiRow_RoundTrip()
    {
        var items = new List<WithTransient>
        {
            new() { Id = 1, Computed = "a", Name = "first" },
            new() { Id = 2, Computed = "b", Name = "second" },
        };
        var batch = WithTransient.ToRecordBatch(items);
        var restored = WithTransient.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.Equal(1, restored[0].Id);
        Assert.Equal("first", restored[0].Name);
        Assert.Equal("default", restored[0].Computed);
        Assert.Equal(2, restored[1].Id);
        Assert.Equal("second", restored[1].Name);
        Assert.Equal("default", restored[1].Computed);
    }
}

public class TimezoneAwarenessTests
{
    [Fact]
    public void WallClock_DateTime_PreservesRawValue()
    {
        // A local DateTime with no timezone override should preserve the raw value (wall clock)
        var local = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Local);
        var original = new WithWallClockTimestamp
        {
            WallDateTime = local,
            WallDateTimeOffset = new DateTimeOffset(2024, 6, 15, 14, 30, 0, TimeSpan.FromHours(5)),
            UtcDateTime = local,
            UtcDateTimeOffset = new DateTimeOffset(2024, 6, 15, 14, 30, 0, TimeSpan.FromHours(5)),
        };

        var batch = WithWallClockTimestamp.ToRecordBatch(original);
        var restored = WithWallClockTimestamp.FromRecordBatch(batch);

        // Wall clock: raw ticks preserved, no UTC conversion
        Assert.Equal(local.Ticks, restored.WallDateTime.Ticks);
        // Wall clock DateTimeOffset: wall-clock time preserved (14:30), offset stripped
        Assert.Equal(new DateTime(2024, 6, 15, 14, 30, 0).Ticks, restored.WallDateTimeOffset.DateTime.Ticks);

        // UTC: should be converted to UTC
        Assert.Equal(DateTimeKind.Utc, restored.UtcDateTime.Kind);
        Assert.Equal(local.ToUniversalTime(), restored.UtcDateTime);
        // UTC DateTimeOffset: converted to UTC (14:30 +05:00 = 09:30 UTC)
        Assert.Equal(TimeSpan.Zero, restored.UtcDateTimeOffset.Offset);
        Assert.Equal(new DateTimeOffset(2024, 6, 15, 9, 30, 0, TimeSpan.Zero), restored.UtcDateTimeOffset);
    }

    [Fact]
    public void WallClock_MultiRow_PreservesRawValues()
    {
        var items = new List<WithWallClockTimestamp>
        {
            new()
            {
                WallDateTime = new DateTime(2024, 1, 1, 10, 0, 0, DateTimeKind.Local),
                WallDateTimeOffset = new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.FromHours(3)),
                UtcDateTime = new DateTime(2024, 1, 1, 10, 0, 0, DateTimeKind.Local),
                UtcDateTimeOffset = new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.FromHours(3)),
            },
            new()
            {
                WallDateTime = new DateTime(2024, 6, 15, 20, 0, 0, DateTimeKind.Unspecified),
                WallDateTimeOffset = new DateTimeOffset(2024, 6, 15, 20, 0, 0, TimeSpan.FromHours(-5)),
                UtcDateTime = new DateTime(2024, 6, 15, 20, 0, 0, DateTimeKind.Utc),
                UtcDateTimeOffset = new DateTimeOffset(2024, 6, 15, 20, 0, 0, TimeSpan.FromHours(-5)),
            },
        };

        var batch = WithWallClockTimestamp.ToRecordBatch(items);
        var restored = WithWallClockTimestamp.ListFromRecordBatch(batch);

        Assert.Equal(2, restored.Count);

        // Row 0: wall clock preserves raw ticks
        Assert.Equal(new DateTime(2024, 1, 1, 10, 0, 0).Ticks, restored[0].WallDateTime.Ticks);
        Assert.Equal(new DateTime(2024, 1, 1, 10, 0, 0).Ticks, restored[0].WallDateTimeOffset.DateTime.Ticks);

        // Row 1: wall clock preserves raw ticks
        Assert.Equal(new DateTime(2024, 6, 15, 20, 0, 0).Ticks, restored[1].WallDateTime.Ticks);
        Assert.Equal(new DateTime(2024, 6, 15, 20, 0, 0).Ticks, restored[1].WallDateTimeOffset.DateTime.Ticks);

        // Row 1 UTC: DateTime 20:00 UTC stays 20:00 UTC, DateTimeOffset 20:00 -05:00 = 01:00+1 UTC
        Assert.Equal(DateTimeKind.Utc, restored[1].UtcDateTime.Kind);
        Assert.Equal(new DateTime(2024, 6, 15, 20, 0, 0, DateTimeKind.Utc), restored[1].UtcDateTime);
        Assert.Equal(new DateTimeOffset(2024, 6, 16, 1, 0, 0, TimeSpan.Zero), restored[1].UtcDateTimeOffset);
    }
}

public class TypeVariantTests
{
    [Fact]
    public void RecordStruct_RoundTrip()
    {
        var obj = new PointRecordStruct { X = 1.5, Y = 2.5 };
        var batch = PointRecordStruct.ToRecordBatch(obj);
        var restored = PointRecordStruct.FromRecordBatch(batch);
        Assert.Equal(obj.X, restored.X);
        Assert.Equal(obj.Y, restored.Y);
    }

    [Fact]
    public void RecordStruct_MultiRow_RoundTrip()
    {
        var items = new List<PointRecordStruct>
        {
            new() { X = 1.0, Y = 2.0 },
            new() { X = 3.0, Y = 4.0 },
        };
        var batch = PointRecordStruct.ToRecordBatch(items);
        var restored = PointRecordStruct.ListFromRecordBatch(batch);
        Assert.Equal(1.0, restored[0].X);
        Assert.Equal(4.0, restored[1].Y);
    }

    [Fact]
    public void Class_RoundTrip()
    {
        var obj = new SimpleClass { Id = 42, Name = "hello" };
        var batch = SimpleClass.ToRecordBatch(obj);
        var restored = SimpleClass.FromRecordBatch(batch);
        Assert.Equal(obj.Id, restored.Id);
        Assert.Equal(obj.Name, restored.Name);
    }

    [Fact]
    public void Class_MultiRow_RoundTrip()
    {
        var items = new List<SimpleClass>
        {
            new() { Id = 1, Name = "a" },
            new() { Id = 2, Name = "b" },
        };
        var batch = SimpleClass.ToRecordBatch(items);
        var restored = SimpleClass.ListFromRecordBatch(batch);
        Assert.Equal(1, restored[0].Id);
        Assert.Equal("b", restored[1].Name);
    }

    [Fact]
    public void Struct_RoundTrip()
    {
        var obj = new SimpleStruct { Value = 7, Label = "test" };
        var batch = SimpleStruct.ToRecordBatch(obj);
        var restored = SimpleStruct.FromRecordBatch(batch);
        Assert.Equal(obj.Value, restored.Value);
        Assert.Equal(obj.Label, restored.Label);
    }

    [Fact]
    public void Struct_MultiRow_RoundTrip()
    {
        var items = new List<SimpleStruct>
        {
            new() { Value = 1, Label = "x" },
            new() { Value = 2, Label = "y" },
        };
        var batch = SimpleStruct.ToRecordBatch(items);
        var restored = SimpleStruct.ListFromRecordBatch(batch);
        Assert.Equal(1, restored[0].Value);
        Assert.Equal("y", restored[1].Label);
    }

    [Fact]
    public void FieldStruct_RoundTrip()
    {
        var obj = new PointWithFields(3, 7);
        var batch = PointWithFields.ToRecordBatch(obj);
        var restored = PointWithFields.FromRecordBatch(batch);
        Assert.Equal(3, restored.X);
        Assert.Equal(7, restored.Y);
    }

    [Fact]
    public void FieldStruct_MultiRow_RoundTrip()
    {
        var items = new List<PointWithFields> { new(1, 2), new(3, 4) };
        var batch = PointWithFields.ToRecordBatch(items);
        var restored = PointWithFields.ListFromRecordBatch(batch);
        Assert.Equal(1, restored[0].X);
        Assert.Equal(4, restored[1].Y);
    }

    [Fact]
    public void FieldStruct_MultiCtor_RoundTrip()
    {
        var obj = new PointMultiCtor(5, 10);
        var batch = PointMultiCtor.ToRecordBatch(obj);
        var restored = PointMultiCtor.FromRecordBatch(batch);
        Assert.Equal(5, restored.X);
        Assert.Equal(10, restored.Y);
    }

    [Fact]
    public void FieldStruct_MixedFieldsAndProps_RoundTrip()
    {
        var obj = new MixedFieldsAndProps(42, "hello");
        var batch = MixedFieldsAndProps.ToRecordBatch(obj);
        var restored = MixedFieldsAndProps.FromRecordBatch(batch);
        Assert.Equal(42, restored.Id);
        Assert.Equal("hello", restored.Name);
    }

    [Fact]
    public void FieldStruct_ArrowFieldAttribute_RoundTrip()
    {
        var obj = new FieldWithArrowField(1.5, 2.5);
        var batch = FieldWithArrowField.ToRecordBatch(obj);
        // Verify schema uses overridden field names
        Assert.Equal("x_coord", batch.Schema.FieldsList[0].Name);
        Assert.Equal("y_coord", batch.Schema.FieldsList[1].Name);
        var restored = FieldWithArrowField.FromRecordBatch(batch);
        Assert.Equal(1.5, restored.X);
        Assert.Equal(2.5, restored.Y);
    }

    [Fact]
    public void FieldStruct_IpcRoundTrip()
    {
        var obj = new PointWithFields(99, -1);
        var bytes = obj.SerializeToBytes();
        var restored = ArrowSerializerExtensions.DeserializeFromBytes<PointWithFields>(bytes);
        Assert.Equal(99, restored.X);
        Assert.Equal(-1, restored.Y);
    }

    [Fact]
    public void Callback_OnBeforeSerialize_Called()
    {
        var obj = new WithCallback { Value = 5 };
        Assert.False(obj.BeforeSerializeCalled);
        var batch = WithCallback.ToRecordBatch(obj);
        Assert.True(obj.BeforeSerializeCalled);
    }

    [Fact]
    public void Callback_OnAfterDeserialize_Called()
    {
        var obj = new WithCallback { Value = 7 };
        var batch = WithCallback.ToRecordBatch(obj);
        var restored = WithCallback.FromRecordBatch(batch);
        Assert.True(restored.AfterDeserializeCalled);
        Assert.Equal(14, restored.ComputedDouble);
    }

    [Fact]
    public void Callback_MultiRow_OnBeforeSerialize_Called()
    {
        var items = new List<WithCallback>
        {
            new() { Value = 1 },
            new() { Value = 2 },
        };
        Assert.False(items[0].BeforeSerializeCalled);
        Assert.False(items[1].BeforeSerializeCalled);
        WithCallback.ToRecordBatch(items);
        Assert.True(items[0].BeforeSerializeCalled);
        Assert.True(items[1].BeforeSerializeCalled);
    }

    [Fact]
    public void Callback_MultiRow_OnAfterDeserialize_Called()
    {
        var items = new List<WithCallback>
        {
            new() { Value = 3 },
            new() { Value = 5 },
        };
        var batch = WithCallback.ToRecordBatch(items);
        var restored = WithCallback.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.True(restored[0].AfterDeserializeCalled);
        Assert.Equal(6, restored[0].ComputedDouble);
        Assert.True(restored[1].AfterDeserializeCalled);
        Assert.Equal(10, restored[1].ComputedDouble);
    }
}

public class IpcTests
{
    [Fact]
    public void SerializeToBytes_DeserializeFromBytes_RoundTrip()
    {
        var obj = new Inner { X = 42, Label = "hello" };
        var bytes = obj.SerializeToBytes();
        Assert.True(bytes.Length > 0);
        var restored = ArrowSerializerExtensions.DeserializeFromBytes<Inner>(bytes);
        Assert.Equal(obj, restored);
    }

    [Fact]
    public void SerializeToStream_DeserializeFromStream_RoundTrip()
    {
        var obj = new Inner { X = 99, Label = "stream" };
        using var ms = new MemoryStream();
        obj.SerializeToStream(ms);
        ms.Position = 0;
        var restored = ArrowSerializerExtensions.DeserializeFromStream<Inner>(ms);
        Assert.Equal(obj, restored);
    }
}

public class PolymorphicTests
{
    [Fact]
    public void Schema_HasDiscriminatorAndUnionFields()
    {
        var schema = Shape.ArrowSchema;
        Assert.Equal("$type", schema.FieldsList[0].Name);
        Assert.Equal(StringType.Default, schema.FieldsList[0].DataType);
        // Union fields: Radius, Width, Height — all nullable
        Assert.Equal(3, schema.FieldsList.Count - 1);
        Assert.True(schema.FieldsList[1].IsNullable);
        Assert.True(schema.FieldsList[2].IsNullable);
        Assert.True(schema.FieldsList[3].IsNullable);
    }

    [Fact]
    public void CustomDiscriminatorFieldName()
    {
        var schema = Message.ArrowSchema;
        Assert.Equal("kind", schema.FieldsList[0].Name);
    }

    [Fact]
    public void Circle_SingleRow_RoundTrip()
    {
        Shape original = new Circle { Radius = 5.0 };
        var batch = Shape.ToRecordBatch(original);
        var restored = Shape.FromRecordBatch(batch);
        var circle = Assert.IsType<Circle>(restored);
        Assert.Equal(5.0, circle.Radius);
    }

    [Fact]
    public void Rectangle_SingleRow_RoundTrip()
    {
        Shape original = new Rectangle { Width = 3.0, Height = 4.0 };
        var batch = Shape.ToRecordBatch(original);
        var restored = Shape.FromRecordBatch(batch);
        var rect = Assert.IsType<Rectangle>(restored);
        Assert.Equal(3.0, rect.Width);
        Assert.Equal(4.0, rect.Height);
    }

    [Fact]
    public void MultiRow_MixedTypes_RoundTrip()
    {
        var items = new List<Shape>
        {
            new Circle { Radius = 1.0 },
            new Rectangle { Width = 2.0, Height = 3.0 },
            new Circle { Radius = 4.5 },
        };
        var batch = Shape.ToRecordBatch(items);
        Assert.Equal(3, batch.Length);

        var restored = Shape.ListFromRecordBatch(batch);
        Assert.Equal(3, restored.Count);

        var c1 = Assert.IsType<Circle>(restored[0]);
        Assert.Equal(1.0, c1.Radius);

        var r1 = Assert.IsType<Rectangle>(restored[1]);
        Assert.Equal(2.0, r1.Width);
        Assert.Equal(3.0, r1.Height);

        var c2 = Assert.IsType<Circle>(restored[2]);
        Assert.Equal(4.5, c2.Radius);
    }

    [Fact]
    public void Message_TextMessage_RoundTrip()
    {
        Message original = new TextMessage { Content = "hello" };
        var batch = Message.ToRecordBatch(original);
        var restored = Message.FromRecordBatch(batch);
        var text = Assert.IsType<TextMessage>(restored);
        Assert.Equal("hello", text.Content);
    }

    [Fact]
    public void Message_ImageMessage_RoundTrip()
    {
        Message original = new ImageMessage { Url = "https://example.com/img.png", Width = 800, Height = 600 };
        var batch = Message.ToRecordBatch(original);
        var restored = Message.FromRecordBatch(batch);
        var img = Assert.IsType<ImageMessage>(restored);
        Assert.Equal("https://example.com/img.png", img.Url);
        Assert.Equal(800, img.Width);
        Assert.Equal(600, img.Height);
    }

    [Fact]
    public void Message_MultiRow_RoundTrip()
    {
        var items = new List<Message>
        {
            new TextMessage { Content = "hi" },
            new ImageMessage { Url = "pic.jpg", Width = 100, Height = 200 },
            new TextMessage { Content = "bye" },
        };
        var batch = Message.ToRecordBatch(items);
        var restored = Message.ListFromRecordBatch(batch);
        Assert.Equal(3, restored.Count);

        Assert.IsType<TextMessage>(restored[0]);
        Assert.IsType<ImageMessage>(restored[1]);
        Assert.IsType<TextMessage>(restored[2]);
        Assert.Equal("hi", ((TextMessage)restored[0]).Content);
        Assert.Equal("pic.jpg", ((ImageMessage)restored[1]).Url);
        Assert.Equal("bye", ((TextMessage)restored[2]).Content);
    }

    // --- Collection tests for newly supported element types ---

    [Fact]
    public void ByteList_RoundTrip()
    {
        var obj = new WithByteList { Values = new List<byte> { 0, 127, 255 } };
        var restored = WithByteList.FromRecordBatch(WithByteList.ToRecordBatch(obj));
        Assert.Equal(obj.Values, restored.Values);
    }

    [Fact]
    public void ByteList_MultiRow_RoundTrip()
    {
        var items = new List<WithByteList>
        {
            new() { Values = new List<byte> { 1, 2, 3 } },
            new() { Values = new List<byte> { 4, 5 } },
            new() { Values = new List<byte>() },
        };
        var batch = WithByteList.ToRecordBatch(items);
        var restored = WithByteList.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Values, restored[0].Values);
        Assert.Equal(items[1].Values, restored[1].Values);
        Assert.Empty(restored[2].Values);
    }

    [Fact]
    public void DateTimeList_RoundTrip()
    {
        var now = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var obj = new WithDateTimeList { Timestamps = new List<DateTime> { now, now.AddHours(1) } };
        var restored = WithDateTimeList.FromRecordBatch(WithDateTimeList.ToRecordBatch(obj));
        Assert.Equal(obj.Timestamps.Count, restored.Timestamps.Count);
        Assert.Equal(obj.Timestamps[0], restored.Timestamps[0]);
        Assert.Equal(obj.Timestamps[1], restored.Timestamps[1]);
    }

    [Fact]
    public void DateTimeList_MultiRow_RoundTrip()
    {
        var dt1 = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var dt2 = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var items = new List<WithDateTimeList>
        {
            new() { Timestamps = new List<DateTime> { dt1 } },
            new() { Timestamps = new List<DateTime> { dt1, dt2 } },
        };
        var batch = WithDateTimeList.ToRecordBatch(items);
        var restored = WithDateTimeList.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Timestamps, restored[0].Timestamps);
        Assert.Equal(items[1].Timestamps, restored[1].Timestamps);
    }

    [Fact]
    public void DecimalList_RoundTrip()
    {
        var obj = new WithDecimalList { Amounts = new List<decimal> { 1.23m, 456.789m, 0m } };
        var restored = WithDecimalList.FromRecordBatch(WithDecimalList.ToRecordBatch(obj));
        Assert.Equal(obj.Amounts, restored.Amounts);
    }

    [Fact]
    public void DecimalList_MultiRow_RoundTrip()
    {
        var items = new List<WithDecimalList>
        {
            new() { Amounts = new List<decimal> { 1.1m, 2.2m } },
            new() { Amounts = new List<decimal> { 3.3m } },
        };
        var batch = WithDecimalList.ToRecordBatch(items);
        var restored = WithDecimalList.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Amounts, restored[0].Amounts);
        Assert.Equal(items[1].Amounts, restored[1].Amounts);
    }

    [Fact]
    public void GuidList_RoundTrip()
    {
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();
        var obj = new WithGuidList { Ids = new List<Guid> { g1, g2 } };
        var restored = WithGuidList.FromRecordBatch(WithGuidList.ToRecordBatch(obj));
        Assert.Equal(obj.Ids, restored.Ids);
    }

    [Fact]
    public void GuidList_MultiRow_RoundTrip()
    {
        var items = new List<WithGuidList>
        {
            new() { Ids = new List<Guid> { Guid.NewGuid(), Guid.NewGuid() } },
            new() { Ids = new List<Guid> { Guid.NewGuid() } },
        };
        var batch = WithGuidList.ToRecordBatch(items);
        var restored = WithGuidList.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Ids, restored[0].Ids);
        Assert.Equal(items[1].Ids, restored[1].Ids);
    }

    [Fact]
    public void BinaryList_RoundTrip()
    {
        var obj = new WithBinaryList { Blobs = new List<byte[]> { new byte[] { 1, 2 }, new byte[] { 3, 4, 5 } } };
        var restored = WithBinaryList.FromRecordBatch(WithBinaryList.ToRecordBatch(obj));
        Assert.Equal(obj.Blobs.Count, restored.Blobs.Count);
        Assert.Equal(obj.Blobs[0], restored.Blobs[0]);
        Assert.Equal(obj.Blobs[1], restored.Blobs[1]);
    }

    [Fact]
    public void BinaryList_MultiRow_RoundTrip()
    {
        var items = new List<WithBinaryList>
        {
            new() { Blobs = new List<byte[]> { new byte[] { 0xFF } } },
            new() { Blobs = new List<byte[]> { new byte[] { 0x00, 0x01 }, new byte[] { 0x02 } } },
        };
        var batch = WithBinaryList.ToRecordBatch(items);
        var restored = WithBinaryList.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Blobs[0], restored[0].Blobs[0]);
        Assert.Equal(items[1].Blobs[0], restored[1].Blobs[0]);
        Assert.Equal(items[1].Blobs[1], restored[1].Blobs[1]);
    }

    [Fact]
    public void IntKeyDict_RoundTrip()
    {
        var obj = new WithIntKeyDict { Lookup = new Dictionary<int, string> { [1] = "one", [2] = "two" } };
        var restored = WithIntKeyDict.FromRecordBatch(WithIntKeyDict.ToRecordBatch(obj));
        Assert.Equal(obj.Lookup, restored.Lookup);
    }

    [Fact]
    public void IntKeyDict_MultiRow_RoundTrip()
    {
        var items = new List<WithIntKeyDict>
        {
            new() { Lookup = new Dictionary<int, string> { [10] = "ten" } },
            new() { Lookup = new Dictionary<int, string> { [20] = "twenty", [30] = "thirty" } },
        };
        var batch = WithIntKeyDict.ToRecordBatch(items);
        var restored = WithIntKeyDict.ListFromRecordBatch(batch);
        Assert.Equal(items[0].Lookup, restored[0].Lookup);
        Assert.Equal(items[1].Lookup, restored[1].Lookup);
    }

    [Fact]
    public void DateTimeOffsetList_RoundTrip()
    {
        var dto1 = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.Zero);
        var dto2 = new DateTimeOffset(2025, 12, 25, 0, 0, 0, TimeSpan.Zero);
        var obj = new WithDateTimeOffsetList { Timestamps = new List<DateTimeOffset> { dto1, dto2 } };
        var restored = WithDateTimeOffsetList.FromRecordBatch(WithDateTimeOffsetList.ToRecordBatch(obj));
        Assert.Equal(obj.Timestamps, restored.Timestamps);
    }

    [Fact]
    public void DateOnlyList_RoundTrip()
    {
        var obj = new WithDateOnlyList { Dates = new List<DateOnly> { new DateOnly(2025, 1, 1), new DateOnly(2025, 12, 31) } };
        var restored = WithDateOnlyList.FromRecordBatch(WithDateOnlyList.ToRecordBatch(obj));
        Assert.Equal(obj.Dates, restored.Dates);
    }

    [Fact]
    public void TimeOnlyList_RoundTrip()
    {
        var obj = new WithTimeOnlyList { Times = new List<TimeOnly> { new TimeOnly(10, 30, 0), new TimeOnly(23, 59, 59) } };
        var restored = WithTimeOnlyList.FromRecordBatch(WithTimeOnlyList.ToRecordBatch(obj));
        Assert.Equal(obj.Times, restored.Times);
    }

    [Fact]
    public void TimeSpanList_RoundTrip()
    {
        var obj = new WithTimeSpanList { Durations = new List<TimeSpan> { TimeSpan.FromHours(1), TimeSpan.FromMinutes(30) } };
        var restored = WithTimeSpanList.FromRecordBatch(WithTimeSpanList.ToRecordBatch(obj));
        Assert.Equal(obj.Durations, restored.Durations);
    }

    [Fact]
    public void HalfList_RoundTrip()
    {
        var obj = new WithHalfList { Values = new List<Half> { (Half)1.5f, (Half)2.25f } };
        var restored = WithHalfList.FromRecordBatch(WithHalfList.ToRecordBatch(obj));
        Assert.Equal(obj.Values, restored.Values);
    }

    [Fact]
    public void AllPrimitiveLists_RoundTrip()
    {
        var obj = new WithAllPrimitiveLists
        {
            SBytes = new List<sbyte> { -128, 0, 127 },
            Shorts = new List<short> { -32768, 0, 32767 },
            UShorts = new List<ushort> { 0, 100, 65535 },
            UInts = new List<uint> { 0, 42, uint.MaxValue },
            ULongs = new List<ulong> { 0, 99, ulong.MaxValue },
        };
        var restored = WithAllPrimitiveLists.FromRecordBatch(WithAllPrimitiveLists.ToRecordBatch(obj));
        Assert.Equal(obj.SBytes, restored.SBytes);
        Assert.Equal(obj.Shorts, restored.Shorts);
        Assert.Equal(obj.UShorts, restored.UShorts);
        Assert.Equal(obj.UInts, restored.UInts);
        Assert.Equal(obj.ULongs, restored.ULongs);
    }

    [Fact]
    public void AllPrimitiveLists_MultiRow_RoundTrip()
    {
        var items = new List<WithAllPrimitiveLists>
        {
            new() { SBytes = new List<sbyte> { 1 }, Shorts = new List<short> { 2 }, UShorts = new List<ushort> { 3 }, UInts = new List<uint> { 4 }, ULongs = new List<ulong> { 5 } },
            new() { SBytes = new List<sbyte> { -1 }, Shorts = new List<short> { -2 }, UShorts = new List<ushort> { 0 }, UInts = new List<uint> { 0 }, ULongs = new List<ulong> { 0 } },
        };
        var batch = WithAllPrimitiveLists.ToRecordBatch(items);
        var restored = WithAllPrimitiveLists.ListFromRecordBatch(batch);
        Assert.Equal(items[0].SBytes, restored[0].SBytes);
        Assert.Equal(items[1].Shorts, restored[1].Shorts);
        Assert.Equal(items[0].UShorts, restored[0].UShorts);
        Assert.Equal(items[1].UInts, restored[1].UInts);
        Assert.Equal(items[0].ULongs, restored[0].ULongs);
    }

    // --- Deeply nested collection tests ---

    [Fact]
    public void ListOfDicts_SingleRow_RoundTrip()
    {
        var original = new WithListOfDicts
        {
            Items =
            [
                new Dictionary<string, long> { ["a"] = 1, ["b"] = 2 },
                new Dictionary<string, long> { ["c"] = 3 },
            ],
        };
        var batch = WithListOfDicts.ToRecordBatch(original);
        var restored = WithListOfDicts.FromRecordBatch(batch);
        Assert.Equal(2, restored.Items.Count);
        Assert.Equal(1, restored.Items[0]["a"]);
        Assert.Equal(2, restored.Items[0]["b"]);
        Assert.Equal(3, restored.Items[1]["c"]);
    }

    [Fact]
    public void ListOfDicts_MultiRow_RoundTrip()
    {
        var items = new List<WithListOfDicts>
        {
            new() { Items = [new Dictionary<string, long> { ["x"] = 10 }] },
            new() { Items = [new Dictionary<string, long> { ["y"] = 20 }, new Dictionary<string, long> { ["z"] = 30 }] },
            new() { Items = [] },
        };
        var batch = WithListOfDicts.ToRecordBatch(items);
        var restored = WithListOfDicts.ListFromRecordBatch(batch);
        Assert.Single(restored[0].Items);
        Assert.Equal(10, restored[0].Items[0]["x"]);
        Assert.Equal(2, restored[1].Items.Count);
        Assert.Equal(20, restored[1].Items[0]["y"]);
        Assert.Equal(30, restored[1].Items[1]["z"]);
        Assert.Empty(restored[2].Items);
    }

    [Fact]
    public void ListOfDicts_IPC_RoundTrip()
    {
        var original = new WithListOfDicts
        {
            Items = [new Dictionary<string, long> { ["key"] = 42 }],
        };
        var bytes = original.SerializeToBytes();
        var restored = ArrowSerializerExtensions.DeserializeFromBytes<WithListOfDicts>(bytes);
        Assert.Single(restored.Items);
        Assert.Equal(42, restored.Items[0]["key"]);
    }

    [Fact]
    public void DictOfLists_SingleRow_RoundTrip()
    {
        var original = new WithDictOfLists
        {
            Groups = new Dictionary<string, List<long>>
            {
                ["evens"] = [2, 4, 6],
                ["odds"] = [1, 3, 5],
            },
        };
        var batch = WithDictOfLists.ToRecordBatch(original);
        var restored = WithDictOfLists.FromRecordBatch(batch);
        Assert.Equal(2, restored.Groups.Count);
        Assert.Equal([2, 4, 6], restored.Groups["evens"]);
        Assert.Equal([1, 3, 5], restored.Groups["odds"]);
    }

    [Fact]
    public void DictOfLists_MultiRow_RoundTrip()
    {
        var items = new List<WithDictOfLists>
        {
            new() { Groups = new() { ["a"] = [1, 2] } },
            new() { Groups = new() { ["b"] = [3], ["c"] = [4, 5, 6] } },
        };
        var batch = WithDictOfLists.ToRecordBatch(items);
        var restored = WithDictOfLists.ListFromRecordBatch(batch);
        Assert.Equal([1, 2], restored[0].Groups["a"]);
        Assert.Equal([3], restored[1].Groups["b"]);
        Assert.Equal([4, 5, 6], restored[1].Groups["c"]);
    }

    [Fact]
    public void DictOfLists_IPC_RoundTrip()
    {
        var original = new WithDictOfLists
        {
            Groups = new() { ["nums"] = [7, 8, 9] },
        };
        var bytes = original.SerializeToBytes();
        var restored = ArrowSerializerExtensions.DeserializeFromBytes<WithDictOfLists>(bytes);
        Assert.Equal([7, 8, 9], restored.Groups["nums"]);
    }

    [Fact]
    public void NestedDicts_SingleRow_RoundTrip()
    {
        var original = new WithNestedDicts
        {
            Nested = new()
            {
                ["group1"] = new() { ["a"] = 1, ["b"] = 2 },
                ["group2"] = new() { ["c"] = 3 },
            },
        };
        var batch = WithNestedDicts.ToRecordBatch(original);
        var restored = WithNestedDicts.FromRecordBatch(batch);
        Assert.Equal(2, restored.Nested.Count);
        Assert.Equal(1, restored.Nested["group1"]["a"]);
        Assert.Equal(2, restored.Nested["group1"]["b"]);
        Assert.Equal(3, restored.Nested["group2"]["c"]);
    }

    [Fact]
    public void NestedDicts_MultiRow_RoundTrip()
    {
        var items = new List<WithNestedDicts>
        {
            new() { Nested = new() { ["x"] = new() { ["k"] = 10 } } },
            new() { Nested = new() { ["y"] = new() { ["m"] = 20, ["n"] = 30 } } },
        };
        var batch = WithNestedDicts.ToRecordBatch(items);
        var restored = WithNestedDicts.ListFromRecordBatch(batch);
        Assert.Equal(10, restored[0].Nested["x"]["k"]);
        Assert.Equal(20, restored[1].Nested["y"]["m"]);
        Assert.Equal(30, restored[1].Nested["y"]["n"]);
    }

    [Fact]
    public void NestedDicts_IPC_RoundTrip()
    {
        var original = new WithNestedDicts
        {
            Nested = new() { ["g"] = new() { ["v"] = 99 } },
        };
        var bytes = original.SerializeToBytes();
        var restored = ArrowSerializerExtensions.DeserializeFromBytes<WithNestedDicts>(bytes);
        Assert.Equal(99, restored.Nested["g"]["v"]);
    }

    [Fact]
    public void ListOfListOfDicts_SingleRow_RoundTrip()
    {
        var original = new WithListOfListOfDicts
        {
            Matrix =
            [
                [new Dictionary<string, long> { ["a"] = 1 }, new Dictionary<string, long> { ["b"] = 2 }],
                [new Dictionary<string, long> { ["c"] = 3 }],
            ],
        };
        var batch = WithListOfListOfDicts.ToRecordBatch(original);
        var restored = WithListOfListOfDicts.FromRecordBatch(batch);
        Assert.Equal(2, restored.Matrix.Count);
        Assert.Equal(2, restored.Matrix[0].Count);
        Assert.Equal(1, restored.Matrix[0][0]["a"]);
        Assert.Equal(2, restored.Matrix[0][1]["b"]);
        Assert.Single(restored.Matrix[1]);
        Assert.Equal(3, restored.Matrix[1][0]["c"]);
    }

    [Fact]
    public void ListOfListOfDicts_MultiRow_RoundTrip()
    {
        var items = new List<WithListOfListOfDicts>
        {
            new() { Matrix = [[new Dictionary<string, long> { ["k"] = 1 }]] },
            new() { Matrix = [[new Dictionary<string, long> { ["m"] = 2 }], [new Dictionary<string, long> { ["n"] = 3 }]] },
        };
        var batch = WithListOfListOfDicts.ToRecordBatch(items);
        var restored = WithListOfListOfDicts.ListFromRecordBatch(batch);
        Assert.Equal(1, restored[0].Matrix[0][0]["k"]);
        Assert.Equal(2, restored[1].Matrix[0][0]["m"]);
        Assert.Equal(3, restored[1].Matrix[1][0]["n"]);
    }

    [Fact]
    public void DictOfDictOfLists_SingleRow_RoundTrip()
    {
        var original = new WithDictOfDictOfLists
        {
            Deep = new()
            {
                ["outer"] = new()
                {
                    ["inner"] = [10, 20, 30],
                },
            },
        };
        var batch = WithDictOfDictOfLists.ToRecordBatch(original);
        var restored = WithDictOfDictOfLists.FromRecordBatch(batch);
        Assert.Equal([10, 20, 30], restored.Deep["outer"]["inner"]);
    }

    [Fact]
    public void DictOfDictOfLists_MultiRow_RoundTrip()
    {
        var items = new List<WithDictOfDictOfLists>
        {
            new() { Deep = new() { ["a"] = new() { ["x"] = [1, 2] } } },
            new() { Deep = new() { ["b"] = new() { ["y"] = [3], ["z"] = [4, 5] } } },
        };
        var batch = WithDictOfDictOfLists.ToRecordBatch(items);
        var restored = WithDictOfDictOfLists.ListFromRecordBatch(batch);
        Assert.Equal([1, 2], restored[0].Deep["a"]["x"]);
        Assert.Equal([3], restored[1].Deep["b"]["y"]);
        Assert.Equal([4, 5], restored[1].Deep["b"]["z"]);
    }

    // --- Nullable Collection Tests ---

    [Fact]
    public void NullableList_SingleRow_NonNull()
    {
        var record = new WithNullableList { Values = [1, 2, 3] };
        var batch = WithNullableList.ToRecordBatch(record);
        var restored = WithNullableList.FromRecordBatch(batch);
        Assert.Equal([1, 2, 3], restored.Values);
    }

    [Fact]
    public void NullableList_SingleRow_Null()
    {
        var record = new WithNullableList { Values = null };
        var batch = WithNullableList.ToRecordBatch(record);
        var restored = WithNullableList.FromRecordBatch(batch);
        Assert.Null(restored.Values);
    }

    [Fact]
    public void NullableList_MultiRow()
    {
        var items = new List<WithNullableList>
        {
            new() { Values = [1, 2] },
            new() { Values = null },
            new() { Values = [3] },
        };
        var batch = WithNullableList.ToRecordBatch(items);
        var restored = WithNullableList.ListFromRecordBatch(batch);
        Assert.Equal(3, restored.Count);
        Assert.Equal([1, 2], restored[0].Values);
        Assert.Null(restored[1].Values);
        Assert.Equal([3], restored[2].Values);
    }

    [Fact]
    public void NullableDict_SingleRow_NonNull()
    {
        var record = new WithNullableDict { Mapping = new() { ["a"] = 1 } };
        var batch = WithNullableDict.ToRecordBatch(record);
        var restored = WithNullableDict.FromRecordBatch(batch);
        Assert.NotNull(restored.Mapping);
        Assert.Equal(1, restored.Mapping!["a"]);
    }

    [Fact]
    public void NullableDict_SingleRow_Null()
    {
        var record = new WithNullableDict { Mapping = null };
        var batch = WithNullableDict.ToRecordBatch(record);
        var restored = WithNullableDict.FromRecordBatch(batch);
        Assert.Null(restored.Mapping);
    }

    [Fact]
    public void NullableDict_MultiRow()
    {
        var items = new List<WithNullableDict>
        {
            new() { Mapping = new() { ["a"] = 1 } },
            new() { Mapping = null },
            new() { Mapping = new() { ["b"] = 2 } },
        };
        var batch = WithNullableDict.ToRecordBatch(items);
        var restored = WithNullableDict.ListFromRecordBatch(batch);
        Assert.Equal(3, restored.Count);
        Assert.Equal(1, restored[0].Mapping!["a"]);
        Assert.Null(restored[1].Mapping);
        Assert.Equal(2, restored[2].Mapping!["b"]);
    }

    [Fact]
    public void NullableListOfDicts_SingleRow_NonNull()
    {
        var record = new WithNullableListOfDicts { Items = [new() { ["x"] = 1L }] };
        var batch = WithNullableListOfDicts.ToRecordBatch(record);
        var restored = WithNullableListOfDicts.FromRecordBatch(batch);
        Assert.NotNull(restored.Items);
        Assert.Equal(1L, restored.Items![0]["x"]);
    }

    [Fact]
    public void NullableListOfDicts_SingleRow_Null()
    {
        var record = new WithNullableListOfDicts { Items = null };
        var batch = WithNullableListOfDicts.ToRecordBatch(record);
        var restored = WithNullableListOfDicts.FromRecordBatch(batch);
        Assert.Null(restored.Items);
    }

    [Fact]
    public void NullableListOfDicts_MultiRow()
    {
        var items = new List<WithNullableListOfDicts>
        {
            new() { Items = [new() { ["x"] = 1L }] },
            new() { Items = null },
            new() { Items = [new() { ["y"] = 2L }, new() { ["z"] = 3L }] },
        };
        var batch = WithNullableListOfDicts.ToRecordBatch(items);
        var restored = WithNullableListOfDicts.ListFromRecordBatch(batch);
        Assert.Equal(3, restored.Count);
        Assert.Equal(1L, restored[0].Items![0]["x"]);
        Assert.Null(restored[1].Items);
        Assert.Equal(2L, restored[2].Items![0]["y"]);
    }

    [Fact]
    public void NullableDictOfLists_SingleRow_NonNull()
    {
        var record = new WithNullableDictOfLists { Groups = new() { ["a"] = [1L, 2L] } };
        var batch = WithNullableDictOfLists.ToRecordBatch(record);
        var restored = WithNullableDictOfLists.FromRecordBatch(batch);
        Assert.NotNull(restored.Groups);
        Assert.Equal([1L, 2L], restored.Groups!["a"]);
    }

    [Fact]
    public void NullableDictOfLists_SingleRow_Null()
    {
        var record = new WithNullableDictOfLists { Groups = null };
        var batch = WithNullableDictOfLists.ToRecordBatch(record);
        var restored = WithNullableDictOfLists.FromRecordBatch(batch);
        Assert.Null(restored.Groups);
    }

    [Fact]
    public void NullableDictOfLists_MultiRow()
    {
        var items = new List<WithNullableDictOfLists>
        {
            new() { Groups = new() { ["a"] = [1L] } },
            new() { Groups = null },
            new() { Groups = new() { ["b"] = [2L, 3L] } },
        };
        var batch = WithNullableDictOfLists.ToRecordBatch(items);
        var restored = WithNullableDictOfLists.ListFromRecordBatch(batch);
        Assert.Equal(3, restored.Count);
        Assert.Equal([1L], restored[0].Groups!["a"]);
        Assert.Null(restored[1].Groups);
        Assert.Equal([2L, 3L], restored[2].Groups!["b"]);
    }

    // --- Dict with Guid values ---

    [Fact]
    public void WithDictOfGuids_SingleRow_RoundTrips()
    {
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();
        var record = new WithDictOfGuids { Items = new() { ["a"] = g1, ["b"] = g2 } };
        var batch = WithDictOfGuids.ToRecordBatch(record);
        var restored = WithDictOfGuids.FromRecordBatch(batch);
        Assert.Equal(g1, restored.Items["a"]);
        Assert.Equal(g2, restored.Items["b"]);
    }

    [Fact]
    public void WithDictOfGuids_MultiRow_RoundTrips()
    {
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();
        var items = new List<WithDictOfGuids>
        {
            new() { Items = new() { ["x"] = g1 } },
            new() { Items = new() { ["y"] = g2 } },
        };
        var batch = WithDictOfGuids.ToRecordBatch(items);
        var restored = WithDictOfGuids.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.Equal(g1, restored[0].Items["x"]);
        Assert.Equal(g2, restored[1].Items["y"]);
    }

    // --- Dict with Enum values ---

    [Fact]
    public void WithDictOfEnums_SingleRow_RoundTrips()
    {
        var record = new WithDictOfEnums { Items = new() { ["a"] = Color.Red, ["b"] = Color.Blue } };
        var batch = WithDictOfEnums.ToRecordBatch(record);
        var restored = WithDictOfEnums.FromRecordBatch(batch);
        Assert.Equal(Color.Red, restored.Items["a"]);
        Assert.Equal(Color.Blue, restored.Items["b"]);
    }

    [Fact]
    public void WithDictOfEnums_MultiRow_RoundTrips()
    {
        var items = new List<WithDictOfEnums>
        {
            new() { Items = new() { ["x"] = Color.Green } },
            new() { Items = new() { ["y"] = Color.Red, ["z"] = Color.Blue } },
        };
        var batch = WithDictOfEnums.ToRecordBatch(items);
        var restored = WithDictOfEnums.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.Equal(Color.Green, restored[0].Items["x"]);
        Assert.Equal(Color.Red, restored[1].Items["y"]);
        Assert.Equal(Color.Blue, restored[1].Items["z"]);
    }

    // --- Dict with NestedRecord values ---

    [Fact]
    public void WithDictOfNested_SingleRow_RoundTrips()
    {
        var record = new WithDictOfNested
        {
            Items = new()
            {
                ["a"] = new Inner { X = 1, Label = "one" },
                ["b"] = new Inner { X = 2, Label = "two" },
            }
        };
        var batch = WithDictOfNested.ToRecordBatch(record);
        var restored = WithDictOfNested.FromRecordBatch(batch);
        Assert.Equal(1, restored.Items["a"].X);
        Assert.Equal("one", restored.Items["a"].Label);
        Assert.Equal(2, restored.Items["b"].X);
        Assert.Equal("two", restored.Items["b"].Label);
    }

    [Fact]
    public void WithDictOfNested_MultiRow_RoundTrips()
    {
        var items = new List<WithDictOfNested>
        {
            new() { Items = new() { ["x"] = new Inner { X = 10, Label = "ten" } } },
            new() { Items = new() { ["y"] = new Inner { X = 20, Label = "twenty" }, ["z"] = new Inner { X = 30, Label = "thirty" } } },
        };
        var batch = WithDictOfNested.ToRecordBatch(items);
        var restored = WithDictOfNested.ListFromRecordBatch(batch);
        Assert.Equal(2, restored.Count);
        Assert.Equal(10, restored[0].Items["x"].X);
        Assert.Equal("ten", restored[0].Items["x"].Label);
        Assert.Equal(20, restored[1].Items["y"].X);
        Assert.Equal(30, restored[1].Items["z"].X);
    }

    [Fact]
    public void WithDictOfNested_EmptyDict_RoundTrips()
    {
        var record = new WithDictOfNested { Items = new() };
        var batch = WithDictOfNested.ToRecordBatch(record);
        var restored = WithDictOfNested.FromRecordBatch(batch);
        Assert.Empty(restored.Items);
    }
}
