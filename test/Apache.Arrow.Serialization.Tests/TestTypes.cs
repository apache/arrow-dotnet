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

using Apache.Arrow.Serialization;

namespace Apache.Arrow.Serialization.Tests;

public enum Color
{
    Red,
    Green,
    Blue,
}

[ArrowSerializable]
public partial record Inner
{
    public int X { get; init; }
    public string Label { get; init; } = "";
}

[ArrowSerializable]
public partial record Outer
{
    public Inner Inner { get; init; } = new();
    public string Name { get; init; } = "";
}

[ArrowSerializable]
public partial record WithPrimitives
{
    public string Name { get; init; } = "";
    public int Count { get; init; }
    public long BigCount { get; init; }
    public float Score { get; init; }
    public double Precise { get; init; }
    public bool Flag { get; init; }
    public byte Small { get; init; }
}

[ArrowSerializable]
public partial record WithOptional
{
    public string Value { get; init; } = "";
    public string? Extra { get; init; }
}

[ArrowSerializable]
public partial record WithOptionalInt
{
    public int Value { get; init; }
    public int? Extra { get; init; }
}

[ArrowSerializable]
public partial record WithEnum
{
    public Color Color { get; init; }
}

[ArrowSerializable]
public partial record WithList
{
    public List<int> Items { get; init; } = new();
}

[ArrowSerializable]
public partial record WithStringList
{
    public List<string> Tags { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDict
{
    public Dictionary<string, int> Mapping { get; init; } = new();
}

[ArrowSerializable]
public partial record WithSet
{
    public HashSet<int> Tags { get; init; } = new();
}

[ArrowSerializable]
public partial record WithBytes
{
    public byte[] Data { get; init; } = System.Array.Empty<byte>();
}

[ArrowSerializable]
public partial record WithReadOnlyMemoryBytes
{
    public ReadOnlyMemory<byte> Data { get; init; }
}

[ArrowSerializable]
public partial record WithBool
{
    public bool Flag { get; init; }
}

[ArrowSerializable]
public partial record WithOptionalNested
{
    public Inner? Inner { get; init; }
}

[ArrowSerializable]
public partial record WithNullableEnum
{
    public Color? Color { get; init; }
}

[ArrowSerializable]
public partial record Level2
{
    public Inner Inner { get; init; } = new();
    public string Tag { get; init; } = "";
}

[ArrowSerializable]
public partial record Level3
{
    public Level2 Level2 { get; init; } = new();
    public double Score { get; init; }
}

[ArrowSerializable]
public partial record WithEnumList
{
    public List<string> Colors { get; init; } = new();
}

[ArrowSerializable]
public partial record WithColorEnumList
{
    public List<Color> Colors { get; init; } = new();
}

[ArrowSerializable]
public partial record WithNestedList
{
    public List<Inner> Entries { get; init; } = new();
}

[ArrowSerializable]
public partial record WithArrowTypeOverride
{
    [ArrowType("int64")]
    public int Value { get; init; }
}

[ArrowSerializable]
public partial record WithDateTimeTypes
{
    public DateTime Timestamp { get; init; }
    public DateTimeOffset TimestampOffset { get; init; }
    public DateOnly Date { get; init; }
    public TimeOnly Time { get; init; }
    public TimeSpan Duration { get; init; }
}

[ArrowSerializable]
public partial record WithDecimalAndGuid
{
    public decimal Amount { get; init; }
    public Guid Id { get; init; }
}

[ArrowSerializable]
public partial record WithHalf
{
    public Half Value { get; init; }
}

[ArrowSerializable]
public partial record WithNullableDateTimeTypes
{
    public DateTime? Timestamp { get; init; }
    public DateOnly? Date { get; init; }
    public TimeOnly? Time { get; init; }
    public TimeSpan? Duration { get; init; }
    public decimal? Amount { get; init; }
    public Guid? Id { get; init; }
    public Half? HalfVal { get; init; }
}

[ArrowSerializable]
public partial record WithTimestampOverride
{
    [ArrowType("timestamp[ns, UTC]")]
    public DateTimeOffset Value { get; init; }

    [ArrowType("timestamp[ms, UTC]")]
    public DateTime DateTimeValue { get; init; }
}

[ArrowSerializable]
public partial record WithTimeOverride
{
    [ArrowType("time32[ms]")]
    public TimeOnly Millis { get; init; }

    [ArrowType("time64[ns]")]
    public TimeOnly Nanos { get; init; }
}

[ArrowSerializable]
public partial record WithDecimalOverride
{
    [ArrowType("decimal128(28, 10)")]
    public decimal Value { get; init; }
}

[ArrowSerializable]
[ArrowMetadata("source", "vgi-rpc-cs")]
[ArrowMetadata("version", "1.0")]
public partial record WithMetadata
{
    [ArrowMetadata("unit", "meters")]
    [ArrowMetadata("description", "Distance traveled")]
    public double Distance { get; init; }

    public string Label { get; init; } = "";
}

[ArrowSerializable]
public partial record WithTransient
{
    public int Id { get; init; }

    [ArrowIgnore]
    public string Computed { get; init; } = "default";

    public string Name { get; init; } = "";
}

[ArrowSerializable]
public partial record struct PointRecordStruct
{
    public double X { get; init; }
    public double Y { get; init; }
}

[ArrowSerializable]
public partial class SimpleClass
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
}

[ArrowSerializable]
public partial struct SimpleStruct
{
    public int Value { get; set; }
    public string Label { get; set; }
}

[ArrowSerializable]
public partial record WithWallClockTimestamp
{
    [ArrowType("timestamp[us]")]
    public DateTime WallDateTime { get; init; }

    [ArrowType("timestamp[us]")]
    public DateTimeOffset WallDateTimeOffset { get; init; }

    [ArrowType("timestamp[us, UTC]")]
    public DateTime UtcDateTime { get; init; }

    [ArrowType("timestamp[us, UTC]")]
    public DateTimeOffset UtcDateTimeOffset { get; init; }
}

[ArrowSerializable]
public partial record WithDateOverride
{
    public DateOnly Default { get; init; }

    [ArrowType("date64")]
    public DateOnly Date64Value { get; init; }

    [ArrowType("date32")]
    public DateOnly Date32Value { get; init; }
}

[ArrowSerializable]
public partial record WithBool8
{
    [ArrowType("bool8")]
    public bool Bool8Value { get; init; }

    public bool NormalBool { get; init; }

    [ArrowType("bool8")]
    public bool? NullableBool8 { get; init; }
}

[ArrowSerializable]
public partial record WithViewTypes
{
    [ArrowType("string_view")]
    public string ViewString { get; init; } = "";

    [ArrowType("binary_view")]
    public byte[] ViewBinary { get; init; } = System.Array.Empty<byte>();

    public string NormalString { get; init; } = "";
    public byte[] NormalBinary { get; init; } = System.Array.Empty<byte>();
}

// --- Collection element type override test types ---

[ArrowSerializable]
public partial record WithListStringView
{
    [ArrowType(ElementType = "string_view")]
    public List<string> Items { get; init; } = new();

    public List<string> NormalItems { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDictStringViewKey
{
    [ArrowType(KeyType = "string_view")]
    public Dictionary<string, int> Mapping { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDictValueOverride
{
    [ArrowType(ValueType = "string_view")]
    public Dictionary<string, string> Labels { get; init; } = new();
}

// --- Custom converter test types ---

public struct Point2D
{
    public double X { get; set; }
    public double Y { get; set; }
}

public class Point2DArrowConverter : IArrowConverter<Point2D>
{
    public Apache.Arrow.Types.IArrowType ArrowType => Apache.Arrow.Types.StringType.Default;

    public Apache.Arrow.IArrowArray ToArray(Point2D value)
    {
        return new Apache.Arrow.StringArray.Builder().Append(string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{value.X},{value.Y}")).Build();
    }

    public Apache.Arrow.IArrowArray ToArray(IReadOnlyList<Point2D> values)
    {
        var b = new Apache.Arrow.StringArray.Builder();
        foreach (var v in values) b.Append(string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{v.X},{v.Y}"));
        return b.Build();
    }

    public Point2D FromArray(Apache.Arrow.IArrowArray array, int index)
    {
        var str = ((Apache.Arrow.StringArray)array).GetString(index)!;
        var parts = str.Split(',');
        return new Point2D
        {
            X = double.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture),
            Y = double.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture),
        };
    }
}

[ArrowSerializable]
public partial record WithCustomConverter
{
    public string Name { get; init; } = "";

    [ArrowType(Converter = typeof(Point2DArrowConverter))]
    public Point2D Origin { get; init; }

    [ArrowType(Converter = typeof(Point2DArrowConverter))]
    public Point2D Destination { get; init; }
}

// --- Polymorphic test types ---

[ArrowPolymorphic]
[ArrowDerivedType(typeof(Circle), "circle")]
[ArrowDerivedType(typeof(Rectangle), "rectangle")]
public abstract partial record Shape;

[ArrowSerializable]
public partial record Circle : Shape
{
    public double Radius { get; init; }
}

[ArrowSerializable]
public partial record Rectangle : Shape
{
    public double Width { get; init; }
    public double Height { get; init; }
}

[ArrowPolymorphic(TypeDiscriminatorFieldName = "kind")]
[ArrowDerivedType(typeof(TextMessage), "text")]
[ArrowDerivedType(typeof(ImageMessage), "image")]
public abstract partial record Message;

[ArrowSerializable]
public partial record TextMessage : Message
{
    public string Content { get; init; } = "";
}

[ArrowSerializable]
public partial record ImageMessage : Message
{
    public string Url { get; init; } = "";
    public int Width { get; init; }
    public int Height { get; init; }
}

// --- Collection types with various element types ---

[ArrowSerializable]
public partial record WithByteList
{
    public List<byte> Values { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDateTimeList
{
    public List<DateTime> Timestamps { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDecimalList
{
    public List<decimal> Amounts { get; init; } = new();
}

[ArrowSerializable]
public partial record WithGuidList
{
    public List<Guid> Ids { get; init; } = new();
}

[ArrowSerializable]
public partial record WithBinaryList
{
    public List<byte[]> Blobs { get; init; } = new();
}

[ArrowSerializable]
public partial record WithIntKeyDict
{
    public Dictionary<int, string> Lookup { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDateTimeOffsetList
{
    public List<DateTimeOffset> Timestamps { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDateOnlyList
{
    public List<DateOnly> Dates { get; init; } = new();
}

[ArrowSerializable]
public partial record WithTimeOnlyList
{
    public List<TimeOnly> Times { get; init; } = new();
}

[ArrowSerializable]
public partial record WithTimeSpanList
{
    public List<TimeSpan> Durations { get; init; } = new();
}

[ArrowSerializable]
public partial record WithHalfList
{
    public List<Half> Values { get; init; } = new();
}

[ArrowSerializable]
public partial record WithAllPrimitiveLists
{
    public List<sbyte> SBytes { get; init; } = new();
    public List<short> Shorts { get; init; } = new();
    public List<ushort> UShorts { get; init; } = new();
    public List<uint> UInts { get; init; } = new();
    public List<ulong> ULongs { get; init; } = new();
}

// --- Deeply nested collection types ---

[ArrowSerializable]
public partial record WithListOfDicts
{
    public List<Dictionary<string, long>> Items { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDictOfLists
{
    public Dictionary<string, List<long>> Groups { get; init; } = new();
}

[ArrowSerializable]
public partial record WithNestedDicts
{
    public Dictionary<string, Dictionary<string, long>> Nested { get; init; } = new();
}

[ArrowSerializable]
public partial record WithListOfListOfDicts
{
    public List<List<Dictionary<string, long>>> Matrix { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDictOfDictOfLists
{
    public Dictionary<string, Dictionary<string, List<long>>> Deep { get; init; } = new();
}

// --- Nullable collection types ---

[ArrowSerializable]
public partial record WithNullableList
{
    public List<int>? Values { get; init; }
}

[ArrowSerializable]
public partial record WithNullableDict
{
    public Dictionary<string, int>? Mapping { get; init; }
}

[ArrowSerializable]
public partial record WithNullableListOfDicts
{
    public List<Dictionary<string, long>>? Items { get; init; }
}

[ArrowSerializable]
public partial record WithNullableDictOfLists
{
    public Dictionary<string, List<long>>? Groups { get; init; }
}

// --- Dict/List with NestedRecord/Enum/Guid leaves ---

[ArrowSerializable]
public partial record WithDictOfGuids
{
    public Dictionary<string, Guid> Items { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDictOfEnums
{
    public Dictionary<string, Color> Items { get; init; } = new();
}

[ArrowSerializable]
public partial record WithDictOfNested
{
    public Dictionary<string, Inner> Items { get; init; } = new();
}

// --- Field + constructor types ---

[ArrowSerializable]
public partial struct PointWithFields
{
    public readonly int X;
    public readonly int Y;

    public PointWithFields(int x, int y)
    {
        X = x;
        Y = y;
    }
}

[ArrowSerializable]
public partial struct PointMultiCtor
{
    public readonly int X;
    public readonly int Y;

    public PointMultiCtor(int x)
    {
        X = x;
        Y = -1;
    }

    public PointMultiCtor(int x, int y)
    {
        X = x;
        Y = y;
    }
}

[ArrowSerializable]
public partial struct MixedFieldsAndProps
{
    public readonly int Id;
    public string Name { get; set; }

    public MixedFieldsAndProps(int id, string name)
    {
        Id = id;
        Name = name;
    }
}

[ArrowSerializable]
public partial struct FieldWithArrowField
{
    [ArrowField("x_coord")]
    public readonly double X;
    [ArrowField("y_coord")]
    public readonly double Y;

    public FieldWithArrowField(double x, double y)
    {
        X = x;
        Y = y;
    }
}

[ArrowSerializable]
public partial class WithCallback : IArrowSerializationCallback
{
    public int Value { get; set; }
    [ArrowIgnore] public bool BeforeSerializeCalled { get; set; }
    [ArrowIgnore] public bool AfterDeserializeCalled { get; set; }
    [ArrowIgnore] public int ComputedDouble { get; set; }

    public void OnBeforeSerialize()
    {
        BeforeSerializeCalled = true;
    }

    public void OnAfterDeserialize()
    {
        AfterDeserializeCalled = true;
        ComputedDouble = Value * 2;
    }
}
