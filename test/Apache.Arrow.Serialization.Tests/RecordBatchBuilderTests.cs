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

public class RecordBatchBuilderTests
{
    [Fact]
    public void AnonymousType_BasicColumns()
    {
        var tabelle = new[]
        {
            new { Id = 1, Name = "Alice",   Alter = 30, Stadt = "Berlin"  },
            new { Id = 2, Name = "Bob",     Alter = 25, Stadt = "Hamburg" },
            new { Id = 3, Name = "Charlie", Alter = 35, Stadt = "München" },
        };

        var batch = RecordBatchBuilder.FromObjects(tabelle);

        Assert.Equal(3, batch.Length);
        Assert.Equal(4, batch.Schema.FieldsList.Count);

        // Check schema
        Assert.Equal("Id", batch.Schema.FieldsList[0].Name);
        Assert.IsType<Int32Type>(batch.Schema.FieldsList[0].DataType);
        Assert.Equal("Name", batch.Schema.FieldsList[1].Name);
        Assert.IsType<StringType>(batch.Schema.FieldsList[1].DataType);

        // Check values
        var idCol = (Int32Array)batch.Column(0);
        Assert.Equal(1, idCol.GetValue(0));
        Assert.Equal(2, idCol.GetValue(1));
        Assert.Equal(3, idCol.GetValue(2));

        var nameCol = (StringArray)batch.Column(1);
        Assert.Equal("Alice", nameCol.GetString(0));
        Assert.Equal("Bob", nameCol.GetString(1));
        Assert.Equal("Charlie", nameCol.GetString(2));

        var stadtCol = (StringArray)batch.Column(3);
        Assert.Equal("Berlin", stadtCol.GetString(0));
        Assert.Equal("München", stadtCol.GetString(2));
    }

    [Fact]
    public void AnonymousType_NumericTypes()
    {
        var data = new[]
        {
            new { I = 1, L = 100L, F = 1.5f, D = 2.5, B = true, By = (byte)7 },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(1, batch.Length);
        Assert.Equal(6, batch.Schema.FieldsList.Count);

        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Equal(100L, ((Int64Array)batch.Column(1)).GetValue(0));
        Assert.Equal(1.5f, ((FloatArray)batch.Column(2)).GetValue(0));
        Assert.Equal(2.5, ((DoubleArray)batch.Column(3)).GetValue(0));
        Assert.True(((BooleanArray)batch.Column(4)).GetValue(0));
        Assert.Equal((byte)7, ((UInt8Array)batch.Column(5)).GetValue(0));
    }

    [Fact]
    public void NullableProperties()
    {
        var data = new[]
        {
            new { Value = 1, Extra = (int?)42 },
            new { Value = 2, Extra = (int?)null },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(2, batch.Length);

        var extraCol = (Int32Array)batch.Column(1);
        Assert.Equal(42, extraCol.GetValue(0));
        Assert.True(extraCol.IsNull(1));
    }

    [Fact]
    public void NullableStrings()
    {
        var data = new[]
        {
            new { Name = "Alice", Tag = (string?)"admin" },
            new { Name = "Bob",   Tag = (string?)null },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        var tagCol = (StringArray)batch.Column(1);
        Assert.Equal("admin", tagCol.GetString(0));
        Assert.True(tagCol.IsNull(1));
    }

    [Fact]
    public void SingleObject()
    {
        var obj = new { X = 10, Y = 20.5 };
        var batch = RecordBatchBuilder.FromObject(obj);

        Assert.Equal(1, batch.Length);
        Assert.Equal(10, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Equal(20.5, ((DoubleArray)batch.Column(1)).GetValue(0));
    }

    [Fact]
    public void NamedRecord()
    {
        var data = new List<PointRecord>
        {
            new(1.0, 2.0, "A"),
            new(3.0, 4.0, "B"),
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(2, batch.Length);
        Assert.Equal(1.0, ((DoubleArray)batch.Column(0)).GetValue(0));
        Assert.Equal("B", ((StringArray)batch.Column(2)).GetString(1));
    }

    [Fact]
    public void EnumProperty()
    {
        var data = new[]
        {
            new { Color = Color.Red },
            new { Color = Color.Blue },
            new { Color = Color.Red },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(3, batch.Length);
        var col = (DictionaryArray)batch.Column(0);
        var dict = (StringArray)col.Dictionary;
        var indices = (Int16Array)col.Indices;
        // Red=0, Blue=1
        Assert.Equal((short)0, indices.GetValue(0));
        Assert.Equal((short)1, indices.GetValue(1));
        Assert.Equal((short)0, indices.GetValue(2));
        Assert.Equal("Red", dict.GetString(0));
        Assert.Equal("Blue", dict.GetString(1));
    }

    [Fact]
    public void DateTimeAndGuid()
    {
        var dt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        var guid = Guid.NewGuid();
        var data = new[] { new { When = dt, Id = guid } };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(1, batch.Length);
        Assert.IsType<TimestampType>(batch.Schema.FieldsList[0].DataType);
        Assert.IsType<GuidType>(batch.Schema.FieldsList[1].DataType);
    }

    [Fact]
    public void EmptyCollection_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            RecordBatchBuilder.FromObjects(System.Array.Empty<object>()));
    }

    [Fact]
    public void NestedAnonymousType()
    {
        var data = new[]
        {
            new { Name = "Alice", Address = new { City = "Berlin", Zip = 10115 } },
            new { Name = "Bob",   Address = new { City = "Hamburg", Zip = 20095 } },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(2, batch.Length);
        Assert.Equal(2, batch.Schema.FieldsList.Count);
        Assert.Equal("Name", batch.Schema.FieldsList[0].Name);
        Assert.Equal("Address", batch.Schema.FieldsList[1].Name);
        Assert.IsType<StructType>(batch.Schema.FieldsList[1].DataType);

        var structCol = (StructArray)batch.Column(1);
        var cityCol = (StringArray)structCol.Fields[0];
        var zipCol = (Int32Array)structCol.Fields[1];

        Assert.Equal("Berlin", cityCol.GetString(0));
        Assert.Equal("Hamburg", cityCol.GetString(1));
        Assert.Equal(10115, zipCol.GetValue(0));
        Assert.Equal(20095, zipCol.GetValue(1));
    }

    [Fact]
    public void DeeplyNestedAnonymousType()
    {
        var data = new[]
        {
            new { A = new { B = new { C = 42 } } },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(1, batch.Length);

        var aCol = (StructArray)batch.Column(0);
        var bCol = (StructArray)aCol.Fields[0];
        var cCol = (Int32Array)bCol.Fields[0];
        Assert.Equal(42, cCol.GetValue(0));
    }

    [Fact]
    public void NestedNamedRecord()
    {
        var data = new[]
        {
            new { Point = new PointRecord(1.0, 2.0, "A"), Tag = "first" },
            new { Point = new PointRecord(3.0, 4.0, "B"), Tag = "second" },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(2, batch.Length);

        var pointCol = (StructArray)batch.Column(0);
        var xCol = (DoubleArray)pointCol.Fields[0];
        var yCol = (DoubleArray)pointCol.Fields[1];
        Assert.Equal(1.0, xCol.GetValue(0));
        Assert.Equal(4.0, yCol.GetValue(1));
    }

    [Fact]
    public void NestedArrowSerializable_UsesSourceGeneratedSchema()
    {
        // Inner is [ArrowSerializable] and implements IArrowSerializer<Inner>
        var data = new[]
        {
            new { Tag = "first", Inner = new Inner { X = 10, Label = "A" } },
            new { Tag = "second", Inner = new Inner { X = 20, Label = "B" } },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(2, batch.Length);
        Assert.Equal(2, batch.Schema.FieldsList.Count);
        Assert.Equal("Tag", batch.Schema.FieldsList[0].Name);
        Assert.Equal("Inner", batch.Schema.FieldsList[1].Name);
        Assert.IsType<StructType>(batch.Schema.FieldsList[1].DataType);

        // Verify the struct schema matches the source-generated one
        var structType = (StructType)batch.Schema.FieldsList[1].DataType;
        Assert.Equal(2, structType.Fields.Count);
        Assert.Equal("X", structType.Fields[0].Name);
        Assert.Equal("Label", structType.Fields[1].Name);

        // Verify values
        var tagCol = (StringArray)batch.Column(0);
        Assert.Equal("first", tagCol.GetString(0));

        var innerCol = (StructArray)batch.Column(1);
        var xCol = (Int32Array)innerCol.Fields[0];
        var labelCol = (StringArray)innerCol.Fields[1];
        Assert.Equal(10, xCol.GetValue(0));
        Assert.Equal(20, xCol.GetValue(1));
        Assert.Equal("A", labelCol.GetString(0));
        Assert.Equal("B", labelCol.GetString(1));
    }

    [Fact]
    public void ListOfInts()
    {
        var data = new[]
        {
            new { Name = "Alice", Scores = new List<int> { 90, 85, 92 } },
            new { Name = "Bob",   Scores = new List<int> { 78, 88 } },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.Equal(2, batch.Length);
        Assert.IsType<ListType>(batch.Schema.FieldsList[1].DataType);

        var listCol = (ListArray)batch.Column(1);
        Assert.Equal(3, listCol.GetValueLength(0));
        Assert.Equal(2, listCol.GetValueLength(1));

        var values = (Int32Array)listCol.Values;
        Assert.Equal(90, values.GetValue(0));
        Assert.Equal(85, values.GetValue(1));
        Assert.Equal(92, values.GetValue(2));
        Assert.Equal(78, values.GetValue(3));
        Assert.Equal(88, values.GetValue(4));
    }

    [Fact]
    public void ListOfStrings()
    {
        var data = new[]
        {
            new { Tags = new List<string> { "admin", "user" } },
            new { Tags = new List<string> { "guest" } },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        var listCol = (ListArray)batch.Column(0);
        var values = (StringArray)listCol.Values;
        Assert.Equal("admin", values.GetString(0));
        Assert.Equal("user", values.GetString(1));
        Assert.Equal("guest", values.GetString(2));
    }

    [Fact]
    public void NullableList()
    {
        var data = new[]
        {
            new { Items = (List<int>?)new List<int> { 1, 2 } },
            new { Items = (List<int>?)null },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        var listCol = (ListArray)batch.Column(0);
        Assert.False(listCol.IsNull(0));
        Assert.True(listCol.IsNull(1));
    }

    [Fact]
    public void ArrayProperty()
    {
        var data = new[]
        {
            new { Values = new[] { 1.0, 2.0, 3.0 } },
            new { Values = new[] { 4.0 } },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.IsType<ListType>(batch.Schema.FieldsList[0].DataType);

        var listCol = (ListArray)batch.Column(0);
        var values = (DoubleArray)listCol.Values;
        Assert.Equal(1.0, values.GetValue(0));
        Assert.Equal(4.0, values.GetValue(3));
    }

    [Fact]
    public void DictionaryProperty()
    {
        var data = new[]
        {
            new { Props = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 } },
            new { Props = new Dictionary<string, int> { ["c"] = 3 } },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.IsType<MapType>(batch.Schema.FieldsList[0].DataType);

        var mapCol = (MapArray)batch.Column(0);
        Assert.Equal(2, mapCol.GetValueLength(0));
        Assert.Equal(1, mapCol.GetValueLength(1));

        var keys = (StringArray)mapCol.Keys;
        var vals = (Int32Array)mapCol.Values;
        Assert.Equal("a", keys.GetString(0));
        Assert.Equal(1, vals.GetValue(0));
        Assert.Equal("c", keys.GetString(2));
        Assert.Equal(3, vals.GetValue(2));
    }

    [Fact]
    public void ReadOnlyMemoryByte()
    {
        var bytes1 = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });
        var bytes2 = new ReadOnlyMemory<byte>(new byte[] { 4, 5 });
        var data = new[]
        {
            new { Data = bytes1 },
            new { Data = bytes2 },
        };

        var batch = RecordBatchBuilder.FromObjects(data);
        Assert.IsType<BinaryType>(batch.Schema.FieldsList[0].DataType);

        var binCol = (BinaryArray)batch.Column(0);
        Assert.Equal(new byte[] { 1, 2, 3 }, binCol.GetBytes(0).ToArray());
        Assert.Equal(new byte[] { 4, 5 }, binCol.GetBytes(1).ToArray());
    }

    public record PointRecord(double X, double Y, string Label);
}
