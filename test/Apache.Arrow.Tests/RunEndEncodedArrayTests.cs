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
using System.IO;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests;

public class RunEndEncodedArrayTests
{
    [Fact]
    public void TestRunEndEncodedTypeCreation()
    {
        // Test with explicit fields
        var runEndsField = new Field("run_ends", Int32Type.Default, nullable: false);
        var valuesField = new Field("values", StringType.Default, nullable: true);
        var reeType = new RunEndEncodedType(runEndsField, valuesField);

        Assert.Equal(ArrowTypeId.RunEndEncoded, reeType.TypeId);
        Assert.Equal("run_end_encoded", reeType.Name);
        Assert.Equal(runEndsField, reeType.RunEndsField);
        Assert.Equal(valuesField, reeType.ValuesField);
        Assert.Equal(Int32Type.Default.TypeId, reeType.RunEndsDataType.TypeId);
        Assert.Equal(StringType.Default.TypeId, reeType.ValuesDataType.TypeId);
    }

    [Fact]
    public void TestRunEndEncodedTypeCreationWithDataTypes()
    {
        // Test with data types (uses default field names)
        var reeType = new RunEndEncodedType(Int32Type.Default, StringType.Default);

        Assert.Equal(ArrowTypeId.RunEndEncoded, reeType.TypeId);
        Assert.Equal("run_ends", reeType.RunEndsField.Name);
        Assert.Equal("values", reeType.ValuesField.Name);
    }

    [Fact]
    public void TestRunEndEncodedTypeValidation()
    {
        // Invalid run ends type (must be Int16, Int32, or Int64)
        Assert.Throws<ArgumentException>(() => new RunEndEncodedType(Int8Type.Default, StringType.Default));
        Assert.Throws<ArgumentException>(() => new RunEndEncodedType(FloatType.Default, StringType.Default));
        Assert.Throws<ArgumentException>(() => new RunEndEncodedType(StringType.Default, StringType.Default));

        // Valid run ends types
        Assert.NotNull(new RunEndEncodedType(Int16Type.Default, StringType.Default)); // Should not throw
        Assert.NotNull(new RunEndEncodedType(Int32Type.Default, StringType.Default)); // Should not throw
        Assert.NotNull(new RunEndEncodedType(Int64Type.Default, StringType.Default)); // Should not throw
    }

    [Fact]
    public void TestRunEndEncodedArrayWithInt32RunEnds()
    {
        // Create run ends: [3, 7, 10, 15]
        // This represents: 3 'A's, 4 'B's, 3 'C's, 5 'D's
        var runEndsBuilder = new Int32Array.Builder();
        runEndsBuilder.AppendRange([3, 7, 10, 15]);
        Int32Array runEnds = runEndsBuilder.Build();

        // Create values: ['A', 'B', 'C', 'D']
        var valuesBuilder = new StringArray.Builder();
        valuesBuilder.AppendRange(["A", "B", "C", "D"]);
        StringArray values = valuesBuilder.Build();

        // Create REE array
        var reeArray = new RunEndEncodedArray(runEnds, values);

        Assert.Equal(15, reeArray.Length); // Logical length is the last run end value
        Assert.Equal(0, reeArray.NullCount); // REE arrays don't have nulls at the top level
        Assert.Equal(runEnds, reeArray.RunEnds);
        Assert.Equal(values, reeArray.Values);
    }

    [Fact]
    public void TestRunEndEncodedArrayWithInt16RunEnds()
    {
        var runEndsBuilder = new Int16Array.Builder();
        runEndsBuilder.AppendRange([2, 5, 8]);
        Int16Array runEnds = runEndsBuilder.Build();

        var valuesBuilder = new Int32Array.Builder();
        valuesBuilder.AppendRange([100, 200, 300]);
        Int32Array values = valuesBuilder.Build();

        var reeArray = new RunEndEncodedArray(runEnds, values);

        Assert.Equal(8, reeArray.Length);
        Assert.Equal(runEnds, reeArray.RunEnds);
        Assert.Equal(values, reeArray.Values);
    }

    [Fact]
    public void TestRunEndEncodedArrayWithInt64RunEnds()
    {
        var runEndsBuilder = new Int64Array.Builder();
        runEndsBuilder.AppendRange([1000, 2000, 3000]);
        Int64Array runEnds = runEndsBuilder.Build();

        var valuesBuilder = new DoubleArray.Builder();
        valuesBuilder.AppendRange([1.5, 2.5, 3.5]);
        DoubleArray values = valuesBuilder.Build();

        var reeArray = new RunEndEncodedArray(runEnds, values);

        Assert.Equal(3000, reeArray.Length);
        Assert.Equal(runEnds, reeArray.RunEnds);
        Assert.Equal(values, reeArray.Values);
    }

    [Fact]
    public void TestRunEndEncodedArrayInvalidRunEndsType()
    {
        Int8Array invalidRunEnds = new Int8Array.Builder().AppendRange([1, 2, 3]).Build();
        StringArray values = new StringArray.Builder().AppendRange(["A", "B", "C"]).Build();

        Assert.Throws<ArgumentException>(() => new RunEndEncodedArray(invalidRunEnds, values));
    }

    [Fact]
    public void TestRunEndEncodedArrayEmpty()
    {
        Int32Array runEnds = new Int32Array.Builder().Build();
        StringArray values = new StringArray.Builder().Build();

        var reeArray = new RunEndEncodedArray(runEnds, values);

        Assert.Equal(0, reeArray.Length);
    }

    [Fact]
    public void TestFindPhysicalIndexInt32()
    {
        // Run ends: [3, 7, 10, 15] means:
        // Logical indices 0-2 map to physical index 0 (value 'A')
        // Logical indices 3-6 map to physical index 1 (value 'B')
        // Logical indices 7-9 map to physical index 2 (value 'C')
        // Logical indices 10-14 map to physical index 3 (value 'D')
        Int32Array runEnds = new Int32Array.Builder()
            .AppendRange([3, 7, 10, 15])
            .Build();
        StringArray values = new StringArray.Builder()
            .AppendRange(["A", "B", "C", "D"])
            .Build();

        var reeArray = new RunEndEncodedArray(runEnds, values);

        Assert.Equal(0, reeArray.FindPhysicalIndex(0));
        Assert.Equal(0, reeArray.FindPhysicalIndex(1));
        Assert.Equal(0, reeArray.FindPhysicalIndex(2));
        Assert.Equal(1, reeArray.FindPhysicalIndex(3));
        Assert.Equal(1, reeArray.FindPhysicalIndex(4));
        Assert.Equal(1, reeArray.FindPhysicalIndex(5));
        Assert.Equal(1, reeArray.FindPhysicalIndex(6));
        Assert.Equal(2, reeArray.FindPhysicalIndex(7));
        Assert.Equal(2, reeArray.FindPhysicalIndex(8));
        Assert.Equal(2, reeArray.FindPhysicalIndex(9));
        Assert.Equal(3, reeArray.FindPhysicalIndex(10));
        Assert.Equal(3, reeArray.FindPhysicalIndex(11));
        Assert.Equal(3, reeArray.FindPhysicalIndex(14));
    }

    [Fact]
    public void TestFindPhysicalIndexOutOfRange()
    {
        Int32Array runEnds = new Int32Array.Builder().AppendRange([3, 7]).Build();
        StringArray values = new StringArray.Builder().AppendRange(["A", "B"]).Build();
        var reeArray = new RunEndEncodedArray(runEnds, values);

        Assert.Throws<ArgumentOutOfRangeException>(() => reeArray.FindPhysicalIndex(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => reeArray.FindPhysicalIndex(7));
        Assert.Throws<ArgumentOutOfRangeException>(() => reeArray.FindPhysicalIndex(100));
    }

    [Fact]
    public void TestRunEndEncodedArraySerialization()
    {
        // Create a REE array
        Int32Array runEnds = new Int32Array.Builder().AppendRange([3, 7, 10]).Build();
        StringArray values = new StringArray.Builder().AppendRange(["foo", "bar", "baz"]).Build();
        var reeArray = new RunEndEncodedArray(runEnds, values);

        // Create a record batch with the REE array
        var reeField = new Field("ree_column", reeArray.Data.DataType, nullable: false);
        var schema = new Schema([reeField], null);
        var recordBatch = new RecordBatch(schema, [reeArray], reeArray.Length);

        // Serialize and deserialize
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(recordBatch);
            writer.WriteEnd();
        }

        stream.Position = 0;

        using var reader = new ArrowStreamReader(stream);
        RecordBatch readBatch = reader.ReadNextRecordBatch();

        Assert.NotNull(readBatch);
        Assert.Equal(1, readBatch.ColumnCount);
        Assert.Equal(10, readBatch.Length);

        var readArray = readBatch.Column(0) as RunEndEncodedArray;
        Assert.NotNull(readArray);
        Assert.Equal(10, readArray.Length);
        Assert.Equal(ArrowTypeId.RunEndEncoded, readArray.Data.DataType.TypeId);

        // Verify run ends
        var readRunEnds = readArray.RunEnds as Int32Array;
        Assert.NotNull(readRunEnds);
        Assert.Equal(3, readRunEnds.Length);
        Assert.Equal(3, readRunEnds.GetValue(0));
        Assert.Equal(7, readRunEnds.GetValue(1));
        Assert.Equal(10, readRunEnds.GetValue(2));

        // Verify values
        var readValues = readArray.Values as StringArray;
        Assert.NotNull(readValues);
        Assert.Equal(3, readValues.Length);
        Assert.Equal("foo", readValues.GetString(0));
        Assert.Equal("bar", readValues.GetString(1));
        Assert.Equal("baz", readValues.GetString(2));
    }

    [Fact]
    public void TestRunEndEncodedArrayWithDifferentValueTypes()
    {
        // Test with boolean values
        Int32Array runEnds1 = new Int32Array.Builder().AppendRange([5, 10]).Build();
        BooleanArray values1 = new BooleanArray.Builder().AppendRange([true, false]).Build();
        var reeArray1 = new RunEndEncodedArray(runEnds1, values1);
        Assert.Equal(10, reeArray1.Length);

        // Test with double values
        Int32Array runEnds2 = new Int32Array.Builder().AppendRange([3, 8]).Build();
        DoubleArray values2 = new DoubleArray.Builder().AppendRange([1.5, 2.5]).Build();
        var reeArray2 = new RunEndEncodedArray(runEnds2, values2);
        Assert.Equal(8, reeArray2.Length);

        // Test with list values
        var listBuilder = new ListArray.Builder(Int32Type.Default);
        var int32Builder = (Int32Array.Builder)listBuilder.ValueBuilder;
        listBuilder.Append();
        int32Builder.Append(1);
        int32Builder.Append(2);
        listBuilder.Append();
        int32Builder.Append(3);
        int32Builder.Append(4);
        ListArray listValues = listBuilder.Build();

        Int32Array runEnds3 = new Int32Array.Builder().AppendRange([2, 5]).Build();
        var reeArray3 = new RunEndEncodedArray(runEnds3, listValues);
        Assert.Equal(5, reeArray3.Length);
    }

    [Fact]
    public void TestRunEndEncodedArrayFromArrayData()
    {
        // Create arrays
        Int32Array runEnds = new Int32Array.Builder().AppendRange([2, 5]).Build();
        StringArray values = new StringArray.Builder().AppendRange(["X", "Y"]).Build();

        // Create ArrayData manually
        var reeType = new RunEndEncodedType(Int32Type.Default, StringType.Default);
        var arrayData = new ArrayData(
            reeType,
            length: 5,
            nullCount: 0,
            offset: 0,
            buffers: [],
            children: [runEnds.Data, values.Data]);

        // Create REE array from ArrayData
        var reeArray = new RunEndEncodedArray(arrayData);

        Assert.Equal(5, reeArray.Length);
        Assert.Equal(0, reeArray.NullCount);
        Assert.IsType<Int32Array>(reeArray.RunEnds);
        Assert.IsType<StringArray>(reeArray.Values);
    }

    [Fact]
    public void TestRunEndEncodedArrayFactoryBuild()
    {
        // Test that ArrowArrayFactory can build REE arrays
        Int32Array runEnds = new Int32Array.Builder().AppendRange([3, 6]).Build();
        Int64Array values = new Int64Array.Builder().AppendRange([100, 200]).Build();

        var reeType = new RunEndEncodedType(Int32Type.Default, Int64Type.Default);
        var arrayData = new ArrayData(
            reeType,
            length: 6,
            nullCount: 0,
            offset: 0,
            buffers: [],
            children: [runEnds.Data, values.Data]);

        IArrowArray array = ArrowArrayFactory.BuildArray(arrayData);

        Assert.IsType<RunEndEncodedArray>(array);
        var reeArray = (RunEndEncodedArray)array;
        Assert.Equal(6, reeArray.Length);
    }
}
