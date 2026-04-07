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
using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests;

public class LargeListViewArrayTests
{
    [Fact]
    public void GetSlicedValuesReturnsCorrectValues()
    {
        var values = new int?[][]
        {
            new int?[] { 0, 1, 2 },
            System.Array.Empty<int?>(),
            null,
            new int?[] { 3, 4, null, 6 },
        };

        var array = BuildArray(values);

        Assert.Equal(values.Length, array.Length);
        for (int i = 0; i < values.Length; ++i)
        {
            Assert.Equal(values[i] == null, array.IsNull(i));
            var arrayItem = (Int32Array)array.GetSlicedValues(i);
            if (values[i] == null)
            {
                Assert.Null(arrayItem);
            }
            else
            {
                Assert.Equal(values[i], arrayItem.ToArray());
            }
        }
    }

    [Fact]
    public void GetSlicedValuesChecksForOffsetOverflow()
    {
        var valuesArray = new Int32Array.Builder().Build();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var sizesBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        offsetBuffer.Append((long)int.MaxValue + 1);
        sizesBuffer.Append(1);
        validityBuffer.Append(true);

        var array = new LargeListViewArray(
            new LargeListViewType(new Int32Type()), length: 1,
            offsetBuffer.Build(), sizesBuffer.Build(), valuesArray, validityBuffer.Build(),
            validityBuffer.UnsetBitCount);

        Assert.Throws<OverflowException>(() => array.GetSlicedValues(0));
    }

    [Fact]
    public void GetSlicedValuesChecksForSizeOverflow()
    {
        var valuesArray = new Int32Array.Builder().Build();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var sizesBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        offsetBuffer.Append(0L);
        sizesBuffer.Append((long)int.MaxValue + 1);
        validityBuffer.Append(true);

        var array = new LargeListViewArray(
            new LargeListViewType(new Int32Type()), length: 1,
            offsetBuffer.Build(), sizesBuffer.Build(), valuesArray, validityBuffer.Build(),
            validityBuffer.UnsetBitCount);

        Assert.Throws<OverflowException>(() => array.GetValueLength(0));
    }

    [Fact]
    public void SliceReturnsCorrectValues()
    {
        var values = new int?[][]
        {
            new int?[] { 10, 20 },
            new int?[] { 30 },
            new int?[] { 40, 50, 60 },
            new int?[] { 70 },
        };

        var array = BuildArray(values);
        var sliced = (LargeListViewArray)array.Slice(1, 2);

        Assert.Equal(2, sliced.Length);

        var list0 = (Int32Array)sliced.GetSlicedValues(0);
        Assert.Equal(1, list0.Length);
        Assert.Equal(30, list0.GetValue(0));

        var list1 = (Int32Array)sliced.GetSlicedValues(1);
        Assert.Equal(3, list1.Length);
        Assert.Equal(40, list1.GetValue(0));
        Assert.Equal(60, list1.GetValue(2));
    }

    [Fact]
    public void PropertiesReturnCorrectValues()
    {
        var values = new int?[][]
        {
            new int?[] { 1, 2, 3 },
            null,
            new int?[] { 4, 5 },
        };

        var array = BuildArray(values);

        Assert.Equal(3, array.Length);
        Assert.Equal(1, array.NullCount);
        Assert.Equal(ArrowTypeId.LargeListView, array.Data.DataType.TypeId);

        Assert.Equal(3, array.GetValueLength(0));
        Assert.Equal(0, array.GetValueLength(1));
        Assert.Equal(2, array.GetValueLength(2));
    }

    private static LargeListViewArray BuildArray(int?[][] values)
    {
        var valuesBuilder = new Int32Array.Builder();
        var offsetBuffer = new ArrowBuffer.Builder<long>();
        var sizesBuffer = new ArrowBuffer.Builder<long>();
        var validityBuffer = new ArrowBuffer.BitmapBuilder();

        foreach (var listValue in values)
        {
            if (listValue == null)
            {
                offsetBuffer.Append(valuesBuilder.Length);
                sizesBuffer.Append(0);
                validityBuffer.Append(false);
            }
            else
            {
                offsetBuffer.Append(valuesBuilder.Length);
                sizesBuffer.Append(listValue.Length);
                foreach (var value in listValue)
                {
                    valuesBuilder.Append(value);
                }
                validityBuffer.Append(true);
            }
        }

        return new LargeListViewArray(
            new LargeListViewType(new Int32Type()), values.Length,
            offsetBuffer.Build(), sizesBuffer.Build(), valuesBuilder.Build(), validityBuffer.Build(),
            validityBuffer.UnsetBitCount);
    }
}
