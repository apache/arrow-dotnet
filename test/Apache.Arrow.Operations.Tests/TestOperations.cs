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
using Xunit;


namespace Apache.Arrow.Operations.Tests;

public class ArrowOperationsTests
{

    [Fact]
    public void TestConversion()
    {
        var vals = Conversion.CastDouble([50L, 52L, 510L]);
        Assert.Equal(vals.GetValue(0), 50.0);
        Assert.Equal(vals.GetValue(1), 52.0);
        Assert.Equal(vals.GetValue(2), 510.0);

        var valsF = Conversion.CastFloat(vals);
        Assert.Equal(valsF.GetValue(0), 50.0f);
        Assert.Equal(valsF.GetValue(1), 52.0f);
        Assert.Equal(valsF.GetValue(2), 510.0f);

        var valsI = Conversion.CastInt32(vals);
        Assert.Equal(valsI.GetValue(0), 50);
        Assert.Equal(valsI.GetValue(1), 52);
        Assert.Equal(valsI.GetValue(2), 510);
    }

    [Fact]
    public void TestSelectionTakeIndex()
    {
        var vals = Conversion.CastInt64([50L, 52L, 510L]);
        var items = (Int64Array)Select.Take(vals, [1]);
        Assert.Equal(52, items.GetValue(0));
    }

    [Fact]
    public void TestSelectionFilterMask()
    {
        var vals = Conversion.CastInt64([50L, 52L, 510L]);
        var mask = Comparison.Equal(vals, 52L);
        var items = (Int64Array)Select.Filter(vals, mask);
        Assert.Equal(52, items.GetValue(0));
    }
}


public class ArrowBooleanOperationsTests {
    [Fact]
    public void TestInvert()
    {
        var vals = Enumerable.Repeat(true, 5000);
        var builder = new BooleanArray.Builder(5000);
        builder.AppendRange(vals);
        var array = builder.Build();
        Assert.True(array.All(v => v ?? false));

        var inverted = BitVectorOps.OnesComplement(array.ValueBuffer);
        var invertedArray = new BooleanArray(inverted, array.NullBitmapBuffer.Clone(), array.Length, array.NullCount, 0);
        Assert.Equal(array.Length, invertedArray.Length);
        Assert.False(invertedArray.All(v => v ?? false));
    }

    [Fact]
    public void TesAnd()
    {
        var vals = Enumerable.Repeat(true, 5000);
        var builder = new BooleanArray.Builder(5000);
        builder.AppendRange(vals);
        var array = builder.Build();
        Assert.True(array.All(v => v ?? false));

        var result = Comparison.And(array, array);
        Assert.True(result.All(v => v ?? false));

        vals = Enumerable.Repeat(false, 5000);
        builder = new BooleanArray.Builder(5000);
        builder.AppendRange(vals);
        var inverted = builder.Build();

        result = Comparison.And(array, inverted);
        Assert.Equal(result.Length, inverted.Length);
        Assert.False(result.All(v => v ?? false));
    }

    [Fact]
    public void TestOr()
    {
        var vals = Enumerable.Repeat(true, 5000);
        var builder = new BooleanArray.Builder(5000);
        builder.AppendRange(vals);
        var array = builder.Build();
        Assert.True(array.All(v => v ?? false));

        var result = Comparison.Or(array, array);
        Assert.True(result.All(v => v ?? false));

        vals = Enumerable.Repeat(false, 5000);
        builder = new BooleanArray.Builder(5000);
        builder.AppendRange(vals);
        var inverted = builder.Build();

        result = Comparison.Or(array, inverted);
        Assert.Equal(result.Length, inverted.Length);
        Assert.True(result.All(v => v ?? false));
    }

    [Fact]
    public void TestXor()
    {
        var vals = Enumerable.Repeat(true, 2500);
        var builder = new BooleanArray.Builder(5000);
        builder.AppendRange(vals);
        vals = Enumerable.Repeat(false, 2500);
        builder.AppendRange(vals);
        var array = builder.Build();

        Assert.Equal(2500, array.Count(s => s ?? false));

        builder = new BooleanArray.Builder(5000);
        vals = Enumerable.Repeat(true, 2500);
        builder.AppendRange(vals);
        vals = Enumerable.Repeat(true, 2500);
        builder.AppendRange(vals);
        var array2 = builder.Build();

        var result = Comparison.Xor(array, array2);
        Assert.Equal(2500, result.Count(s => s ?? false));
        Assert.Equal(0, ((BooleanArray)result.Slice(0, 2500)).Count(s => s ?? false));
    }
}