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
using Xunit;

namespace Apache.Arrow.Tests;

public class RunEndEncodedArrayBuilderTests
{
    [Fact]
    public void TestAppendGroupingStringValues()
    {
        var builder = new RunEndEncodedArray.Builder<Int32Array.Builder, StringArray.Builder, Int32Array, StringArray, string>(
            new Int32Array.Builder(),
            new StringArray.Builder());

        builder.Append("A")
               .Append("A")
               .Append("A")
               .Append("B")
               .Append("B")
               .Append("C");

        Assert.Equal(6, builder.Length);

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(6, reeArray.Length);
        Assert.Equal(0, reeArray.NullCount);

        var runEnds = Assert.IsType<Int32Array>(reeArray.RunEnds);
        Assert.Equal(3, runEnds.Length);
        Assert.Equal(3, runEnds.GetValue(0));
        Assert.Equal(5, runEnds.GetValue(1));
        Assert.Equal(6, runEnds.GetValue(2));

        var values = Assert.IsType<StringArray>(reeArray.Values);
        Assert.Equal(3, values.Length);
        Assert.Equal("A", values.GetString(0));
        Assert.Equal("B", values.GetString(1));
        Assert.Equal("C", values.GetString(2));
    }

    [Fact]
    public void TestAppendGroupingPrimitiveValues()
    {
        var builder = new RunEndEncodedArray.Builder<Int32Array.Builder, Int32Array.Builder, Int32Array, Int32Array, int>(
            new Int32Array.Builder(),
            new Int32Array.Builder());

        builder.AppendRange(new[] { 10, 10, 20, 20, 20, 30 });

        Assert.Equal(6, builder.Length);

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(6, reeArray.Length);

        var runEnds = Assert.IsType<Int32Array>(reeArray.RunEnds);
        Assert.Equal(3, runEnds.Length);
        Assert.Equal(2, runEnds.GetValue(0));
        Assert.Equal(5, runEnds.GetValue(1));
        Assert.Equal(6, runEnds.GetValue(2));

        var values = Assert.IsType<Int32Array>(reeArray.Values);
        Assert.Equal(3, values.Length);
        Assert.Equal(10, values.GetValue(0));
        Assert.Equal(20, values.GetValue(1));
        Assert.Equal(30, values.GetValue(2));
    }

    [Fact]
    public void TestAppendGroupingInt16RunEnds()
    {
        var builder = new RunEndEncodedArray.Builder<Int16Array.Builder, DoubleArray.Builder, Int16Array, DoubleArray, double>(
            new Int16Array.Builder(),
            new DoubleArray.Builder());

        builder.Append(1.1)
               .Append(1.1)
               .Append(2.2);

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(3, reeArray.Length);

        var runEnds = Assert.IsType<Int16Array>(reeArray.RunEnds);
        Assert.Equal(2, runEnds.Length);
        Assert.Equal((short)2, runEnds.GetValue(0));
        Assert.Equal((short)3, runEnds.GetValue(1));

        var values = Assert.IsType<DoubleArray>(reeArray.Values);
        Assert.Equal(2, values.Length);
        Assert.Equal(1.1, values.GetValue(0));
        Assert.Equal(2.2, values.GetValue(1));
    }

    [Fact]
    public void TestAppendGroupingInt64RunEnds()
    {
        var builder = new RunEndEncodedArray.Builder<Int64Array.Builder, StringArray.Builder, Int64Array, StringArray, string>(
            new Int64Array.Builder(),
            new StringArray.Builder());

        builder.AppendRange(new[] { "X", "X", "Y" });

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(3, reeArray.Length);

        var runEnds = Assert.IsType<Int64Array>(reeArray.RunEnds);
        Assert.Equal(2, runEnds.Length);
        Assert.Equal(2L, runEnds.GetValue(0));
        Assert.Equal(3L, runEnds.GetValue(1));

        var values = Assert.IsType<StringArray>(reeArray.Values);
        Assert.Equal(2, values.Length);
        Assert.Equal("X", values.GetString(0));
        Assert.Equal("Y", values.GetString(1));
    }

    [Fact]
    public void TestAppendNullHandling()
    {
        var builder = new RunEndEncodedArray.Builder<Int32Array.Builder, StringArray.Builder, Int32Array, StringArray, string>(
            new Int32Array.Builder(),
            new StringArray.Builder());

        builder.Append("A")
               .Append("A")
               .AppendNull()
               .Append((string)null)
               .Append("B");

        Assert.Equal(5, builder.Length);

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(5, reeArray.Length);

        var runEnds = Assert.IsType<Int32Array>(reeArray.RunEnds);
        Assert.Equal(3, runEnds.Length);
        Assert.Equal(2, runEnds.GetValue(0));
        Assert.Equal(4, runEnds.GetValue(1));
        Assert.Equal(5, runEnds.GetValue(2));

        var values = Assert.IsType<StringArray>(reeArray.Values);
        Assert.Equal(3, values.Length);
        Assert.Equal("A", values.GetString(0));
        Assert.True(values.IsNull(1));
        Assert.Equal("B", values.GetString(2));
    }

    [Fact]
    public void TestEmptyArrayBuilding()
    {
        var builder = new RunEndEncodedArray.Builder<Int32Array.Builder, StringArray.Builder, Int32Array, StringArray, string>(
            new Int32Array.Builder(),
            new StringArray.Builder());

        Assert.Equal(0, builder.Length);

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(0, reeArray.Length);
        Assert.Equal(0, reeArray.NullCount);
        Assert.Equal(0, reeArray.RunEnds.Length);
        Assert.Equal(0, reeArray.Values.Length);
    }

    [Fact]
    public void TestAppendSpanAndRange()
    {
        var builder = new RunEndEncodedArray.Builder<Int32Array.Builder, Int32Array.Builder, Int32Array, Int32Array, int>(
            new Int32Array.Builder(),
            new Int32Array.Builder());

        ReadOnlySpan<int> span = new[] { 5, 5, 5 };
        builder.Append(span);
        builder.AppendRange(new[] { 5, 10, 10 });

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(6, reeArray.Length);

        var runEnds = Assert.IsType<Int32Array>(reeArray.RunEnds);
        Assert.Equal(2, runEnds.Length);
        Assert.Equal(4, runEnds.GetValue(0));
        Assert.Equal(6, runEnds.GetValue(1));

        var values = Assert.IsType<Int32Array>(reeArray.Values);
        Assert.Equal(2, values.Length);
        Assert.Equal(5, values.GetValue(0));
        Assert.Equal(10, values.GetValue(1));
    }

    [Fact]
    public void TestClear()
    {
        var builder = new RunEndEncodedArray.Builder<Int32Array.Builder, StringArray.Builder, Int32Array, StringArray, string>(
            new Int32Array.Builder(),
            new StringArray.Builder());

        builder.Append("A").Append("A");
        builder.Clear();

        Assert.Equal(0, builder.Length);

        builder.Append("B").Append("B").Append("B");

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(3, reeArray.Length);

        var runEnds = Assert.IsType<Int32Array>(reeArray.RunEnds);
        Assert.Equal(1, runEnds.Length);
        Assert.Equal(3, runEnds.GetValue(0));

        var values = Assert.IsType<StringArray>(reeArray.Values);
        Assert.Equal(1, values.Length);
        Assert.Equal("B", values.GetString(0));
    }

    [Fact]
    public void TestCustomComparer()
    {
        var builder = new RunEndEncodedArray.Builder<Int32Array.Builder, StringArray.Builder, Int32Array, StringArray, string>(
            new Int32Array.Builder(),
            new StringArray.Builder(),
            StringComparer.OrdinalIgnoreCase);

        builder.Append("abc")
               .Append("ABC")
               .Append("aBc")
               .Append("def");

        RunEndEncodedArray reeArray = builder.Build();

        Assert.Equal(4, reeArray.Length);

        var runEnds = Assert.IsType<Int32Array>(reeArray.RunEnds);
        Assert.Equal(2, runEnds.Length);
        Assert.Equal(3, runEnds.GetValue(0));
        Assert.Equal(4, runEnds.GetValue(1));

        var values = Assert.IsType<StringArray>(reeArray.Values);
        Assert.Equal(2, values.Length);
        Assert.Equal("abc", values.GetString(0));
        Assert.Equal("def", values.GetString(1));
    }
}
