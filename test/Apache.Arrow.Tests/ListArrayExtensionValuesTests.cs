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

/// <summary>
/// GetSlicedValues must slice values that are an <see cref="ExtensionArray"/>,
/// which implements <see cref="IArrowArray"/> but doesn't derive from <see cref="Array"/>.
/// </summary>
public class ListArrayExtensionValuesTests
{
    private static readonly Guid[] s_guids = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).ToArray();

    private static readonly Field s_itemField = new Field("item", GuidType.Default, true);

    private static GuidArray BuildGuids() => new GuidArray.Builder().AppendRange(s_guids).Build();

    // Each list type below holds [[g0], [g1, g2, g3]].
    private static void AssertSlices(Func<int, IArrowArray> getSlicedValues)
    {
        GuidArray first = Assert.IsType<GuidArray>(getSlicedValues(0));
        Assert.Equal(new Guid?[] { s_guids[0] }, first.ToArray());

        GuidArray second = Assert.IsType<GuidArray>(getSlicedValues(1));
        Assert.Equal(new Guid?[] { s_guids[1], s_guids[2], s_guids[3] }, second.ToArray());
    }

    [Fact]
    public void ListArray()
    {
        ListArray list = new ListArray(
            new ListType(s_itemField), 2,
            new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(4).Build(),
            BuildGuids(), ArrowBuffer.Empty);

        AssertSlices(list.GetSlicedValues);
    }

    [Fact]
    public void LargeListArray()
    {
        LargeListArray list = new LargeListArray(
            new LargeListType(s_itemField), 2,
            new ArrowBuffer.Builder<long>().Append(0).Append(1).Append(4).Build(),
            BuildGuids(), ArrowBuffer.Empty);

        AssertSlices(list.GetSlicedValues);
    }

    [Fact]
    public void ListViewArray()
    {
        ListViewArray list = new ListViewArray(
            new ListViewType(s_itemField), 2,
            new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(),
            new ArrowBuffer.Builder<int>().Append(1).Append(3).Build(),
            BuildGuids(), ArrowBuffer.Empty);

        AssertSlices(list.GetSlicedValues);
    }

    [Fact]
    public void LargeListViewArray()
    {
        LargeListViewArray list = new LargeListViewArray(
            new LargeListViewType(s_itemField), 2,
            new ArrowBuffer.Builder<long>().Append(0).Append(1).Build(),
            new ArrowBuffer.Builder<long>().Append(1).Append(3).Build(),
            BuildGuids(), ArrowBuffer.Empty);

        AssertSlices(list.GetSlicedValues);
    }

    [Fact]
    public void FixedSizeListArray()
    {
        FixedSizeListArray list = new FixedSizeListArray(
            new FixedSizeListType(s_itemField, 2), 2,
            BuildGuids(), ArrowBuffer.Empty);

        GuidArray first = Assert.IsType<GuidArray>(list.GetSlicedValues(0));
        Assert.Equal(new Guid?[] { s_guids[0], s_guids[1] }, first.ToArray());

        GuidArray second = Assert.IsType<GuidArray>(list.GetSlicedValues(1));
        Assert.Equal(new Guid?[] { s_guids[2], s_guids[3] }, second.ToArray());
    }
}
