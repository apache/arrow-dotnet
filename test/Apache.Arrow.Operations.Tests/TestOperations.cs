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
using Apache.Arrow.Ipc;
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