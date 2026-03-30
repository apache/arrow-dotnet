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

using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class LargeListArrayBuilderTests
    {
        [Fact]
        public void AppendBuildsCorrectArray()
        {
            var builder = new LargeListArray.Builder(Int32Type.Default);
            var valueBuilder = (Int32Array.Builder)builder.ValueBuilder;

            builder.Append();
            valueBuilder.Append(1);
            valueBuilder.Append(2);

            builder.Append();
            valueBuilder.Append(3);

            builder.AppendNull();

            builder.Append();

            var array = builder.Build();

            Assert.Equal(4, array.Length);

            var slice0 = (Int32Array)array.GetSlicedValues(0);
            Assert.Equal(2, slice0.Length);
            Assert.Equal(1, slice0.GetValue(0));
            Assert.Equal(2, slice0.GetValue(1));

            var slice1 = (Int32Array)array.GetSlicedValues(1);
            Assert.Equal(1, slice1.Length);
            Assert.Equal(3, slice1.GetValue(0));

            Assert.Null(array.GetSlicedValues(2));

            var slice3 = (Int32Array)array.GetSlicedValues(3);
            Assert.Equal(0, slice3.Length);
        }

        [Fact]
        public void NullCountIsCorrect()
        {
            var builder = new LargeListArray.Builder(Int32Type.Default);
            var valueBuilder = (Int32Array.Builder)builder.ValueBuilder;

            builder.Append();
            valueBuilder.Append(1);

            builder.AppendNull();
            builder.AppendNull();

            builder.Append();
            valueBuilder.Append(2);

            var array = builder.Build();

            Assert.Equal(4, array.Length);
            Assert.Equal(2, array.NullCount);
        }

        [Fact]
        public void ClearResetsBuilder()
        {
            var builder = new LargeListArray.Builder(Int32Type.Default);
            var valueBuilder = (Int32Array.Builder)builder.ValueBuilder;

            builder.Append();
            valueBuilder.Append(1);

            builder.Clear();

            builder.Append();
            valueBuilder.Append(99);

            var array = builder.Build();

            Assert.Equal(1, array.Length);
            var slice = (Int32Array)array.GetSlicedValues(0);
            Assert.Equal(1, slice.Length);
            Assert.Equal(99, slice.GetValue(0));
        }

        [Fact]
        public void NestedStringListBuildsCorrectly()
        {
            var builder = new LargeListArray.Builder(StringType.Default);
            var valueBuilder = (StringArray.Builder)builder.ValueBuilder;

            builder.Append();
            valueBuilder.Append("hello");
            valueBuilder.Append("world");

            builder.Append();
            valueBuilder.Append("foo");

            var array = builder.Build();

            Assert.Equal(2, array.Length);

            var slice0 = (StringArray)array.GetSlicedValues(0);
            Assert.Equal(2, slice0.Length);
            Assert.Equal("hello", slice0.GetString(0));
            Assert.Equal("world", slice0.GetString(1));

            var slice1 = (StringArray)array.GetSlicedValues(1);
            Assert.Equal(1, slice1.Length);
            Assert.Equal("foo", slice1.GetString(0));
        }
    }
}
