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

using Xunit;

namespace Apache.Arrow.Tests
{
    public class LargeStringArrayBuilderTests
    {
        [Fact]
        public void AppendStringBuildsCorrectArray()
        {
            var builder = new LargeStringArray.Builder();
            builder.Append("hello");
            builder.Append("world");
            builder.AppendNull();
            builder.Append("");

            var array = builder.Build();

            Assert.Equal(4, array.Length);
            Assert.Equal("hello", array.GetString(0));
            Assert.Equal("world", array.GetString(1));
            Assert.Null(array.GetString(2));
            Assert.Equal("", array.GetString(3));
        }

        [Fact]
        public void AppendRangeBuildsCorrectArray()
        {
            var builder = new LargeStringArray.Builder();
            builder.AppendRange(new[] { "foo", null, "bar", "baz" });

            var array = builder.Build();

            Assert.Equal(4, array.Length);
            Assert.Equal("foo", array.GetString(0));
            Assert.Null(array.GetString(1));
            Assert.Equal("bar", array.GetString(2));
            Assert.Equal("baz", array.GetString(3));
        }

        [Fact]
        public void ClearResetsBuilder()
        {
            var builder = new LargeStringArray.Builder();
            builder.Append("hello");
            builder.Clear();
            builder.Append("world");

            var array = builder.Build();

            Assert.Equal(1, array.Length);
            Assert.Equal("world", array.GetString(0));
        }

        [Fact]
        public void NullCountIsCorrect()
        {
            var builder = new LargeStringArray.Builder();
            builder.Append("a");
            builder.AppendNull();
            builder.Append("b");
            builder.AppendNull();

            var array = builder.Build();

            Assert.Equal(4, array.Length);
            Assert.Equal(2, array.NullCount);
        }
    }
}
