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

namespace Apache.Arrow.Tests
{
    public class LargeBinaryArrayBuilderTests
    {
        [Fact]
        public void AppendByteSpanBuildsCorrectArray()
        {
            var builder = new LargeBinaryArray.Builder();
            builder.Append(new byte[] { 1, 2, 3 }.AsSpan());
            builder.Append(new byte[] { 4, 5 }.AsSpan());
            builder.AppendNull();
            builder.Append(ReadOnlySpan<byte>.Empty);

            var array = builder.Build();

            Assert.Equal(4, array.Length);
            Assert.Equal(new byte[] { 1, 2, 3 }, array.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { 4, 5 }, array.GetBytes(1).ToArray());
            Assert.True(array.IsNull(2));
            Assert.Empty(array.GetBytes(3).ToArray());
        }

        [Fact]
        public void AppendSingleByteBuildsCorrectArray()
        {
            var builder = new LargeBinaryArray.Builder();
            builder.Append((byte)10);
            builder.Append((byte)20);

            var array = builder.Build();

            Assert.Equal(2, array.Length);
            Assert.Equal(new byte[] { 10 }, array.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { 20 }, array.GetBytes(1).ToArray());
        }

        [Fact]
        public void AppendRangeByteArraysBuildsCorrectArray()
        {
            var builder = new LargeBinaryArray.Builder();
            builder.AppendRange(new byte[][]
            {
                new byte[] { 1, 2, 3 },
                null,
                new byte[] { 4, 5 },
            });

            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(new byte[] { 1, 2, 3 }, array.GetBytes(0).ToArray());
            Assert.True(array.IsNull(1));
            Assert.Equal(new byte[] { 4, 5 }, array.GetBytes(2).ToArray());
        }

        [Fact]
        public void ClearResetsBuilder()
        {
            var builder = new LargeBinaryArray.Builder();
            builder.Append(new byte[] { 1, 2, 3 }.AsSpan());
            builder.Clear();
            builder.Append(new byte[] { 4, 5 }.AsSpan());

            var array = builder.Build();

            Assert.Equal(1, array.Length);
            Assert.Equal(new byte[] { 4, 5 }, array.GetBytes(0).ToArray());
        }

        [Fact]
        public void LengthTracksAppendedItems()
        {
            var builder = new LargeBinaryArray.Builder();
            Assert.Equal(0, builder.Length);

            builder.Append(new byte[] { 1 }.AsSpan());
            Assert.Equal(1, builder.Length);

            builder.AppendNull();
            Assert.Equal(2, builder.Length);

            builder.Append((byte)5);
            Assert.Equal(3, builder.Length);
        }

        [Fact]
        public void NullCountIsCorrect()
        {
            var builder = new LargeBinaryArray.Builder();
            builder.Append(new byte[] { 1 }.AsSpan());
            builder.AppendNull();
            builder.Append(new byte[] { 2 }.AsSpan());
            builder.AppendNull();

            var array = builder.Build();

            Assert.Equal(4, array.Length);
            Assert.Equal(2, array.NullCount);
        }
    }
}
