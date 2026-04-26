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

using System.Text;
using System.Collections.Generic;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class StringArrayTests
    {
        public class GetString
        {
            [Theory]
            [InlineData(null, null)]
            [InlineData(null, "")]
            [InlineData(null, "value")]
            [InlineData("", null)]
            [InlineData("", "")]
            [InlineData("", "value")]
            [InlineData("value", null)]
            [InlineData("value", "")]
            [InlineData("value", "value")]
            public void ReturnsAppendedValue(string firstValue, string secondValue)
            {
                // Arrange
                // Create an array with two elements. The second element being null,
                // empty, or non-empty may influence the underlying BinaryArray
                // storage such that retrieving an empty first element could result
                // in an empty span or a 0-length span backed by storage.
                var array = new StringArray.Builder()
                    .Append(firstValue)
                    .Append(secondValue)
                    .Build();

                // Act
                var retrievedValue = array.GetString(0);

                // Assert
                Assert.Equal(firstValue, retrievedValue);
            }

            [Theory]
            [InlineData(null, null)]
            [InlineData(null, "")]
            [InlineData(null, "value")]
            [InlineData("", null)]
            [InlineData("", "")]
            [InlineData("", "value")]
            [InlineData("value", null)]
            [InlineData("value", "")]
            [InlineData("value", "value")]
            public void ReturnsAppendedValueMaterialize(string firstValue, string secondValue)
            {
                // Arrange
                // Create an array with two elements. The second element being null,
                // empty, or non-empty may influence the underlying BinaryArray
                // storage such that retrieving an empty first element could result
                // in an empty span or a 0-length span backed by storage.
                var array = new StringArray.Builder()
                    .Append(firstValue)
                    .Append(secondValue)
                    .Build();

                // Act
                array.Materialize();
                var retrievedValue = array.GetString(0);

                // Assert
                Assert.True(array.IsMaterialized());
                Assert.Equal(firstValue, retrievedValue);
            }
        }

        public class Builder
        {
            [Fact]
            public void AppendUsesCustomEncoding()
            {
                const string expected = "héllø";

                var array = new StringArray.Builder()
                    .Append(expected, Encoding.Unicode)
                    .Build();

                Assert.Equal(expected, array.GetString(0, Encoding.Unicode));
            }

            [Fact]
            public void AppendLargeStringUsesFallbackPath()
            {
                string expected = new string('x', 512);

                var array = new StringArray.Builder()
                    .Append(expected)
                    .Build();

                Assert.Equal(expected, array.GetString(0));
            }

            [Fact]
            public void AppendRangePreservesCollectionValues()
            {
                string[] values = { "first", null, string.Empty, "last" };

                var array = new StringArray.Builder()
                    .AppendRange(values)
                    .Build();

                Assert.Equal("first", array.GetString(0));
                Assert.Null(array.GetString(1));
                Assert.Equal(string.Empty, array.GetString(2));
                Assert.Equal("last", array.GetString(3));
            }

            [Fact]
            public void AppendRangePreservesCollectionValuesWithCustomEncoding()
            {
                string[] values = { "héllø", null, string.Empty, "wørld" };

                var array = new StringArray.Builder()
                    .AppendRange(values, Encoding.Unicode)
                    .Build();

                Assert.Equal("héllø", array.GetString(0, Encoding.Unicode));
                Assert.Null(array.GetString(1, Encoding.Unicode));
                Assert.Equal(string.Empty, array.GetString(2, Encoding.Unicode));
                Assert.Equal("wørld", array.GetString(3, Encoding.Unicode));
            }

            [Fact]
            public void AppendRangePreservesEnumerableValues()
            {
                var array = new StringArray.Builder()
                    .AppendRange(YieldValues())
                    .Build();

                Assert.Equal("first", array.GetString(0));
                Assert.Null(array.GetString(1));
                Assert.Equal(string.Empty, array.GetString(2));
                Assert.Equal("last", array.GetString(3));
            }

            private static IEnumerable<string> YieldValues()
            {
                yield return "first";
                yield return null;
                yield return string.Empty;
                yield return "last";
            }
        }
    }
}
