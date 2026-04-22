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

using System.Collections.Generic;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Tests for ReadOnlyListAdapters transparent encoding readers,
    /// covering plain, dictionary-encoded, and run-end encoded arrays
    /// for Int32 and String value types.
    /// </summary>
    public class ReadOnlyListAdapterTests
    {
        // =============================================================
        // Test data helpers
        // =============================================================

        private static Int32Array BuildPlainInt32Array(int?[] values)
        {
            var builder = new Int32Array.Builder();
            foreach (var v in values)
            {
                if (v.HasValue)
                    builder.Append(v.Value);
                else
                    builder.AppendNull();
            }
            return builder.Build();
        }

        private static StringArray BuildPlainStringArray(string[] values)
        {
            var builder = new StringArray.Builder();
            foreach (var v in values)
            {
                if (v != null)
                    builder.Append(v);
                else
                    builder.AppendNull();
            }
            return builder.Build();
        }

        private static DictionaryArray BuildDictionaryInt32Array(int?[] logicalValues)
        {
            // Build a dictionary of unique non-null values
            var uniqueValues = new List<int>();
            var uniqueMap = new Dictionary<int, int>();
            foreach (var v in logicalValues)
            {
                if (v.HasValue && !uniqueMap.ContainsKey(v.Value))
                {
                    uniqueMap[v.Value] = uniqueValues.Count;
                    uniqueValues.Add(v.Value);
                }
            }

            var dictBuilder = new Int32Array.Builder();
            foreach (var v in uniqueValues)
                dictBuilder.Append(v);
            Int32Array dictionary = dictBuilder.Build();

            var indicesBuilder = new Int32Array.Builder();
            foreach (var v in logicalValues)
            {
                if (v.HasValue)
                    indicesBuilder.Append(uniqueMap[v.Value]);
                else
                    indicesBuilder.AppendNull();
            }
            Int32Array indices = indicesBuilder.Build();

            var dictType = new DictionaryType(Int32Type.Default, Int32Type.Default, false);
            return new DictionaryArray(dictType, indices, dictionary);
        }

        private static DictionaryArray BuildDictionaryStringArray(string[] logicalValues)
        {
            var uniqueValues = new List<string>();
            var uniqueMap = new Dictionary<string, int>();
            foreach (var v in logicalValues)
            {
                if (v != null && !uniqueMap.ContainsKey(v))
                {
                    uniqueMap[v] = uniqueValues.Count;
                    uniqueValues.Add(v);
                }
            }

            var dictBuilder = new StringArray.Builder();
            foreach (var v in uniqueValues)
                dictBuilder.Append(v);
            StringArray dictionary = dictBuilder.Build();

            var indicesBuilder = new Int32Array.Builder();
            foreach (var v in logicalValues)
            {
                if (v != null)
                    indicesBuilder.Append(uniqueMap[v]);
                else
                    indicesBuilder.AppendNull();
            }
            Int32Array indices = indicesBuilder.Build();

            var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
            return new DictionaryArray(dictType, indices, dictionary);
        }

        private static RunEndEncodedArray BuildReeInt32Array(int?[] logicalValues)
        {
            // Run-length encode: consecutive equal values form a run
            var runEndsList = new List<int>();
            var valuesList = new List<int?>();

            if (logicalValues.Length > 0)
            {
                int? current = logicalValues[0];
                for (int i = 1; i < logicalValues.Length; i++)
                {
                    if (!Equals(logicalValues[i], current))
                    {
                        runEndsList.Add(i);
                        valuesList.Add(current);
                        current = logicalValues[i];
                    }
                }
                runEndsList.Add(logicalValues.Length);
                valuesList.Add(current);
            }

            var runEndsBuilder = new Int32Array.Builder();
            foreach (var re in runEndsList)
                runEndsBuilder.Append(re);
            Int32Array runEnds = runEndsBuilder.Build();

            Int32Array values = BuildPlainInt32Array(valuesList.ToArray());

            return new RunEndEncodedArray(runEnds, values);
        }

        private static RunEndEncodedArray BuildReeStringArray(string[] logicalValues)
        {
            var runEndsList = new List<int>();
            var valuesList = new List<string>();

            if (logicalValues.Length > 0)
            {
                string current = logicalValues[0];
                for (int i = 1; i < logicalValues.Length; i++)
                {
                    if (logicalValues[i] != current)
                    {
                        runEndsList.Add(i);
                        valuesList.Add(current);
                        current = logicalValues[i];
                    }
                }
                runEndsList.Add(logicalValues.Length);
                valuesList.Add(current);
            }

            var runEndsBuilder = new Int32Array.Builder();
            foreach (var re in runEndsList)
                runEndsBuilder.Append(re);
            Int32Array runEnds = runEndsBuilder.Build();

            StringArray values = BuildPlainStringArray(valuesList.ToArray());

            return new RunEndEncodedArray(runEnds, values);
        }

        // =============================================================
        // Shared test data
        // =============================================================

        private static readonly int?[] Int32Values = new int?[] { 10, 20, 20, null, 30, 30, 30, 10 };
        private static readonly string[] StringValues = new[] { "hello", "world", "world", null, "foo", "foo", "foo", "hello" };

        // =============================================================
        // ReadOnlyListAdapters tests
        // =============================================================

        [Fact]
        public void PlainInt32()
        {
            Int32Array array = BuildPlainInt32Array(Int32Values);
            IReadOnlyList<int?> reader = array.AsDecodedReadOnlyList<int?>();

            // Returns the array itself (zero overhead)
            Assert.Same(array, reader);
            AssertInt32Values(reader);
        }

        [Fact]
        public void DictionaryInt32()
        {
            DictionaryArray array = BuildDictionaryInt32Array(Int32Values);
            IReadOnlyList<int?> reader = array.AsDecodedReadOnlyList<int?>();

            AssertInt32Values(reader);
        }

        [Fact]
        public void ReeInt32()
        {
            RunEndEncodedArray array = BuildReeInt32Array(Int32Values);
            IReadOnlyList<int?> reader = array.AsDecodedReadOnlyList<int?>();

            AssertInt32Values(reader);
        }

        [Fact]
        public void PlainString()
        {
            StringArray array = BuildPlainStringArray(StringValues);
            IReadOnlyList<string> reader = array.AsDecodedReadOnlyList<string>();

            // Returns the array itself (zero overhead)
            Assert.Same(array, reader);
            AssertStringValues(reader);
        }

        [Fact]
        public void DictionaryString()
        {
            DictionaryArray array = BuildDictionaryStringArray(StringValues);
            IReadOnlyList<string> reader = array.AsDecodedReadOnlyList<string>();

            AssertStringValues(reader);
        }

        [Fact]
        public void ReeString()
        {
            RunEndEncodedArray array = BuildReeStringArray(StringValues);
            IReadOnlyList<string> reader = array.AsDecodedReadOnlyList<string>();

            AssertStringValues(reader);
        }

        // =============================================================
        // Edge cases
        // =============================================================

        [Fact]
        public void EmptyArrays()
        {
            var emptyInt = BuildPlainInt32Array(new int?[0]);
            var emptyStr = BuildPlainStringArray(new string[0]);

            Assert.Empty(emptyInt.AsDecodedReadOnlyList<int?>());
            Assert.Empty(emptyStr.AsDecodedReadOnlyList<string>());
        }

        [Fact]
        public void AllNullsInt32()
        {
            var values = new int?[] { null, null, null };

            // Plain
            var plain = BuildPlainInt32Array(values);
            AssertAllNullInt32(plain.AsDecodedReadOnlyList<int?>(), 3);

            // Dictionary
            var dict = BuildDictionaryInt32Array(values);
            AssertAllNullInt32(dict.AsDecodedReadOnlyList<int?>(), 3);
        }

        [Fact]
        public void AllNullsString()
        {
            var values = new string[] { null, null, null };

            var plain = BuildPlainStringArray(values);
            AssertAllNullString(plain.AsDecodedReadOnlyList<string>(), 3);

            var dict = BuildDictionaryStringArray(values);
            AssertAllNullString(dict.AsDecodedReadOnlyList<string>(), 3);
        }

        [Fact]
        public void SingleValueRuns()
        {
            // All same value => single run
            var values = new int?[] { 42, 42, 42, 42, 42 };
            var ree = BuildReeInt32Array(values);

            var reader = ree.AsDecodedReadOnlyList<int?>();

            Assert.Equal(5, reader.Count);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(42, reader[i]);
            }
        }

        [Fact]
        public void EnumerationWorks()
        {
            var array = BuildPlainInt32Array(new int?[] { 1, 2, 3 });

            var list = new List<int?>(array.AsDecodedReadOnlyList<int?>());
            Assert.Equal(new int?[] { 1, 2, 3 }, list.ToArray());
        }

        // =============================================================
        // Sliced array tests
        // =============================================================

        // Full data: { 10, 20, 20, null, 30, 30, 30, 10 }
        // Slice(2, 4) => { 20, null, 30, 30 }

        [Fact]
        public void SlicedPlainInt32()
        {
            Int32Array array = BuildPlainInt32Array(Int32Values);
            var sliced = (Int32Array)array.Slice(2, 4);

            IReadOnlyList<int?> reader = sliced.AsDecodedReadOnlyList<int?>();

            Assert.Same(sliced, reader);
            Assert.Equal(4, reader.Count);
            Assert.Equal(20, reader[0]);
            Assert.Null(reader[1]);
            Assert.Equal(30, reader[2]);
            Assert.Equal(30, reader[3]);
        }

        [Fact]
        public void SlicedDictionaryInt32()
        {
            DictionaryArray array = BuildDictionaryInt32Array(Int32Values);
            // DictionaryArray.Slice returns a new DictionaryArray with sliced indices
            var sliced = (DictionaryArray)ArrowArrayFactory.Slice(array, 2, 4);

            IReadOnlyList<int?> reader = sliced.AsDecodedReadOnlyList<int?>();

            Assert.Equal(4, reader.Count);
            Assert.Equal(20, reader[0]);
            Assert.Null(reader[1]);
            Assert.Equal(30, reader[2]);
            Assert.Equal(30, reader[3]);
        }

        [Fact]
        public void SlicedReeInt32()
        {
            RunEndEncodedArray array = BuildReeInt32Array(Int32Values);
            // REE slice adjusts the offset but keeps the same children
            var sliced = (RunEndEncodedArray)ArrowArrayFactory.Slice(array, 2, 4);

            IReadOnlyList<int?> reader = sliced.AsDecodedReadOnlyList<int?>();

            Assert.Equal(4, reader.Count);
            Assert.Equal(20, reader[0]);
            Assert.Null(reader[1]);
            Assert.Equal(30, reader[2]);
            Assert.Equal(30, reader[3]);
        }

        // Full data: { "hello", "world", "world", null, "foo", "foo", "foo", "hello" }
        // Slice(1, 5) => { "world", "world", null, "foo", "foo" }

        [Fact]
        public void SlicedPlainString()
        {
            StringArray array = BuildPlainStringArray(StringValues);
            var sliced = (StringArray)array.Slice(1, 5);

            IReadOnlyList<string> reader = sliced.AsDecodedReadOnlyList<string>();

            Assert.Same(sliced, reader);
            Assert.Equal(5, reader.Count);
            Assert.Equal("world", reader[0]);
            Assert.Equal("world", reader[1]);
            Assert.Null(reader[2]);
            Assert.Equal("foo", reader[3]);
            Assert.Equal("foo", reader[4]);
        }

        [Fact]
        public void SlicedDictionaryString()
        {
            DictionaryArray array = BuildDictionaryStringArray(StringValues);
            var sliced = (DictionaryArray)ArrowArrayFactory.Slice(array, 1, 5);

            IReadOnlyList<string> reader = sliced.AsDecodedReadOnlyList<string>();

            Assert.Equal(5, reader.Count);
            Assert.Equal("world", reader[0]);
            Assert.Equal("world", reader[1]);
            Assert.Null(reader[2]);
            Assert.Equal("foo", reader[3]);
            Assert.Equal("foo", reader[4]);
        }

        [Fact]
        public void SlicedReeString()
        {
            RunEndEncodedArray array = BuildReeStringArray(StringValues);
            var sliced = (RunEndEncodedArray)ArrowArrayFactory.Slice(array, 1, 5);

            IReadOnlyList<string> reader = sliced.AsDecodedReadOnlyList<string>();

            Assert.Equal(5, reader.Count);
            Assert.Equal("world", reader[0]);
            Assert.Equal("world", reader[1]);
            Assert.Null(reader[2]);
            Assert.Equal("foo", reader[3]);
            Assert.Equal("foo", reader[4]);
        }

        [Fact]
        public void SlicedReeEnumerationIsEfficient()
        {
            // Verify the enumerator produces the same results as indexed access
            RunEndEncodedArray array = BuildReeInt32Array(Int32Values);
            var sliced = (RunEndEncodedArray)ArrowArrayFactory.Slice(array, 2, 4);

            IReadOnlyList<int?> reader = sliced.AsDecodedReadOnlyList<int?>();

            var enumerated = new List<int?>(reader);
            Assert.Equal(4, enumerated.Count);
            for (int i = 0; i < 4; i++)
            {
                Assert.Equal(reader[i], enumerated[i]);
            }
        }

        // =============================================================
        // Assertion helpers
        // =============================================================

        private static void AssertInt32Values(IReadOnlyList<int?> reader)
        {
            Assert.Equal(Int32Values.Length, reader.Count);
            for (int i = 0; i < Int32Values.Length; i++)
            {
                Assert.Equal(Int32Values[i], reader[i]);
            }

            int position = 0;
            foreach (int? value in reader)
            {
                Assert.Equal(Int32Values[position], value);
                position++;
            }
        }

        private static void AssertStringValues(IReadOnlyList<string> reader)
        {
            Assert.Equal(StringValues.Length, reader.Count);
            for (int i = 0; i < StringValues.Length; i++)
            {
                Assert.Equal(StringValues[i], reader[i]);
            }

            int position = 0;
            foreach (string value in reader)
            {
                Assert.Equal(StringValues[position], value);
                position++;
            }
        }

        private static void AssertAllNullInt32(IReadOnlyList<int?> reader, int count)
        {
            Assert.Equal(count, reader.Count);
            for (int i = 0; i < count; i++)
            {
                Assert.Null(reader[i]);
            }

            foreach (int? value in reader)
            {
                Assert.Null(value);
            }
        }

        private static void AssertAllNullString(IReadOnlyList<string> reader, int count)
        {
            Assert.Equal(count, reader.Count);
            for (int i = 0; i < count; i++)
            {
                Assert.Null(reader[i]);
            }

            foreach (string value in reader)
            {
                Assert.Null(value);
            }
        }
    }
}
