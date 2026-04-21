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
    /// Tests for both Design A (ReadOnlyListAdapters) and Design B (ArrayReader)
    /// transparent encoding reader prototypes, covering plain, dictionary-encoded,
    /// and run-end encoded arrays for Int32 and String value types.
    /// </summary>
    public class ArrayReaderTests
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
        // Design A: ReadOnlyListAdapters tests
        // =============================================================

        [Fact]
        public void DesignA_PlainInt32()
        {
            Int32Array array = BuildPlainInt32Array(Int32Values);
            IReadOnlyList<int?> reader = array.AsInt32ReadOnlyList();

            // Returns the array itself (zero overhead)
            Assert.Same(array, reader);
            AssertInt32Values(reader);
        }

        [Fact]
        public void DesignA_DictionaryInt32()
        {
            DictionaryArray array = BuildDictionaryInt32Array(Int32Values);
            IReadOnlyList<int?> reader = array.AsInt32ReadOnlyList();

            AssertInt32Values(reader);
        }

        [Fact]
        public void DesignA_ReeInt32()
        {
            RunEndEncodedArray array = BuildReeInt32Array(Int32Values);
            IReadOnlyList<int?> reader = array.AsInt32ReadOnlyList();

            AssertInt32Values(reader);
        }

        [Fact]
        public void DesignA_PlainString()
        {
            StringArray array = BuildPlainStringArray(StringValues);
            IReadOnlyList<string> reader = array.AsStringReadOnlyList();

            // Returns the array itself (zero overhead)
            Assert.Same(array, reader);
            AssertStringValues(reader);
        }

        [Fact]
        public void DesignA_DictionaryString()
        {
            DictionaryArray array = BuildDictionaryStringArray(StringValues);
            IReadOnlyList<string> reader = array.AsStringReadOnlyList();

            AssertStringValues(reader);
        }

        [Fact]
        public void DesignA_ReeString()
        {
            RunEndEncodedArray array = BuildReeStringArray(StringValues);
            IReadOnlyList<string> reader = array.AsStringReadOnlyList();

            AssertStringValues(reader);
        }

        // =============================================================
        // Design B: ArrayReader<T> tests
        // =============================================================

        [Fact]
        public void DesignB_PlainInt32()
        {
            Int32Array array = BuildPlainInt32Array(Int32Values);
            ArrayReader<int?> reader = ArrayReader.GetInt32Reader(array);

            Assert.Same(array, reader.Array);
            AssertInt32Values(reader);
            Assert.False(reader.IsNull(0));
            Assert.True(reader.IsNull(3));
        }

        [Fact]
        public void DesignB_DictionaryInt32()
        {
            DictionaryArray array = BuildDictionaryInt32Array(Int32Values);
            ArrayReader<int?> reader = ArrayReader.GetInt32Reader(array);

            Assert.Same(array, reader.Array);
            AssertInt32Values(reader);
            Assert.False(reader.IsNull(0));
            Assert.True(reader.IsNull(3));
        }

        [Fact]
        public void DesignB_ReeInt32()
        {
            RunEndEncodedArray array = BuildReeInt32Array(Int32Values);
            ArrayReader<int?> reader = ArrayReader.GetInt32Reader(array);

            Assert.Same(array, reader.Array);
            AssertInt32Values(reader);
            Assert.False(reader.IsNull(0));
            Assert.True(reader.IsNull(3));
        }

        [Fact]
        public void DesignB_PlainString()
        {
            StringArray array = BuildPlainStringArray(StringValues);
            ArrayReader<string> reader = ArrayReader.GetStringReader(array);

            Assert.Same(array, reader.Array);
            AssertStringValues(reader);
            Assert.False(reader.IsNull(0));
            Assert.True(reader.IsNull(3));
        }

        [Fact]
        public void DesignB_DictionaryString()
        {
            DictionaryArray array = BuildDictionaryStringArray(StringValues);
            ArrayReader<string> reader = ArrayReader.GetStringReader(array);

            Assert.Same(array, reader.Array);
            AssertStringValues(reader);
            Assert.False(reader.IsNull(0));
            Assert.True(reader.IsNull(3));
        }

        [Fact]
        public void DesignB_ReeString()
        {
            RunEndEncodedArray array = BuildReeStringArray(StringValues);
            ArrayReader<string> reader = ArrayReader.GetStringReader(array);

            Assert.Same(array, reader.Array);
            AssertStringValues(reader);
            Assert.False(reader.IsNull(0));
            Assert.True(reader.IsNull(3));
        }

        // =============================================================
        // Edge cases
        // =============================================================

        [Fact]
        public void EmptyArrays()
        {
            var emptyInt = BuildPlainInt32Array(new int?[0]);
            var emptyStr = BuildPlainStringArray(new string[0]);

            Assert.Empty(emptyInt.AsInt32ReadOnlyList());
            Assert.Empty(emptyStr.AsStringReadOnlyList());
            Assert.Empty(ArrayReader.GetInt32Reader(emptyInt));
            Assert.Empty(ArrayReader.GetStringReader(emptyStr));
        }

        [Fact]
        public void AllNullsInt32()
        {
            var values = new int?[] { null, null, null };

            // Plain
            var plain = BuildPlainInt32Array(values);
            AssertAllNullInt32(plain.AsInt32ReadOnlyList(), 3);
            AssertAllNullInt32(ArrayReader.GetInt32Reader(plain), 3);

            // Dictionary
            var dict = BuildDictionaryInt32Array(values);
            AssertAllNullInt32(dict.AsInt32ReadOnlyList(), 3);
            AssertAllNullInt32(ArrayReader.GetInt32Reader(dict), 3);
        }

        [Fact]
        public void AllNullsString()
        {
            var values = new string[] { null, null, null };

            var plain = BuildPlainStringArray(values);
            AssertAllNullString(plain.AsStringReadOnlyList(), 3);
            AssertAllNullString(ArrayReader.GetStringReader(plain), 3);

            var dict = BuildDictionaryStringArray(values);
            AssertAllNullString(dict.AsStringReadOnlyList(), 3);
            AssertAllNullString(ArrayReader.GetStringReader(dict), 3);
        }

        [Fact]
        public void SingleValueRuns()
        {
            // All same value => single run
            var values = new int?[] { 42, 42, 42, 42, 42 };
            var ree = BuildReeInt32Array(values);

            var readerA = ree.AsInt32ReadOnlyList();
            var readerB = ArrayReader.GetInt32Reader(ree);

            Assert.Equal(5, readerA.Count);
            Assert.Equal(5, readerB.Count);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(42, readerA[i]);
                Assert.Equal(42, readerB[i]);
            }
        }

        [Fact]
        public void EnumerationWorks()
        {
            var array = BuildPlainInt32Array(new int?[] { 1, 2, 3 });

            // Design A
            var listA = new List<int?>(array.AsInt32ReadOnlyList());
            Assert.Equal(new int?[] { 1, 2, 3 }, listA.ToArray());

            // Design B
            var listB = new List<int?>(ArrayReader.GetInt32Reader(array));
            Assert.Equal(new int?[] { 1, 2, 3 }, listB.ToArray());
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
        }

        private static void AssertStringValues(IReadOnlyList<string> reader)
        {
            Assert.Equal(StringValues.Length, reader.Count);
            for (int i = 0; i < StringValues.Length; i++)
            {
                Assert.Equal(StringValues[i], reader[i]);
            }
        }

        private static void AssertAllNullInt32(IReadOnlyList<int?> reader, int count)
        {
            Assert.Equal(count, reader.Count);
            for (int i = 0; i < count; i++)
            {
                Assert.Null(reader[i]);
            }
        }

        private static void AssertAllNullString(IReadOnlyList<string> reader, int count)
        {
            Assert.Equal(count, reader.Count);
            for (int i = 0; i < count; i++)
            {
                Assert.Null(reader[i]);
            }
        }
    }
}
