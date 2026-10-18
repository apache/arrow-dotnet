// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Benchmarks
{
    [MemoryDiagnoser]
    public class ArrayConcatenatorBenchmark
    {
        private const int ChunkCount = 10;
        private const int ChunkLength = 100_000;

        private List<IArrowArray> _int32Arrays;
        private List<IArrowArray> _booleanArrays;
        private List<IArrowArray> _stringArrays;
        private List<IArrowArray> _stringViewArrays;
        private List<IArrowArray> _listArrays;
        private List<IArrowArray> _denseUnionArrays;

        /// <summary>
        /// When true, every input is sliced at an odd offset so its bitmaps don't start on a byte boundary.
        /// </summary>
        [Params(false, true)]
        public bool Sliced { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _int32Arrays = new List<IArrowArray>();
            _booleanArrays = new List<IArrowArray>();
            _stringArrays = new List<IArrowArray>();
            _stringViewArrays = new List<IArrowArray>();
            _listArrays = new List<IArrowArray>();
            _denseUnionArrays = new List<IArrowArray>();

            var unionType = new UnionType(
                new[] { new Field("i", Int32Type.Default, true), new Field("s", StringType.Default, true) },
                new[] { 0, 1 },
                UnionMode.Dense);

            for (int chunk = 0; chunk < ChunkCount; chunk++)
            {
                var int32Builder = new Int32Array.Builder();
                var booleanBuilder = new BooleanArray.Builder();
                var stringBuilder = new StringArray.Builder();
                var stringViewBuilder = new StringViewArray.Builder();
                var listBuilder = new ListArray.Builder(Int32Type.Default);
                var listValueBuilder = (Int32Array.Builder)listBuilder.ValueBuilder;
                var unionTypeIds = new ArrowBuffer.Builder<byte>();
                var unionOffsets = new ArrowBuffer.Builder<int>();
                var unionInts = new Int32Array.Builder();
                var unionStrings = new StringArray.Builder();

                for (int i = 0; i < ChunkLength; i++)
                {
                    bool isNull = i % 7 == 3;
                    if (isNull)
                    {
                        int32Builder.AppendNull();
                        booleanBuilder.AppendNull();
                        stringBuilder.AppendNull();
                        stringViewBuilder.AppendNull();
                        listBuilder.AppendNull();
                    }
                    else
                    {
                        int32Builder.Append(i);
                        booleanBuilder.Append(i % 3 == 0);
                        string value = i % 2 == 0 ? "short" : "a somewhat longer string value";
                        stringBuilder.Append(value);
                        stringViewBuilder.Append(value);
                        listBuilder.Append();
                        listValueBuilder.Append(i);
                        listValueBuilder.Append(i + 1);
                    }

                    if (i % 2 == 0)
                    {
                        unionTypeIds.Append(0);
                        unionOffsets.Append(unionInts.Length);
                        unionInts.Append(i);
                    }
                    else
                    {
                        unionTypeIds.Append(1);
                        unionOffsets.Append(unionStrings.Length);
                        unionStrings.Append("x");
                    }
                }

                var union = new DenseUnionArray(
                    unionType,
                    ChunkLength,
                    new IArrowArray[] { unionInts.Build(), unionStrings.Build() },
                    unionTypeIds.Build(),
                    unionOffsets.Build());

                Add(_int32Arrays, int32Builder.Build());
                Add(_booleanArrays, booleanBuilder.Build());
                Add(_stringArrays, stringBuilder.Build());
                Add(_stringViewArrays, stringViewBuilder.Build());
                Add(_listArrays, listBuilder.Build());
                Add(_denseUnionArrays, union);
            }
        }

        private void Add(List<IArrowArray> arrays, IArrowArray array)
        {
            arrays.Add(Sliced ? ArrowArrayFactory.Slice(array, 3, array.Length - 5) : array);
        }

        [Benchmark]
        public int Int32() => Concatenate(_int32Arrays);

        [Benchmark]
        public int Boolean() => Concatenate(_booleanArrays);

        [Benchmark]
        public int String() => Concatenate(_stringArrays);

        [Benchmark]
        public int StringView() => Concatenate(_stringViewArrays);

        [Benchmark]
        public int List() => Concatenate(_listArrays);

        [Benchmark]
        public int DenseUnion() => Concatenate(_denseUnionArrays);

        private static int Concatenate(List<IArrowArray> arrays)
        {
            using IArrowArray result = ArrowArrayConcatenator.Concatenate(arrays);
            return result.Length;
        }
    }
}
