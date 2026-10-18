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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class CDataInterfaceDataTests
    {
        private IArrowArray GetTestArray()
        {
            var builder = new StringArray.Builder();
            builder.Append("hello");
            builder.Append("world");
            builder.AppendNull();
            builder.Append("foo");
            builder.Append("bar");
            return builder.Build();
        }

        // Zero-length arrays whose offsets buffers (including those of nested children) are empty,
        // as they are when read from IPC written by Arrow C++ or nanoarrow.
        internal static IEnumerable<IArrowArray> GetZeroLengthArraysWithEmptyOffsets()
        {
            IArrowArray[] arrays =
            {
                new StringArray.Builder().Build(),
                new BinaryArray.Builder().Build(),
                new LargeStringArray.Builder().Build(),
                new LargeBinaryArray.Builder().Build(),
                new ListArray.Builder(StringType.Default).Build(),
                new LargeListArray.Builder(Int32Type.Default).Build(),
                new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default)).Build(),
            };
            return arrays.Select(array => ArrowArrayFactory.BuildArray(WithEmptyOffsets(array.Data)));
        }

        private static ArrayData WithEmptyOffsets(ArrayData data)
        {
            ArrowBuffer[] buffers = data.Buffers.ToArray();
            switch (data.DataType.TypeId)
            {
                case ArrowTypeId.Binary:
                case ArrowTypeId.String:
                case ArrowTypeId.LargeBinary:
                case ArrowTypeId.LargeString:
                case ArrowTypeId.List:
                case ArrowTypeId.LargeList:
                case ArrowTypeId.Map:
                    buffers[1] = ArrowBuffer.Empty;
                    break;
            }
            ArrayData[] children = data.Children?.Select(WithEmptyOffsets).ToArray();
            return new ArrayData(data.DataType, data.Length, data.NullCount, data.Offset, buffers, children);
        }

        private static long[] GetValueOffsets(IArrowArray array) => array switch
        {
            BinaryArray a => a.ValueOffsets.ToArray().Select(o => (long)o).ToArray(),
            LargeBinaryArray a => a.ValueOffsets.ToArray(),
            ListArray a => a.ValueOffsets.ToArray().Select(o => (long)o).ToArray(),
            LargeListArray a => a.ValueOffsets.ToArray(),
            _ => throw new ArgumentException($"Unexpected array type {array.GetType()}"),
        };

        [Fact]
        public void ValueOffsetsOfZeroLengthArrayWithEmptyOffsets()
        {
            foreach (IArrowArray array in GetZeroLengthArraysWithEmptyOffsets())
            {
                Assert.Equal(new long[] { 0 }, GetValueOffsets(array));
            }
        }

        [Fact]
        public unsafe void ExportZeroLengthArrayWithEmptyOffsets()
        {
            foreach (IArrowArray array in GetZeroLengthArraysWithEmptyOffsets())
            {
                CArrowArray* cArray = CArrowArray.Create();
                CArrowArrayExporter.ExportArray(array, cArray);

                // Consumers expect a single zero offset, not a null pointer
                Assert.True(cArray->buffers[1] != null, $"{array.Data.DataType.Name} offsets were not exported");
                Assert.Equal(0, *(int*)cArray->buffers[1]);

                using (IArrowArray imported = CArrowArrayImporter.ImportArray(cArray, array.Data.DataType))
                {
                    Assert.Equal(0, imported.Length);
                    Assert.Equal(new long[] { 0 }, GetValueOffsets(imported));
                }
                CArrowArray.Free(cArray);
            }
        }

        [Fact]
        public unsafe void ImportZeroLengthArrayWithNullOffsets()
        {
            foreach (IArrowArray array in GetZeroLengthArraysWithEmptyOffsets())
            {
                CArrowArray* cArray = CArrowArray.Create();
                CArrowArrayExporter.ExportArray(array, cArray);

                // The C Data interface allows a null pointer where the buffer would be empty,
                // and some producers export a zero-length array's offsets that way.
                cArray->buffers[1] = null;

                using (IArrowArray imported = CArrowArrayImporter.ImportArray(cArray, array.Data.DataType))
                {
                    Assert.Equal(0, imported.Length);
                    Assert.Equal(new long[] { 0 }, GetValueOffsets(imported));
                }
                CArrowArray.Free(cArray);
            }
        }

        [Fact]
        public unsafe void ImportNonEmptyArrayWithNullOffsetsThrows()
        {
            IArrowArray array = GetTestArray();
            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);
            cArray->buffers[1] = null;

            Assert.Throws<InvalidOperationException>(() => CArrowArrayImporter.ImportArray(cArray, array.Data.DataType));
            Assert.True(cArray->release == default);
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void InitializeArrayZeroed()
        {
            CArrowArray* cArray = CArrowArray.Create();

            Assert.Equal(0, cArray->length);
            Assert.Equal(0, cArray->null_count);
            Assert.Equal(0, cArray->offset);
            Assert.Equal(0, cArray->n_buffers);
            Assert.Equal(0, cArray->n_children);
            Assert.True(cArray->buffers == null);
            Assert.True(cArray->children == null);
            Assert.True(cArray->dictionary == null);
            Assert.True(cArray->release == default);
            Assert.True(cArray->private_data == null);

            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void CallsReleaseForValid()
        {
            IArrowArray array = GetTestArray();
            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);
            Assert.False(cArray->release == default);
            CArrowArrayImporter.ImportArray(cArray, array.Data.DataType).Dispose();
            Assert.True(cArray->release == default);
            CArrowArray.Free(cArray);
        }

#if NET5_0_OR_GREATER
        [Fact]
        public unsafe void CallsReleaseForInvalid()
        {
            // Make sure we call release callback, even if the imported array is invalid.
            CArrowArray* cArray = CArrowArray.Create();

            bool wasCalled = false;
            var releaseCallback = (CArrowArray* cArray) =>
            {
                wasCalled = true;
                cArray->release = default;
            };
            cArray->release = (delegate* unmanaged<CArrowArray*, void>)Marshal.GetFunctionPointerForDelegate(
                releaseCallback);

            Assert.Throws<InvalidOperationException>(() =>
            {
                CArrowArrayImporter.ImportArray(cArray, GetTestArray().Data.DataType);
            });
            Assert.True(wasCalled);

            CArrowArray.Free(cArray);

            GC.KeepAlive(releaseCallback);
        }
#endif

        [Fact]
        public unsafe void RoundTripInt32ArrayWithOffset()
        {
            Int32Array array = new Int32Array.Builder()
                .AppendRange(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 })
                .Build();
            IArrowArray sliced = array.Slice(2, 6);
            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(sliced, cArray);
            using (var importedSlice = (Int32Array)CArrowArrayImporter.ImportArray(cArray, array.Data.DataType))
            {
                Assert.Equal(6, importedSlice.Length);
                Assert.Equal(2, importedSlice.Offset);
                Assert.Equal(2, importedSlice.GetValue(0));
            }
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void ExportArrayPreservesSource()
        {
            Int32Array array = new Int32Array.Builder()
                .AppendRange(new[] { 10, 20, 30, 40, 50 })
                .Build();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);

            // Source array should still be valid after export
            Assert.Equal(5, array.Length);
            Assert.Equal(10, array.GetValue(0));
            Assert.Equal(50, array.GetValue(4));

            // Import and verify the exported copy is also valid
            using (var imported = (Int32Array)CArrowArrayImporter.ImportArray(cArray, array.Data.DataType))
            {
                Assert.Equal(5, imported.Length);
                Assert.Equal(10, imported.GetValue(0));
                Assert.Equal(50, imported.GetValue(4));
            }

            // Source should still be valid after the imported copy is disposed
            Assert.Equal(30, array.GetValue(2));

            CArrowArray.Free(cArray);
            array.Dispose();
        }

        [Fact]
        public unsafe void ExportRecordBatch_LargerThan2GB_Succeeds()
        {
            RecordBatch GetTestRecordBatch()
            {
                const int rows = 50_000;
                var doubles = new double[rows];
                for (var i = 0; i < rows; ++i)
                {
                    doubles[i] = i * 1.1;
                }

                var batchBuilder = new RecordBatch.Builder();
                for (var i = 0; i < 10_000; i++)
                {
                    batchBuilder.Append($"doubles{i}", true, ab => ab.Double(b => b.Append(doubles)));
                }

                return batchBuilder.Build();
            }

            RecordBatch batch = GetTestRecordBatch();

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            CArrowArray.Free(cArray);
        }
    }
}
