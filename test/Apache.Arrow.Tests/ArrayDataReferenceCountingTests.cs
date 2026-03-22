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
using Apache.Arrow.C;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrayDataReferenceCountingTests
    {
        private static Int32Array BuildInt32Array(params int[] values)
        {
            var builder = new Int32Array.Builder();
            builder.AppendRange(values);
            return builder.Build();
        }

        private static Int32Array BuildInt32Array(MemoryAllocator allocator, params int[] values)
        {
            var builder = new Int32Array.Builder();
            builder.AppendRange(values);
            return builder.Build(allocator);
        }

        [Fact]
        public void AcquireAndDispose_SingleOwner()
        {
            var array = BuildInt32Array(1, 2, 3);
            var data = array.Data;

            // After build, data is usable
            Assert.Equal(3, data.Length);

            // Dispose releases the data
            array.Dispose();
        }

        [Fact]
        public void AcquireAndDispose_TwoOwners()
        {
            var array = BuildInt32Array(1, 2, 3);
            var data = array.Data;

            // Acquire a second reference
            var shared = data.Acquire();
            Assert.Same(data, shared);

            // First dispose does not free the buffers (ref count goes from 2 to 1)
            array.Dispose();

            // Data is still usable via the shared reference
            Assert.Equal(3, shared.Length);
            Assert.Equal(Int32Type.Default, shared.DataType);

            // Verify we can still read the buffer contents
            var span = shared.Buffers[1].Span;
            Assert.True(span.Length > 0);

            // Second dispose frees the buffers (ref count goes from 1 to 0)
            shared.Dispose();
        }

        [Fact]
        public void Acquire_ThrowsOnDisposed()
        {
            var array = BuildInt32Array(1, 2, 3);
            var data = array.Data;

            array.Dispose();

            Assert.Throws<ObjectDisposedException>(() => data.Acquire());
        }

        [Fact]
        public void SliceShared_KeepsParentAlive()
        {
            var array = BuildInt32Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var sliced = array.Data.SliceShared(2, 5);

            // Dispose the original — the slice keeps the parent alive
            array.Dispose();

            // Sliced data should still be usable
            Assert.Equal(5, sliced.Length);
            Assert.Equal(2, sliced.Offset);
            var span = sliced.Buffers[1].Span;
            Assert.True(span.Length > 0);

            // Disposing the slice releases the parent
            sliced.Dispose();
        }

        [Fact]
        public void SliceShared_OfSliceShared_PointsToRoot()
        {
            var array = BuildInt32Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var slice1 = array.Data.SliceShared(2, 6);
            var slice2 = slice1.SliceShared(1, 3);

            // Dispose original and first slice
            array.Dispose();
            slice1.Dispose();

            // Second slice keeps the root alive
            Assert.Equal(3, slice2.Length);
            Assert.Equal(3, slice2.Offset); // 2 + 1

            var span = slice2.Buffers[1].Span;
            Assert.True(span.Length > 0);

            slice2.Dispose();
        }

        [Fact]
        public void SliceShared_DisposeSliceFirst_ThenOriginal()
        {
            var array = BuildInt32Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var sliced = array.Data.SliceShared(3, 4);

            // Disposing the slice first is fine — it just decrements the parent ref
            sliced.Dispose();

            // Original is still valid
            Assert.Equal(10, array.Length);
            Assert.Equal(0, array.GetValue(0));

            array.Dispose();
        }

        [Fact]
        public void ShareColumnsBetweenRecordBatches()
        {
            // Build a record batch with two columns
            var col1 = BuildInt32Array(1, 2, 3);
            var col2 = BuildInt32Array(4, 5, 6);

            var schema = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Field(new Field("b", Int32Type.Default, false))
                .Build();

            var batch1 = new RecordBatch(schema, new IArrowArray[] { col1, col2 }, 3);

            // Share column "a" into a new batch with a different column "c"
            var sharedA = batch1.Column(0).Data.Acquire();
            var col3 = BuildInt32Array(7, 8, 9);

            var schema2 = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Field(new Field("c", Int32Type.Default, false))
                .Build();

            var batch2 = new RecordBatch(schema2, new IArrowArray[]
            {
                ArrowArrayFactory.BuildArray(sharedA),
                col3,
            }, 3);

            // Dispose original batch — shared column should stay alive in batch2
            batch1.Dispose();

            // Verify batch2's column "a" is still readable
            var aArray = (Int32Array)batch2.Column(0);
            Assert.Equal(3, aArray.Length);
            Assert.Equal(1, aArray.GetValue(0));
            Assert.Equal(2, aArray.GetValue(1));
            Assert.Equal(3, aArray.GetValue(2));

            batch2.Dispose();
        }

        [Fact]
        public unsafe void ExportArray_OriginalRemainsValid()
        {
            var array = BuildInt32Array(10, 20, 30);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);

            // Original array should still be valid after export
            Assert.Equal(3, array.Length);
            Assert.Equal(10, array.GetValue(0));
            Assert.Equal(20, array.GetValue(1));
            Assert.Equal(30, array.GetValue(2));

            // Import the exported copy and verify it
            using (var imported = (Int32Array)CArrowArrayImporter.ImportArray(cArray, array.Data.DataType))
            {
                Assert.Equal(3, imported.Length);
                Assert.Equal(10, imported.GetValue(0));
                Assert.Equal(20, imported.GetValue(1));
                Assert.Equal(30, imported.GetValue(2));
            }

            // Original should still be usable after import is disposed
            Assert.Equal(10, array.GetValue(0));

            array.Dispose();
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void ExportRecordBatch_OriginalRemainsValid()
        {
            var col1 = BuildInt32Array(1, 2, 3);
            var col2 = BuildInt32Array(4, 5, 6);

            var schema = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Field(new Field("b", Int32Type.Default, false))
                .Build();

            var batch = new RecordBatch(schema, new IArrowArray[] { col1, col2 }, 3);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            // Original batch should still be valid
            Assert.Equal(3, batch.Length);
            var a = (Int32Array)batch.Column(0);
            Assert.Equal(1, a.GetValue(0));
            Assert.Equal(2, a.GetValue(1));
            Assert.Equal(3, a.GetValue(2));

            // Import and verify the exported copy
            using (var imported = CArrowArrayImporter.ImportRecordBatch(cArray, schema))
            {
                Assert.Equal(3, imported.Length);
                var importedA = (Int32Array)imported.Column(0);
                Assert.Equal(1, importedA.GetValue(0));
            }

            // Original still usable
            Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(0));

            batch.Dispose();
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void ExportSlicedArray_OriginalRemainsValid()
        {
            var array = BuildInt32Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            IArrowArray sliced = array.Slice(2, 6);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(sliced, cArray);

            // Original array should still be valid
            Assert.Equal(10, array.Length);
            Assert.Equal(0, array.GetValue(0));

            // Import the sliced export
            using (var imported = (Int32Array)CArrowArrayImporter.ImportArray(cArray, array.Data.DataType))
            {
                Assert.Equal(6, imported.Length);
                Assert.Equal(2, imported.GetValue(0));
            }

            sliced.Dispose();
            array.Dispose();
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void ExportArray_DisposeOriginalBeforeImportRelease()
        {
            // Verify that disposing the original C# array before the C consumer
            // releases the export does not cause issues.
            var array = BuildInt32Array(10, 20, 30);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);

            // Dispose the original first
            array.Dispose();

            // The export should still be valid — the ref count keeps the data alive
            using (var imported = (Int32Array)CArrowArrayImporter.ImportArray(cArray, Int32Type.Default))
            {
                Assert.Equal(3, imported.Length);
                Assert.Equal(10, imported.GetValue(0));
                Assert.Equal(20, imported.GetValue(1));
                Assert.Equal(30, imported.GetValue(2));
            }

            CArrowArray.Free(cArray);
        }

        // ---------------------------------------------------------------
        // Array.SliceShared tests
        // ---------------------------------------------------------------

        [Fact]
        public void Array_SliceShared_KeepsParentAlive()
        {
            var array = BuildInt32Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var sliced = (Int32Array)array.SliceShared(2, 5);

            array.Dispose();

            Assert.Equal(5, sliced.Length);
            Assert.Equal(2, sliced.GetValue(0));
            Assert.Equal(6, sliced.GetValue(4));

            sliced.Dispose();
        }

        [Fact]
        public void Array_SliceShared_DisposeSliceFirst()
        {
            var array = BuildInt32Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var sliced = array.SliceShared(3, 4);

            sliced.Dispose();

            Assert.Equal(10, array.Length);
            Assert.Equal(0, array.GetValue(0));

            array.Dispose();
        }

        // ---------------------------------------------------------------
        // RecordBatch.SliceShared tests
        // ---------------------------------------------------------------

        [Fact]
        public void RecordBatch_SliceShared_KeepsParentAlive()
        {
            var col1 = BuildInt32Array(0, 1, 2, 3, 4);
            var col2 = BuildInt32Array(10, 11, 12, 13, 14);
            var schema = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Field(new Field("b", Int32Type.Default, false))
                .Build();

            var batch = new RecordBatch(schema, new IArrowArray[] { col1, col2 }, 5);
            var sliced = batch.SliceShared(1, 3);

            batch.Dispose();

            Assert.Equal(3, sliced.Length);
            var a = (Int32Array)sliced.Column(0);
            var b = (Int32Array)sliced.Column(1);
            Assert.Equal(1, a.GetValue(0));
            Assert.Equal(3, a.GetValue(2));
            Assert.Equal(11, b.GetValue(0));
            Assert.Equal(13, b.GetValue(2));

            sliced.Dispose();
        }

        [Fact]
        public void RecordBatch_SliceShared_DisposeSliceFirst()
        {
            var col1 = BuildInt32Array(0, 1, 2, 3, 4);
            var schema = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Build();

            var batch = new RecordBatch(schema, new IArrowArray[] { col1 }, 5);
            var sliced = batch.SliceShared(1, 2);

            sliced.Dispose();

            Assert.Equal(5, batch.Length);
            Assert.Equal(0, ((Int32Array)batch.Column(0)).GetValue(0));

            batch.Dispose();
        }

        // ---------------------------------------------------------------
        // ChunkedArray.SliceShared tests
        // ---------------------------------------------------------------

        [Fact]
        public void ChunkedArray_SliceShared_KeepsParentAlive()
        {
            var chunk1 = BuildInt32Array(0, 1, 2);
            var chunk2 = BuildInt32Array(3, 4, 5);
            var chunked = new ChunkedArray(new IArrowArray[] { chunk1, chunk2 });
            var sliced = chunked.SliceShared(1, 4);

            chunk1.Dispose();
            chunk2.Dispose();

            Assert.Equal(4, sliced.Length);
            var arr0 = (Int32Array)sliced.ArrowArray(0);
            Assert.Equal(1, arr0.GetValue(0));

            sliced.Dispose();
        }

        [Fact]
        public void ChunkedArray_SliceShared_SingleArgOverload()
        {
            var chunk1 = BuildInt32Array(0, 1, 2, 3, 4);
            var chunked = new ChunkedArray(new IArrowArray[] { chunk1 });
            var sliced = chunked.SliceShared(2);

            chunk1.Dispose();

            Assert.Equal(3, sliced.Length);
            var arr = (Int32Array)sliced.ArrowArray(0);
            Assert.Equal(2, arr.GetValue(0));
            Assert.Equal(4, arr.GetValue(2));

            sliced.Dispose();
        }

        // ---------------------------------------------------------------
        // Column.SliceShared tests
        // ---------------------------------------------------------------

        [Fact]
        public void Column_SliceShared_KeepsParentAlive()
        {
            var array = BuildInt32Array(0, 1, 2, 3, 4);
            var field = new Field("x", Int32Type.Default, false);
            var column = new Column(field, new IArrowArray[] { array });
            var sliced = column.SliceShared(1, 3);

            array.Dispose();

            Assert.Equal(3, sliced.Length);
            var arr = (Int32Array)sliced.Data.ArrowArray(0);
            Assert.Equal(1, arr.GetValue(0));
            Assert.Equal(3, arr.GetValue(2));

            sliced.Dispose();
        }

        [Fact]
        public void Column_SliceShared_SingleArgOverload()
        {
            var array = BuildInt32Array(10, 20, 30, 40);
            var field = new Field("x", Int32Type.Default, false);
            var column = new Column(field, new IArrowArray[] { array });
            var sliced = column.SliceShared(2);

            array.Dispose();

            Assert.Equal(2, sliced.Length);
            var arr = (Int32Array)sliced.Data.ArrowArray(0);
            Assert.Equal(30, arr.GetValue(0));
            Assert.Equal(40, arr.GetValue(1));

            sliced.Dispose();
        }

        // ---------------------------------------------------------------
        // Tracking-allocator tests: verify no leaks and no double-frees.
        // TestMemoryAllocator tracks outstanding allocations via Rented,
        // and throws ObjectDisposedException on double-free.
        // ---------------------------------------------------------------

        [Fact]
        public void Tracked_SingleOwner_FreesAll()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 1, 2, 3);
            Assert.True(allocator.Rented > 0);

            array.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_Acquire_FreesOnlyWhenLastRefDisposed()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 1, 2, 3);
            var shared = array.Data.Acquire();

            array.Dispose();
            Assert.True(allocator.Rented > 0); // Still held by shared ref

            shared.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_MultipleAcquires_FreesOnlyWhenAllDisposed()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 1, 2, 3);
            var ref1 = array.Data.Acquire();
            var ref2 = array.Data.Acquire();

            array.Dispose();
            Assert.True(allocator.Rented > 0);

            ref1.Dispose();
            Assert.True(allocator.Rented > 0);

            ref2.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_SliceShared_FreesWhenSliceDisposed()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var sliced = array.Data.SliceShared(2, 5);

            array.Dispose();
            Assert.True(allocator.Rented > 0); // Slice keeps parent alive

            sliced.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_SliceShared_DisposeSliceFirst()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var sliced = array.Data.SliceShared(3, 4);

            sliced.Dispose();
            Assert.True(allocator.Rented > 0); // Original still holds buffers

            array.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_ChainedSliceShared_FreesWhenLastDisposed()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var slice1 = array.Data.SliceShared(2, 6);
            var slice2 = slice1.SliceShared(1, 3);

            array.Dispose();
            Assert.True(allocator.Rented > 0);

            slice1.Dispose();
            Assert.True(allocator.Rented > 0);

            slice2.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_ShareColumnsBetweenBatches_FreesAll()
        {
            var allocator = new TestMemoryAllocator();
            var col1 = BuildInt32Array(allocator, 1, 2, 3);
            var col2 = BuildInt32Array(allocator, 4, 5, 6);

            var schema = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Field(new Field("b", Int32Type.Default, false))
                .Build();

            var batch1 = new RecordBatch(schema, new IArrowArray[] { col1, col2 }, 3);

            var sharedA = batch1.Column(0).Data.Acquire();
            var col3 = BuildInt32Array(allocator, 7, 8, 9);

            var schema2 = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Field(new Field("c", Int32Type.Default, false))
                .Build();

            var batch2 = new RecordBatch(schema2, new IArrowArray[]
            {
                ArrowArrayFactory.BuildArray(sharedA),
                col3,
            }, 3);

            batch1.Dispose();
            Assert.True(allocator.Rented > 0); // shared column + col3 still alive

            batch2.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public unsafe void Tracked_ExportArray_FreesAfterBothSidesDispose()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 10, 20, 30);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);

            // Dispose C# side first
            array.Dispose();
            Assert.True(allocator.Rented > 0); // Export keeps data alive

            // Release the C side via import + dispose
            using (var imported = CArrowArrayImporter.ImportArray(cArray, Int32Type.Default))
            {
                Assert.Equal(10, ((Int32Array)imported).GetValue(0));
            }

            Assert.Equal(0, allocator.Rented);
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void Tracked_ExportArray_ReleaseExportFirst()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 10, 20, 30);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportArray(array, cArray);

            // Release the C side first
            using (var imported = CArrowArrayImporter.ImportArray(cArray, Int32Type.Default))
            {
                Assert.Equal(10, ((Int32Array)imported).GetValue(0));
            }
            Assert.True(allocator.Rented > 0); // C# side still holds data

            // Now dispose the original
            array.Dispose();
            Assert.Equal(0, allocator.Rented);
            CArrowArray.Free(cArray);
        }

        [Fact]
        public unsafe void Tracked_ExportRecordBatch_FreesAll()
        {
            var allocator = new TestMemoryAllocator();
            var col1 = BuildInt32Array(allocator, 1, 2, 3);
            var col2 = BuildInt32Array(allocator, 4, 5, 6);

            var schema = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Field(new Field("b", Int32Type.Default, false))
                .Build();

            var batch = new RecordBatch(schema, new IArrowArray[] { col1, col2 }, 3);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            batch.Dispose();
            Assert.True(allocator.Rented > 0);

            using (var imported = CArrowArrayImporter.ImportRecordBatch(cArray, schema))
            {
                Assert.Equal(3, imported.Length);
            }

            Assert.Equal(0, allocator.Rented);
            CArrowArray.Free(cArray);
        }
        [Fact]
        public void Tracked_Array_SliceShared_FreesWhenSliceDisposed()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            var sliced = array.SliceShared(2, 5);

            array.Dispose();
            Assert.True(allocator.Rented > 0);

            sliced.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_RecordBatch_SliceShared_FreesWhenSliceDisposed()
        {
            var allocator = new TestMemoryAllocator();
            var col1 = BuildInt32Array(allocator, 0, 1, 2, 3, 4);
            var col2 = BuildInt32Array(allocator, 10, 11, 12, 13, 14);
            var schema = new Schema.Builder()
                .Field(new Field("a", Int32Type.Default, false))
                .Field(new Field("b", Int32Type.Default, false))
                .Build();

            var batch = new RecordBatch(schema, new IArrowArray[] { col1, col2 }, 5);
            var sliced = batch.SliceShared(1, 3);

            batch.Dispose();
            Assert.True(allocator.Rented > 0);

            sliced.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_ChunkedArray_SliceShared_FreesWhenSliceDisposed()
        {
            var allocator = new TestMemoryAllocator();
            var chunk1 = BuildInt32Array(allocator, 0, 1, 2);
            var chunk2 = BuildInt32Array(allocator, 3, 4, 5);
            var chunked = new ChunkedArray(new IArrowArray[] { chunk1, chunk2 });
            var sliced = chunked.SliceShared(1, 4);

            chunk1.Dispose();
            chunk2.Dispose();
            Assert.True(allocator.Rented > 0);

            sliced.Dispose();
            Assert.Equal(0, allocator.Rented);
        }

        [Fact]
        public void Tracked_Column_SliceShared_FreesWhenSliceDisposed()
        {
            var allocator = new TestMemoryAllocator();
            var array = BuildInt32Array(allocator, 0, 1, 2, 3, 4);
            var field = new Field("x", Int32Type.Default, false);
            var column = new Column(field, new IArrowArray[] { array });
            var sliced = column.SliceShared(1, 3);

            array.Dispose();
            Assert.True(allocator.Rented > 0);

            sliced.Dispose();
            Assert.Equal(0, allocator.Rented);
        }
    }
}
