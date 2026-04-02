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
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    [Experimental("ArrowDeviceDataApi")]
    public class CDeviceDataInterfaceTests
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

        private RecordBatch GetTestRecordBatch()
        {
            return new RecordBatch.Builder()
                .Append("strings", false, col => col.String(arr =>
                {
                    arr.Append("hello");
                    arr.Append("world");
                    arr.AppendNull();
                }))
                .Append("ints", false, col => col.Int32(arr =>
                {
                    arr.Append(1);
                    arr.Append(2);
                    arr.Append(3);
                }))
                .Build();
        }

        [Fact]
        public unsafe void InitializeDeviceArrayZeroed()
        {
            CArrowDeviceArray* ptr = CArrowDeviceArray.Create();

            Assert.Equal(0, ptr->array.length);
            Assert.Equal(0, ptr->array.null_count);
            Assert.Equal(0, ptr->array.offset);
            Assert.Equal(0, ptr->array.n_buffers);
            Assert.Equal(0, ptr->array.n_children);
            Assert.True(ptr->array.buffers == null);
            Assert.True(ptr->array.children == null);
            Assert.True(ptr->array.dictionary == null);
            Assert.True(ptr->array.release == default);
            Assert.True(ptr->array.private_data == null);
            Assert.Equal(0, ptr->device_id);
            Assert.Equal((ArrowDeviceType)0, ptr->device_type);
            Assert.True(ptr->sync_event == null);

            CArrowDeviceArray.Free(ptr);
        }

        [Fact]
        public unsafe void InitializeDeviceArrayStreamZeroed()
        {
            CArrowDeviceArrayStream* ptr = CArrowDeviceArrayStream.Create();

            Assert.Equal((ArrowDeviceType)0, ptr->device_type);
            Assert.True(ptr->get_schema == default);
            Assert.True(ptr->get_next == default);
            Assert.True(ptr->get_last_error == default);
            Assert.True(ptr->release == default);
            Assert.True(ptr->private_data == null);

            CArrowDeviceArrayStream.Free(ptr);
        }

        [Fact]
        public unsafe void ExportArraySetsDeviceFields()
        {
            IArrowArray array = GetTestArray();
            CArrowDeviceArray* ptr = CArrowDeviceArray.Create();

            CArrowDeviceArrayExporter.ExportArray(array, ptr);

            Assert.Equal(ArrowDeviceType.Cpu, ptr->device_type);
            Assert.Equal(-1, ptr->device_id);
            Assert.True(ptr->sync_event == null);
            Assert.False(ptr->array.release == default);

            CArrowDeviceArray.Free(ptr);
        }

        [Fact]
        public unsafe void ExportRecordBatchSetsDeviceFields()
        {
            RecordBatch batch = GetTestRecordBatch();
            CArrowDeviceArray* ptr = CArrowDeviceArray.Create();

            CArrowDeviceArrayExporter.ExportRecordBatch(batch, ptr);

            Assert.Equal(ArrowDeviceType.Cpu, ptr->device_type);
            Assert.Equal(-1, ptr->device_id);
            Assert.True(ptr->sync_event == null);
            Assert.False(ptr->array.release == default);

            CArrowDeviceArray.Free(ptr);
        }

        [Fact]
        public unsafe void ExportImportArrayRoundTrip()
        {
            IArrowArray array = GetTestArray();
            IArrowType dataType = array.Data.DataType;
            CArrowDeviceArray* ptr = CArrowDeviceArray.Create();

            CArrowDeviceArrayExporter.ExportArray(array, ptr);
            using IArrowArray imported = CArrowDeviceArrayImporter.ImportArray(ptr, dataType);

            StringArray importedStrings = (StringArray)imported;

            Assert.Equal(5, importedStrings.Length);
            Assert.Equal("hello", importedStrings.GetString(0));
            Assert.Equal("world", importedStrings.GetString(1));
            Assert.True(importedStrings.IsNull(2));
            Assert.Equal("foo", importedStrings.GetString(3));
            Assert.Equal("bar", importedStrings.GetString(4));

            CArrowDeviceArray.Free(ptr);
        }

        [Fact]
        public unsafe void ExportImportRecordBatchRoundTrip()
        {
            RecordBatch batch = GetTestRecordBatch();
            Schema schema = batch.Schema;
            CArrowDeviceArray* ptr = CArrowDeviceArray.Create();
            CArrowSchema* schemaPtr = CArrowSchema.Create();

            CArrowSchemaExporter.ExportSchema(schema, schemaPtr);
            CArrowDeviceArrayExporter.ExportRecordBatch(batch, ptr);

            Schema importedSchema = CArrowSchemaImporter.ImportSchema(schemaPtr);
            RecordBatch imported = CArrowDeviceArrayImporter.ImportRecordBatch(ptr, importedSchema);

            Assert.Equal(3, imported.Length);
            Assert.Equal(2, imported.ColumnCount);

            StringArray importedStrings = (StringArray)imported.Column(0);
            Assert.Equal("hello", importedStrings.GetString(0));
            Assert.Equal("world", importedStrings.GetString(1));
            Assert.True(importedStrings.IsNull(2));

            Int32Array importedInts = (Int32Array)imported.Column(1);
            Assert.Equal(1, importedInts.GetValue(0));
            Assert.Equal(2, importedInts.GetValue(1));
            Assert.Equal(3, importedInts.GetValue(2));

            imported.Dispose();
            CArrowDeviceArray.Free(ptr);
            CArrowSchema.Free(schemaPtr);
        }

        [Fact]
        public unsafe void ImportNonCpuDeviceArrayThrows()
        {
            IArrowArray array = GetTestArray();
            CArrowDeviceArray* ptr = CArrowDeviceArray.Create();

            CArrowDeviceArrayExporter.ExportArray(array, ptr);

            // Override the device type to simulate a non-CPU array
            ptr->device_type = ArrowDeviceType.Cuda;

            Assert.Throws<NotSupportedException>(() =>
            {
                CArrowDeviceArrayImporter.ImportArray(ptr, array.Data.DataType);
            });

            CArrowDeviceArray.Free(ptr);
        }

        [Fact]
        public unsafe void ImportNonCpuDeviceRecordBatchThrows()
        {
            RecordBatch batch = GetTestRecordBatch();
            CArrowDeviceArray* ptr = CArrowDeviceArray.Create();

            CArrowDeviceArrayExporter.ExportRecordBatch(batch, ptr);

            // Override the device type to simulate a non-CPU batch
            ptr->device_type = ArrowDeviceType.Cuda;

            Assert.Throws<NotSupportedException>(() =>
            {
                CArrowDeviceArrayImporter.ImportRecordBatch(ptr, batch.Schema);
            });

            CArrowDeviceArray.Free(ptr);
        }

        [Fact]
        public unsafe void CallsReleaseForValidDeviceArray()
        {
            IArrowArray array = GetTestArray();
            CArrowDeviceArray* ptr = CArrowDeviceArray.Create();
            CArrowDeviceArrayExporter.ExportArray(array, ptr);
            Assert.False(ptr->array.release == default);
            CArrowDeviceArrayImporter.ImportArray(ptr, array.Data.DataType).Dispose();
            Assert.True(ptr->array.release == default);
            CArrowDeviceArray.Free(ptr);
        }

        [Fact]
        public unsafe void ExportStreamSetsDeviceType()
        {
            RecordBatch batch = GetTestRecordBatch();
            IArrowArrayStream arrayStream = new TestArrayStream(batch.Schema, batch);
            CArrowDeviceArrayStream* ptr = CArrowDeviceArrayStream.Create();

            CArrowDeviceArrayStreamExporter.ExportArrayStream(arrayStream, ptr);

            Assert.Equal(ArrowDeviceType.Cpu, ptr->device_type);
            Assert.False(ptr->release == default);

            CArrowDeviceArrayStream.Free(ptr);
        }

        [Fact]
        public async Task ExportImportDeviceStreamRoundTrip()
        {
            RecordBatch batch1 = GetTestRecordBatch();
            RecordBatch batch2 = GetTestRecordBatch();
            IArrowArrayStream arrayStream = new TestArrayStream(batch1.Schema, batch1, batch2);

            IArrowArrayStream imported;
            unsafe
            {
                CArrowDeviceArrayStream* ptr = CArrowDeviceArrayStream.Create();
                CArrowDeviceArrayStreamExporter.ExportArrayStream(arrayStream, ptr);

                imported = CArrowDeviceArrayStreamImporter.ImportDeviceArrayStream(ptr);

                // Free the unmanaged allocation (stream ownership transferred to imported)
                Marshal.FreeHGlobal((IntPtr)ptr);
            }

            using (imported)
            {
                Assert.Equal(batch1.Schema.FieldsList.Count, imported.Schema.FieldsList.Count);

                RecordBatch importedBatch1 = await imported.ReadNextRecordBatchAsync();
                Assert.NotNull(importedBatch1);
                Assert.Equal(batch1.Length, importedBatch1.Length);
                Assert.Equal(batch1.ColumnCount, importedBatch1.ColumnCount);

                RecordBatch importedBatch2 = await imported.ReadNextRecordBatchAsync();
                Assert.NotNull(importedBatch2);
                Assert.Equal(batch2.Length, importedBatch2.Length);

                RecordBatch importedBatch3 = await imported.ReadNextRecordBatchAsync();
                Assert.Null(importedBatch3);

                importedBatch1.Dispose();
                importedBatch2.Dispose();
            }
        }

        [Fact]
        public unsafe void ImportNonCpuDeviceStreamThrows()
        {
            CArrowDeviceArrayStream* ptr = CArrowDeviceArrayStream.Create();

            // Simulate a non-CPU stream
            ptr->device_type = ArrowDeviceType.Cuda;
            // Set a dummy release to make it look valid
#if NET5_0_OR_GREATER
            ptr->release = &DummyRelease;
#else
            ptr->release = Marshal.GetFunctionPointerForDelegate(new DummyReleaseDelegate(DummyReleaseManaged));
#endif

            Assert.Throws<NotSupportedException>(() =>
            {
                CArrowDeviceArrayStreamImporter.ImportDeviceArrayStream(ptr);
            });

            Marshal.FreeHGlobal((IntPtr)ptr);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
        private unsafe static void DummyRelease(CArrowDeviceArrayStream* stream)
        {
            stream->release = default;
        }
#else
        private unsafe delegate void DummyReleaseDelegate(CArrowDeviceArrayStream* stream);
        private unsafe static void DummyReleaseManaged(CArrowDeviceArrayStream* stream)
        {
            stream->release = default;
        }
#endif

        /// <summary>
        /// Simple IArrowArrayStream implementation for testing.
        /// </summary>
        private class TestArrayStream : IArrowArrayStream
        {
            private readonly Queue<RecordBatch> _batches;

            public TestArrayStream(Schema schema, params RecordBatch[] batches)
            {
                Schema = schema;
                _batches = new Queue<RecordBatch>(batches);
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(System.Threading.CancellationToken cancellationToken = default)
            {
                if (_batches.Count > 0)
                {
                    return new ValueTask<RecordBatch>(_batches.Dequeue());
                }
                return new ValueTask<RecordBatch>((RecordBatch)null);
            }

            public void Dispose() { }
        }
    }
}
