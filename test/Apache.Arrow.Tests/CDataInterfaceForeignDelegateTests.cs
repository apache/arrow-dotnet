// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Tests importing C Data Interface structures whose callbacks were exported in this
    /// process by another library, using that library's own delegate types rather than
    /// Arrow's internal ones.
    /// </summary>
    [Experimental("ArrowDeviceDataApi")]
    public class CDataInterfaceForeignDelegateTests
    {
        private unsafe delegate void ForeignReleaseSchema(CArrowSchema* schema);
        private unsafe delegate void ForeignReleaseArray(CArrowArray* array);
        private unsafe delegate int ForeignGetSchema(CArrowArrayStream* stream, CArrowSchema* schema);
        private unsafe delegate int ForeignGetNext(CArrowArrayStream* stream, CArrowArray* array);
        private unsafe delegate byte* ForeignGetLastError(CArrowArrayStream* stream);
        private unsafe delegate void ForeignReleaseStream(CArrowArrayStream* stream);
        private unsafe delegate int ForeignGetDeviceSchema(CArrowDeviceArrayStream* stream, CArrowSchema* schema);
        private unsafe delegate int ForeignGetDeviceNext(CArrowDeviceArrayStream* stream, CArrowDeviceArray* array);
        private unsafe delegate byte* ForeignGetDeviceLastError(CArrowDeviceArrayStream* stream);
        private unsafe delegate void ForeignReleaseDeviceStream(CArrowDeviceArrayStream* stream);

        private const string ErrorMessage = "foreign stream failure";
        private const int ErrorCode = 5; // EIO

        // The struct fields are function pointers on .NET 5+ and IntPtr otherwise; both are pointer-sized.
        private static unsafe void SetCallback(void* field, IntPtr callback)
        {
            *(IntPtr*)field = callback;
        }

        [Fact]
        public unsafe void ImportSchema()
        {
            bool released = false;
            ForeignReleaseSchema release = schema =>
            {
                released = true;
                schema->release = default;
            };

            CArrowSchema* cSchema = CArrowSchema.Create();
            IntPtr format = Marshal.StringToHGlobalAnsi("n");
            try
            {
                cSchema->format = (byte*)format;
                SetCallback(&cSchema->release, Marshal.GetFunctionPointerForDelegate(release));

                ArrowType type = CArrowSchemaImporter.ImportType(cSchema);

                Assert.IsType<NullType>(type);
                Assert.True(released);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
                Marshal.FreeHGlobal(format);
                GC.KeepAlive(release);
            }
        }

        [Fact]
        public unsafe void ImportArray()
        {
            bool released = false;
            ForeignReleaseArray release = array =>
            {
                released = true;
                array->release = default;
            };

            CArrowArray* cArray = CArrowArray.Create();
            try
            {
                cArray->length = 3;
                cArray->null_count = 3;
                SetCallback(&cArray->release, Marshal.GetFunctionPointerForDelegate(release));

                using (IArrowArray array = CArrowArrayImporter.ImportArray(cArray, NullType.Default))
                {
                    Assert.Equal(3, array.Length);
                }

                Assert.True(released);
            }
            finally
            {
                CArrowArray.Free(cArray);
                GC.KeepAlive(release);
            }
        }

        [Fact]
        public unsafe void CallReleaseFuncOnArray()
        {
            bool released = false;
            ForeignReleaseArray release = array =>
            {
                released = true;
                array->release = default;
            };

            CArrowArray* cArray = CArrowArray.Create();
            SetCallback(&cArray->release, Marshal.GetFunctionPointerForDelegate(release));
            CArrowArray.Free(cArray);

            Assert.True(released);
            GC.KeepAlive(release);
        }

        [Fact]
        public unsafe void FreeSchema()
        {
            bool released = false;
            ForeignReleaseSchema release = schema =>
            {
                released = true;
                schema->release = default;
            };

            CArrowSchema* cSchema = CArrowSchema.Create();
            SetCallback(&cSchema->release, Marshal.GetFunctionPointerForDelegate(release));
            CArrowSchema.Free(cSchema);

            Assert.True(released);
            GC.KeepAlive(release);
        }

        [Fact]
        public async Task ImportArrayStream()
        {
            using (var foreign = new ForeignArrayStream(failGetNext: false))
            {
                using (IArrowArrayStream stream = foreign.Import())
                {
                    Assert.Equal(foreign.Batch.Schema.FieldsList[0].Name, stream.Schema.FieldsList[0].Name);

                    using (RecordBatch batch = await stream.ReadNextRecordBatchAsync())
                    {
                        Assert.Equal(foreign.Batch.Length, batch.Length);
                    }
                    Assert.Null(await stream.ReadNextRecordBatchAsync());
                    Assert.False(foreign.Released);
                }

                Assert.True(foreign.Released);
            }
        }

        [Fact]
        public async Task ImportArrayStreamGetsLastError()
        {
            using (var foreign = new ForeignArrayStream(failGetNext: true))
            {
                using (IArrowArrayStream stream = foreign.Import())
                {
                    Exception ex = await Assert.ThrowsAsync<Exception>(
                        async () => await stream.ReadNextRecordBatchAsync());
                    Assert.Equal(ErrorMessage, ex.Message);
                }

                Assert.True(foreign.Released);
            }
        }

        [Fact]
        public void FreeArrayStream()
        {
            using (var foreign = new ForeignArrayStream(failGetNext: false))
            {
                foreign.Free();

                Assert.True(foreign.Released);
            }
        }

        [Fact]
        public async Task ImportDeviceArrayStream()
        {
            using (var foreign = new ForeignDeviceArrayStream(failGetNext: false))
            {
                using (IArrowArrayStream stream = foreign.Import())
                {
                    Assert.Equal(foreign.Batch.Schema.FieldsList[0].Name, stream.Schema.FieldsList[0].Name);

                    using (RecordBatch batch = await stream.ReadNextRecordBatchAsync())
                    {
                        Assert.Equal(foreign.Batch.Length, batch.Length);
                    }
                    Assert.Null(await stream.ReadNextRecordBatchAsync());
                    Assert.False(foreign.Released);
                }

                Assert.True(foreign.Released);
            }
        }

        [Fact]
        public async Task ImportDeviceArrayStreamGetsLastError()
        {
            using (var foreign = new ForeignDeviceArrayStream(failGetNext: true))
            {
                using (IArrowArrayStream stream = foreign.Import())
                {
                    Exception ex = await Assert.ThrowsAsync<Exception>(
                        async () => await stream.ReadNextRecordBatchAsync());
                    Assert.Equal(ErrorMessage, ex.Message);
                }

                Assert.True(foreign.Released);
            }
        }

        [Fact]
        public void FreeDeviceArrayStream()
        {
            using (var foreign = new ForeignDeviceArrayStream(failGetNext: false))
            {
                foreign.Free();

                Assert.True(foreign.Released);
            }
        }

        private static RecordBatch CreateBatch()
        {
            return new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(builder => builder.AppendRange(new[] { 1, 2, 3 })))
                .Build();
        }

        /// <summary>
        /// A single-batch stream implemented with this test's own delegate types.
        /// </summary>
        private sealed unsafe class ForeignArrayStream : IDisposable
        {
            private readonly bool _failGetNext;
            private readonly IntPtr _error;
            private readonly ForeignGetSchema _getSchema;
            private readonly ForeignGetNext _getNext;
            private readonly ForeignGetLastError _getLastError;
            private readonly ForeignReleaseStream _release;
            private CArrowArrayStream* _stream;
            private bool _batchReturned;

            public ForeignArrayStream(bool failGetNext)
            {
                _failGetNext = failGetNext;
                _error = Marshal.StringToHGlobalAnsi(ErrorMessage);
                Batch = CreateBatch();
                _getSchema = (stream, schema) =>
                {
                    CArrowSchemaExporter.ExportSchema(Batch.Schema, schema);
                    return 0;
                };
                _getNext = (stream, array) =>
                {
                    if (_failGetNext)
                    {
                        return ErrorCode;
                    }
                    if (_batchReturned)
                    {
                        *array = default;
                    }
                    else
                    {
                        CArrowArrayExporter.ExportRecordBatch(Batch, array);
                        _batchReturned = true;
                    }
                    return 0;
                };
                _getLastError = stream => (byte*)_error;
                _release = stream =>
                {
                    Released = true;
                    stream->release = default;
                };

                _stream = CArrowArrayStream.Create();
                Export(_stream);
            }

            public RecordBatch Batch { get; }

            public bool Released { get; private set; }

            public IArrowArrayStream Import() => CArrowArrayStreamImporter.ImportArrayStream(_stream);

            public void Free()
            {
                CArrowArrayStream.Free(_stream);
                _stream = null;
            }

            private void Export(CArrowArrayStream* stream)
            {
                SetCallback(&stream->get_schema, Marshal.GetFunctionPointerForDelegate(_getSchema));
                SetCallback(&stream->get_next, Marshal.GetFunctionPointerForDelegate(_getNext));
                SetCallback(&stream->get_last_error, Marshal.GetFunctionPointerForDelegate(_getLastError));
                SetCallback(&stream->release, Marshal.GetFunctionPointerForDelegate(_release));
            }

            public void Dispose()
            {
                if (_stream != null)
                {
                    Free();
                }
                Batch.Dispose();
                Marshal.FreeHGlobal(_error);
            }
        }

        /// <summary>
        /// A single-batch device stream implemented with this test's own delegate types.
        /// </summary>
        private sealed unsafe class ForeignDeviceArrayStream : IDisposable
        {
            private readonly bool _failGetNext;
            private readonly IntPtr _error;
            private readonly ForeignGetDeviceSchema _getSchema;
            private readonly ForeignGetDeviceNext _getNext;
            private readonly ForeignGetDeviceLastError _getLastError;
            private readonly ForeignReleaseDeviceStream _release;
            private CArrowDeviceArrayStream* _stream;
            private bool _batchReturned;

            public ForeignDeviceArrayStream(bool failGetNext)
            {
                _failGetNext = failGetNext;
                _error = Marshal.StringToHGlobalAnsi(ErrorMessage);
                Batch = CreateBatch();
                _getSchema = (stream, schema) =>
                {
                    CArrowSchemaExporter.ExportSchema(Batch.Schema, schema);
                    return 0;
                };
                _getNext = (stream, array) =>
                {
                    if (_failGetNext)
                    {
                        return ErrorCode;
                    }
                    if (_batchReturned)
                    {
                        *array = default;
                    }
                    else
                    {
                        CArrowDeviceArrayExporter.ExportRecordBatch(Batch, array);
                        _batchReturned = true;
                    }
                    return 0;
                };
                _getLastError = stream => (byte*)_error;
                _release = stream =>
                {
                    Released = true;
                    stream->release = default;
                };

                _stream = CArrowDeviceArrayStream.Create();
                Export(_stream);
            }

            public RecordBatch Batch { get; }

            public bool Released { get; private set; }

            public IArrowArrayStream Import() => CArrowDeviceArrayStreamImporter.ImportDeviceArrayStream(_stream);

            public void Free()
            {
                CArrowDeviceArrayStream.Free(_stream);
                _stream = null;
            }

            private void Export(CArrowDeviceArrayStream* stream)
            {
                stream->device_type = ArrowDeviceType.Cpu;
                SetCallback(&stream->get_schema, Marshal.GetFunctionPointerForDelegate(_getSchema));
                SetCallback(&stream->get_next, Marshal.GetFunctionPointerForDelegate(_getNext));
                SetCallback(&stream->get_last_error, Marshal.GetFunctionPointerForDelegate(_getLastError));
                SetCallback(&stream->release, Marshal.GetFunctionPointerForDelegate(_release));
            }

            public void Dispose()
            {
                if (_stream != null)
                {
                    Free();
                }
                Batch.Dispose();
                Marshal.FreeHGlobal(_error);
            }
        }
    }
}
