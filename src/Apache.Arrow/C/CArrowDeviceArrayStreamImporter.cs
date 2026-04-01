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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.C
{
    public static class CArrowDeviceArrayStreamImporter
    {
        /// <summary>
        /// Import C pointer as an <see cref="IArrowArrayStream"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback on the passed struct if the function fails.
        /// Otherwise, the release callback is called when the IArrowArrayStream is disposed.
        /// Only CPU device streams are supported.
        /// </remarks>
        /// <param name="ptr">The pointer to the device array stream being imported</param>
        /// <returns>The imported C# array stream</returns>
        public static unsafe IArrowArrayStream ImportDeviceArrayStream(CArrowDeviceArrayStream* ptr)
        {
            if (ptr == null)
            {
                throw new ArgumentNullException(nameof(ptr));
            }
            if (ptr->device_type != ArrowDeviceType.Cpu)
            {
                throw new NotSupportedException(
                    $"Importing device array streams from device type {ptr->device_type} is not supported. Only CPU streams can be imported.");
            }

            return new ImportedArrowDeviceArrayStream(ptr);
        }

        private sealed unsafe class ImportedArrowDeviceArrayStream : IArrowArrayStream
        {
            private readonly CArrowDeviceArrayStream _cDeviceArrayStream;
            private readonly Schema _schema;
            private bool _disposed;

            internal static string GetLastError(CArrowDeviceArrayStream* stream, int errno)
            {
#if NET5_0_OR_GREATER
                byte* error = stream->get_last_error(stream);
#else
                byte* error = Marshal.GetDelegateForFunctionPointer<CArrowDeviceArrayStreamExporter.GetLastErrorDeviceArrayStream>(stream->get_last_error)(stream);
#endif
                if (error == null)
                {
                    return $"Device array stream operation failed with no message. Error code: {errno}";
                }
                return StringUtil.PtrToStringUtf8(error);
            }

            public ImportedArrowDeviceArrayStream(CArrowDeviceArrayStream* cDeviceArrayStream)
            {
                if (cDeviceArrayStream == null)
                {
                    throw new ArgumentNullException(nameof(cDeviceArrayStream));
                }
                if (cDeviceArrayStream->release == default)
                {
                    throw new ArgumentException("Tried to import a device array stream that has already been released.", nameof(cDeviceArrayStream));
                }

                CArrowSchema cSchema = new CArrowSchema();
#if NET5_0_OR_GREATER
                int errno = cDeviceArrayStream->get_schema(cDeviceArrayStream, &cSchema);
#else
                int errno = Marshal.GetDelegateForFunctionPointer<CArrowDeviceArrayStreamExporter.GetSchemaDeviceArrayStream>(cDeviceArrayStream->get_schema)(cDeviceArrayStream, &cSchema);
#endif
                if (errno != 0)
                {
                    throw new Exception(GetLastError(cDeviceArrayStream, errno));
                }
                _schema = CArrowSchemaImporter.ImportSchema(&cSchema);

                _cDeviceArrayStream = *cDeviceArrayStream;
                cDeviceArrayStream->release = default;
            }

            ~ImportedArrowDeviceArrayStream()
            {
                Dispose();
            }

            public Schema Schema => _schema;

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(typeof(ImportedArrowDeviceArrayStream).Name);
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return new(Task.FromCanceled<RecordBatch>(cancellationToken));
                }

                RecordBatch result = null;
                CArrowDeviceArray cDeviceArray = new CArrowDeviceArray();
                fixed (CArrowDeviceArrayStream* cDeviceArrayStream = &_cDeviceArrayStream)
                {
#if NET5_0_OR_GREATER
                    int errno = cDeviceArrayStream->get_next(cDeviceArrayStream, &cDeviceArray);
#else
                    int errno = Marshal.GetDelegateForFunctionPointer<CArrowDeviceArrayStreamExporter.GetNextDeviceArrayStream>(cDeviceArrayStream->get_next)(cDeviceArrayStream, &cDeviceArray);
#endif
                    if (errno != 0)
                    {
                        return new(Task.FromException<RecordBatch>(new Exception(GetLastError(cDeviceArrayStream, errno))));
                    }
                    if (cDeviceArray.array.release != default)
                    {
                        result = CArrowDeviceArrayImporter.ImportRecordBatch(&cDeviceArray, _schema);
                    }
                }

                return new ValueTask<RecordBatch>(result);
            }

            public void Dispose()
            {
                if (!_disposed && _cDeviceArrayStream.release != default)
                {
                    _disposed = true;
                    fixed (CArrowDeviceArrayStream* cDeviceArrayStream = &_cDeviceArrayStream)
                    {
#if NET5_0_OR_GREATER
                        cDeviceArrayStream->release(cDeviceArrayStream);
#else
                        Marshal.GetDelegateForFunctionPointer<CArrowDeviceArrayStreamExporter.ReleaseDeviceArrayStream>(cDeviceArrayStream->release)(cDeviceArrayStream);
#endif
                    }
                }
                GC.SuppressFinalize(this);
            }
        }
    }
}
