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
using Apache.Arrow.Ipc;

namespace Apache.Arrow.C
{
    public static class CArrowDeviceArrayStreamExporter
    {
#if NET5_0_OR_GREATER
        private static unsafe delegate* unmanaged<CArrowDeviceArrayStream*, CArrowSchema*, int> GetSchemaPtr => &GetSchema;
        private static unsafe delegate* unmanaged<CArrowDeviceArrayStream*, CArrowDeviceArray*, int> GetNextPtr => &GetNext;
        private static unsafe delegate* unmanaged<CArrowDeviceArrayStream*, byte*> GetLastErrorPtr => &GetLastError;
        private static unsafe delegate* unmanaged<CArrowDeviceArrayStream*, void> ReleasePtr => &Release;
#else
        internal unsafe delegate int GetSchemaDeviceArrayStream(CArrowDeviceArrayStream* cDeviceArrayStream, CArrowSchema* cSchema);
        private static unsafe NativeDelegate<GetSchemaDeviceArrayStream> s_getSchema = new NativeDelegate<GetSchemaDeviceArrayStream>(GetSchema);
        private static unsafe IntPtr GetSchemaPtr => s_getSchema.Pointer;
        internal unsafe delegate int GetNextDeviceArrayStream(CArrowDeviceArrayStream* cDeviceArrayStream, CArrowDeviceArray* cDeviceArray);
        private static unsafe NativeDelegate<GetNextDeviceArrayStream> s_getNext = new NativeDelegate<GetNextDeviceArrayStream>(GetNext);
        private static unsafe IntPtr GetNextPtr => s_getNext.Pointer;
        internal unsafe delegate byte* GetLastErrorDeviceArrayStream(CArrowDeviceArrayStream* cDeviceArrayStream);
        private static unsafe NativeDelegate<GetLastErrorDeviceArrayStream> s_getLastError = new NativeDelegate<GetLastErrorDeviceArrayStream>(GetLastError);
        private static unsafe IntPtr GetLastErrorPtr => s_getLastError.Pointer;
        internal unsafe delegate void ReleaseDeviceArrayStream(CArrowDeviceArrayStream* cDeviceArrayStream);
        private static unsafe NativeDelegate<ReleaseDeviceArrayStream> s_release = new NativeDelegate<ReleaseDeviceArrayStream>(Release);
        private static unsafe IntPtr ReleasePtr => s_release.Pointer;
#endif

        /// <summary>
        /// Export an <see cref="IArrowArrayStream"/> to a <see cref="CArrowDeviceArrayStream"/>.
        /// </summary>
        /// <param name="arrayStream">The array stream to export</param>
        /// <param name="deviceArrayStream">An allocated but uninitialized CArrowDeviceArrayStream pointer.</param>
        /// <example>
        /// <code>
        /// CArrowDeviceArrayStream* exportPtr = CArrowDeviceArrayStream.Create();
        /// CArrowDeviceArrayStreamExporter.ExportArrayStream(arrayStream, exportPtr);
        /// foreign_import_function(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportArrayStream(IArrowArrayStream arrayStream, CArrowDeviceArrayStream* deviceArrayStream)
        {
            if (arrayStream == null)
            {
                throw new ArgumentNullException(nameof(arrayStream));
            }
            if (deviceArrayStream == null)
            {
                throw new ArgumentNullException(nameof(deviceArrayStream));
            }

            deviceArrayStream->device_type = ArrowDeviceType.Cpu;
            deviceArrayStream->private_data = ExportedDeviceArrayStream.Export(arrayStream);
            deviceArrayStream->get_schema = GetSchemaPtr;
            deviceArrayStream->get_next = GetNextPtr;
            deviceArrayStream->get_last_error = GetLastErrorPtr;
            deviceArrayStream->release = ReleasePtr;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static int GetSchema(CArrowDeviceArrayStream* cDeviceArrayStream, CArrowSchema* cSchema)
        {
            ExportedDeviceArrayStream stream = null;
            try
            {
                stream = ExportedDeviceArrayStream.FromPointer(cDeviceArrayStream->private_data);
                CArrowSchemaExporter.ExportSchema(stream.ArrowArrayStream.Schema, cSchema);
                return stream.ClearError();
            }
            catch (Exception ex)
            {
                return stream?.SetError(ex) ?? ExportedDeviceArrayStream.EOTHER;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static int GetNext(CArrowDeviceArrayStream* cDeviceArrayStream, CArrowDeviceArray* cDeviceArray)
        {
            ExportedDeviceArrayStream stream = null;
            try
            {
                cDeviceArray->array.release = default;
                stream = ExportedDeviceArrayStream.FromPointer(cDeviceArrayStream->private_data);
                RecordBatch recordBatch = stream.ArrowArrayStream.ReadNextRecordBatchAsync().Result;
                if (recordBatch != null)
                {
                    CArrowDeviceArrayExporter.ExportRecordBatch(recordBatch, cDeviceArray);
                }
                return stream.ClearError();
            }
            catch (Exception ex)
            {
                return stream?.SetError(ex) ?? ExportedDeviceArrayStream.EOTHER;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static byte* GetLastError(CArrowDeviceArrayStream* cDeviceArrayStream)
        {
            try
            {
                ExportedDeviceArrayStream stream = ExportedDeviceArrayStream.FromPointer(cDeviceArrayStream->private_data);
                return stream.LastError;
            }
            catch (Exception)
            {
                return null;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void Release(CArrowDeviceArrayStream* cDeviceArrayStream)
        {
            ExportedDeviceArrayStream.Free(&cDeviceArrayStream->private_data);
            cDeviceArrayStream->release = default;
        }

        sealed unsafe class ExportedDeviceArrayStream : IDisposable
        {
            public const int EOTHER = 131;

            ExportedDeviceArrayStream(IArrowArrayStream arrayStream)
            {
                ArrowArrayStream = arrayStream;
                LastError = null;
            }

            public IArrowArrayStream ArrowArrayStream { get; private set; }
            public byte* LastError { get; private set; }

            public static void* Export(IArrowArrayStream arrayStream)
            {
                ExportedDeviceArrayStream result = new ExportedDeviceArrayStream(arrayStream);
                GCHandle gch = GCHandle.Alloc(result);
                return (void*)GCHandle.ToIntPtr(gch);
            }

            public static void Free(void** ptr)
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)(*ptr));
                if (!gch.IsAllocated)
                {
                    return;
                }
                ((ExportedDeviceArrayStream)gch.Target).Dispose();
                gch.Free();
                *ptr = null;
            }

            public static ExportedDeviceArrayStream FromPointer(void* ptr)
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)ptr);
                return (ExportedDeviceArrayStream)gch.Target;
            }

            public int SetError(Exception ex)
            {
                ReleaseLastError();
                LastError = StringUtil.ToCStringUtf8(ex.Message);
                return EOTHER;
            }

            public int ClearError()
            {
                ReleaseLastError();
                return 0;
            }

            public void Dispose()
            {
                ReleaseLastError();
                ArrowArrayStream?.Dispose();
                ArrowArrayStream = null;
            }

            void ReleaseLastError()
            {
                if (LastError != null)
                {
                    Marshal.FreeHGlobal((IntPtr)LastError);
                    LastError = null;
                }
            }
        }
    }
}
