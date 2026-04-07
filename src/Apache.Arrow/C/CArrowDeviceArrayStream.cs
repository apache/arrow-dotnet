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
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Apache.Arrow.C
{
    /// <summary>
    /// An Arrow C Device Data Interface ArrowDeviceArrayStream, which represents a stream
    /// of record batches with device information.
    /// </summary>
    /// <remarks>
    /// This is used to export <see cref="Ipc.IArrowArrayStream"/> with device information
    /// to other languages. It matches the layout of the ArrowDeviceArrayStream struct
    /// described in https://arrow.apache.org/docs/format/CDeviceDataInterface.html.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    [Experimental("ArrowDeviceDataApi")]
    public unsafe struct CArrowDeviceArrayStream
    {
        public ArrowDeviceType device_type;

        /// <summary>
        /// Callback to get the stream schema. Will be the same for all arrays in the stream.
        /// If successful, the ArrowSchema must be released independently from the stream.
        ///
        /// Return value: 0 if successful, an `errno`-compatible error code otherwise.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowDeviceArrayStream*, CArrowSchema*, int> get_schema;
#else
        internal IntPtr get_schema;
#endif

        /// <summary>
        /// Callback to get the next device array. If no error and the array is released,
        /// the stream has ended. If successful, the ArrowDeviceArray must be released
        /// independently from the stream.
        ///
        /// Return value: 0 if successful, an `errno`-compatible error code otherwise.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowDeviceArrayStream*, CArrowDeviceArray*, int> get_next;
#else
        internal IntPtr get_next;
#endif

        /// <summary>
        /// Callback to get optional detailed error information. This must only
        /// be called if the last stream operation failed with a non-0 return code.
        /// The returned pointer is only valid until the next operation on this stream
        /// (including release).
        ///
        /// Return value: pointer to a null-terminated character array describing the last
        /// error, or NULL if no description is available.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowDeviceArrayStream*, byte*> get_last_error;
#else
        internal IntPtr get_last_error;
#endif

        /// <summary>
        /// Release callback: release the stream's own resources. Note that arrays returned by
        /// get_next must be individually released.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowDeviceArrayStream*, void> release;
#else
        internal IntPtr release;
#endif

        public void* private_data;

        /// <summary>
        /// Allocate and zero-initialize an unmanaged pointer of this type.
        /// </summary>
        /// <remarks>
        /// This pointer must later be freed by <see cref="Free"/>.
        /// </remarks>
        public static CArrowDeviceArrayStream* Create()
        {
            var ptr = (CArrowDeviceArrayStream*)Marshal.AllocHGlobal(sizeof(CArrowDeviceArrayStream));

            *ptr = default;

            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="Create"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void Free(CArrowDeviceArrayStream* stream)
        {
            if (stream->release != default)
            {
                // Call release if not already called.
#if NET5_0_OR_GREATER
                stream->release(stream);
#else
                Marshal.GetDelegateForFunctionPointer<CArrowDeviceArrayStreamExporter.ReleaseDeviceArrayStream>(stream->release)(stream);
#endif
            }
            Marshal.FreeHGlobal((IntPtr)stream);
        }
    }
}
