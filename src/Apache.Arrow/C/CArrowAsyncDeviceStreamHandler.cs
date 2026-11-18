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
    /// An Arrow C Async Device Stream Interface ArrowAsyncDeviceStreamHandler, which
    /// is created by the consumer to receive pushed data from a producer.
    /// </summary>
    /// <remarks>
    /// This matches the layout of the ArrowAsyncDeviceStreamHandler struct described in
    /// https://arrow.apache.org/docs/format/CDeviceDataInterface.html.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    [Experimental("ArrowAsyncDeviceStreamApi")]
    public unsafe struct CArrowAsyncDeviceStreamHandler
    {
        /// <summary>
        /// Callback invoked by the producer to deliver the stream schema.
        /// Called once before any on_next_task calls.
        ///
        /// Return value: 0 if successful, an errno-compatible error code otherwise.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowAsyncDeviceStreamHandler*, CArrowSchema*, int> on_schema;
#else
        internal IntPtr on_schema;
#endif

        /// <summary>
        /// Callback invoked by the producer to deliver a data batch.
        /// A null task pointer signals end-of-stream.
        ///
        /// Return value: 0 if successful, an errno-compatible error code otherwise.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowAsyncDeviceStreamHandler*, CArrowAsyncTask*, byte*, int> on_next_task;
#else
        internal IntPtr on_next_task;
#endif

        /// <summary>
        /// Callback invoked by the producer when an error occurs.
        /// The producer must still call release after calling on_error.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowAsyncDeviceStreamHandler*, int, byte*, byte*, void> on_error;
#else
        internal IntPtr on_error;
#endif

        /// <summary>
        /// Release callback: release the handler's own resources.
        /// Called exactly once by the producer after all other callbacks.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowAsyncDeviceStreamHandler*, void> release;
#else
        internal IntPtr release;
#endif

        /// <summary>
        /// Pointer to the ArrowAsyncProducer. Populated by the producer before
        /// calling on_schema. Provides flow control via request() and cancel().
        /// </summary>
        public CArrowAsyncProducer* producer;

        public void* private_data;

        /// <summary>
        /// Allocate and zero-initialize an unmanaged pointer of this type.
        /// </summary>
        /// <remarks>
        /// This pointer must later be freed by <see cref="Free"/>.
        /// </remarks>
        public static CArrowAsyncDeviceStreamHandler* Create()
        {
            var ptr = (CArrowAsyncDeviceStreamHandler*)Marshal.AllocHGlobal(sizeof(CArrowAsyncDeviceStreamHandler));

            *ptr = default;

            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="Create"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void Free(CArrowAsyncDeviceStreamHandler* handler)
        {
            Marshal.FreeHGlobal((IntPtr)handler);
        }
    }
}
