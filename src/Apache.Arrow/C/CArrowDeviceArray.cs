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

namespace Apache.Arrow.C
{
    /// <summary>
    /// An Arrow C Device Data Interface ArrowDeviceArray, which represents an exported array
    /// along with the device on which the array data resides.
    /// </summary>
    /// <remarks>
    /// This is used to export <see cref="IArrowArray"/> or <see cref="RecordBatch"/> with device
    /// information to other languages. It matches the layout of the ArrowDeviceArray struct
    /// described in https://arrow.apache.org/docs/format/CDeviceDataInterface.html.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CArrowDeviceArray
    {
        public CArrowArray array;
        public long device_id;
        public ArrowDeviceType device_type;
        // 4 bytes implicit padding on 64-bit (matches C layout)
        public void* sync_event;
        private fixed long reserved[3];

        /// <summary>
        /// Allocate and zero-initialize an unmanaged pointer of this type.
        /// </summary>
        /// <remarks>
        /// This pointer must later be freed by <see cref="Free"/>.
        /// </remarks>
        public static CArrowDeviceArray* Create()
        {
            var ptr = (CArrowDeviceArray*)Marshal.AllocHGlobal(sizeof(CArrowDeviceArray));

            *ptr = default;

            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="Create"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void Free(CArrowDeviceArray* deviceArray)
        {
            CArrowArray.CallReleaseFunc(&deviceArray->array);
            Marshal.FreeHGlobal((IntPtr)deviceArray);
        }
    }
}
