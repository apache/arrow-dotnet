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

namespace Apache.Arrow.C
{
    [Experimental("ArrowDeviceDataApi")]
    public static class CArrowDeviceArrayExporter
    {
        /// <summary>
        /// Export an <see cref="IArrowArray"/> to a <see cref="CArrowDeviceArray"/>. The exported array
        /// shares the underlying buffers via reference counting, so the original array remains valid
        /// after export.
        /// </summary>
        /// <param name="array">The array to export</param>
        /// <param name="deviceArray">An allocated but uninitialized CArrowDeviceArray pointer.</param>
        /// <example>
        /// <code>
        /// CArrowDeviceArray* exportPtr = CArrowDeviceArray.Create();
        /// CArrowDeviceArrayExporter.ExportArray(array, exportPtr);
        /// foreign_import_function(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportArray(IArrowArray array, CArrowDeviceArray* deviceArray)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }
            if (deviceArray == null)
            {
                throw new ArgumentNullException(nameof(deviceArray));
            }

            CArrowArrayExporter.ExportArray(array, &deviceArray->array);
            deviceArray->device_type = ArrowDeviceType.Cpu;
            deviceArray->device_id = -1;
            deviceArray->sync_event = null;
        }

        /// <summary>
        /// Export a <see cref="RecordBatch"/> to a <see cref="CArrowDeviceArray"/>. The exported record
        /// batch shares the underlying buffers via reference counting, so the original batch remains
        /// valid after export.
        /// </summary>
        /// <param name="batch">The record batch to export</param>
        /// <param name="deviceArray">An allocated but uninitialized CArrowDeviceArray pointer.</param>
        /// <example>
        /// <code>
        /// CArrowDeviceArray* exportPtr = CArrowDeviceArray.Create();
        /// CArrowDeviceArrayExporter.ExportRecordBatch(batch, exportPtr);
        /// foreign_import_function(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportRecordBatch(RecordBatch batch, CArrowDeviceArray* deviceArray)
        {
            if (batch == null)
            {
                throw new ArgumentNullException(nameof(batch));
            }
            if (deviceArray == null)
            {
                throw new ArgumentNullException(nameof(deviceArray));
            }

            CArrowArrayExporter.ExportRecordBatch(batch, &deviceArray->array);
            deviceArray->device_type = ArrowDeviceType.Cpu;
            deviceArray->device_id = -1;
            deviceArray->sync_event = null;
        }
    }
}
