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
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Python.Runtime;
using Xunit;

namespace Apache.Arrow.Tests
{
    [Collection("PythonNet")]
    public class CDeviceDataInterfacePythonTest
    {
        public CDeviceDataInterfacePythonTest(PythonNetFixture pythonNet)
        {
            pythonNet.EnsureInitialized();
        }

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

        private dynamic GetPythonArray()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                return pa.array(new[] { "hello", "world", null, "foo", "bar" });
            }
        }

        private RecordBatch GetTestRecordBatch()
        {
            Field[] fields = new[]
            {
                new Field("col1", Int64Type.Default, true),
                new Field("col2", StringType.Default, true),
                new Field("col3", DoubleType.Default, true),
            };
            return new RecordBatch(
                new Schema(fields, null),
                new IArrowArray[]
                {
                    new Int64Array.Builder().AppendRange(new long[] { 1, 2, 3 }).AppendNull().Append(5).Build(),
                    GetTestArray(),
                    new DoubleArray.Builder().AppendRange(new double[] { 0.0, 1.4, 2.5, 3.6, 4.7 }).Build(),
                },
                5);
        }

        private dynamic GetPythonRecordBatch()
        {
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic table = pa.table(
                    new PyList(new PyObject[]
                    {
                        pa.array(new long?[] { 1, 2, 3, null, 5 }),
                        pa.array(new[] { "hello", "world", null, "foo", "bar" }),
                        pa.array(new[] { 0.0, 1.4, 2.5, 3.6, 4.7 })
                    }),
                    new[] { "col1", "col2", "col3" });

                return table.to_batches()[0];
            }
        }

        [SkippableFact]
        public unsafe void ExportArrayToDeviceAndImportInPython()
        {
            IArrowArray array = GetTestArray();
            dynamic pyArray = GetPythonArray();

            CArrowDeviceArray* cDeviceArray = CArrowDeviceArray.Create();
            CArrowDeviceArrayExporter.ExportArray(array, cDeviceArray);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportType(array.Data.DataType, cSchema);

            long deviceArrayPtr = ((IntPtr)cDeviceArray).ToInt64();
            long schemaPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyArray = pa.Array._import_from_c_device(deviceArrayPtr, schemaPtr);
                Assert.True(exportedPyArray == pyArray);
            }

            CArrowDeviceArray.Free(cDeviceArray);
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void ImportArrayFromPythonDevice()
        {
            CArrowDeviceArray* cDeviceArray = CArrowDeviceArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic pyArray = pa.array(new[] { "hello", "world", null, "foo", "bar" });

                long deviceArrayPtr = ((IntPtr)cDeviceArray).ToInt64();
                long schemaPtr = ((IntPtr)cSchema).ToInt64();
                pyArray._export_to_c_device(deviceArrayPtr, schemaPtr);
            }

            // Verify device fields set by PyArrow
            Assert.Equal(ArrowDeviceType.Cpu, cDeviceArray->device_type);
            Assert.Equal(-1, cDeviceArray->device_id);
            Assert.True(cDeviceArray->sync_event == null);

            ArrowType type = CArrowSchemaImporter.ImportType(cSchema);
            IArrowArray importedArray = CArrowDeviceArrayImporter.ImportArray(cDeviceArray, type);
            StringArray importedStrings = (StringArray)importedArray;

            Assert.Equal(5, importedStrings.Length);
            Assert.Equal("hello", importedStrings.GetString(0));
            Assert.Equal("world", importedStrings.GetString(1));
            Assert.Null(importedStrings.GetString(2));
            Assert.Equal("foo", importedStrings.GetString(3));
            Assert.Equal("bar", importedStrings.GetString(4));

            CArrowDeviceArray.Free(cDeviceArray);
        }

        [SkippableFact]
        public unsafe void ExportRecordBatchToDeviceAndImportInPython()
        {
            RecordBatch batch = GetTestRecordBatch();
            dynamic pyBatch = GetPythonRecordBatch();

            CArrowDeviceArray* cDeviceArray = CArrowDeviceArray.Create();
            CArrowDeviceArrayExporter.ExportRecordBatch(batch, cDeviceArray);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);

            long deviceArrayPtr = ((IntPtr)cDeviceArray).ToInt64();
            long schemaPtr = ((IntPtr)cSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyBatch = pa.RecordBatch._import_from_c_device(deviceArrayPtr, schemaPtr);
                Assert.True(exportedPyBatch == pyBatch);
            }

            CArrowDeviceArray.Free(cDeviceArray);
            CArrowSchema.Free(cSchema);
        }

        [SkippableFact]
        public unsafe void ImportRecordBatchFromPythonDevice()
        {
            CArrowDeviceArray* cDeviceArray = CArrowDeviceArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic table = pa.table(
                    new PyList(new PyObject[]
                    {
                        pa.array(new long?[] { 1, 2, 3, null, 5 }),
                        pa.array(new[] { "hello", "world", null, "foo", "bar" }),
                        pa.array(new[] { 0.0, 1.4, 2.5, 3.6, 4.7 })
                    }),
                    new[] { "col1", "col2", "col3" });

                dynamic pyBatch = table.to_batches()[0];

                long deviceArrayPtr = ((IntPtr)cDeviceArray).ToInt64();
                long schemaPtr = ((IntPtr)cSchema).ToInt64();
                pyBatch._export_to_c_device(deviceArrayPtr, schemaPtr);
            }

            Assert.Equal(ArrowDeviceType.Cpu, cDeviceArray->device_type);
            Assert.Equal(-1, cDeviceArray->device_id);

            Schema schema = CArrowSchemaImporter.ImportSchema(cSchema);
            RecordBatch imported = CArrowDeviceArrayImporter.ImportRecordBatch(cDeviceArray, schema);

            Assert.Equal(5, imported.Length);

            Int64Array col1 = (Int64Array)imported.Column("col1");
            Assert.Equal(1, col1.GetValue(0));
            Assert.Equal(2, col1.GetValue(1));
            Assert.Equal(3, col1.GetValue(2));
            Assert.Null(col1.GetValue(3));
            Assert.Equal(5, col1.GetValue(4));

            StringArray col2 = (StringArray)imported.Column("col2");
            Assert.Equal("hello", col2.GetString(0));
            Assert.Equal("world", col2.GetString(1));
            Assert.Null(col2.GetString(2));
            Assert.Equal("foo", col2.GetString(3));
            Assert.Equal("bar", col2.GetString(4));

            DoubleArray col3 = (DoubleArray)imported.Column("col3");
            Assert.Equal(new double[] { 0.0, 1.4, 2.5, 3.6, 4.7 }, col3.Values.ToArray());

            imported.Dispose();
            CArrowDeviceArray.Free(cDeviceArray);
        }

        [SkippableFact]
        public unsafe void RoundTripTestBatchViaDevice()
        {
            // C# -> Python (via device) -> C# round trip
            HashSet<ArrowTypeId> unsupported = new HashSet<ArrowTypeId> { ArrowTypeId.ListView, ArrowTypeId.BinaryView, ArrowTypeId.StringView, ArrowTypeId.Decimal32, ArrowTypeId.Decimal64 };
            RecordBatch batch1 = TestData.CreateSampleRecordBatch(4, excludedTypes: unsupported);
            RecordBatch batch2 = batch1.Clone();

            CArrowDeviceArray* cExportDeviceArray = CArrowDeviceArray.Create();
            CArrowDeviceArrayExporter.ExportRecordBatch(batch1, cExportDeviceArray);

            CArrowSchema* cExportSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(batch1.Schema, cExportSchema);

            CArrowDeviceArray* cImportDeviceArray = CArrowDeviceArray.Create();
            CArrowSchema* cImportSchema = CArrowSchema.Create();

            long exportDeviceArrayPtr = ((IntPtr)cExportDeviceArray).ToInt64();
            long exportSchemaPtr = ((IntPtr)cExportSchema).ToInt64();
            long importDeviceArrayPtr = ((IntPtr)cImportDeviceArray).ToInt64();
            long importSchemaPtr = ((IntPtr)cImportSchema).ToInt64();

            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic exportedPyBatch = pa.RecordBatch._import_from_c_device(exportDeviceArrayPtr, exportSchemaPtr);
                // Re-export back via device interface
                exportedPyBatch._export_to_c_device(importDeviceArrayPtr, importSchemaPtr);
            }

            Assert.Equal(ArrowDeviceType.Cpu, cImportDeviceArray->device_type);

            Schema schema = CArrowSchemaImporter.ImportSchema(cImportSchema);
            RecordBatch importedBatch = CArrowDeviceArrayImporter.ImportRecordBatch(cImportDeviceArray, schema);

            ArrowReaderVerifier.CompareBatches(batch2, importedBatch, strictCompare: false);

            CArrowDeviceArray.Free(cExportDeviceArray);
            CArrowSchema.Free(cExportSchema);
            CArrowDeviceArray.Free(cImportDeviceArray);
            CArrowSchema.Free(cImportSchema);
        }
    }
}
