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
using System.IO;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Python.Runtime;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Structs and unions with no children, exchanged with pyarrow through the C data interface and the IPC
    /// stream format, in both directions.
    /// </summary>
    [Collection("PythonNet")]
    public class EmptyStructPythonTests
    {
        public EmptyStructPythonTests(PythonNetFixture pythonNet)
        {
            pythonNet.EnsureInitialized();
        }

        /// <summary>
        /// What <see cref="EmptyStructTests.Batch"/> holds, built by pyarrow. <c>check(batch)</c> validates a
        /// batch fully and returns an empty string when it matches, or a description of the difference.
        /// </summary>
        private const string ExpectedBatch = @"
import pyarrow as pa

expected = pa.record_batch(
    [
        pa.array([{}, None, {}], type=pa.struct([])),
        pa.array([[{}, None], [], [{}]], type=pa.list_(pa.field('item', pa.struct([]), nullable=True))),
        pa.array([1, 2, 3], type=pa.int32()),
    ],
    schema=pa.schema([
        pa.field('empty', pa.struct([]), nullable=True),
        pa.field('list_of_empty', pa.list_(pa.field('item', pa.struct([]), nullable=True)), nullable=True),
        pa.field('ints', pa.int32(), nullable=False),
    ]))

def check(batch):
    batch.validate(full=True)
    if not batch.schema.equals(expected.schema):
        return 'schema: ' + str(batch.schema)
    if batch.to_pylist() != expected.to_pylist():
        return 'values: ' + str(batch.to_pylist())
    return ''
";

        private static void AssertIsExpectedBatch(RecordBatch batch)
        {
            Assert.Equal(3, batch.Length);
            var empty = (StructArray)batch.Column("empty");
            Assert.Empty(((StructType)empty.Data.DataType).Fields);
            Assert.True(empty.IsValid(0));
            Assert.True(empty.IsNull(1));
            Assert.True(empty.IsValid(2));

            var list = (ListArray)batch.Column("list_of_empty");
            Assert.Equal(new[] { 0, 2, 2, 3 }, list.ValueOffsets.ToArray());
            var items = (StructArray)list.Values;
            Assert.Empty(((StructType)items.Data.DataType).Fields);
            Assert.True(items.IsValid(0));
            Assert.True(items.IsNull(1));
            Assert.True(items.IsValid(2));

            Assert.Equal(new[] { 1, 2, 3 }, ((Int32Array)batch.Column("ints")).Values.ToArray());
        }

        [SkippableFact]
        public unsafe void ABatchFromPyarrow_ImportsThroughTheCDataInterface()
        {
            CArrowArray* cArray = CArrowArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                using (Py.GIL())
                using (PyModule scope = Py.CreateScope())
                {
                    scope.Exec(ExpectedBatch);
                    scope.Get("expected").InvokeMethod(
                        "_export_to_c",
                        new PyInt(((IntPtr)cArray).ToInt64()),
                        new PyInt(((IntPtr)cSchema).ToInt64()));
                }

                Schema schema = CArrowSchemaImporter.ImportSchema(cSchema);
                using RecordBatch batch = CArrowArrayImporter.ImportRecordBatch(cArray, schema);
                AssertIsExpectedBatch(batch);
            }
            finally
            {
                CArrowArray.Free(cArray);
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public unsafe void ABatchExportedThroughTheCDataInterface_ReadsInPyarrow()
        {
            RecordBatch batch = EmptyStructTests.Batch();
            CArrowArray* cArray = CArrowArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportSchema(batch.Schema, cSchema);
                CArrowArrayExporter.ExportRecordBatch(batch, cArray);

                using (Py.GIL())
                using (PyModule scope = Py.CreateScope())
                {
                    scope.Exec(ExpectedBatch);
                    scope.Set("array_ptr", ((IntPtr)cArray).ToInt64());
                    scope.Set("schema_ptr", ((IntPtr)cSchema).ToInt64());
                    scope.Exec("result = check(pa.RecordBatch._import_from_c(array_ptr, schema_ptr))");
                    Assert.Equal("", scope.Get<string>("result"));
                }
            }
            finally
            {
                // pyarrow took ownership by calling release; Free only releases what is still unreleased.
                CArrowArray.Free(cArray);
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableFact]
        public void AStreamWrittenByPyarrow_Reads()
        {
            string path = Path.Combine(Path.GetTempPath(), $"arrow_empty_struct_{Guid.NewGuid():N}.arrows");
            try
            {
                using (Py.GIL())
                using (PyModule scope = Py.CreateScope())
                {
                    scope.Exec(ExpectedBatch);
                    scope.Set("path", path);
                    scope.Exec(@"
with pa.OSFile(path, 'wb') as sink:
    with pa.ipc.new_stream(sink, expected.schema) as writer:
        writer.write_batch(expected)
");
                }

                using var stream = File.OpenRead(path);
                using var reader = new ArrowStreamReader(stream);
                AssertIsExpectedBatch(reader.ReadNextRecordBatch());
            }
            finally
            {
                File.Delete(path);
            }
        }

        [SkippableFact]
        public void AStreamWrittenHere_ReadsInPyarrow()
        {
            RecordBatch batch = EmptyStructTests.Batch();
            string path = Path.Combine(Path.GetTempPath(), $"arrow_empty_struct_{Guid.NewGuid():N}.arrows");
            try
            {
                using (var stream = File.Create(path))
                using (var writer = new ArrowStreamWriter(stream, batch.Schema))
                {
                    writer.WriteRecordBatch(batch);
                    writer.WriteEnd();
                }

                using (Py.GIL())
                using (PyModule scope = Py.CreateScope())
                {
                    scope.Exec(ExpectedBatch);
                    scope.Set("path", path);
                    scope.Exec(@"
with pa.OSFile(path, 'rb') as source:
    batches = pa.ipc.open_stream(source).read_all().to_batches()
result = check(batches[0]) if len(batches) == 1 else 'batches: ' + str(len(batches))
");
                    Assert.Equal("", scope.Get<string>("result"));
                }
            }
            finally
            {
                File.Delete(path);
            }
        }

        [SkippableTheory]
        [InlineData("sparse")]
        [InlineData("dense")]
        public unsafe void AnEmptyUnionTypeFromPyarrow_Imports(string mode)
        {
            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                using (Py.GIL())
                using (PyModule scope = Py.CreateScope())
                {
                    scope.Set("schema_ptr", ((IntPtr)cSchema).ToInt64());
                    scope.Exec($"import pyarrow as pa\npa.{mode}_union([])._export_to_c(schema_ptr)");
                }

                var type = (UnionType)CArrowSchemaImporter.ImportType(cSchema);
                Assert.Equal(mode == "sparse" ? UnionMode.Sparse : UnionMode.Dense, type.Mode);
                Assert.Empty(type.Fields);
                Assert.Empty(type.TypeIds);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }

        [SkippableTheory]
        [InlineData(UnionMode.Sparse)]
        [InlineData(UnionMode.Dense)]
        public unsafe void AnEmptyUnionTypeExportedHere_ImportsInPyarrow(UnionMode mode)
        {
            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportType(new UnionType(new List<Field>(), new int[0], mode), cSchema);

                using (Py.GIL())
                using (PyModule scope = Py.CreateScope())
                {
                    scope.Set("schema_ptr", ((IntPtr)cSchema).ToInt64());
                    scope.Set("mode", mode == UnionMode.Sparse ? "sparse" : "dense");
                    scope.Exec(@"
import pyarrow as pa
imported = pa.DataType._import_from_c(schema_ptr)
expected = pa.sparse_union([]) if mode == 'sparse' else pa.dense_union([])
result = '' if imported.equals(expected) else str(imported)
");
                    Assert.Equal("", scope.Get<string>("result"));
                }
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }
    }
}
