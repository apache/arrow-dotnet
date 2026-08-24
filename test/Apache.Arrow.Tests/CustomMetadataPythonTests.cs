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
using Apache.Arrow.Ipc;
using Python.Runtime;
using Xunit;

namespace Apache.Arrow.Tests
{

    // -------------------------------------------------------------------
    // Cross-language Python tests for custom_metadata
    // -------------------------------------------------------------------

    [Collection("PythonNet")]
    public class CustomMetadataPythonTests
    {
        public CustomMetadataPythonTests(PythonNetFixture pythonNet)
        {
            pythonNet.EnsureInitialized();
        }

        // -------------------------------------------------------------------
        // C# writes IPC with custom_metadata → Python reads
        // -------------------------------------------------------------------

        [SkippableFact]
        public void ExportCustomMetadata_PythonReads()
        {
            RecordBatch batch = TestData.CreateSampleRecordBatch(length: 5);
            var batchMetadata = new Dictionary<string, string>
            {
                ["rpc.method"] = "greet",
                ["request_id"] = "abc-123",
                ["custom_key"] = "custom_value",
            };

            // Serialize to IPC stream with custom batch metadata
            byte[] ipcBytes;
            using (var ms = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(ms, batch.Schema, leaveOpen: true))
                {
                    writer.WriteRecordBatch(batch, batchMetadata);
                    writer.WriteEnd();
                }
                ipcBytes = ms.ToArray();
            }

            // Python reads and verifies custom_metadata
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic reader = pa.ipc.open_stream(pa.BufferReader(ipcBytes.ToPython()));

                PyObject result = reader.read_next_batch_with_custom_metadata();
                dynamic pyBatch = result[0];
                dynamic customMeta = result[1];

                // Verify batch data round-tripped
                Assert.Equal(5, (int)pyBatch.num_rows);

                // Verify custom_metadata (pyarrow returns bytes — decode to str)
                Assert.Equal("greet", (string)customMeta["rpc.method"].decode());
                Assert.Equal("abc-123", (string)customMeta["request_id"].decode());
                Assert.Equal("custom_value", (string)customMeta["custom_key"].decode());
            }
        }

        // -------------------------------------------------------------------
        // Python writes IPC with custom_metadata → C# reads
        // -------------------------------------------------------------------

        [SkippableFact]
        public void ImportCustomMetadata_PythonWrites()
        {
            byte[] ipcBytes;

            // Python creates a batch with custom_metadata and serializes to IPC
            using (Py.GIL())
            {
                dynamic pa = Py.Import("pyarrow");
                dynamic io = Py.Import("io");

                dynamic pyBatch = pa.record_batch(new PyList(new PyObject[]
                {
                    pa.array(new int[] { 1, 2, 3, 4, 5 }),
                }), new[] { "x" });

                dynamic buf = io.BytesIO();
                dynamic writer = pa.ipc.new_stream(buf, pyBatch.schema);
                dynamic customMeta = pa.KeyValueMetadata(new PyDict
                {
                    ["origin"] = "python".ToPython(),
                    ["version"] = "2".ToPython(),
                });
                writer.write_batch(pyBatch, custom_metadata: customMeta);
                writer.close();

                ipcBytes = ((PyObject)buf.getvalue()).As<byte[]>();
            }

            // C# reads and verifies custom_metadata
            using var ms = new MemoryStream(ipcBytes);
            using var reader = new ArrowStreamReader(ms);

            RecordBatch batch = reader.ReadNextRecordBatch();
            Assert.NotNull(batch);
            Assert.Equal(5, batch.Length);

            var metadata = reader.LastBatchCustomMetadata;
            Assert.NotNull(metadata);
            Assert.Equal("python", metadata["origin"]);
            Assert.Equal("2", metadata["version"]);
        }
    }
}
