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

using System.IO;
using Apache.Arrow.Ipc;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class SerializationHelperTests
    {
        [Fact]
        public void SchemaRoundTrip()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(100);
            var serialized = ArrowSerializationHelpers.SerializeSchema(originalBatch.Schema);
            var deserialized = ArrowSerializationHelpers.DeserializeSchema(serialized);

            SchemaComparer.Compare(originalBatch.Schema, deserialized);
        }

        [Fact]
        public void RecordBatchRoundTrip()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(100, createDictionaryArray: false);
            var serialized = ArrowSerializationHelpers.SerializeRecordBatch(originalBatch);
            var deserialized = ArrowSerializationHelpers.DeserializeRecordBatch(originalBatch.Schema, serialized);

            ArrowReaderVerifier.CompareBatches(originalBatch, deserialized);
        }

        [Fact]
        public void ConcatSchemaAndBatchWrite()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(100, createDictionaryArray: false);
            var schema = ArrowSerializationHelpers.SerializeSchema(originalBatch.Schema);
            var serialized = ArrowSerializationHelpers.SerializeRecordBatch(originalBatch);

            var buffer = new byte[schema.Length + serialized.Length];
            System.Array.Copy(schema, buffer, schema.Length);
            System.Array.Copy(serialized, 0, buffer, schema.Length, serialized.Length);

            using (var stream = new MemoryStream(buffer))
            using (var reader = new ArrowStreamReader(stream))
            {
                var deserialized = reader.ReadNextRecordBatch();
                ArrowReaderVerifier.CompareBatches(originalBatch, deserialized);
            }
        }
    }
}
