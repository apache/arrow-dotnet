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
using System.IO;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Ipc;

namespace Apache.Arrow
{
    /// <summary>
    /// Helpers for serializing partial Arrow structures to and from buffers.
    /// </summary>
    public static class ArrowSerializationHelpers
    {
        public static Schema DeserializeSchema(ReadOnlyMemory<byte> serializedSchema)
        {
            ArrowMemoryReaderImplementation implementation = new ArrowMemoryReaderImplementation(serializedSchema, null);
            return implementation.Schema;
        }

        public static RecordBatch DeserializeRecordBatch(Schema schema, ReadOnlyMemory<byte> serializedRecordBatch)
        {
            ArrowMemoryReaderImplementation implementation = new ArrowMemoryReaderImplementation(schema, serializedRecordBatch, null);
            return implementation.ReadNextRecordBatch();
        }

        public static byte[] SerializeSchema(Schema schema)
        {
            using (var stream = new MemoryStream())
            {
                var writer = new SchemaWriter(stream, schema);
                writer.WriteSchema(schema);
                return stream.ToArray();
            }
        }

        public static byte[] SerializeRecordBatch(RecordBatch recordBatch)
        {
            using (var stream = new MemoryStream())
            {
                var writer = new SchemaWriter(stream, recordBatch.Schema);
                writer.WriteBatch(recordBatch);
                return stream.ToArray();
            }
        }

        /// <summary>
        /// Helper useful when writing just individual parts of the Arrow IPC format.
        /// </summary>
        internal class SchemaWriter : ArrowStreamWriter
        {
            internal SchemaWriter(Stream baseStream, Schema schema) : base(baseStream, schema)
            {
            }

            public void WriteSchema(Schema schema)
            {
                var offset = base.SerializeSchema(schema);
                WriteMessage(MessageHeader.Schema, offset, 0);
            }

            public void WriteBatch(RecordBatch recordBatch)
            {
                HasWrittenSchema = true; // Avoid serializing the schema
                WriteRecordBatch(recordBatch);
                WriteEnd();
            }

            private protected override void StartingWritingDictionary()
            {
                throw new InvalidOperationException("Dictionary batches not supported");
            }
        }
    }
}
