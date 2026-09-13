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

using System.Collections.Generic;

namespace Apache.Arrow.Ipc
{
    /// <summary>
    /// A record batch read from an Arrow IPC source, together with the custom metadata
    /// carried on the IPC Message that held it.
    /// </summary>
    public readonly struct RecordBatchWithMetadata
    {
        public RecordBatchWithMetadata(RecordBatch batch, IReadOnlyDictionary<string, string> customMetadata)
        {
            Batch = batch;
            CustomMetadata = customMetadata;
        }

        /// <summary>
        /// The record batch that was read, or null at the end of the stream.
        /// </summary>
        public RecordBatch Batch { get; }

        /// <summary>
        /// The Message-level custom metadata accompanying <see cref="Batch"/>, or null if the
        /// message carried none.
        /// </summary>
        public IReadOnlyDictionary<string, string> CustomMetadata { get; }

        public void Deconstruct(out RecordBatch batch, out IReadOnlyDictionary<string, string> customMetadata)
        {
            batch = Batch;
            customMetadata = CustomMetadata;
        }
    }
}
