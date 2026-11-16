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

using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    /// <summary>
    /// Carries configuration through Arrow operations: memory allocator,
    /// compression codec factory, and extension type registry.
    /// </summary>
    public class ArrowContext
    {
        /// <summary>
        /// The allocator for memory allocated by operations given this context. <c>null</c> means
        /// <see cref="MemoryAllocator.Default"/>.
        /// </summary>
        public MemoryAllocator Allocator { get; }

        /// <summary>
        /// The compression codec factory used to decompress when reading, and to compress when
        /// writing if the <see cref="IpcOptions"/> passed to the writer do not specify one.
        /// </summary>
        public ICompressionCodecFactory CompressionCodecFactory { get; }

        /// <summary>
        /// The extension type registry. When none is passed to the constructor, this is a snapshot
        /// of <see cref="ExtensionTypeRegistry.Default"/> as it was when the context was created;
        /// taking the snapshot is cheap, as the registries share storage until either changes.
        /// </summary>
        public ExtensionTypeRegistry ExtensionRegistry { get; }

        public ArrowContext(
            MemoryAllocator allocator = null,
            ICompressionCodecFactory compressionCodecFactory = null,
            ExtensionTypeRegistry extensionRegistry = null)
        {
            Allocator = allocator;
            CompressionCodecFactory = compressionCodecFactory;
            ExtensionRegistry = extensionRegistry ?? ExtensionTypeRegistry.Default.Clone();
        }
    }
}
