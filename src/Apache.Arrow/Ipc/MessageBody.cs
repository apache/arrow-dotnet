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
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Ipc
{
    /// <summary>
    /// The body of an IPC message whose buffers are shared by reference counting, so that the
    /// body is released once the reader and every buffer that refers to it have released it.
    /// Dictionary batches need this: a dictionary is shared by every record batch that uses it,
    /// and can outlive the reader and later dictionaries that replace it.
    /// </summary>
    internal sealed class MessageBody
    {
        private readonly SharedMemoryOwner _owner;

        /// <param name="owner">The body's memory, of which this takes ownership.</param>
        /// <param name="lifetime">The lifetime of the allocator <paramref name="owner"/> came from.</param>
        public MessageBody(IMemoryOwner<byte> owner, AllocationLifetime lifetime)
        {
            _owner = new SharedMemoryOwner(owner, finalizeWhenUnreachable: lifetime == AllocationLifetime.Finalized);
        }

        /// <summary>
        /// Creates a buffer over <paramref name="source"/>, which must lie within the body, that
        /// holds its own reference to the body.
        /// </summary>
        public ArrowBuffer CreateBuffer(ReadOnlyMemory<byte> source)
        {
            if (source.IsEmpty)
            {
                return ArrowBuffer.Empty;
            }

            Memory<byte> body = _owner.Memory;
            long offset = Unsafe.ByteOffset(
                ref MemoryMarshal.GetReference(body.Span),
                ref MemoryMarshal.GetReference(source.Span)).ToInt64();
            if (offset < 0 || offset > body.Length - source.Length)
            {
                throw new InvalidOperationException("The buffer does not lie within the message body.");
            }

            return new ArrowBuffer(_owner.Retain((int)offset, source.Length));
        }

        /// <summary>
        /// Releases the reference held since construction. Buffers created from the body keep
        /// it alive until they are disposed too.
        /// </summary>
        public void Release() => _owner.Release();
    }
}
