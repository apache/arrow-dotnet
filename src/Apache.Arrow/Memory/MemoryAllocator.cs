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
using System.Threading;

namespace Apache.Arrow.Memory
{
    public abstract class MemoryAllocator
    {
        public const int DefaultAlignment = 64;

        private static IMemoryOwner<byte> NullMemoryOwner { get; } = new NullMemoryOwner();

        /// <summary>
        /// The allocator used wherever none is passed explicitly: a <see cref="NativeMemoryAllocator"/>
        /// with the default alignment. It cannot be replaced; to use a different allocator, pass
        /// it to the builder, reader, writer or operation that allocates.
        /// </summary>
        public static Lazy<MemoryAllocator> Default { get; } = new Lazy<MemoryAllocator>(BuildDefault, true);

        /// <summary>
        /// Describes what happens to buffers from this allocator that are never disposed.
        /// </summary>
        /// <remarks>
        /// Defaults to <see cref="AllocationLifetime.Finalized"/> so that allocators written
        /// against earlier versions of this type continue to describe themselves
        /// conservatively. Implementations should override this.
        /// </remarks>
        public virtual AllocationLifetime Lifetime => AllocationLifetime.Finalized;

        public class Stats
        {
            private long _bytesAllocated;
            private long _allocations;

            public long Allocations => Interlocked.Read(ref _allocations);
            public long BytesAllocated => Interlocked.Read(ref _bytesAllocated);

            internal void Allocate(int n)
            {
                Interlocked.Increment(ref _allocations);
                Interlocked.Add(ref _bytesAllocated, n);
            }
        }

        public Stats Statistics { get; }

        protected int Alignment { get; }

        protected MemoryAllocator(int alignment = DefaultAlignment)
        {
            Statistics = new Stats();
            Alignment = alignment;
        }

        public IMemoryOwner<byte> Allocate(int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            if (length == 0)
            {
                return NullMemoryOwner;
            }

            IMemoryOwner<byte> memoryOwner = AllocateInternal(length, out int bytesAllocated);

            Statistics.Allocate(bytesAllocated);

            return memoryOwner;
        }

        /// <summary>
        /// Returns a buffer of <paramref name="newLength"/> bytes containing the contents of
        /// <paramref name="owner"/>, and releases <paramref name="owner"/>.
        /// </summary>
        /// <remarks>
        /// The default implementation allocates, copies and releases. Allocators backed by a
        /// native heap that supports reallocation can override this to grow in place. This
        /// exists so that buffer builders can grow through the allocator rather than
        /// allocating managed scratch space that the allocator never sees.
        /// </remarks>
        public virtual IMemoryOwner<byte> Resize(IMemoryOwner<byte> owner, int newLength)
        {
            if (owner == null)
            {
                throw new ArgumentNullException(nameof(owner));
            }

            if (newLength < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(newLength));
            }

            IMemoryOwner<byte> resized = Allocate(newLength);
            int copyLength = Math.Min(owner.Memory.Length, newLength);
            if (copyLength > 0)
            {
                owner.Memory.Slice(0, copyLength).CopyTo(resized.Memory);
            }

            owner.Dispose();
            return resized;
        }

        private static MemoryAllocator BuildDefault()
        {
            return new NativeMemoryAllocator(DefaultAlignment);
        }

        protected abstract IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated);
    }
}
