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

namespace Apache.Arrow.Memory
{
    /// <summary>
    /// Describes what happens to a buffer produced by a <see cref="MemoryAllocator"/>
    /// if the consumer never disposes it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This describes a correctness contract, not a performance strategy. Disposing a
    /// buffer is always at least as fast as not disposing it, whatever the value here.
    /// </para>
    /// <para>
    /// Whatever the lifetime, a span over a buffer must not be used after the buffer is
    /// disposed: disposal may free the memory or make it available for reuse. The values
    /// here describe only what happens to a buffer that is <em>never</em> disposed.
    /// </para>
    /// </remarks>
    public enum AllocationLifetime
    {
        /// <summary>
        /// Memory is reclaimed by the garbage collector, so disposal is optional. A span over
        /// a buffer that has not been disposed keeps the memory alive while the span is in use.
        /// </summary>
        /// <remarks>
        /// An allocator that pools memory makes a disposed buffer's memory available to later
        /// allocations, so using a span after disposal can read or overwrite another buffer's
        /// data. <see cref="ManagedMemoryAllocator"/> does not pool, so with it a span stays
        /// valid even after disposal.
        /// </remarks>
        GarbageCollected,

        /// <summary>
        /// Memory is leaked unless the consumer disposes the buffer. A span over the buffer
        /// is valid until disposal, and is never invalidated by garbage collection.
        /// </summary>
        CallerOwned,

        /// <summary>
        /// Memory is reclaimed by a finalizer. Disposal is optional, but a span over the
        /// buffer does not keep it alive, so the memory may be freed while the span is
        /// still in use.
        /// </summary>
        /// <remarks>
        /// Provided for compatibility with the historical behaviour of
        /// <see cref="NativeMemoryAllocator"/>. See
        /// https://github.com/apache/arrow-dotnet/issues/397.
        /// </remarks>
        Finalized,
    }
}
