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

using System.Runtime.Intrinsics;

using Apache.Arrow.Memory;

namespace Apache.Arrow.Operations;

internal static class BitVectorOps
{
    internal static ArrowBuffer AllOnes(int numBytes, MemoryAllocator? allocator = default)
    {
        var zeros = AllZeros(numBytes, allocator);
        return OnesComplement(zeros, allocator);
    }

    internal static ArrowBuffer AllZeros(int numBytes, MemoryAllocator? allocator = default)
    {
        // Exploit that this uses new byte[...] to allocate the memory which necessarily
        // zeros out everything.
        var builder = new ArrowBuffer.BitmapBuilder(numBytes * 8);
        builder.Set(numBytes * 8 - 1, false);
        return builder.Build(allocator);
    }

    internal static ArrowBuffer OnesComplement(ArrowBuffer buffer, MemoryAllocator? allocator = default)
    {
        var builder = new ArrowBuffer.BitmapBuilder(buffer.Length * 8);
        var store = builder.Span;
        int offset = 0;
        int size = buffer.Span.Length;

        if (Vector512.IsHardwareAccelerated)
        {
            while ((size - offset) >= 64)
            {
                var part = buffer.Span.Slice(offset, 64);
                Vector512<byte> vector = Vector512.Create(part);
                vector = Vector512.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 64));
                offset += 64;
            }
        }
        if (Vector256.IsHardwareAccelerated)
        {
            while ((size - offset) >= 32)
            {
                var part = buffer.Span.Slice(offset, 32);
                Vector256<byte> vector = Vector256.Create(part);
                vector = Vector256.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 32));
                offset += 32;
            }
        }
        while ((size - offset) >= 16)
        {
            var part = buffer.Span.Slice(offset, 16);
            Vector128<byte> vector = Vector128.Create(part);
            vector = Vector128.OnesComplement(vector);
            vector.CopyTo(store.Slice(offset, 16));
            offset += 16;
        }
        while ((size - offset) >= 8)
        {
            var part = buffer.Span.Slice(offset, 8);
            Vector64<byte> vector = Vector64.Create(part);
            vector = Vector64.OnesComplement(vector);
            vector.CopyTo(store.Slice(offset, 8));
            offset += 8;
        }
        for (var i = offset; i < size; i++)
        {
            store[i] = (byte)~buffer.Span[i];
        }
        return builder.Build(allocator);
    }

    internal static ArrowBuffer And(ArrowBuffer lhs, ArrowBuffer rhs, MemoryAllocator? allocator = default)
    {
        if (lhs.IsEmpty)
        {
            if (rhs.IsEmpty)
            {
                return ArrowBuffer.Empty;
            }
            else
            {
                return rhs;
            }
        }
        else if (rhs.IsEmpty) return lhs;

        var builder = new ArrowBuffer.BitmapBuilder(lhs.Length * 8);
        var store = builder.Span;
        int offset = 0;
        int size = lhs.Span.Length;

        if (Vector512.IsHardwareAccelerated)
        {
            while ((size - offset) >= 64)
            {
                var part = lhs.Span.Slice(offset, 64);
                Vector512<byte> vlhs = Vector512.Create(part);
                part = rhs.Span.Slice(offset, 64);
                Vector512<byte> vrhs = Vector512.Create(part);
                vlhs = vlhs & vrhs;
                vlhs.CopyTo(store.Slice(offset, 64));
                offset += 64;
            }
        }
        if (Vector256.IsHardwareAccelerated)
        {
            while ((size - offset) >= 32)
            {
                var part = lhs.Span.Slice(offset, 32);
                Vector256<byte> vlhs = Vector256.Create(part);
                part = rhs.Span.Slice(offset, 32);
                Vector256<byte> vrhs = Vector256.Create(part);
                vlhs = vlhs & vrhs;
                vlhs.CopyTo(store.Slice(offset, 32));
                offset += 32;
            }
        }
        while ((size - offset) >= 16)
        {
            var part = lhs.Span.Slice(offset, 16);
            Vector128<byte> vlhs = Vector128.Create(part);
            part = rhs.Span.Slice(offset, 16);
            Vector128<byte> vrhs = Vector128.Create(part);
            vlhs = vlhs & vrhs;
            vlhs.CopyTo(store.Slice(offset, 16));
            offset += 16;
        }
        while ((size - offset) >= 8)
        {
            var part = lhs.Span.Slice(offset, 8);
            Vector64<byte> vlhs = Vector64.Create(part);
            part = rhs.Span.Slice(offset, 8);
            Vector64<byte> vrhs = Vector64.Create(part);
            vlhs = vlhs & vrhs;
            vlhs.CopyTo(store.Slice(offset, 8));
            offset += 8;
        }
        for (var i = offset; i < size; i++)
        {
            store[i] = (byte)(lhs.Span[i] & rhs.Span[i]);
        }
        return builder.Build(allocator);
    }

    internal static ArrowBuffer Or(ArrowBuffer lhs, ArrowBuffer rhs, MemoryAllocator? allocator = default)
    {
        if (lhs.IsEmpty)
        {
            return lhs;
        }
        else if (rhs.IsEmpty) return rhs;

        var builder = new ArrowBuffer.BitmapBuilder(lhs.Length * 8);
        var store = builder.Span;
        int offset = 0;
        int size = lhs.Span.Length;

        if (Vector512.IsHardwareAccelerated)
        {
            while ((size - offset) >= 64)
            {
                var part = lhs.Span.Slice(offset, 64);
                Vector512<byte> vlhs = Vector512.Create(part);
                part = rhs.Span.Slice(offset, 64);
                Vector512<byte> vrhs = Vector512.Create(part);
                vlhs = vlhs | vrhs;
                vlhs.CopyTo(store.Slice(offset, 64));
                offset += 64;
            }
        }
        if (Vector256.IsHardwareAccelerated)
        {
            while ((size - offset) >= 32)
            {
                var part = lhs.Span.Slice(offset, 32);
                Vector256<byte> vlhs = Vector256.Create(part);
                part = rhs.Span.Slice(offset, 32);
                Vector256<byte> vrhs = Vector256.Create(part);
                vlhs = vlhs | vrhs;
                vlhs.CopyTo(store.Slice(offset, 32));
                offset += 32;
            }
        }
        while ((size - offset) >= 16)
        {
            var part = lhs.Span.Slice(offset, 16);
            Vector128<byte> vlhs = Vector128.Create(part);
            part = rhs.Span.Slice(offset, 16);
            Vector128<byte> vrhs = Vector128.Create(part);
            vlhs = vlhs | vrhs;
            vlhs.CopyTo(store.Slice(offset, 16));
            offset += 16;
        }
        while ((size - offset) >= 8)
        {
            var part = lhs.Span.Slice(offset, 8);
            Vector64<byte> vlhs = Vector64.Create(part);
            part = rhs.Span.Slice(offset, 8);
            Vector64<byte> vrhs = Vector64.Create(part);
            vlhs = vlhs | vrhs;
            vlhs.CopyTo(store.Slice(offset, 8));
            offset += 8;
        }
        for (var i = offset; i < size; i++)
        {
            store[i] = (byte)(lhs.Span[i] | rhs.Span[i]);
        }
        return builder.Build(allocator);
    }

    internal static ArrowBuffer Xor(ArrowBuffer lhs, ArrowBuffer rhs, MemoryAllocator? allocator = default)
    {
        if (lhs.IsEmpty)
        {
            if (rhs.IsEmpty)
            {
                return ArrowBuffer.Empty;
            }
            else
            {
                return OnesComplement(rhs, allocator);
            }
        }
        else if (rhs.IsEmpty)
        {
            return OnesComplement(lhs, allocator);
        }
        var builder = new ArrowBuffer.BitmapBuilder(lhs.Length * 8);
        var store = builder.Span;
        int offset = 0;
        int size = lhs.Span.Length;

        if (Vector512.IsHardwareAccelerated)
        {
            while ((size - offset) >= 64)
            {
                var part = lhs.Span.Slice(offset, 64);
                Vector512<byte> vlhs = Vector512.Create(part);
                part = rhs.Span.Slice(offset, 64);
                Vector512<byte> vrhs = Vector512.Create(part);
                vlhs = vlhs ^ vrhs;
                vlhs.CopyTo(store.Slice(offset, 64));
                offset += 64;
            }
        }
        if (Vector256.IsHardwareAccelerated)
        {
            while ((size - offset) >= 32)
            {
                var part = lhs.Span.Slice(offset, 32);
                Vector256<byte> vlhs = Vector256.Create(part);
                part = rhs.Span.Slice(offset, 32);
                Vector256<byte> vrhs = Vector256.Create(part);
                vlhs = vlhs ^ vrhs;
                vlhs.CopyTo(store.Slice(offset, 32));
                offset += 32;
            }
        }
        while ((size - offset) >= 16)
        {
            var part = lhs.Span.Slice(offset, 16);
            Vector128<byte> vlhs = Vector128.Create(part);
            part = rhs.Span.Slice(offset, 16);
            Vector128<byte> vrhs = Vector128.Create(part);
            vlhs = vlhs ^ vrhs;
            vlhs.CopyTo(store.Slice(offset, 16));
            offset += 16;
        }
        while ((size - offset) >= 8)
        {
            var part = lhs.Span.Slice(offset, 8);
            Vector64<byte> vlhs = Vector64.Create(part);
            part = rhs.Span.Slice(offset, 8);
            Vector64<byte> vrhs = Vector64.Create(part);
            vlhs = vlhs ^ vrhs;
            vlhs.CopyTo(store.Slice(offset, 8));
            offset += 8;
        }

        for (var i = offset; i < size; i++)
        {
            store[i] = (byte)(lhs.Span[i] ^ rhs.Span[i]);
        }
        return builder.Build(allocator);
    }
}
