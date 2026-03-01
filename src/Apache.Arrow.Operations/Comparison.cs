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
using System.Numerics;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

using System.Runtime.Intrinsics;

namespace Apache.Arrow.Operations;

public static class BitVectorOps
{
    public static ArrowBuffer OnesComplement(ArrowBuffer buffer)
    {
        var builder = new ArrowBuffer.BitmapBuilder(buffer.Length * 8);
        var store = builder.Span;
        int offset = 0;
        int size = buffer.Span.Length;

        while ((size - offset) >= 8)
        {
            if ((size - offset) >= 64)
            {
                var part = buffer.Span.Slice(offset, 64);
                Vector512<byte> vector = Vector512.Create(part);
                vector = Vector512.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 64));
                offset += 64;
            }
            else if ((size - offset) >= 32)
            {
                var part = buffer.Span.Slice(offset, 32);
                Vector256<byte> vector = Vector256.Create(part);
                vector = Vector256.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 32));
                offset += 32;
            }
            else if ((size - offset) >= 16)
            {
                var part = buffer.Span.Slice(offset, 16);
                Vector128<byte> vector = Vector128.Create(part);
                vector = Vector128.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 16));
                offset += 16;
            }
            else if ((size - offset) >= 8)
            {
                var part = buffer.Span.Slice(offset, 8);
                Vector64<byte> vector = Vector64.Create(part);
                vector = Vector64.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 8));
                offset += 8;
            }
            else break;
        }

        for(var i = offset; i < size; i++)
        {
            store[i] = (byte)~buffer.Span[i];
        }
        return builder.Build();
    }

    public static ArrowBuffer And(ArrowBuffer buffer, ArrowBuffer buffer2)
    {
        var builder = new ArrowBuffer.BitmapBuilder(buffer.Length * 8);
        var store = builder.Span;
        int offset = 0;
        int size = buffer.Span.Length;

        while ((size - offset) >= 8)
        {
            if ((size - offset) >= 64)
            {
                var part = buffer.Span.Slice(offset, 64);
                Vector512<byte> vector = Vector512.Create(part);
                part = buffer2.Span.Slice(offset, 64);
                Vector512<byte> vector2 = Vector512.Create(part);
                vector = vector & vector2;
                vector.CopyTo(store.Slice(offset, 64));
                offset += 64;
            }
            else if ((size - offset) >= 32)
            {
                var part = buffer.Span.Slice(offset, 32);
                Vector256<byte> vector = Vector256.Create(part);
                part = buffer2.Span.Slice(offset, 32);
                Vector256<byte> vector2 = Vector256.Create(part);
                vector = vector & vector2;
                vector.CopyTo(store.Slice(offset, 32));
                offset += 32;
            }
            else if ((size - offset) >= 16)
            {
                var part = buffer.Span.Slice(offset, 16);
                Vector128<byte> vector = Vector128.Create(part);
                part = buffer2.Span.Slice(offset, 16);
                Vector128<byte> vector2 = Vector128.Create(part);
                vector = vector & vector2;
                vector = Vector128.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 16));
                offset += 16;
            }
            else if ((size - offset) >= 8)
            {
                var part = buffer.Span.Slice(offset, 8);
                Vector64<byte> vector = Vector64.Create(part);
                part = buffer2.Span.Slice(offset, 8);
                Vector64<byte> vector2 = Vector64.Create(part);
                vector = vector & vector2;
                vector.CopyTo(store.Slice(offset, 8));
                offset += 8;
            }
            else break;
        }

        for (var i = offset; i < size; i++)
        {
            store[i] = (byte)(buffer.Span[i] & buffer2.Span[i]);
        }
        return builder.Build();
    }

    public static ArrowBuffer Or(ArrowBuffer buffer, ArrowBuffer buffer2)
    {
        var builder = new ArrowBuffer.BitmapBuilder(buffer.Length * 8);
        var store = builder.Span;
        int offset = 0;
        int size = buffer.Span.Length;

        while ((size - offset) >= 8)
        {
            if ((size - offset) >= 64)
            {
                var part = buffer.Span.Slice(offset, 64);
                Vector512<byte> vector = Vector512.Create(part);
                part = buffer2.Span.Slice(offset, 64);
                Vector512<byte> vector2 = Vector512.Create(part);
                vector = vector | vector2;
                vector.CopyTo(store.Slice(offset, 64));
                offset += 64;
            }
            else if ((size - offset) >= 32)
            {
                var part = buffer.Span.Slice(offset, 32);
                Vector256<byte> vector = Vector256.Create(part);
                part = buffer2.Span.Slice(offset, 32);
                Vector256<byte> vector2 = Vector256.Create(part);
                vector = vector | vector2;
                vector.CopyTo(store.Slice(offset, 32));
                offset += 32;
            }
            else if ((size - offset) >= 16)
            {
                var part = buffer.Span.Slice(offset, 16);
                Vector128<byte> vector = Vector128.Create(part);
                part = buffer2.Span.Slice(offset, 16);
                Vector128<byte> vector2 = Vector128.Create(part);
                vector = vector | vector2;
                vector = Vector128.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 16));
                offset += 16;
            }
            else if ((size - offset) >= 8)
            {
                var part = buffer.Span.Slice(offset, 8);
                Vector64<byte> vector = Vector64.Create(part);
                part = buffer2.Span.Slice(offset, 8);
                Vector64<byte> vector2 = Vector64.Create(part);
                vector = vector | vector2;
                vector.CopyTo(store.Slice(offset, 8));
                offset += 8;
            }
            else break;
        }

        for (var i = offset; i < size; i++)
        {
            store[i] = (byte)(buffer.Span[i] | buffer2.Span[i]);
        }
        return builder.Build();
    }

    public static ArrowBuffer Xor(ArrowBuffer buffer, ArrowBuffer buffer2)
    {
        var builder = new ArrowBuffer.BitmapBuilder(buffer.Length * 8);
        var store = builder.Span;
        int offset = 0;
        int size = buffer.Span.Length;

        while ((size - offset) >= 8)
        {
            if ((size - offset) >= 64)
            {
                var part = buffer.Span.Slice(offset, 64);
                Vector512<byte> vector = Vector512.Create(part);
                part = buffer2.Span.Slice(offset, 64);
                Vector512<byte> vector2 = Vector512.Create(part);
                vector = vector ^ vector2;
                vector.CopyTo(store.Slice(offset, 64));
                offset += 64;
            }
            else if ((size - offset) >= 32)
            {
                var part = buffer.Span.Slice(offset, 32);
                Vector256<byte> vector = Vector256.Create(part);
                part = buffer2.Span.Slice(offset, 32);
                Vector256<byte> vector2 = Vector256.Create(part);
                vector = vector ^ vector2;
                vector.CopyTo(store.Slice(offset, 32));
                offset += 32;
            }
            else if ((size - offset) >= 16)
            {
                var part = buffer.Span.Slice(offset, 16);
                Vector128<byte> vector = Vector128.Create(part);
                part = buffer2.Span.Slice(offset, 16);
                Vector128<byte> vector2 = Vector128.Create(part);
                vector = vector ^ vector2;
                vector = Vector128.OnesComplement(vector);
                vector.CopyTo(store.Slice(offset, 16));
                offset += 16;
            }
            else if ((size - offset) >= 8)
            {
                var part = buffer.Span.Slice(offset, 8);
                Vector64<byte> vector = Vector64.Create(part);
                part = buffer2.Span.Slice(offset, 8);
                Vector64<byte> vector2 = Vector64.Create(part);
                vector = vector ^ vector2;
                vector.CopyTo(store.Slice(offset, 8));
                offset += 8;
            }
            else break;
        }

        for (var i = offset; i < size; i++)
        {
            store[i] = (byte)(buffer.Span[i] ^ buffer2.Span[i]);
        }
        return builder.Build();
    }
}

public static class Comparison
{
    /// <summary>
    /// Negate a boolean array, flipping true to false, false to true. Nulls remain null
    /// </summary>
    /// <param name="mask"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static BooleanArray Invert(BooleanArray mask, MemoryAllocator? allocator = null)
    {
        var inverted = BitVectorOps.OnesComplement(mask.ValueBuffer);
        var invertedmask = new BooleanArray(inverted, mask.NullBitmapBuffer.Clone(), mask.Length, mask.NullCount, 0);
        return invertedmask;
    }

    /// <summary>
    /// An alias for <see cref="Invert"/> that is idiomatic.
    /// </summary>
    /// <param name="mask"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static BooleanArray OnesComplement(BooleanArray mask, MemoryAllocator? allocator = null) => Invert(mask, allocator);

    /// <summary>
    /// Perform a pairwise boolean AND operation.
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static BooleanArray And(BooleanArray lhs, BooleanArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var combined = BitVectorOps.And(lhs.ValueBuffer, rhs.ValueBuffer);
        var combinedMask = BitVectorOps.And(lhs.NullBitmapBuffer, rhs.NullBitmapBuffer);
        var nullCount = BitUtility.CountBits(combinedMask.Span);
        return new BooleanArray(combined, combinedMask, lhs.Length, nullCount, 0);
    }

    /// <summary>
    /// Performa a pairwise boolean OR operation.
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static BooleanArray Or(BooleanArray lhs, BooleanArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var combined = BitVectorOps.Or(lhs.ValueBuffer, rhs.ValueBuffer);
        var combinedMask = BitVectorOps.And(lhs.NullBitmapBuffer, rhs.NullBitmapBuffer);
        var nullCount = BitUtility.CountBits(combinedMask.Span);
        return new BooleanArray(combined, combinedMask, lhs.Length, nullCount, 0);
    }

    /// <summary>
    /// Performa a pairwise boolean equality operation.
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static BooleanArray Equals(BooleanArray lhs, BooleanArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var combined = BitVectorOps.OnesComplement(BitVectorOps.Xor(lhs.ValueBuffer, rhs.ValueBuffer));
        var combinedMask = BitVectorOps.And(lhs.NullBitmapBuffer, rhs.NullBitmapBuffer);
        var nullCount = BitUtility.CountBits(combinedMask.Span);
        return new BooleanArray(combined, combinedMask, lhs.Length, nullCount, 0);
    }

    /// <summary>
    /// Performa a pairwise boolean XOR operation.
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static BooleanArray Xor(BooleanArray lhs, BooleanArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var combined = BitVectorOps.Xor(lhs.ValueBuffer, rhs.ValueBuffer);
        var combinedMask = BitVectorOps.And(lhs.NullBitmapBuffer, rhs.NullBitmapBuffer);
        var nullCount = BitUtility.CountBits(combinedMask.Span);
        return new BooleanArray(combined, combinedMask, lhs.Length, nullCount, 0);
    }

    /// <summary>
    /// Compare each value in `lhs` to a scalar `rhs`, returning boolean mask
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static BooleanArray Equal<T>(PrimitiveArray<T> lhs, T? rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        if (rhs == null)
        {
            return new BooleanArray(lhs.NullBitmapBuffer.Clone(), ArrowBuffer.Empty, lhs.Length, 0, 0);
        }
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var flag = a == rhs;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    /// <summary>
    /// Perform a pairwise comparison between each position in `lhs` and `rhs`, returning a boolean mask
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static BooleanArray Equal<T>(PrimitiveArray<T> lhs, PrimitiveArray<T> rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a == b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    /// <summary>
    /// Compare each value in `lhs` to a scalar `rhs`, returning boolean mask
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static BooleanArray Equal(StringArray lhs, string? rhs, MemoryAllocator? allocator = null)
    {
        if (rhs == null)
        {
            return new BooleanArray(lhs.NullBitmapBuffer.Clone(), ArrowBuffer.Empty, lhs.Length, 0, 0);
        }
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetString(i);
            var flag = a == rhs;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    /// <summary>
    /// Perform a pairwise comparison between each position in `lhs` and `rhs`, returning a boolean mask
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static BooleanArray Equal(StringArray lhs, StringArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetString(i);
            var b = rhs.GetString(i);
            var flag = a == b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    /// <summary>
    /// Compare each value in `lhs` to a scalar `rhs`, returning boolean mask
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static BooleanArray Equal(LargeStringArray lhs, string? rhs, MemoryAllocator? allocator = null)
    {
        if (rhs == null)
        {
            return new BooleanArray(lhs.NullBitmapBuffer.Clone(), ArrowBuffer.Empty, lhs.Length, 0, 0);
        }
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetString(i);
            var flag = a == rhs;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    /// <summary>
    /// Perform a pairwise comparison between each position in `lhs` and `rhs`, returning a boolean mask
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static BooleanArray Equal(LargeStringArray lhs, LargeStringArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetString(i);
            var b = rhs.GetString(i);
            var flag = a == b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    /// <summary>
    /// A dispatching comparison between a string array and a single string. If the `lhs` is not some flavor
    /// of string array, an exception is thrown.
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="InvalidDataException"></exception>
    public static BooleanArray Equal(IArrowArray lhs, string? rhs, MemoryAllocator? allocator = null)
    {
        switch (lhs.Data.DataType.TypeId)
        {
            case ArrowTypeId.String:
                return Equal((StringArray)lhs, rhs, allocator);
            case ArrowTypeId.LargeString:
                return Equal((LargeStringArray)lhs, rhs, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + lhs.Data.DataType.Name);
        }
    }

    public static BooleanArray GreaterThan<T>(PrimitiveArray<T> lhs, T? rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var flag = a > rhs;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray GreaterThan<T>(PrimitiveArray<T> lhs, PrimitiveArray<T> rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a > b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray LessThan<T>(PrimitiveArray<T> lhs, T? rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var flag = a < rhs;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray LessThan<T>(PrimitiveArray<T> lhs, PrimitiveArray<T> rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a < b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray GreaterThanOrEqual<T>(PrimitiveArray<T> lhs, T? rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var flag = a >= rhs;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray GreaterThanOrEqual<T>(PrimitiveArray<T> lhs, PrimitiveArray<T> rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a >= b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray LessThanOrEqual<T>(PrimitiveArray<T> lhs, T? rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var flag = a <= rhs;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray LessThanOrEqual<T>(PrimitiveArray<T> lhs, PrimitiveArray<T> rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        if (lhs.Length != rhs.Length) throw new ArgumentException("Arrays must have the same length");
        var cmp = new BooleanArray.Builder(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a <= b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

}

