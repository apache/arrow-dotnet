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


using System.Numerics;

using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations;


/// <summary>
/// Copy primitive arraays between types to explicitly known numerical types. When the type already
/// matches, no copy is performed.
/// </summary>
public static partial class Conversion
{
    static void NullToZero<T, TBuilder>(PrimitiveArray<T> array, IArrowArrayBuilder<T, PrimitiveArray<T>, TBuilder> accumulator)
        where T : struct, INumber<T> where TBuilder : IArrowArrayBuilder<PrimitiveArray<T>>
    {
        accumulator.Reserve(array.Length);
        foreach (var value in array)
        {
            accumulator.Append(value == null ? T.Zero : (T)value);
        }
    }

    public static Array NullToZero<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                {
                    var builder = new DoubleArray.Builder();
                    NullToZero((DoubleArray)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.Float:
                {
                    var builder = new FloatArray.Builder();
                    NullToZero((FloatArray)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.Int32:
                {
                    var builder = new Int32Array.Builder();
                    NullToZero((Int32Array)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.Int64:
                {
                    var builder = new Int64Array.Builder();
                    NullToZero((Int64Array)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.UInt32:
                {
                    var builder = new UInt32Array.Builder();
                    NullToZero((UInt32Array)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.UInt64:
                {
                    var builder = new UInt64Array.Builder();
                    NullToZero((UInt64Array)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.Int16:
                {
                    var builder = new Int16Array.Builder();
                    NullToZero((Int16Array)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.Int8:
                {
                    var builder = new Int8Array.Builder();
                    NullToZero((Int8Array)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.UInt16:
                {
                    var builder = new UInt16Array.Builder();
                    NullToZero((UInt16Array)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            case ArrowTypeId.UInt8:
                {
                    var builder = new UInt8Array.Builder();
                    NullToZero((UInt8Array)(IArrowArray)array, builder);
                    return builder.Build(allocator);
                }
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static DoubleArray CastDouble<T>(IList<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new DoubleArray.Builder();
        builder.Reserve(array.Count);
        foreach (var val in array)
            builder.Append(double.CreateChecked(val));
        return builder.Build(allocator);
    }

    public static FloatArray CastFloat<T>(IList<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new FloatArray.Builder();
        builder.Reserve(array.Count);
        foreach (var val in array)
            builder.Append(float.CreateChecked(val));
        return builder.Build(allocator);
    }

    public static Int32Array CastInt32<T>(IList<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new Int32Array.Builder();
        builder.Reserve(array.Count);
        foreach (var val in array)
            builder.Append(int.CreateChecked(val));
        return builder.Build(allocator);
    }

    public static Int64Array CastInt64<T>(IList<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new Int64Array.Builder();
        builder.Reserve(array.Count);
        foreach (var val in array)
            builder.Append(long.CreateChecked(val));
        return builder.Build(allocator);
    }

    public static UInt16Array CastUInt16<T>(IList<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new UInt16Array.Builder();
        builder.Reserve(array.Count);
        foreach (var val in array)
            builder.Append(ushort.CreateChecked(val));
        return builder.Build(allocator);
    }

    public static Int16Array CastInt16<T>(IList<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new Int16Array.Builder();
        builder.Reserve(array.Count);
        foreach (var val in array)
            builder.Append(short.CreateChecked(val));
        return builder.Build(allocator);
    }

    public static UInt8Array CastUInt8<T>(IList<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new UInt8Array.Builder();
        builder.Reserve(array.Count);
        foreach (var val in array)
            builder.Append(byte.CreateChecked(val));
        return builder.Build(allocator);
    }

    public static Int8Array CastInt8<T>(IList<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new Int8Array.Builder();
        builder.Reserve(array.Count);
        foreach (var val in array)
            builder.Append(sbyte.CreateChecked(val));
        return builder.Build(allocator);
    }

    public static BooleanArray CastBool<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new BooleanArray.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(val.Value != T.Zero);
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static Int64Array CastInt64<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new Int64Array.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(long.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static Int32Array CastInt32<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new Int32Array.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(int.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static Int16Array CastInt16<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new Int16Array.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(short.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static Int8Array CastInt8<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new Int8Array.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(sbyte.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static UInt64Array CastUInt64<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new UInt64Array.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(ulong.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static UInt32Array CastUInt32<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new UInt32Array.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(uint.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static UInt16Array CastUInt16<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new UInt16Array.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(ushort.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static UInt8Array CastUInt8<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new UInt8Array.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(byte.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static FloatArray CastFloat<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new FloatArray.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(float.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static DoubleArray CastDouble<T>(PrimitiveArray<T> array, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var builder = new DoubleArray.Builder();
        builder.Reserve(array.Length);
        foreach (var val in array)
        {
            if (val != null) builder.Append(double.CreateChecked((T)val));
            else builder.AppendNull();
        }
        return builder.Build(allocator);
    }

    public static Int64Array CastInt64(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastInt64((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastInt64((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastInt64((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return (Int64Array)array;
            case ArrowTypeId.UInt32:
                return CastInt64((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastInt64((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastInt64((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastInt64((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastInt64((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastInt64((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static Int32Array CastInt32(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastInt32((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastInt32((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return (Int32Array)array;
            case ArrowTypeId.Int64:
                return CastInt32((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastInt32((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastInt32((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastInt32((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastInt32((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastInt32((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastInt32((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static Int16Array CastInt16(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastInt16((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastInt16((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastInt16((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastInt16((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastInt16((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastInt16((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastInt16((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastInt16((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastInt16((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastInt16((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static Int8Array CastInt8(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastInt8((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastInt8((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastInt8((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastInt8((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastInt8((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastInt8((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastInt8((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastInt8((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastInt8((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastInt8((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static FloatArray CastFloat(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastFloat((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return (FloatArray)array;
            case ArrowTypeId.Int32:
                return CastFloat((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastFloat((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastFloat((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastFloat((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastFloat((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastFloat((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastFloat((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastFloat((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static DoubleArray CastDouble(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return (DoubleArray)array;
            case ArrowTypeId.Float:
                return CastDouble((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastDouble((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastDouble((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastDouble((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastDouble((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastDouble((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastDouble((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastDouble((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastDouble((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static UInt64Array CastUInt64(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastUInt64((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastUInt64((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastUInt64((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastUInt64((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastUInt64((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastUInt64((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastUInt64((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastUInt64((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastUInt64((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastUInt64((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static UInt32Array CastUInt32(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastUInt32((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastUInt32((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastUInt32((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastUInt32((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastUInt32((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastUInt32((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastUInt32((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastUInt32((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastUInt32((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastUInt32((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static UInt16Array CastUInt16(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastUInt16((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastUInt16((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastUInt16((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastUInt16((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastUInt16((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastUInt16((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastUInt16((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastUInt16((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastUInt16((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastUInt16((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static UInt8Array CastUInt8(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastUInt8((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastUInt8((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastUInt8((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastUInt8((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastUInt8((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastUInt8((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastUInt8((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastUInt8((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastUInt8((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastUInt8((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    public static BooleanArray CastBool(IArrowArray array, MemoryAllocator? allocator = null)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return CastBool((DoubleArray)array, allocator);
            case ArrowTypeId.Float:
                return CastBool((FloatArray)array, allocator);
            case ArrowTypeId.Int32:
                return CastBool((Int32Array)array, allocator);
            case ArrowTypeId.Int64:
                return CastBool((Int64Array)array, allocator);
            case ArrowTypeId.UInt32:
                return CastBool((UInt32Array)array, allocator);
            case ArrowTypeId.UInt64:
                return CastBool((UInt64Array)array, allocator);
            case ArrowTypeId.Int16:
                return CastBool((Int16Array)array, allocator);
            case ArrowTypeId.Int8:
                return CastBool((Int8Array)array, allocator);
            case ArrowTypeId.UInt16:
                return CastBool((UInt16Array)array, allocator);
            case ArrowTypeId.UInt8:
                return CastBool((UInt8Array)array, allocator);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }
}

