using System;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations;

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
        var builder = new BooleanArray.Builder();
        builder.Reserve(mask.Length);
        foreach (var val in mask)
        {
            if (val != null)
            {
                builder.Append(!(bool)val);
            }
            else
            {
                builder.AppendNull();
            }
        }
        return builder.Build(allocator);
    }

    /// <summary>
    /// Perform a pairwise boolean AND operation.
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static BooleanArray And(BooleanArray lhs, BooleanArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
        var builder = new BooleanArray.Builder();
        builder.Reserve(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            if (a != null && b != null)
            {
                builder.Append((bool)a && (bool)b);
            }
            else
            {
                builder.AppendNull();
            }
        }
        return builder.Build(allocator);
    }

    /// <summary>
    /// Performa a pairwise boolean OR operation.
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static BooleanArray Or(BooleanArray lhs, BooleanArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
        var builder = new BooleanArray.Builder();
        builder.Reserve(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            if (a != null && b != null)
            {
                builder.Append((bool)a || (bool)b);
            }
            else
            {
                builder.AppendNull();
            }
        }
        return builder.Build(allocator);
    }

    /// <summary>
    /// Performa a pairwise boolean XOR operation.
    /// </summary>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static BooleanArray Xor(BooleanArray lhs, BooleanArray rhs, MemoryAllocator? allocator = null)
    {
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
        var builder = new BooleanArray.Builder();
        builder.Reserve(lhs.Length);
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            if (a != null && b != null)
            {
                builder.Append((bool)a ^ (bool)b);
            }
            else
            {
                builder.AppendNull();
            }
        }
        return builder.Build(allocator);
    }

    /// <summary>
    /// Compare each value in `lhs` to a scalar `rhs`, returning boolean mask
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="lhs"></param>
    /// <param name="rhs"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static BooleanArray Equal<T>(PrimitiveArray<T> lhs, T rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder();
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
    /// <exception cref="InvalidOperationException"></exception>
    public static BooleanArray Equal<T>(PrimitiveArray<T> lhs, PrimitiveArray<T> rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder();
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
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
    public static BooleanArray Equal(StringArray lhs, string rhs, MemoryAllocator? allocator = null)
    {
        var cmp = new BooleanArray.Builder();
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
    /// <exception cref="InvalidOperationException"></exception>
    public static BooleanArray Equal(StringArray lhs, StringArray rhs, MemoryAllocator? allocator = null)
    {
        var cmp = new BooleanArray.Builder();
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
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
    public static BooleanArray Equal(LargeStringArray lhs, string rhs, MemoryAllocator? allocator = null)
    {
        var cmp = new BooleanArray.Builder();
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
    /// <exception cref="InvalidOperationException"></exception>
    public static BooleanArray Equal(LargeStringArray lhs, LargeStringArray rhs, MemoryAllocator? allocator = null)
    {
        var cmp = new BooleanArray.Builder();
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
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
    public static BooleanArray Equal(IArrowArray lhs, string rhs, MemoryAllocator? allocator = null)
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

    public static BooleanArray GreaterThan<T>(PrimitiveArray<T> lhs, T rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder();
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
        var cmp = new BooleanArray.Builder();
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a > b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray LessThan<T>(PrimitiveArray<T> lhs, T rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder();
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
        var cmp = new BooleanArray.Builder();
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a < b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray GreaterThanOrEqual<T>(PrimitiveArray<T> lhs, T rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder();
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
        var cmp = new BooleanArray.Builder();
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
        for (int i = 0; i < lhs.Length; i++)
        {
            var a = lhs.GetValue(i);
            var b = rhs.GetValue(i);
            var flag = a >= b;
            cmp.Append(flag);
        }
        return cmp.Build(allocator);
    }

    public static BooleanArray LessThanOrEqual<T>(PrimitiveArray<T> lhs, T rhs, MemoryAllocator? allocator = null) where T : struct, INumber<T>
    {
        var cmp = new BooleanArray.Builder();
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
        var cmp = new BooleanArray.Builder();
        if (lhs.Length != rhs.Length) throw new InvalidOperationException("Arrays must have the same length");
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

