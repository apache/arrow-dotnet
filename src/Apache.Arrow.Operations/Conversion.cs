using System.Numerics;

using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations;

public class ArrowCompatibilityVisitor : IArrowArrayVisitor<StructArray>, IArrowArrayVisitor<LargeListArray>, IArrowArrayVisitor<LargeStringArray>, IArrowArrayVisitor<LargeBinaryArray>
{
    public IArrowArray? Result = null;

    public static IArrowArray Convert(IArrowArray array)
    {
        var visitor = new ArrowCompatibilityVisitor();
        visitor.Visit(array);
        if (visitor.Result == null) throw new InvalidOperationException();
        return visitor.Result;
    }

    public StructArray HandleStruct(StructArray array)
    {
        var dtype = (StructType)array.Data.DataType;
        var newFields = new List<Field>();
        var newVals = new List<IArrowArray>();
        int size = 0;
        foreach (var (field, arr) in dtype.Fields.Zip(array.Fields))
        {
            var visitor = new ArrowCompatibilityVisitor();
            visitor.Visit(arr);
            if (visitor.Result == null) throw new InvalidOperationException();
            newFields.Add(new Field(field.Name, visitor.Result.Data.DataType, field.IsNullable));
            newVals.Add(visitor.Result);
            if (size != 0 && visitor.Result.Length != 0 && visitor.Result.Length != size) throw new InvalidDataException();
            size = visitor.Result.Length;
        }
        var result = new StructArray(new StructType(newFields), size, newVals, array.NullBitmapBuffer);
        if (result.Fields.Count > 0) { }
        return result;
    }

    public void Visit(StructArray array)
    {
        Result = HandleStruct(array);
    }

    public void Visit(IArrowArray array)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Struct:
                {
                    Visit((StructArray)array);
                    break;
                }
            case ArrowTypeId.LargeList:
                {
                    Visit((LargeListArray)array);
                    break;
                }
            case ArrowTypeId.LargeString:
                {
                    Visit((LargeStringArray)array);
                    break;
                }
            case ArrowTypeId.LargeBinary:
                {
                    Visit((LargeBinaryArray)array);
                    break;
                }
            default:
                {
                    Result = array;
                    break;
                }
        }
    }

    public void Visit(LargeListArray array)
    {
        ArrowCompatibilityVisitor visitor = new();
        visitor.Visit(array.Values);
        var offsetsBuffer = new ArrowBuffer.Builder<int>();
        foreach (var v in array.ValueOffsets)
        {
            offsetsBuffer.Append((int)v);
        }
        if (visitor.Result == null) throw new InvalidOperationException();
        Result = new ListArray(
            new ListType(((LargeListType)array.Data.DataType).ValueDataType),
            array.Length,
            offsetsBuffer.Build(),
            visitor.Result,
            array.NullBitmapBuffer,
            array.NullCount,
            array.Offset
        );
    }

    public void Visit(LargeStringArray array)
    {
        var offsetsBuffer = new ArrowBuffer.Builder<int>();
        foreach (var v in array.ValueOffsets)
        {
            offsetsBuffer.Append((int)v);
        }
        Result = new StringArray(
            array.Length,
            offsetsBuffer.Build(),
            array.ValueBuffer,
            array.NullBitmapBuffer,
            array.NullCount,
            array.Offset
        );
    }

    public void Visit(LargeBinaryArray type)
    {
        throw new NotImplementedException();
    }
}


/// <summary>
/// Specifies how null values should be handled in aggregate computations.
/// </summary>
public enum NullHandling
{
    /// <summary>
    /// Skip null values when computing the result.
    /// Returns null only if the array is empty or all values are null.
    /// </summary>
    Skip,

    /// <summary>
    /// Propagate null: if any value in the array is null, return null.
    /// </summary>
    Propagate
}


/// <summary>
/// Copy primitive arraays between types to explicitly known numerical types. When the type already
/// matches, no copy is performed.
/// </summary>
public static class Conversion
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
}

