using System.Numerics;

using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations;

public static class Select
{
    /// <summary>
    /// Returns a copy of the positions in the array where the mask is true. All other values in the array will be
    /// excluded.
    ///
    /// This internally reduces to building a true-value run index map and calling `Take`
    /// </summary>
    /// <param name="array">The array to select from</param>
    /// <param name="mask">The mask defining which values to keep or exclude</param>
    /// <param name="allocator">The memory allocator to build the new array from</param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">If the mask and the array are not of equal size</exception>
    public static Array Filter(Array array, BooleanArray mask, MemoryAllocator? allocator = null)
    {
        if (array.Length != mask.Length) throw new InvalidOperationException("Array and mask must have the same length");
        List<(int, int)> spans = new();
        int? start = null;
        for (int i = 0; i < mask.Length; i++)
        {
            var v = mask.GetValue(i);
            if (v != null && (bool)v)
            {
                if (start != null) { }
                else start = i;
            }
            else if (v != null && !(bool)v)
            {
                if (start != null)
                {
                    // Slices in Take include the trailing index
                    spans.Add(((int)start, i - 1));
                    start = null;
                }
                else { }
            }
        }
        if (start != null)
        {
            spans.Add(((int)start, mask.Length - 1));
        }
        return Take(array, spans, allocator);
    }

    /// <summary>
    /// Returns a copy of the positions in the array included in the provided start-end spans. All other values in the array will be
    /// excluded.
    /// </summary>
    /// <param name="array">The array to select from</param>
    /// <param name="spans">The index ranges to select</param>
    /// <param name="allocator">The memory allocator to build the new array from</param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static Array Take(Array array, IList<(int, int)> spans, MemoryAllocator? allocator = null)
    {
        if (spans.Count == 0)
        {
            return array.Slice(0, 0);
        }
        List<Array> chunks = new();
        foreach (var (start, end) in spans)
        {
            if (end < start || end < 0 || start < 0) throw new InvalidOperationException(string.Format("Invalid span: {0} {1}", start, end));
            chunks.Add(array.Slice(start, end - start + 1));
        }
        return (Array)ArrowArrayConcatenator.Concatenate(chunks, allocator);
    }

    /// <summary>
    /// Returns a copy of the positions in the array included in the provided indices list. All other values in the array will be
    /// excluded.
    /// </summary>
    /// <param name="array">The array to select from</param>
    /// <param name="indices">The indices to select</param>
    /// <param name="allocator">The memory allocator to build the new array from</param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static Array Take(Array array, IList<int> indices, MemoryAllocator? allocator = null)
    {
        if (indices.Count == 0)
        {
            return array.Slice(0, 0);
        }
        List<Array> chunks = new();
        for (var i = 0; i < indices.Count; i++)
        {
            chunks.Add(array.Slice(indices[i], 1));
        }
        return (Array)ArrowArrayConcatenator.Concatenate(chunks, allocator);
    }

    /// <summary>
    /// Apply `Take` to each array in `batch` using the same `indices`
    /// </summary>
    /// <param name="batch"></param>
    /// <param name="indices"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static List<Array> Take(List<Array> batch, IList<int> indices, MemoryAllocator? allocator = null)
    {
        return batch.Select(arr => Take(arr, indices, allocator)).ToList();
    }

    /// <summary>
    /// Apply `Filter` to each array in `batch` using the same `mask`
    /// </summary>
    /// <param name="batch"></param>
    /// <param name="mask"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static List<Array> Filter(List<Array> batch, BooleanArray mask, MemoryAllocator? allocator = null)
    {
        return batch.Select(arr => Filter(arr, mask, allocator)).ToList();
    }

    /// <summary>
    /// Apply `Take` to each array in `batch` using the same `indices`
    /// </summary>
    /// <param name="batch"></param>
    /// <param name="indices"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static Dictionary<T, Array> Take<T>(Dictionary<T, Array> batch, IList<int> indices, MemoryAllocator? allocator = null) where T : notnull
    {
        Dictionary<T, Array> result = new();
        foreach (var kv in batch)
        {
            result[kv.Key] = Take(kv.Value, indices, allocator);
        }
        return result;
    }

    /// <summary>
    /// Apply `Filter` to each array in `batch` using the same `mask`
    /// </summary>
    /// <param name="batch"></param>
    /// <param name="mask"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static Dictionary<T, Array> Filter<T>(Dictionary<T, Array> batch, BooleanArray mask, MemoryAllocator? allocator = null) where T : notnull
    {
        Dictionary<T, Array> result = new();
        foreach (var kv in batch)
        {
            result[kv.Key] = Filter(kv.Value, mask, allocator);
        }
        return result;
    }

    /// <summary>
    /// Apply `Filter` to each array in `batch` using the same `mask`
    /// </summary>
    /// <param name="batch"></param>
    /// <param name="mask"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static RecordBatch Filter(RecordBatch batch, BooleanArray mask, MemoryAllocator? allocator = null)
    {
        if (batch.Length != mask.Length) throw new InvalidOperationException("Array and mask must have the same length");
        List<(int, int)> spans = new();
        int? start = null;
        for (int i = 0; i < mask.Length; i++)
        {
            var v = mask.GetValue(i);
            if (v != null && (bool)v)
            {
                if (start != null) { }
                else start = i;
            }
            else if (v != null && !(bool)v)
            {
                if (start != null)
                {
                    // Slices in Take include the trailing index
                    spans.Add(((int)start, i - 1));
                    start = null;
                }
                else { }
            }
        }
        if (start != null)
        {
            spans.Add(((int)start, mask.Length - 1));
        }
        return Take(batch, spans, allocator);
    }

    /// <summary>
    /// Apply `Take` to each array in `batch` using the same `indices`
    /// </summary>
    /// <param name="batch"></param>
    /// <param name="spans"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static RecordBatch Take(RecordBatch batch, IList<(int, int)> spans, MemoryAllocator? allocator = null)
    {
        if (spans.Count == 0)
        {
            return batch.Slice(0, 0);
        }
        List<Array> columns = new();
        var size = 0;
        foreach (var col in batch.Arrays)
        {
            columns.Add(Take((Array)col, spans, allocator));
            size = columns.Last().Length;
        }
        return new RecordBatch(batch.Schema, columns, size);
    }

    /// <summary>
    /// Apply `Take` to each array in `batch` using the same `indices`
    /// </summary>
    /// <param name="batch"></param>
    /// <param name="indices"></param>
    /// <param name="allocator"></param>
    /// <returns></returns>
    public static RecordBatch Take(RecordBatch batch, IList<int> indices, MemoryAllocator? allocator = null)
    {
        var spans = IndicesToSpans(indices);
        return Take(batch, spans, allocator);
    }

    /// <summary>
    /// Convert a list of indices into a list of index start-end spans for ease-of selection
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="indices"></param>
    /// <returns></returns>
    public static List<(T, T)> IndicesToSpans<T>(IList<T> indices) where T : struct, INumber<T>
    {
        List<(T, T)> acc = new();
        T? start = null;
        T? last = null;
        foreach (var i in indices)
        {
            if (last == null)
            {
                start = i;
                last = i;
            }
            else
            {
                if (i - last == T.One)
                {
                    last = i;
                }
                else if (start != null)
                {
                    acc.Add(((T)start, (T)last));
                    start = i;
                    last = i;
                }
            }
        }
        if (start != null && last != null)
        {
            acc.Add(((T)start, indices.Last()));
        }
        return acc;
    }
}


public static class Aggregate
{

    /// <summary>
    /// Returns the minimum value in the array.
    /// </summary>
    /// <typeparam name="T">The numeric type of array elements.</typeparam>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The minimum value, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static T? Min<T>(PrimitiveArray<T> array, NullHandling nullHandling = NullHandling.Skip)
        where T : struct, INumber<T>
    {
        if (array.Length == 0)
            return null;

        T? min = null;
        for (int i = 0; i < array.Length; i++)
        {
            var value = array.GetValue(i);
            if (value == null)
            {
                if (nullHandling == NullHandling.Propagate)
                    return null;
                continue;
            }

            if (min == null || (T)value < min)
                min = value;
        }
        return min;
    }

    /// <summary>
    /// Returns the minimum value in the array.
    /// </summary>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The minimum value, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static double? Min(IArrowArray array, NullHandling nullHandling = NullHandling.Skip)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return Min((DoubleArray)array, nullHandling);
            case ArrowTypeId.Float:
                return Min((FloatArray)array, nullHandling);
            case ArrowTypeId.Int32:
                return Min((Int32Array)array, nullHandling);
            case ArrowTypeId.Int64:
                return Min((Int64Array)array, nullHandling);
            case ArrowTypeId.UInt32:
                return Min((UInt32Array)array, nullHandling);
            case ArrowTypeId.UInt64:
                return Min((UInt64Array)array, nullHandling);
            case ArrowTypeId.Int16:
                return Min((Int16Array)array, nullHandling);
            case ArrowTypeId.Int8:
                return Min((Int8Array)array, nullHandling);
            case ArrowTypeId.UInt16:
                return Min((UInt16Array)array, nullHandling);
            case ArrowTypeId.UInt8:
                return Min((UInt8Array)array, nullHandling);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    /// <summary>
    /// Returns the maximum value in the array.
    /// </summary>
    /// <typeparam name="T">The numeric type of array elements.</typeparam>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The maximum value, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static T? Max<T>(PrimitiveArray<T> array, NullHandling nullHandling = NullHandling.Skip)
        where T : struct, INumber<T>
    {
        if (array.Length == 0)
            return null;

        T? max = null;
        for (int i = 0; i < array.Length; i++)
        {
            var value = array.GetValue(i);
            if (value == null)
            {
                if (nullHandling == NullHandling.Propagate)
                    return null;
                continue;
            }

            if (max == null || (T)value > max)
                max = value;
        }
        return max;
    }

    /// <summary>
    /// Returns the maximum value in the array.
    /// </summary>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The maximum value, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static double? Max(IArrowArray array, NullHandling nullHandling = NullHandling.Skip)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return Max((DoubleArray)array, nullHandling);
            case ArrowTypeId.Float:
                return Max((FloatArray)array, nullHandling);
            case ArrowTypeId.Int32:
                return Max((Int32Array)array, nullHandling);
            case ArrowTypeId.Int64:
                return Max((Int64Array)array, nullHandling);
            case ArrowTypeId.UInt32:
                return Max((UInt32Array)array, nullHandling);
            case ArrowTypeId.UInt64:
                return Max((UInt64Array)array, nullHandling);
            case ArrowTypeId.Int16:
                return Max((Int16Array)array, nullHandling);
            case ArrowTypeId.Int8:
                return Max((Int8Array)array, nullHandling);
            case ArrowTypeId.UInt16:
                return Max((UInt16Array)array, nullHandling);
            case ArrowTypeId.UInt8:
                return Max((UInt8Array)array, nullHandling);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    /// <summary>
    /// Returns the index of the minimum value in the array (first occurrence).
    /// </summary>
    /// <typeparam name="T">The numeric type of array elements.</typeparam>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The index of the minimum value, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static int? ArgMin<T>(PrimitiveArray<T> array, NullHandling nullHandling = NullHandling.Skip)
        where T : struct, INumber<T>
    {
        if (array.Length == 0)
            return null;

        T? min = null;
        int? minIndex = null;
        for (int i = 0; i < array.Length; i++)
        {
            var value = array.GetValue(i);
            if (value == null)
            {
                if (nullHandling == NullHandling.Propagate)
                    return null;
                continue;
            }

            if (min == null || (T)value < min)
            {
                min = value;
                minIndex = i;
            }
        }
        return minIndex;
    }

    /// <summary>
    /// Returns the index of the minimum value in the array (first occurrence).
    /// </summary>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The index of the minimum value, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static int? ArgMin(IArrowArray array, NullHandling nullHandling = NullHandling.Skip)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return ArgMin((DoubleArray)array, nullHandling);
            case ArrowTypeId.Float:
                return ArgMin((FloatArray)array, nullHandling);
            case ArrowTypeId.Int32:
                return ArgMin((Int32Array)array, nullHandling);
            case ArrowTypeId.Int64:
                return ArgMin((Int64Array)array, nullHandling);
            case ArrowTypeId.UInt32:
                return ArgMin((UInt32Array)array, nullHandling);
            case ArrowTypeId.UInt64:
                return ArgMin((UInt64Array)array, nullHandling);
            case ArrowTypeId.Int16:
                return ArgMin((Int16Array)array, nullHandling);
            case ArrowTypeId.Int8:
                return ArgMin((Int8Array)array, nullHandling);
            case ArrowTypeId.UInt16:
                return ArgMin((UInt16Array)array, nullHandling);
            case ArrowTypeId.UInt8:
                return ArgMin((UInt8Array)array, nullHandling);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    /// <summary>
    /// Returns the index of the maximum value in the array (first occurrence).
    /// </summary>
    /// <typeparam name="T">The numeric type of array elements.</typeparam>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The index of the maximum value, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static int? ArgMax<T>(PrimitiveArray<T> array, NullHandling nullHandling = NullHandling.Skip)
        where T : struct, INumber<T>
    {
        if (array.Length == 0)
            return null;

        T? max = null;
        int? maxIndex = null;
        for (int i = 0; i < array.Length; i++)
        {
            var value = array.GetValue(i);
            if (value == null)
            {
                if (nullHandling == NullHandling.Propagate)
                    return null;
                continue;
            }

            if (max == null || (T)value > max)
            {
                max = value;
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    /// <summary>
    /// Returns the index of the maximum value in the array (first occurrence).
    /// </summary>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The index of the maximum value, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static int? ArgMax(IArrowArray array, NullHandling nullHandling = NullHandling.Skip)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return ArgMax((DoubleArray)array, nullHandling);
            case ArrowTypeId.Float:
                return ArgMax((FloatArray)array, nullHandling);
            case ArrowTypeId.Int32:
                return ArgMax((Int32Array)array, nullHandling);
            case ArrowTypeId.Int64:
                return ArgMax((Int64Array)array, nullHandling);
            case ArrowTypeId.UInt32:
                return ArgMax((UInt32Array)array, nullHandling);
            case ArrowTypeId.UInt64:
                return ArgMax((UInt64Array)array, nullHandling);
            case ArrowTypeId.Int16:
                return ArgMax((Int16Array)array, nullHandling);
            case ArrowTypeId.Int8:
                return ArgMax((Int8Array)array, nullHandling);
            case ArrowTypeId.UInt16:
                return ArgMax((UInt16Array)array, nullHandling);
            case ArrowTypeId.UInt8:
                return ArgMax((UInt8Array)array, nullHandling);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    /// <summary>
    /// Returns the sum of all values in the array.
    /// </summary>
    /// <typeparam name="T">The numeric type of array elements.</typeparam>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The sum of values, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static T? Sum<T>(PrimitiveArray<T> array, NullHandling nullHandling = NullHandling.Skip)
        where T : struct, INumber<T>
    {
        if (array.Length == 0)
            return null;

        T sum = T.Zero;
        bool hasValue = false;
        for (int i = 0; i < array.Length; i++)
        {
            var value = array.GetValue(i);
            if (value == null)
            {
                if (nullHandling == NullHandling.Propagate)
                    return null;
                continue;
            }

            sum += (T)value;
            hasValue = true;
        }
        return hasValue ? sum : null;
    }

    /// <summary>
    /// Returns the sum of all values in the array.
    /// </summary>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The sum of values, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static double? Sum(IArrowArray array, NullHandling nullHandling = NullHandling.Skip)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return Sum((DoubleArray)array, nullHandling);
            case ArrowTypeId.Float:
                return Sum((FloatArray)array, nullHandling);
            case ArrowTypeId.Int32:
                return Sum((Int32Array)array, nullHandling);
            case ArrowTypeId.Int64:
                return Sum((Int64Array)array, nullHandling);
            case ArrowTypeId.UInt32:
                return Sum((UInt32Array)array, nullHandling);
            case ArrowTypeId.UInt64:
                return Sum((UInt64Array)array, nullHandling);
            case ArrowTypeId.Int16:
                return Sum((Int16Array)array, nullHandling);
            case ArrowTypeId.Int8:
                return Sum((Int8Array)array, nullHandling);
            case ArrowTypeId.UInt16:
                return Sum((UInt16Array)array, nullHandling);
            case ArrowTypeId.UInt8:
                return Sum((UInt8Array)array, nullHandling);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }

    /// <summary>
    /// Returns the arithmetic mean of all values in the array.
    /// </summary>
    /// <typeparam name="T">The numeric type of array elements.</typeparam>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The mean as a double, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static double? Mean<T>(PrimitiveArray<T> array, NullHandling nullHandling = NullHandling.Skip)
        where T : struct, INumber<T>
    {
        if (array.Length == 0)
            return null;

        T sum = T.Zero;
        long count = 0;
        for (int i = 0; i < array.Length; i++)
        {
            var value = array.GetValue(i);
            if (value == null)
            {
                if (nullHandling == NullHandling.Propagate)
                    return null;
                continue;
            }

            sum += (T)value;
            count++;
        }
        return count > 0 ? double.CreateChecked(sum) / count : null;
    }

    /// <summary>
    /// Returns the arithmetic mean of all values in the array.
    /// </summary>
    /// <param name="array">The input array.</param>
    /// <param name="nullHandling">How to handle null values.</param>
    /// <returns>The mean as a double, or null if the array is empty, all values are null,
    /// or nullHandling is Propagate and any null exists.</returns>
    public static double? Mean(IArrowArray array, NullHandling nullHandling = NullHandling.Skip)
    {
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Double:
                return Mean((DoubleArray)array, nullHandling);
            case ArrowTypeId.Float:
                return Mean((FloatArray)array, nullHandling);
            case ArrowTypeId.Int32:
                return Mean((Int32Array)array, nullHandling);
            case ArrowTypeId.Int64:
                return Mean((Int64Array)array, nullHandling);
            case ArrowTypeId.UInt32:
                return Mean((UInt32Array)array, nullHandling);
            case ArrowTypeId.UInt64:
                return Mean((UInt64Array)array, nullHandling);
            case ArrowTypeId.Int16:
                return Mean((Int16Array)array, nullHandling);
            case ArrowTypeId.Int8:
                return Mean((Int8Array)array, nullHandling);
            case ArrowTypeId.UInt16:
                return Mean((UInt16Array)array, nullHandling);
            case ArrowTypeId.UInt8:
                return Mean((UInt8Array)array, nullHandling);
            default:
                throw new InvalidDataException("Unsupported data type " + array.Data.DataType.Name);
        }
    }
}