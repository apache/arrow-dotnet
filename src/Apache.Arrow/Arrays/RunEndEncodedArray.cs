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
using System.Collections.Generic;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow;

/// <summary>
/// Represents a run-end encoded array.
/// A run-end encoded array stores consecutive runs of the same value more efficiently.
/// It contains two child arrays: run_ends (Int16/Int32/Int64) and values (any type).
/// The run_ends array stores the cumulative end positions of each run.
/// </summary>
public class RunEndEncodedArray : Array
{
    /// <summary>
    /// Gets the run ends array (Int16Array, Int32Array, or Int64Array).
    /// This array contains the cumulative end indices for each run.
    /// </summary>
    public IArrowArray RunEnds { get; }

    /// <summary>
    /// Gets the values array.
    /// This array contains the actual values that are run-length encoded.
    /// </summary>
    public IArrowArray Values { get; }

    /// <summary>
    /// Creates a new RunEndEncodedArray from ArrayData.
    /// </summary>
    /// <param name="data">The array data containing run ends and values as children.</param>
    public RunEndEncodedArray(ArrayData data)
        : this(data, ArrowArrayFactory.BuildArray(data.Children[0]), ArrowArrayFactory.BuildArray(data.Children[1]))
    {
    }

    /// <summary>
    /// Creates a new RunEndEncodedArray with specified run ends and values arrays.
    /// </summary>
    /// <param name="runEnds">The run ends array (must be Int16Array, Int32Array, or Int64Array).</param>
    /// <param name="values">The values array (can be any type).</param>
    public RunEndEncodedArray(IArrowArray runEnds, IArrowArray values)
        : this(CreateArrayData(runEnds, values), runEnds, values)
    {
    }

    private RunEndEncodedArray(ArrayData data, IArrowArray runEnds, IArrowArray values)
        : base(data)
    {
        data.EnsureBufferCount(0); // REE arrays have no buffers, only children
        data.EnsureDataType(ArrowTypeId.RunEndEncoded);

        if (data.NullCount != 0)
        {
            throw new ArgumentException(
                $"Run-end encoded arrays have no top-level validity bitmap and must report null count 0, but got {data.NullCount}.",
                nameof(data));
        }

        ValidateRunEndsType(runEnds);

        if (runEnds.Length != values.Length)
        {
            throw new ArgumentException(
                $"Run ends array length ({runEnds.Length}) must equal values array length ({values.Length}).");
        }
        if (runEnds.NullCount != 0)
        {
            throw new ArgumentException(
                $"Run ends array must not contain nulls, but had {runEnds.NullCount} null(s).",
                nameof(runEnds));
        }

        RunEnds = runEnds;
        Values = values;
    }

    private static ArrayData CreateArrayData(IArrowArray runEnds, IArrowArray values)
    {
        ValidateRunEndsType(runEnds);

        if (runEnds.Length != values.Length)
        {
            throw new ArgumentException(
                $"Run ends array length ({runEnds.Length}) must equal values array length ({values.Length}).");
        }
        if (runEnds.NullCount != 0)
        {
            throw new ArgumentException(
                $"Run ends array must not contain nulls, but had {runEnds.NullCount} null(s).",
                nameof(runEnds));
        }

        // The logical length of a REE array is determined by the last value in run_ends
        int logicalLength = GetLogicalLength(runEnds);

        var dataType = new RunEndEncodedType(runEnds.Data.DataType, values.Data.DataType);

        return new ArrayData(
            dataType,
            logicalLength,
            nullCount: 0, // REE arrays don't have a validity bitmap
            offset: 0,
            buffers: [],
            children: [runEnds.Data, values.Data]);
    }

    private static void ValidateRunEndsType(IArrowArray runEnds)
    {
        ArrowTypeId typeId = runEnds.Data.DataType.TypeId;
        if (typeId != ArrowTypeId.Int16 &&
            typeId != ArrowTypeId.Int32 &&
            typeId != ArrowTypeId.Int64)
        {
            throw new ArgumentException(
                $"Run ends array must be Int16, Int32, or Int64, but got {typeId}",
                nameof(runEnds));
        }
    }

    private static int GetLogicalLength(IArrowArray runEnds)
    {
        if (runEnds.Length == 0)
        {
            return 0;
        }

        // Get the last run end value which represents the logical length
        switch (runEnds)
        {
            case Int16Array int16Array:
                return int16Array.GetValue(int16Array.Length - 1) ?? throw new ArgumentException("invalid length");
            case Int32Array int32Array:
                return int32Array.GetValue(int32Array.Length - 1) ?? throw new ArgumentException("invalid length");
            case Int64Array int64Array:
                {
                    long? lastValue = int64Array.GetValue(int64Array.Length - 1);
                    if (lastValue.HasValue && lastValue.Value > int.MaxValue)
                    {
                        throw new ArgumentException("Run ends value exceeds maximum supported length.");
                    }
                    return (int)(lastValue ?? throw new ArgumentException("invalid length"));
                }
            default:
                throw new InvalidOperationException($"Unexpected run ends array type: {runEnds.Data.DataType.TypeId}");
        }
    }

    /// <summary>
    /// Finds the physical index in the run_ends array that contains the specified logical index.
    /// </summary>
    /// <param name="logicalIndex">The logical index in the decoded array.</param>
    /// <returns>The physical index in the run_ends/values arrays.</returns>
    public int FindPhysicalIndex(int logicalIndex)
    {
        if (logicalIndex < 0 || logicalIndex >= Length)
        {
            throw new ArgumentOutOfRangeException(nameof(logicalIndex));
        }

        // Run ends are stored as cumulative positions in the underlying physical array,
        // so the search target must be expressed in those same coordinates by adding
        // the slice's logical offset.
        int searchIndex = logicalIndex + Data.Offset;

        // Binary search to find the run that contains this logical index
        return RunEnds switch
        {
            Int16Array int16Array => BinarySearchRunEnds(int16Array, searchIndex),
            Int32Array int32Array => BinarySearchRunEnds(int32Array, searchIndex),
            Int64Array int64Array => BinarySearchRunEnds(int64Array, searchIndex),
            _ => throw new InvalidOperationException($"Unexpected run ends array type: {RunEnds.GetType()}"),
        };
    }

    private static int BinarySearchRunEnds(Int16Array runEnds, int logicalIndex)
    {
        int left = 0;
        int right = runEnds.Length - 1;

        while (left < right)
        {
            int mid = left + (right - left) / 2;
            int runEnd = runEnds.GetValue(mid) ?? throw new ArgumentException("invalid length");

            if (logicalIndex < runEnd)
            {
                right = mid;
            }
            else
            {
                left = mid + 1;
            }
        }

        return left;
    }

    private static int BinarySearchRunEnds(Int32Array runEnds, int logicalIndex)
    {
        int left = 0;
        int right = runEnds.Length - 1;

        while (left < right)
        {
            int mid = left + (right - left) / 2;
            int runEnd = runEnds.GetValue(mid) ?? throw new ArgumentException("invalid length");

            if (logicalIndex < runEnd)
            {
                right = mid;
            }
            else
            {
                left = mid + 1;
            }
        }

        return left;
    }

    private static int BinarySearchRunEnds(Int64Array runEnds, int logicalIndex)
    {
        int left = 0;
        int right = runEnds.Length - 1;

        while (left < right)
        {
            int mid = left + (right - left) / 2;
            long runEnd = runEnds.GetValue(mid) ?? throw new ArgumentException("invalid length");

            if (logicalIndex < runEnd)
            {
                right = mid;
            }
            else
            {
                left = mid + 1;
            }
        }

        return left;
    }

    /// <summary>
    /// Returns a logically equivalent <see cref="RunEndEncodedArray"/> whose underlying
    /// children have been normalized to the slice range:
    /// <list type="bullet">
    ///   <item><description><c>Offset</c> is 0,</description></item>
    ///   <item><description>the run-ends array contains only the runs covering this slice,
    ///     with values shifted so that the last run-end equals the slice <c>Length</c>,</description></item>
    ///   <item><description>the values array is sliced to the corresponding physical range.</description></item>
    /// </list>
    /// <para>
    /// The returned array is independently disposable: it owns its own references to the
    /// underlying buffers via reference counting, so it remains valid even if this
    /// instance is later disposed. Callers are responsible for disposing the returned
    /// array when done with it. The result may or may not be the same instance as
    /// <c>this</c>; either way, it carries an independent reference.
    /// </para>
    /// </summary>
    public RunEndEncodedArray Normalize()
    {
        if (Offset == 0 && GetLogicalLength(RunEnds) == Length)
        {
            // Already normalized — return an independently-owned reference so the caller
            // can dispose the result without affecting this instance.
            return (RunEndEncodedArray)ArrowArrayFactory.SliceShared(this, 0, Length);
        }
        if (Length == 0)
        {
            return new RunEndEncodedArray(
                ArrowArrayFactory.SliceShared(RunEnds, 0, 0),
                ArrowArrayFactory.SliceShared(Values, 0, 0));
        }

        int logicalEnd = Offset + Length;
        int physicalStart;
        int physicalEndExclusive;
        IArrowArray normalizedRunEnds;

        switch (RunEnds)
        {
            case Int16Array re16:
                {
                    ReadOnlySpan<short> re = re16.Values;
                    physicalStart = FindNormalizePhysicalStart(re, Offset);
                    physicalEndExclusive = FindNormalizePhysicalEndExclusive(re, logicalEnd, physicalStart);
                    int count = physicalEndExclusive - physicalStart;

                    using var native = new NativeBuffer<short, NoOpAllocationTracker>(count, zeroFill: false);
                    Span<short> dst = native.Span;
                    for (int p = 0; p < count; p++)
                    {
                        int adjusted = Math.Min((int)re[physicalStart + p], logicalEnd) - Offset;
                        dst[p] = checked((short)adjusted);
                    }
                    normalizedRunEnds = new Int16Array(native.Build(), ArrowBuffer.Empty, count, 0, 0);
                    break;
                }
            case Int32Array re32:
                {
                    ReadOnlySpan<int> re = re32.Values;
                    physicalStart = FindNormalizePhysicalStart(re, Offset);
                    physicalEndExclusive = FindNormalizePhysicalEndExclusive(re, logicalEnd, physicalStart);
                    int count = physicalEndExclusive - physicalStart;

                    using var native = new NativeBuffer<int, NoOpAllocationTracker>(count, zeroFill: false);
                    Span<int> dst = native.Span;
                    for (int p = 0; p < count; p++)
                    {
                        dst[p] = Math.Min(re[physicalStart + p], logicalEnd) - Offset;
                    }
                    normalizedRunEnds = new Int32Array(native.Build(), ArrowBuffer.Empty, count, 0, 0);
                    break;
                }
            case Int64Array re64:
                {
                    ReadOnlySpan<long> re = re64.Values;
                    physicalStart = FindNormalizePhysicalStart(re, Offset);
                    physicalEndExclusive = FindNormalizePhysicalEndExclusive(re, logicalEnd, physicalStart);
                    int count = physicalEndExclusive - physicalStart;

                    using var native = new NativeBuffer<long, NoOpAllocationTracker>(count, zeroFill: false);
                    Span<long> dst = native.Span;
                    for (int p = 0; p < count; p++)
                    {
                        dst[p] = Math.Min(re[physicalStart + p], (long)logicalEnd) - Offset;
                    }
                    normalizedRunEnds = new Int64Array(native.Build(), ArrowBuffer.Empty, count, 0, 0);
                    break;
                }
            default:
                throw new InvalidOperationException($"Unexpected run-ends array type: {RunEnds.Data.DataType.TypeId}");
        }

        int physicalCount = physicalEndExclusive - physicalStart;
        IArrowArray normalizedValues = ArrowArrayFactory.SliceShared(Values, physicalStart, physicalCount);
        return new RunEndEncodedArray(normalizedRunEnds, normalizedValues);
    }

    private static int FindNormalizePhysicalStart(ReadOnlySpan<short> runEnds, int logicalOffset)
    {
        int lo = 0, hi = runEnds.Length;
        while (lo < hi) { int mid = lo + (hi - lo) / 2; if (runEnds[mid] > logicalOffset) hi = mid; else lo = mid + 1; }
        return lo;
    }

    private static int FindNormalizePhysicalEndExclusive(ReadOnlySpan<short> runEnds, int logicalEnd, int physicalStart)
    {
        int lo = physicalStart, hi = runEnds.Length;
        while (lo < hi) { int mid = lo + (hi - lo) / 2; if (runEnds[mid] >= logicalEnd) hi = mid; else lo = mid + 1; }
        return Math.Min(lo + 1, runEnds.Length);
    }

    private static int FindNormalizePhysicalStart(ReadOnlySpan<int> runEnds, int logicalOffset)
    {
        int lo = 0, hi = runEnds.Length;
        while (lo < hi) { int mid = lo + (hi - lo) / 2; if (runEnds[mid] > logicalOffset) hi = mid; else lo = mid + 1; }
        return lo;
    }

    private static int FindNormalizePhysicalEndExclusive(ReadOnlySpan<int> runEnds, int logicalEnd, int physicalStart)
    {
        int lo = physicalStart, hi = runEnds.Length;
        while (lo < hi) { int mid = lo + (hi - lo) / 2; if (runEnds[mid] >= logicalEnd) hi = mid; else lo = mid + 1; }
        return Math.Min(lo + 1, runEnds.Length);
    }

    private static int FindNormalizePhysicalStart(ReadOnlySpan<long> runEnds, int logicalOffset)
    {
        int lo = 0, hi = runEnds.Length;
        while (lo < hi) { int mid = lo + (hi - lo) / 2; if (runEnds[mid] > logicalOffset) hi = mid; else lo = mid + 1; }
        return lo;
    }

    private static int FindNormalizePhysicalEndExclusive(ReadOnlySpan<long> runEnds, int logicalEnd, int physicalStart)
    {
        int lo = physicalStart, hi = runEnds.Length;
        while (lo < hi) { int mid = lo + (hi - lo) / 2; if (runEnds[mid] >= logicalEnd) hi = mid; else lo = mid + 1; }
        return Math.Min(lo + 1, runEnds.Length);
    }

    /// <summary>
    /// Enumerates the physical index for every logical position in order.
    /// Unlike repeated calls to <see cref="FindPhysicalIndex"/>, this walks the
    /// run-ends array linearly, yielding O(n + m) total work instead of O(n·log m).
    /// </summary>
    public IEnumerable<int> EnumeratePhysicalIndices()
    {
        int length = Length;
        if (length == 0)
            yield break;

        int offset = Data.Offset;
        int physicalIndex = FindPhysicalIndex(0);

        switch (RunEnds)
        {
            case Int16Array int16RunEnds:
            {
                long currentRunEnd = int16RunEnds.GetValue(physicalIndex).Value;
                for (int logical = 0; logical < length; logical++)
                {
                    while (logical + offset >= currentRunEnd)
                    {
                        physicalIndex++;
                        currentRunEnd = int16RunEnds.GetValue(physicalIndex).Value;
                    }
                    yield return physicalIndex;
                }
                break;
            }
            case Int32Array int32RunEnds:
            {
                long currentRunEnd = int32RunEnds.GetValue(physicalIndex).Value;
                for (int logical = 0; logical < length; logical++)
                {
                    while (logical + offset >= currentRunEnd)
                    {
                        physicalIndex++;
                        currentRunEnd = int32RunEnds.GetValue(physicalIndex).Value;
                    }
                    yield return physicalIndex;
                }
                break;
            }
            case Int64Array int64RunEnds:
            {
                long currentRunEnd = int64RunEnds.GetValue(physicalIndex).Value;
                for (int logical = 0; logical < length; logical++)
                {
                    while (logical + offset >= currentRunEnd)
                    {
                        physicalIndex++;
                        currentRunEnd = int64RunEnds.GetValue(physicalIndex).Value;
                    }
                    yield return physicalIndex;
                }
                break;
            }
            default:
                throw new InvalidOperationException(
                    $"Unexpected run ends array type: {RunEnds.Data.DataType.TypeId}");
        }
    }

    public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);
}
