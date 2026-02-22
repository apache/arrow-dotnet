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

        ValidateRunEndsType(runEnds);
        RunEnds = runEnds;
        Values = values;
    }

    private static ArrayData CreateArrayData(IArrowArray runEnds, IArrowArray values)
    {
        ValidateRunEndsType(runEnds);

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
                return int16Array.GetValue(int16Array.Length - 1) ?? 0;
            case Int32Array int32Array:
                return int32Array.GetValue(int32Array.Length - 1) ?? 0;
            case Int64Array int64Array:
                {
                    long? lastValue = int64Array.GetValue(int64Array.Length - 1);
                    if (lastValue.HasValue && lastValue.Value > int.MaxValue)
                    {
                        throw new ArgumentException("Run ends value exceeds maximum supported length.");
                    }
                    return (int)(lastValue ?? 0);
                }
            default:
                throw new InvalidOperationException($"Unexpected run ends array type: {runEnds.GetType()}");
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

        // Binary search to find the run that contains this logical index
        return RunEnds switch
        {
            Int16Array int16Array => BinarySearchRunEnds(int16Array, logicalIndex),
            Int32Array int32Array => BinarySearchRunEnds(int32Array, logicalIndex),
            Int64Array int64Array => BinarySearchRunEnds(int64Array, logicalIndex),
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
            int runEnd = runEnds.GetValue(mid) ?? 0;

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
            int runEnd = runEnds.GetValue(mid) ?? 0;

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
            long runEnd = runEnds.GetValue(mid) ?? 0;

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

    public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);
}
