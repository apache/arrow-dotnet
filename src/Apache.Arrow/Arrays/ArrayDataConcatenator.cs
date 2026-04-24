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
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class ArrayDataConcatenator
    {
        public static ArrayData Concatenate(IReadOnlyList<ArrayData> arrayDataList, MemoryAllocator allocator = default)
        {
            if (arrayDataList == null || arrayDataList.Count == 0)
            {
                return null;
            }

            if (arrayDataList.Count == 1)
            {
                return arrayDataList[0];
            }

            var arrowArrayConcatenationVisitor = new ArrayDataConcatenationVisitor(arrayDataList, allocator);

            IArrowType type = arrayDataList[0].DataType;
            type.Accept(arrowArrayConcatenationVisitor);

            return arrowArrayConcatenationVisitor.Result;
        }

        private class ArrayDataConcatenationVisitor :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<FixedWidthType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<BinaryViewType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<StringViewType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<ListViewType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<UnionType>,
            IArrowTypeVisitor<LargeBinaryType>,
            IArrowTypeVisitor<LargeStringType>,
            IArrowTypeVisitor<LargeListType>,
            IArrowTypeVisitor<LargeListViewType>,
            IArrowTypeVisitor<MapType>,
            IArrowTypeVisitor<RunEndEncodedType>
        {
            public ArrayData Result { get; private set; }
            private readonly IReadOnlyList<ArrayData> _arrayDataList;
            private readonly int _totalLength;
            private readonly int _totalNullCount;
            private readonly MemoryAllocator _allocator;

            public ArrayDataConcatenationVisitor(IReadOnlyList<ArrayData> arrayDataList, MemoryAllocator allocator = default)
            {
                _arrayDataList = arrayDataList;
                _allocator = allocator;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    _totalLength += arrayData.Length;
                    _totalNullCount += arrayData.GetNullCount();
                }
            }

            public void Visit(BooleanType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer valueBuffer = ConcatenateBitmapBuffer(1);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, valueBuffer });
            }

            public void Visit(FixedWidthType type)
            {
                CheckData(type, 2);
                var resolvedType = CheckAndResolveFixedWidthType(type);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer valueBuffer = ConcatenateFixedWidthTypeValueBuffer(1, resolvedType);

                Result = new ArrayData(resolvedType, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, valueBuffer });
            }

            public void Visit(BinaryType type) => ConcatenateVariableBinaryArrayData(type);

            public void Visit(BinaryViewType type) => ConcatenateBinaryViewArrayData(type);

            public void Visit(StringType type) => ConcatenateVariableBinaryArrayData(type);

            public void Visit(StringViewType type) => ConcatenateBinaryViewArrayData(type);

            public void Visit(LargeBinaryType type) => ConcatenateLargeVariableBinaryArrayData(type);

            public void Visit(LargeStringType type) => ConcatenateLargeVariableBinaryArrayData(type);

            public void Visit(LargeListType type) => ConcatenateLargeLists(type);

            public void Visit(LargeListViewType type)
            {
                CheckData(type, 3);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer sizesBuffer = ConcatenateFixedWidthTypeValueBuffer(2, Int64Type.Default);

                var children = new List<ArrayData>(_arrayDataList.Count);
                var offsetsBuilder = new ArrowBuffer.Builder<long>(_totalLength);
                long baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    var child = arrayData.Children[0];
                    ReadOnlySpan<long> offsets = arrayData.Buffers[1].Span.CastTo<long>().Slice(arrayData.Offset, arrayData.Length);
                    ReadOnlySpan<long> sizes = arrayData.Buffers[2].Span.CastTo<long>().Slice(arrayData.Offset, arrayData.Length);
                    long minOffset = 0;
                    long maxEnd = 0;
                    bool hasNonEmptyValue = false;

                    for (int i = 0; i < arrayData.Length; ++i)
                    {
                        if (sizes[i] > 0)
                        {
                            minOffset = hasNonEmptyValue ? Math.Min(minOffset, offsets[i]) : offsets[i];
                            maxEnd = Math.Max(maxEnd, offsets[i] + sizes[i]);
                            hasNonEmptyValue = true;
                        }
                    }

                    if (!hasNonEmptyValue)
                    {
                        for (int i = 0; i < offsets.Length; ++i)
                        {
                            offsetsBuilder.Append(baseOffset);
                        }

                        children.Add(child.Slice(0, 0));
                        continue;
                    }

                    foreach (long offset in offsets)
                    {
                        offsetsBuilder.Append(baseOffset + offset - minOffset);
                    }

                    var childLength = maxEnd - minOffset;
                    if (minOffset != 0 || childLength != child.Length)
                    {
                        child = child.Slice(checked((int)minOffset), checked((int)childLength));
                    }

                    baseOffset += childLength;
                    children.Add(child);
                }

                ArrowBuffer offsetBuffer = offsetsBuilder.Build(_allocator);
                ArrayData combinedChild = ConcatenateChildrenOrEmpty(children);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer, sizesBuffer }, new[] { combinedChild });
            }

            public void Visit(ListType type) => ConcatenateLists(type);

            public void Visit(ListViewType type)
            {
                CheckData(type, 3);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer sizesBuffer = ConcatenateFixedWidthTypeValueBuffer(2, Int32Type.Default);

                var children = new List<ArrayData>(_arrayDataList.Count);
                var offsetsBuilder = new ArrowBuffer.Builder<int>(_totalLength);
                int baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    var child = arrayData.Children[0];
                    ReadOnlySpan<int> offsets = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length);
                    ReadOnlySpan<int> sizes = arrayData.Buffers[2].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length);
                    int minOffset = 0;
                    int maxEnd = 0;
                    bool hasNonEmptyValue = false;

                    for (int i = 0; i < arrayData.Length; ++i)
                    {
                        if (sizes[i] > 0)
                        {
                            minOffset = hasNonEmptyValue ? Math.Min(minOffset, offsets[i]) : offsets[i];
                            maxEnd = Math.Max(maxEnd, offsets[i] + sizes[i]);
                            hasNonEmptyValue = true;
                        }
                    }

                    if (!hasNonEmptyValue)
                    {
                        for (int i = 0; i < offsets.Length; ++i)
                        {
                            offsetsBuilder.Append(baseOffset);
                        }

                        children.Add(child.Slice(0, 0));
                        continue;
                    }

                    foreach (int offset in offsets)
                    {
                        offsetsBuilder.Append(baseOffset + offset - minOffset);
                    }

                    var childLength = maxEnd - minOffset;
                    if (minOffset != 0 || childLength != child.Length)
                    {
                        child = child.Slice(minOffset, childLength);
                    }

                    baseOffset += childLength;
                    children.Add(child);
                }

                ArrowBuffer offsetBuffer = offsetsBuilder.Build(_allocator);
                ArrayData combinedChild = ConcatenateChildrenOrEmpty(children);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer, sizesBuffer }, new[] { combinedChild });
            }

            public void Visit(FixedSizeListType type)
            {
                CheckData(type, 1);
                CheckFixedSizeListCompatibility(type);
                var listSize = type.ListSize;
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();

                var children = new List<ArrayData>(_arrayDataList.Count);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    var offset = arrayData.Offset;
                    var length = arrayData.Length;
                    var child = arrayData.Children[0];
                    if (offset != 0 || child.Length != length * listSize)
                    {
                        child = child.Slice(offset * listSize, length * listSize);
                    }

                    children.Add(child);
                }

                ArrayData combinedChild = ConcatenateChildrenOrEmpty(children);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer }, new[] { combinedChild });
            }

            public void Visit(StructType type)
            {
                CheckData(type, 1);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                List<ArrayData> children = new List<ArrayData>(type.Fields.Count);

                for (int i = 0; i < type.Fields.Count; i++)
                {
                    children.Add(Concatenate(SelectSlicedChildren(i), _allocator));
                }

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer }, children);
            }

            public void Visit(UnionType type)
            {
                int bufferCount = type.Mode switch
                {
                    UnionMode.Sparse => 1,
                    UnionMode.Dense => 2,
                    _ => throw new InvalidOperationException("TODO"),
                };

                CheckData(type, bufferCount);
                List<ArrayData> children = new List<ArrayData>(type.Fields.Count);

                for (int i = 0; i < type.Fields.Count; i++)
                {
                    // For dense mode, the offsets aren't adjusted so are into the non-sliced child arrays
                    var fieldChildren = type.Mode == UnionMode.Sparse
                        ? SelectSlicedChildren(i)
                        : SelectChildren(i);
                    children.Add(Concatenate(fieldChildren, _allocator));
                }

                ArrowBuffer[] buffers = new ArrowBuffer[bufferCount];
                buffers[0] = ConcatenateUnionTypeBuffer();
                if (bufferCount > 1)
                {
                    buffers[1] = ConcatenateUnionOffsetBuffer();
                }

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, buffers, children);
            }

            public void Visit(MapType type) => ConcatenateLists(type.UnsortedKey()); /* Can't tell if the output is still sorted */

            public void Visit(RunEndEncodedType type)
            {
                ArrowTypeId runEndsTypeId = type.RunEndsDataType.TypeId;
                if (runEndsTypeId != ArrowTypeId.Int16 &&
                    runEndsTypeId != ArrowTypeId.Int32 &&
                    runEndsTypeId != ArrowTypeId.Int64)
                {
                    throw new InvalidOperationException(
                        $"Run-ends array must be Int16, Int32, or Int64, but got {runEndsTypeId}");
                }

                var slicedValues = new List<ArrayData>(_arrayDataList.Count);
                ArrowBuffer.Builder<short> int16Builder = null;
                ArrowBuffer.Builder<int> int32Builder = null;
                ArrowBuffer.Builder<long> int64Builder = null;

                switch (runEndsTypeId)
                {
                    case ArrowTypeId.Int16: int16Builder = new ArrowBuffer.Builder<short>(); break;
                    case ArrowTypeId.Int32: int32Builder = new ArrowBuffer.Builder<int>(); break;
                    case ArrowTypeId.Int64: int64Builder = new ArrowBuffer.Builder<long>(); break;
                }

                long baseOffset = 0;
                int physicalRunCount = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    arrayData.EnsureDataType(type.TypeId);

                    ArrayData runEndsData = arrayData.Children[0];
                    ArrayData valuesData = arrayData.Children[1];

                    if (runEndsData.DataType.TypeId != runEndsTypeId)
                    {
                        throw new ArgumentException(
                            $"All run-end encoded arrays must have the same run-ends type. Expected <{runEndsTypeId}> but got <{runEndsData.DataType.TypeId}>.");
                    }
                    if (valuesData.DataType.TypeId != type.ValuesDataType.TypeId)
                    {
                        throw new ArgumentException(
                            $"All run-end encoded arrays must have the same values type. Expected <{type.ValuesDataType.TypeId}> but got <{valuesData.DataType.TypeId}>.");
                    }

                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    int logicalOffset = arrayData.Offset;
                    int logicalLength = arrayData.Length;
                    int logicalEnd = logicalOffset + logicalLength;

                    int physicalStart;
                    int physicalEndExclusive;

                    switch (runEndsTypeId)
                    {
                        case ArrowTypeId.Int16:
                            {
                                ReadOnlySpan<short> re = runEndsData.Buffers[1].Span.CastTo<short>()
                                    .Slice(runEndsData.Offset, runEndsData.Length);
                                physicalStart = FindPhysicalStartInt16(re, logicalOffset);
                                physicalEndExclusive = FindPhysicalEndExclusiveInt16(re, logicalEnd, physicalStart);
                                for (int p = physicalStart; p < physicalEndExclusive; p++)
                                {
                                    int adjustedEnd = Math.Min((int)re[p], logicalEnd) - logicalOffset;
                                    int16Builder.Append(checked((short)(baseOffset + adjustedEnd)));
                                }
                                break;
                            }
                        case ArrowTypeId.Int32:
                            {
                                ReadOnlySpan<int> re = runEndsData.Buffers[1].Span.CastTo<int>()
                                    .Slice(runEndsData.Offset, runEndsData.Length);
                                physicalStart = FindPhysicalStartInt32(re, logicalOffset);
                                physicalEndExclusive = FindPhysicalEndExclusiveInt32(re, logicalEnd, physicalStart);
                                for (int p = physicalStart; p < physicalEndExclusive; p++)
                                {
                                    int adjustedEnd = Math.Min(re[p], logicalEnd) - logicalOffset;
                                    int32Builder.Append(checked((int)(baseOffset + adjustedEnd)));
                                }
                                break;
                            }
                        default: // Int64
                            {
                                ReadOnlySpan<long> re = runEndsData.Buffers[1].Span.CastTo<long>()
                                    .Slice(runEndsData.Offset, runEndsData.Length);
                                physicalStart = FindPhysicalStartInt64(re, logicalOffset);
                                physicalEndExclusive = FindPhysicalEndExclusiveInt64(re, logicalEnd, physicalStart);
                                for (int p = physicalStart; p < physicalEndExclusive; p++)
                                {
                                    long adjustedEnd = Math.Min(re[p], (long)logicalEnd) - logicalOffset;
                                    int64Builder.Append(baseOffset + adjustedEnd);
                                }
                                break;
                            }
                    }

                    int physicalCount = physicalEndExclusive - physicalStart;
                    physicalRunCount += physicalCount;
                    slicedValues.Add(valuesData.Slice(physicalStart, physicalCount));
                    baseOffset += logicalLength;
                }

                ArrowBuffer runEndsValueBuffer = runEndsTypeId switch
                {
                    ArrowTypeId.Int16 => int16Builder.Build(_allocator),
                    ArrowTypeId.Int32 => int32Builder.Build(_allocator),
                    _ => int64Builder.Build(_allocator),
                };

                ArrayData runEndsResult = new ArrayData(
                    type.RunEndsDataType, physicalRunCount, 0, 0,
                    new[] { ArrowBuffer.Empty, runEndsValueBuffer });

                ArrayData valuesResult;
                if (slicedValues.Count == 0)
                {
                    // All inputs were empty. Reuse the first input's values child sliced to length
                    // 0 so we get a valid ArrayData with the correct buffer/child layout for the
                    // values type, regardless of what that type is.
                    valuesResult = _arrayDataList[0].Children[1].Slice(0, 0);
                }
                else
                {
                    valuesResult = Concatenate(slicedValues, _allocator);
                }

                Result = new ArrayData(
                    type, _totalLength, 0, 0,
                    System.Array.Empty<ArrowBuffer>(),
                    new[] { runEndsResult, valuesResult });
            }

            private static int FindPhysicalStartInt16(ReadOnlySpan<short> runEnds, int logicalOffset)
            {
                // Smallest physical index p where runEnds[p] > logicalOffset
                int lo = 0, hi = runEnds.Length;
                while (lo < hi)
                {
                    int mid = lo + (hi - lo) / 2;
                    if (runEnds[mid] > logicalOffset) hi = mid; else lo = mid + 1;
                }
                return lo;
            }

            private static int FindPhysicalEndExclusiveInt16(ReadOnlySpan<short> runEnds, int logicalEnd, int physicalStart)
            {
                // Smallest physical index p (>= physicalStart) where runEnds[p] >= logicalEnd, then p+1
                int lo = physicalStart, hi = runEnds.Length;
                while (lo < hi)
                {
                    int mid = lo + (hi - lo) / 2;
                    if (runEnds[mid] >= logicalEnd) hi = mid; else lo = mid + 1;
                }
                return Math.Min(lo + 1, runEnds.Length);
            }

            private static int FindPhysicalStartInt32(ReadOnlySpan<int> runEnds, int logicalOffset)
            {
                int lo = 0, hi = runEnds.Length;
                while (lo < hi)
                {
                    int mid = lo + (hi - lo) / 2;
                    if (runEnds[mid] > logicalOffset) hi = mid; else lo = mid + 1;
                }
                return lo;
            }

            private static int FindPhysicalEndExclusiveInt32(ReadOnlySpan<int> runEnds, int logicalEnd, int physicalStart)
            {
                int lo = physicalStart, hi = runEnds.Length;
                while (lo < hi)
                {
                    int mid = lo + (hi - lo) / 2;
                    if (runEnds[mid] >= logicalEnd) hi = mid; else lo = mid + 1;
                }
                return Math.Min(lo + 1, runEnds.Length);
            }

            private static int FindPhysicalStartInt64(ReadOnlySpan<long> runEnds, int logicalOffset)
            {
                int lo = 0, hi = runEnds.Length;
                while (lo < hi)
                {
                    int mid = lo + (hi - lo) / 2;
                    if (runEnds[mid] > logicalOffset) hi = mid; else lo = mid + 1;
                }
                return lo;
            }

            private static int FindPhysicalEndExclusiveInt64(ReadOnlySpan<long> runEnds, int logicalEnd, int physicalStart)
            {
                int lo = physicalStart, hi = runEnds.Length;
                while (lo < hi)
                {
                    int mid = lo + (hi - lo) / 2;
                    if (runEnds[mid] >= logicalEnd) hi = mid; else lo = mid + 1;
                }
                return Math.Min(lo + 1, runEnds.Length);
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"Concatenation for {type.Name} is not supported yet.");
            }

            private void CheckData(IArrowType type, int expectedBufferCount)
            {
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    arrayData.EnsureDataType(type.TypeId);
                    arrayData.EnsureBufferCount(expectedBufferCount);
                }
            }

            private void CheckDataVariadicCount(IArrowType type, int expectedBufferCount)
            {
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    arrayData.EnsureDataType(type.TypeId);
                    arrayData.EnsureVariadicBufferCount(expectedBufferCount);
                }
            }

            private FixedWidthType CheckAndResolveFixedWidthType(FixedWidthType type)
            {
                switch (type)
                {
                    case Decimal32Type d:
                        return CheckDecimalCompatibility<Decimal32Type>(d, d.Precision, d.Scale,
                            (other) => ((Decimal32Type)other).Precision,
                            (other) => ((Decimal32Type)other).Scale,
                            (p, s) => new Decimal32Type(p, s));
                    case Decimal64Type d:
                        return CheckDecimalCompatibility<Decimal64Type>(d, d.Precision, d.Scale,
                            (other) => ((Decimal64Type)other).Precision,
                            (other) => ((Decimal64Type)other).Scale,
                            (p, s) => new Decimal64Type(p, s));
                    case Decimal128Type d:
                        return CheckDecimalCompatibility<Decimal128Type>(d, d.Precision, d.Scale,
                            (other) => ((Decimal128Type)other).Precision,
                            (other) => ((Decimal128Type)other).Scale,
                            (p, s) => new Decimal128Type(p, s));
                    case Decimal256Type d:
                        return CheckDecimalCompatibility<Decimal256Type>(d, d.Precision, d.Scale,
                            (other) => ((Decimal256Type)other).Precision,
                            (other) => ((Decimal256Type)other).Scale,
                            (p, s) => new Decimal256Type(p, s));
                    case TimestampType ts:
                        CheckTimestampCompatibility(ts);
                        return type;
                    case TimeBasedType tb:
                        CheckTimeUnitCompatibility(tb);
                        return type;
                    case IntervalType iv:
                        CheckIntervalCompatibility(iv);
                        return type;
                    case FixedSizeBinaryType fsb:
                        CheckFixedSizeBinaryCompatibility(fsb);
                        return type;
                    default:
                        return type;
                }
            }

            private T CheckDecimalCompatibility<T>(T firstType, int firstPrecision, int firstScale,
                Func<IArrowType, int> getPrecision, Func<IArrowType, int> getScale,
                Func<int, int, T> factory) where T : FixedSizeBinaryType
            {
                int maxPrecision = firstPrecision;

                for (int i = 1; i < _arrayDataList.Count; i++)
                {
                    IArrowType otherType = _arrayDataList[i].DataType;
                    int otherScale = getScale(otherType);
                    if (otherScale != firstScale)
                    {
                        throw new ArgumentException(
                            $"Cannot concatenate {firstType.Name} arrays with different scales: {firstScale} vs {otherScale}");
                    }
                    maxPrecision = Math.Max(maxPrecision, getPrecision(otherType));
                }

                if (maxPrecision != firstPrecision)
                {
                    return factory(maxPrecision, firstScale);
                }
                return firstType;
            }

            private void CheckTimestampCompatibility(TimestampType firstType)
            {
                for (int i = 1; i < _arrayDataList.Count; i++)
                {
                    var otherType = (TimestampType)_arrayDataList[i].DataType;
                    if (otherType.Unit != firstType.Unit)
                    {
                        throw new ArgumentException(
                            $"Cannot concatenate Timestamp arrays with different time units: {firstType.Unit} vs {otherType.Unit}");
                    }
                    if (otherType.Timezone != firstType.Timezone)
                    {
                        throw new ArgumentException(
                            $"Cannot concatenate Timestamp arrays with different timezones: \"{firstType.Timezone}\" vs \"{otherType.Timezone}\"");
                    }
                }
            }

            private void CheckTimeUnitCompatibility(TimeBasedType firstType)
            {
                for (int i = 1; i < _arrayDataList.Count; i++)
                {
                    var otherType = (TimeBasedType)_arrayDataList[i].DataType;
                    if (otherType.Unit != firstType.Unit)
                    {
                        throw new ArgumentException(
                            $"Cannot concatenate {firstType.Name} arrays with different time units: {firstType.Unit} vs {otherType.Unit}");
                    }
                }
            }

            private void CheckIntervalCompatibility(IntervalType firstType)
            {
                for (int i = 1; i < _arrayDataList.Count; i++)
                {
                    var otherType = (IntervalType)_arrayDataList[i].DataType;
                    if (otherType.Unit != firstType.Unit)
                    {
                        throw new ArgumentException(
                            $"Cannot concatenate Interval arrays with different units: {firstType.Unit} vs {otherType.Unit}");
                    }
                }
            }

            private void CheckFixedSizeBinaryCompatibility(FixedSizeBinaryType firstType)
            {
                for (int i = 1; i < _arrayDataList.Count; i++)
                {
                    var otherType = (FixedSizeBinaryType)_arrayDataList[i].DataType;
                    if (otherType.ByteWidth != firstType.ByteWidth)
                    {
                        throw new ArgumentException(
                            $"Cannot concatenate FixedSizeBinary arrays with different byte widths: {firstType.ByteWidth} vs {otherType.ByteWidth}");
                    }
                }
            }

            private void CheckFixedSizeListCompatibility(FixedSizeListType firstType)
            {
                for (int i = 1; i < _arrayDataList.Count; i++)
                {
                    var otherType = (FixedSizeListType)_arrayDataList[i].DataType;
                    if (otherType.ListSize != firstType.ListSize)
                    {
                        throw new ArgumentException(
                            $"Cannot concatenate FixedSizeList arrays with different list sizes: {firstType.ListSize} vs {otherType.ListSize}");
                    }
                }
            }

            private void ConcatenateVariableBinaryArrayData(IArrowType type)
            {
                CheckData(type, 3);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateOffsetBuffer();
                ArrowBuffer valueBuffer = ConcatenateVariableBinaryValueBuffer();

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer, valueBuffer });
            }

            private void ConcatenateBinaryViewArrayData(IArrowType type)
            {
                CheckDataVariadicCount(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer viewBuffer = ConcatenateViewBuffer(out int variadicBufferCount);
                ArrowBuffer[] buffers = new ArrowBuffer[2 + variadicBufferCount];
                buffers[0] = validityBuffer;
                buffers[1] = viewBuffer;
                int index = 2;
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    for (int i = 2; i < arrayData.Buffers.Length; i++)
                    {
                        buffers[index++] = arrayData.Buffers[i];
                    }
                }

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, buffers);
            }

            private void ConcatenateLists(NestedType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateOffsetBuffer();

                var children = new List<ArrayData>(_arrayDataList.Count);
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    var child = arrayData.Children[0];
                    ReadOnlySpan<int> offsets = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length + 1);
                    var firstOffset = offsets[0];
                    var lastOffset = offsets[arrayData.Length];
                    if (firstOffset != 0 || lastOffset != child.Length)
                    {
                        child = child.Slice(firstOffset, lastOffset - firstOffset);
                    }

                    children.Add(child);
                }

                ArrayData combinedChild = ConcatenateChildrenOrEmpty(children);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer }, new[] { combinedChild });
            }

            private void ConcatenateLargeVariableBinaryArrayData(IArrowType type)
            {
                CheckData(type, 3);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateLargeOffsetBuffer();
                ArrowBuffer valueBuffer = ConcatenateLargeVariableBinaryValueBuffer();

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer, valueBuffer });
            }

            private void ConcatenateLargeLists(LargeListType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateLargeOffsetBuffer();

                var children = new List<ArrayData>(_arrayDataList.Count);
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    var child = arrayData.Children[0];
                    ReadOnlySpan<long> offsets = arrayData.Buffers[1].Span.CastTo<long>().Slice(arrayData.Offset, arrayData.Length + 1);
                    var firstOffset = offsets[0];
                    var lastOffset = offsets[arrayData.Length];
                    if (firstOffset != 0 || lastOffset != child.Length)
                    {
                        child = child.Slice(checked((int)firstOffset), checked((int)(lastOffset - firstOffset)));
                    }

                    children.Add(child);
                }

                ArrayData combinedChild = ConcatenateChildrenOrEmpty(children);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer }, new[] { combinedChild });
            }

            private ArrayData ConcatenateChildrenOrEmpty(List<ArrayData> children)
            {
                if (children.Count > 0)
                {
                    return Concatenate(children, _allocator);
                }

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Children?.Length > 0 && arrayData.Children[0] != null)
                    {
                        // All parent arrays are empty, but the nested array still needs a real
                        // zero-length child to preserve Arrow's structural invariant.
                        return arrayData.Children[0].Slice(0, 0);
                    }
                }

                throw new InvalidOperationException("Nested array data must contain child data.");
            }

            private ArrowBuffer ConcatenateLargeOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<long>(_totalLength + 1);
                long baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    ReadOnlySpan<long> span = arrayData.Buffers[1].Span.CastTo<long>().Slice(arrayData.Offset, arrayData.Length + 1);
                    var firstOffset = span[0];

                    foreach (long offset in span.Slice(0, arrayData.Length))
                    {
                        builder.Append(baseOffset + offset - firstOffset);
                    }

                    baseOffset += span[arrayData.Length] - firstOffset;
                }

                builder.Append(baseOffset);

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateLargeVariableBinaryValueBuffer()
            {
                var builder = new ArrowBuffer.Builder<byte>();

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    var offsets = arrayData.Buffers[1].Span.CastTo<long>().Slice(arrayData.Offset, arrayData.Length + 1);
                    var firstOffset = checked((int)offsets[0]);
                    var lastOffset = checked((int)offsets[arrayData.Length]);
                    builder.Append(arrayData.Buffers[2].Span.Slice(firstOffset, lastOffset - firstOffset));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateValidityBuffer()
            {
                if (_totalNullCount == 0)
                {
                    return ArrowBuffer.Empty;
                }

                var builder = new ArrowBuffer.BitmapBuilder(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    int offset = arrayData.Offset;
                    ReadOnlySpan<byte> span = arrayData.Buffers[0].Span;

                    if (length > 0 && span.Length == 0)
                    {
                        if (arrayData.NullCount == 0)
                        {
                            builder.AppendRange(true, length);
                        }
                        else if (arrayData.NullCount == length)
                        {
                            builder.AppendRange(false, length);
                        }
                        else
                        {
                            throw new Exception("Array has no validity buffer and null count != 0 or length");
                        }
                    }
                    else if (offset == 0)
                    {
                        builder.Append(span, length);
                    }
                    else
                    {
                        for (int i = 0; i < length; ++i)
                        {
                            builder.Append(BitUtility.GetBit(span, offset + i));
                        }
                    }
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateBitmapBuffer(int bufferIndex)
            {
                var builder = new ArrowBuffer.BitmapBuilder(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    int offset = arrayData.Offset;
                    ReadOnlySpan<byte> span = arrayData.Buffers[bufferIndex].Span;

                    if (offset == 0)
                    {
                        builder.Append(span, length);
                    }
                    else
                    {
                        for (int i = 0; i < length; ++i)
                        {
                            builder.Append(BitUtility.GetBit(span, offset + i));
                        }
                    }
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateFixedWidthTypeValueBuffer(int bufferIndex, FixedWidthType type)
            {
                int typeByteWidth = type.BitWidth / 8;
                var builder = new ArrowBuffer.Builder<byte>(_totalLength * typeByteWidth);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int byteLength = arrayData.Length * typeByteWidth;
                    int byteOffset = arrayData.Offset * typeByteWidth;

                    builder.Append(arrayData.Buffers[bufferIndex].Span.Slice(byteOffset, byteLength));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateVariableBinaryValueBuffer()
            {
                var builder = new ArrowBuffer.Builder<byte>();

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    var offsets = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length + 1);
                    var firstOffset = offsets[0];
                    var lastOffset = offsets[arrayData.Length];
                    builder.Append(arrayData.Buffers[2].Span.Slice(firstOffset, lastOffset - firstOffset));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<int>(_totalLength + 1);
                int baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    ReadOnlySpan<int> span = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length + 1);
                    // First offset may be non-zero for sliced arrays
                    var firstOffset = span[0];

                    foreach (int offset in span.Slice(0, arrayData.Length))
                    {
                        builder.Append(baseOffset + offset - firstOffset);
                    }

                    baseOffset += span[arrayData.Length] - firstOffset;
                }

                builder.Append(baseOffset);

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateViewBuffer(out int variadicBufferCount)
            {
                var builder = new ArrowBuffer.Builder<BinaryView>(_totalLength);
                variadicBufferCount = 0;
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    ReadOnlySpan<BinaryView> span = arrayData.Buffers[1].Span.CastTo<BinaryView>().Slice(arrayData.Offset, arrayData.Length);
                    foreach (BinaryView view in span)
                    {
                        if (view.Length > BinaryView.MaxInlineLength)
                        {
                            builder.Append(view.AdjustBufferIndex(variadicBufferCount));
                        }
                        else
                        {
                            builder.Append(view);
                        }
                    }

                    variadicBufferCount += (arrayData.Buffers.Length - 2);
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateUnionTypeBuffer()
            {
                var builder = new ArrowBuffer.Builder<byte>(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    builder.Append(arrayData.Buffers[0].Span.Slice(arrayData.Offset, arrayData.Length));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateUnionOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<int>(_totalLength);
                var typeCount = _arrayDataList.Count > 0 ? _arrayDataList[0].Children.Length : 0;
                var baseOffsets = new int[typeCount];

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    ReadOnlySpan<byte> typeSpan = arrayData.Buffers[0].Span.Slice(arrayData.Offset, arrayData.Length);
                    ReadOnlySpan<int> offsetSpan = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length);
                    for (int i = 0; i < arrayData.Length; ++i)
                    {
                        var typeId = typeSpan[i];
                        builder.Append(checked(baseOffsets[typeId] + offsetSpan[i]));
                    }

                    for (int i = 0; i < typeCount; ++i)
                    {
                        checked
                        {
                            baseOffsets[i] += arrayData.Children[i].Length;
                        }
                    }
                }

                return builder.Build(_allocator);
            }

            private List<ArrayData> SelectChildren(int index)
            {
                var children = new List<ArrayData>(_arrayDataList.Count);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    children.Add(arrayData.Children[index]);
                }

                return children;
            }

            private List<ArrayData> SelectSlicedChildren(int index)
            {
                var children = new List<ArrayData>(_arrayDataList.Count);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    var offset = arrayData.Offset;
                    var length = arrayData.Length;
                    var child = arrayData.Children[index];
                    if (offset != 0 || child.Length != length)
                    {
                        child = child.Slice(offset, length);
                    }

                    children.Add(child);
                }

                return children;
            }
        }
    }
}
