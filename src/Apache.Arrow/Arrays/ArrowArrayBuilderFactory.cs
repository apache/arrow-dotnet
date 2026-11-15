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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    static class ArrowArrayBuilderFactory
    {
        internal static IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> Build(IArrowType dataType, MemoryAllocator allocator)
        {
            switch (dataType.TypeId)
            {
                case ArrowTypeId.Boolean:
                    return new BooleanArray.Builder(allocator);
                case ArrowTypeId.UInt8:
                    return new UInt8Array.Builder(0, allocator);
                case ArrowTypeId.Int8:
                    return new Int8Array.Builder(0, allocator);
                case ArrowTypeId.UInt16:
                    return new UInt16Array.Builder(0, allocator);
                case ArrowTypeId.Int16:
                    return new Int16Array.Builder(0, allocator);
                case ArrowTypeId.UInt32:
                    return new UInt32Array.Builder(0, allocator);
                case ArrowTypeId.Int32:
                    return new Int32Array.Builder(0, allocator);
                case ArrowTypeId.UInt64:
                    return new UInt64Array.Builder(0, allocator);
                case ArrowTypeId.Int64:
                    return new Int64Array.Builder(0, allocator);
                case ArrowTypeId.HalfFloat:
#if NET5_0_OR_GREATER
                    return new HalfFloatArray.Builder(0, allocator);
#else
                    throw new NotSupportedException("Half-float arrays are not supported by this target framework.");
#endif
                case ArrowTypeId.Float:
                    return new FloatArray.Builder(0, allocator);
                case ArrowTypeId.Double:
                    return new DoubleArray.Builder(0, allocator);
                case ArrowTypeId.String:
                    return new StringArray.Builder(allocator);
                case ArrowTypeId.StringView:
                    return new StringViewArray.Builder(allocator);
                case ArrowTypeId.Binary:
                    return new BinaryArray.Builder(BinaryType.Default, allocator);
                case ArrowTypeId.BinaryView:
                    return new BinaryViewArray.Builder(BinaryViewType.Default, allocator);
                case ArrowTypeId.Timestamp:
                    return new TimestampArray.Builder((TimestampType)dataType, 0, allocator);
                case ArrowTypeId.Date64:
                    return new Date64Array.Builder(0, allocator);
                case ArrowTypeId.Date32:
                    return new Date32Array.Builder(0, allocator);
                case ArrowTypeId.Time32:
                    return new Time32Array.Builder(dataType as Time32Type, 0, allocator);
                case ArrowTypeId.Time64:
                    return new Time64Array.Builder(dataType as Time64Type, 0, allocator);
                case ArrowTypeId.Duration:
                    return new DurationArray.Builder(dataType as DurationType, 0, allocator);
                case ArrowTypeId.List:
                    return new ListArray.Builder(dataType as ListType, allocator);
                case ArrowTypeId.ListView:
                    return new ListViewArray.Builder(dataType as ListViewType, allocator);
                case ArrowTypeId.FixedSizeList:
                    return new FixedSizeListArray.Builder(dataType as FixedSizeListType, allocator);
                case ArrowTypeId.Decimal32:
                    return new Decimal32Array.Builder(dataType as Decimal32Type, allocator);
                case ArrowTypeId.Decimal64:
                    return new Decimal64Array.Builder(dataType as Decimal64Type, allocator);
                case ArrowTypeId.Decimal128:
                    return new Decimal128Array.Builder(dataType as Decimal128Type, allocator);
                case ArrowTypeId.Decimal256:
                    return new Decimal256Array.Builder(dataType as Decimal256Type, allocator);
                case ArrowTypeId.Interval:
                    var intervalType = (IntervalType)dataType;
                    return intervalType.Unit switch
                    {
                        IntervalUnit.YearMonth => new YearMonthIntervalArray.Builder(0, allocator),
                        IntervalUnit.DayTime => new DayTimeIntervalArray.Builder(0, allocator),
                        IntervalUnit.MonthDayNanosecond => new MonthDayNanosecondIntervalArray.Builder(0, allocator),
                        _ => throw new ArgumentOutOfRangeException($"unsupported interval unit <{intervalType.Unit}>")
                    };
                case ArrowTypeId.LargeBinary:
                    return new LargeBinaryArray.Builder(LargeBinaryType.Default, allocator);
                case ArrowTypeId.LargeString:
                    return new LargeStringArray.Builder(allocator);
                case ArrowTypeId.LargeList:
                    return new LargeListArray.Builder(dataType as LargeListType, allocator);
                case ArrowTypeId.LargeListView:
                    return new LargeListViewArray.Builder(dataType as LargeListViewType, allocator);
                case ArrowTypeId.Map:
                    return new MapArray.Builder(dataType as MapType, allocator);
                case ArrowTypeId.Struct:
                case ArrowTypeId.Union:
                case ArrowTypeId.Dictionary:
                case ArrowTypeId.FixedSizedBinary:
                case ArrowTypeId.Extension:
                default:
                    throw new NotSupportedException($"An ArrowArrayBuilder cannot be built for type {dataType.TypeId}.");
            }
        }
    }
}
