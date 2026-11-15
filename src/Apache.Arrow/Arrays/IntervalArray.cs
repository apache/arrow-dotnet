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
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    internal static class IntervalArray
    {
        internal static IArrowArray Create(ArrayData data) => ((IntervalType)data.DataType).Unit switch
        {
            IntervalUnit.YearMonth => new YearMonthIntervalArray(data),
            IntervalUnit.DayTime => new DayTimeIntervalArray(data),
            IntervalUnit.MonthDayNanosecond => new MonthDayNanosecondIntervalArray(data),
            _ => throw new InvalidOperationException($"Unsupported interval unit {((IntervalType)data.DataType).Unit}"),
        };
    }

    public abstract class IntervalArray<T> : PrimitiveArray<T>
        where T : struct, IEquatable<T>
    {
        protected IntervalArray(ArrayData data)
            : base(data)
        {
            data.EnsureBufferCount(2);
            data.EnsureDataType(ArrowTypeId.Interval);
        }

        public IntervalType Type => (IntervalType)Data.DataType;

        public IntervalUnit Unit => Type.Unit;

        internal static IArrowArray Create(ArrayData data) => ((IntervalType)data.DataType).Unit switch
        {
            IntervalUnit.YearMonth => new YearMonthIntervalArray(data),
            IntervalUnit.DayTime => new DayTimeIntervalArray(data),
            IntervalUnit.MonthDayNanosecond => new MonthDayNanosecondIntervalArray(data),
            _ => throw new InvalidOperationException($"Unsupported interval unit {((IntervalType)data.DataType).Unit}"),
        };

        internal static void ValidateUnit(IntervalUnit expected, IntervalUnit actual)
        {
            if (expected != actual)
            {
                throw new ArgumentException(
                    $"Specified interval unit <{actual}> does not match expected unit <{expected}>",
                    "Unit");
            }
        }
    }

    public sealed class YearMonthIntervalArray : IntervalArray<YearMonthInterval>
    {
        public class Builder : PrimitiveArrayBuilder<YearMonthInterval, YearMonthIntervalArray, Builder>
        {
            /// <summary>
            /// Creates a builder with room for a few values, which grows as values are appended.
            /// </summary>
            public Builder() { }

            /// <summary>
            /// Creates a builder with room for <paramref name="capacity"/> values.
            /// </summary>
            /// <param name="capacity">Number of values to allocate room for.</param>
            /// <param name="allocator">
            /// Allocator for the builder's working memory, which usually becomes the built array's
            /// memory. When <c>null</c>, the builder works in managed memory and copies
            /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
            /// </param>
            public Builder(int capacity, MemoryAllocator allocator = null) : base(capacity, allocator) { }

            protected override YearMonthIntervalArray Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new YearMonthIntervalArray(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
        }

        public YearMonthIntervalArray(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(IntervalType.YearMonth, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public YearMonthIntervalArray(ArrayData data) : base(data)
        {
            ValidateUnit(IntervalUnit.YearMonth, Unit);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);
    }

    public sealed class DayTimeIntervalArray : IntervalArray<DayTimeInterval>
    {
        public class Builder : PrimitiveArrayBuilder<DayTimeInterval, DayTimeIntervalArray, Builder>
        {
            /// <summary>
            /// Creates a builder with room for a few values, which grows as values are appended.
            /// </summary>
            public Builder() { }

            /// <summary>
            /// Creates a builder with room for <paramref name="capacity"/> values.
            /// </summary>
            /// <param name="capacity">Number of values to allocate room for.</param>
            /// <param name="allocator">
            /// Allocator for the builder's working memory, which usually becomes the built array's
            /// memory. When <c>null</c>, the builder works in managed memory and copies
            /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
            /// </param>
            public Builder(int capacity, MemoryAllocator allocator = null) : base(capacity, allocator) { }

            protected override DayTimeIntervalArray Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new DayTimeIntervalArray(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
        }

        public DayTimeIntervalArray(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(IntervalType.DayTime, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public DayTimeIntervalArray(ArrayData data) : base(data)
        {
            ValidateUnit(IntervalUnit.DayTime, Unit);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);
    }

    public sealed class MonthDayNanosecondIntervalArray : IntervalArray<MonthDayNanosecondInterval>
    {
        public class Builder : PrimitiveArrayBuilder<MonthDayNanosecondInterval, MonthDayNanosecondIntervalArray, Builder>
        {
            /// <summary>
            /// Creates a builder with room for a few values, which grows as values are appended.
            /// </summary>
            public Builder() { }

            /// <summary>
            /// Creates a builder with room for <paramref name="capacity"/> values.
            /// </summary>
            /// <param name="capacity">Number of values to allocate room for.</param>
            /// <param name="allocator">
            /// Allocator for the builder's working memory, which usually becomes the built array's
            /// memory. When <c>null</c>, the builder works in managed memory and copies
            /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
            /// </param>
            public Builder(int capacity, MemoryAllocator allocator = null) : base(capacity, allocator) { }

            protected override MonthDayNanosecondIntervalArray Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new MonthDayNanosecondIntervalArray(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
        }

        public MonthDayNanosecondIntervalArray(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(IntervalType.MonthDayNanosecond, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public MonthDayNanosecondIntervalArray(ArrayData data) : base(data)
        {
            ValidateUnit(IntervalUnit.MonthDayNanosecond, Unit);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);
    }
}
