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
using System.Diagnostics;
using System.IO;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class TimestampArray : PrimitiveArray<long>, IReadOnlyList<DateTimeOffset?>, ICollection<DateTimeOffset?>
    {
        private static readonly DateTimeOffset s_epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        public class Builder : PrimitiveArrayBuilder<DateTimeOffset, long, TimestampArray, Builder>
        {
            internal class TimestampBuilder : PrimitiveArrayBuilder<long, TimestampArray, TimestampBuilder>
            {
                internal TimestampBuilder(TimestampType type)
                {
                    DataType = type ?? throw new ArgumentNullException(nameof(type));
                }

                internal TimestampBuilder(TimestampType type, int capacity, MemoryAllocator allocator)
                    : base(capacity, allocator)
                {
                    DataType = type ?? throw new ArgumentNullException(nameof(type));
                }

                protected TimestampType DataType { get; }

                protected override TimestampArray Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new TimestampArray(DataType, valueBuffer, nullBitmapBuffer,
                        length, nullCount, offset);
            }

            protected TimestampType DataType { get; }

            public Builder()
                : this(TimestampType.Default) { }

            public Builder(TimeUnit unit, TimeZoneInfo timezone)
                : this(new TimestampType(unit, timezone)) { }

            public Builder(TimeUnit unit = TimeUnit.Millisecond, string timezone = "+00:00")
                : this(new TimestampType(unit, timezone)) { }

            public Builder(TimeUnit unit)
                : this(new TimestampType(unit, (string)null)) { }

            public Builder(TimestampType type)
                : base(new TimestampBuilder(type))
            {
                DataType = type;
            }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class with room for
            /// <paramref name="capacity"/> values.
            /// </summary>
            /// <param name="capacity">Number of values to allocate room for.</param>
            /// <param name="allocator">
            /// Allocator for the builder's working memory, which usually becomes the built array's
            /// memory. When <c>null</c>, the builder works in managed memory and copies
            /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
            /// </param>
            public Builder(TimestampType type, int capacity, MemoryAllocator allocator = null)
                // Checked before the inner builder allocates, so that a failure leaks nothing.
                : base(new TimestampBuilder(type ?? throw new ArgumentNullException(nameof(type)), capacity, allocator))
            {
                DataType = type;
            }

            protected override long ConvertTo(DateTimeOffset value)
            {
                // We must return the absolute time since the UNIX epoch while
                // respecting the timezone offset; the calculation is as follows:
                //
                // - Compute time span between epoch and specified time
                // - Compute time divisions per tick

                TimeSpan timeSpan = value - s_epoch;
                long ticks = timeSpan.Ticks;

                switch (DataType.Unit)
                {
                    case TimeUnit.Nanosecond:
                        return checked(ticks * 100);
                    case TimeUnit.Microsecond:
                        return ticks / 10;
                    case TimeUnit.Millisecond:
                        return ticks / TimeSpan.TicksPerMillisecond;
                    case TimeUnit.Second:
                        return ticks / TimeSpan.TicksPerSecond;
                    default:
                        throw new InvalidOperationException($"unsupported time unit <{DataType.Unit}>");
                }
            }
        }

        public TimestampArray(
            TimestampType type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public TimestampArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Timestamp);

            Debug.Assert(Data.DataType is TimestampType);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public DateTimeOffset GetTimestampUnchecked(int index)
        {
            var type = (TimestampType)Data.DataType;
            long value = Values[index];

            long ticks;

            switch (type.Unit)
            {
                case TimeUnit.Nanosecond:
                    ticks = value / 100;
                    break;
                case TimeUnit.Microsecond:
                    ticks = value * 10;
                    break;
                case TimeUnit.Millisecond:
                    ticks = value * TimeSpan.TicksPerMillisecond;
                    break;
                case TimeUnit.Second:
                    ticks = value * TimeSpan.TicksPerSecond;
                    break;
                default:
                    throw new InvalidDataException(
                        $"Unsupported timestamp unit <{type.Unit}>");
            }

            return new DateTimeOffset(s_epoch.Ticks + ticks, TimeSpan.Zero);
        }

        public DateTimeOffset? GetTimestamp(int index)
        {
            if (IsNull(index))
            {
                return null;
            }

            return GetTimestampUnchecked(index);
        }

        int IReadOnlyCollection<DateTimeOffset?>.Count => Length;

        DateTimeOffset? IReadOnlyList<DateTimeOffset?>.this[int index] => GetTimestamp(index);

        IEnumerator<DateTimeOffset?> IEnumerable<DateTimeOffset?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetTimestamp(index);
            }
        }

        int ICollection<DateTimeOffset?>.Count => Length;
        bool ICollection<DateTimeOffset?>.IsReadOnly => true;
        void ICollection<DateTimeOffset?>.Add(DateTimeOffset? item) => throw new NotSupportedException("Collection is read-only.");
        bool ICollection<DateTimeOffset?>.Remove(DateTimeOffset? item) => throw new NotSupportedException("Collection is read-only.");
        void ICollection<DateTimeOffset?>.Clear() => throw new NotSupportedException("Collection is read-only.");

        bool ICollection<DateTimeOffset?>.Contains(DateTimeOffset? item)
        {
            for (int index = 0; index < Length; index++)
            {
                if (GetTimestamp(index).Equals(item))
                    return true;
            }

            return false;
        }

        void ICollection<DateTimeOffset?>.CopyTo(DateTimeOffset?[] array, int arrayIndex)
        {
            for (int srcIndex = 0, destIndex = arrayIndex; srcIndex < Length; srcIndex++, destIndex++)
            {
                array[destIndex] = GetTimestamp(srcIndex);
            }
        }
    }
}
