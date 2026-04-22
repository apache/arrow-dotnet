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
using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class TimestampWithOffsetArrayTests
    {
        // =============================================================
        // Builder tests
        // =============================================================

        [Fact]
        public void BuilderAppendAndRead()
        {
            var values = new DateTimeOffset[]
            {
                new DateTimeOffset(2024, 3, 15, 10, 30, 0, TimeSpan.FromHours(5)),
                new DateTimeOffset(2024, 6, 1, 14, 0, 0, TimeSpan.FromHours(-8)),
                new DateTimeOffset(2024, 12, 31, 23, 59, 59, TimeSpan.Zero),
                new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(5.5)),
            };

            var builder = new TimestampWithOffsetArray.Builder();
            foreach (var v in values)
                builder.Append(v);
            var array = builder.Build();

            Assert.Equal(4, array.Length);
            Assert.Equal(0, array.NullCount);

            for (int i = 0; i < values.Length; i++)
            {
                DateTimeOffset? result = array.GetValue(i);
                Assert.NotNull(result);
                Assert.Equal(values[i], result.Value);
                Assert.Equal(values[i].Offset, result.Value.Offset);
            }
        }

        [Fact]
        public void BuilderAppendNull()
        {
            var builder = new TimestampWithOffsetArray.Builder();
            builder.Append(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
            builder.AppendNull();
            builder.Append(new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.FromHours(3)));
            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(1, array.NullCount);

            Assert.NotNull(array.GetValue(0));
            Assert.Null(array.GetValue(1));
            Assert.NotNull(array.GetValue(2));
        }

        [Fact]
        public void BuilderAppendRange()
        {
            var values = new DateTimeOffset[]
            {
                new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(1)),
                new DateTimeOffset(2024, 2, 1, 0, 0, 0, TimeSpan.FromHours(-5)),
            };

            var builder = new TimestampWithOffsetArray.Builder();
            builder.AppendRange(values);
            var array = builder.Build();

            Assert.Equal(2, array.Length);
            for (int i = 0; i < values.Length; i++)
            {
                Assert.Equal(values[i], array.GetValue(i));
            }
        }

        [Fact]
        public void BuilderAppendRangeNullable()
        {
            var values = new DateTimeOffset?[]
            {
                new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(1)),
                null,
                new DateTimeOffset(2024, 3, 1, 0, 0, 0, TimeSpan.FromHours(-5)),
            };

            var builder = new TimestampWithOffsetArray.Builder();
            builder.AppendRange(values);
            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(1, array.NullCount);
            Assert.NotNull(array.GetValue(0));
            Assert.Null(array.GetValue(1));
            Assert.NotNull(array.GetValue(2));
        }

        [Fact]
        public void EmptyArray()
        {
            var builder = new TimestampWithOffsetArray.Builder();
            var array = builder.Build();

            Assert.Equal(0, array.Length);
            Assert.Equal(0, array.NullCount);
        }

        // =============================================================
        // TimeUnit tests
        // =============================================================

        [Theory]
        [InlineData(TimeUnit.Second)]
        [InlineData(TimeUnit.Millisecond)]
        [InlineData(TimeUnit.Microsecond)]
        [InlineData(TimeUnit.Nanosecond)]
        public void AllTimeUnitsWork(TimeUnit unit)
        {
            var value = new DateTimeOffset(2024, 6, 15, 12, 30, 45, TimeSpan.FromHours(5));
            var builder = new TimestampWithOffsetArray.Builder(unit);
            builder.Append(value);
            var array = builder.Build();

            var result = array.GetValue(0);
            Assert.NotNull(result);
            Assert.Equal(value.Offset, result.Value.Offset);

            // Verify the UTC instant is the same (within the unit's precision)
            Assert.Equal(value.ToUniversalTime().Ticks / GetTicksPerUnit(unit),
                         result.Value.ToUniversalTime().Ticks / GetTicksPerUnit(unit));
        }

        private static long GetTicksPerUnit(TimeUnit unit)
        {
            switch (unit)
            {
                case TimeUnit.Second: return TimeSpan.TicksPerSecond;
                case TimeUnit.Millisecond: return TimeSpan.TicksPerMillisecond;
                case TimeUnit.Microsecond: return 10;
                case TimeUnit.Nanosecond: return 1;
                default: throw new ArgumentOutOfRangeException(nameof(unit));
            }
        }

        // =============================================================
        // Extension type tests
        // =============================================================

        [Fact]
        public void ExtensionTypeProperties()
        {
            var type = TimestampWithOffsetType.Default;
            Assert.Equal("arrow.timestamp_with_offset", type.Name);
            Assert.Equal("", type.ExtensionMetadata);
            Assert.IsType<StructType>(type.StorageType);

            var structType = (StructType)type.StorageType;
            Assert.Equal(2, structType.Fields.Count);
            Assert.Equal("timestamp", structType.Fields[0].Name);
            Assert.Equal("offset_minutes", structType.Fields[1].Name);
        }

        [Fact]
        public void ExtensionDefinitionCreatesType()
        {
            var storageType = new StructType(new[]
            {
                new Field("timestamp", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false),
                new Field("offset_minutes", Int16Type.Default, nullable: false),
            });

            bool result = TimestampWithOffsetExtensionDefinition.Instance.TryCreateType(
                storageType, "", out ExtensionType type);

            Assert.True(result);
            Assert.IsType<TimestampWithOffsetType>(type);
            Assert.Equal(TimeUnit.Microsecond, ((TimestampWithOffsetType)type).Unit);
        }

        [Fact]
        public void ExtensionDefinitionRejectsInvalidStorage()
        {
            // Wrong type entirely
            Assert.False(TimestampWithOffsetExtensionDefinition.Instance.TryCreateType(
                Int32Type.Default, "", out _));

            // Wrong field names
            var wrongNames = new StructType(new[]
            {
                new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false),
                new Field("offset", Int16Type.Default, nullable: false),
            });
            Assert.False(TimestampWithOffsetExtensionDefinition.Instance.TryCreateType(
                wrongNames, "", out _));

            // Wrong timestamp timezone
            var wrongTz = new StructType(new[]
            {
                new Field("timestamp", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: false),
                new Field("offset_minutes", Int16Type.Default, nullable: false),
            });
            Assert.False(TimestampWithOffsetExtensionDefinition.Instance.TryCreateType(
                wrongTz, "", out _));

            // Wrong offset type
            var wrongOffset = new StructType(new[]
            {
                new Field("timestamp", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false),
                new Field("offset_minutes", Int32Type.Default, nullable: false),
            });
            Assert.False(TimestampWithOffsetExtensionDefinition.Instance.TryCreateType(
                wrongOffset, "", out _));
        }

        [Fact]
        public void ExtensionDefinitionAcceptsDictionaryEncodedOffset()
        {
            var dictOffsetType = new DictionaryType(Int32Type.Default, Int16Type.Default, false);
            var storageType = new StructType(new[]
            {
                new Field("timestamp", new TimestampType(TimeUnit.Second, "UTC"), nullable: false),
                new Field("offset_minutes", dictOffsetType, nullable: false),
            });

            bool result = TimestampWithOffsetExtensionDefinition.Instance.TryCreateType(
                storageType, "", out ExtensionType type);

            Assert.True(result);
            Assert.IsType<TimestampWithOffsetType>(type);
            Assert.Equal(TimeUnit.Second, ((TimestampWithOffsetType)type).Unit);
        }

        // =============================================================
        // IReadOnlyList tests
        // =============================================================

        [Fact]
        public void ReadOnlyListInterface()
        {
            var values = new DateTimeOffset[]
            {
                new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(1)),
                new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.FromHours(-5)),
            };

            var builder = new TimestampWithOffsetArray.Builder();
            builder.AppendRange(values);
            var array = builder.Build();

            IReadOnlyList<DateTimeOffset?> list = array;

            Assert.Equal(2, list.Count);
            Assert.Equal(values[0], list[0]);
            Assert.Equal(values[1], list[1]);
        }

        [Fact]
        public void Enumeration()
        {
            var builder = new TimestampWithOffsetArray.Builder();
            builder.Append(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(1)));
            builder.AppendNull();
            builder.Append(new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.FromHours(-5)));
            var array = builder.Build();

            var items = array.ToList();
            Assert.Equal(3, items.Count);
            Assert.NotNull(items[0]);
            Assert.Null(items[1]);
            Assert.NotNull(items[2]);
        }

        // =============================================================
        // Slicing tests
        // =============================================================

        [Fact]
        public void SlicedArray()
        {
            var values = new DateTimeOffset[]
            {
                new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(1)),
                new DateTimeOffset(2024, 3, 15, 10, 30, 0, TimeSpan.FromHours(5)),
                new DateTimeOffset(2024, 6, 1, 14, 0, 0, TimeSpan.FromHours(-8)),
                new DateTimeOffset(2024, 12, 31, 23, 59, 59, TimeSpan.Zero),
            };

            var builder = new TimestampWithOffsetArray.Builder();
            builder.AppendRange(values);
            var array = builder.Build();

            // Slice(1, 2) => { values[1], values[2] }
            var slicedStorage = ArrowArrayFactory.Slice(array.Storage, 1, 2);
            var sliced = new TimestampWithOffsetArray((TimestampWithOffsetType)array.ExtensionType, slicedStorage);

            Assert.Equal(2, sliced.Length);
            Assert.Equal(values[1], sliced.GetValue(0));
            Assert.Equal(values[2], sliced.GetValue(1));
        }

        // =============================================================
        // Edge cases
        // =============================================================

        [Fact]
        public void NegativeOffset()
        {
            var value = new DateTimeOffset(2024, 6, 15, 8, 0, 0, TimeSpan.FromHours(-12));
            var builder = new TimestampWithOffsetArray.Builder();
            builder.Append(value);
            var array = builder.Build();

            var result = array.GetValue(0);
            Assert.Equal(value, result);
            Assert.Equal(TimeSpan.FromHours(-12), result.Value.Offset);
        }

        [Fact]
        public void HalfHourOffset()
        {
            // India Standard Time: +05:30
            var value = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.FromMinutes(330));
            var builder = new TimestampWithOffsetArray.Builder();
            builder.Append(value);
            var array = builder.Build();

            var result = array.GetValue(0);
            Assert.Equal(value, result);
            Assert.Equal(TimeSpan.FromMinutes(330), result.Value.Offset);
        }

        [Fact]
        public void UtcOffset()
        {
            var value = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero);
            var builder = new TimestampWithOffsetArray.Builder();
            builder.Append(value);
            var array = builder.Build();

            var result = array.GetValue(0);
            Assert.Equal(value, result);
            Assert.Equal(TimeSpan.Zero, result.Value.Offset);
        }

        [Fact]
        public void AllNulls()
        {
            var builder = new TimestampWithOffsetArray.Builder();
            builder.AppendNull();
            builder.AppendNull();
            builder.AppendNull();
            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(3, array.NullCount);
            for (int i = 0; i < 3; i++)
                Assert.Null(array.GetValue(i));
        }

        [Fact]
        public void GetValueOutOfRangeThrows()
        {
            var builder = new TimestampWithOffsetArray.Builder();
            builder.Append(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
            var array = builder.Build();

            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetValue(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetValue(1));
        }

        [Fact]
        public void PreservesUtcInstant()
        {
            // Same instant, different offsets
            var utcInstant = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero);
            var sameInstantEast = utcInstant.ToOffset(TimeSpan.FromHours(5));
            var sameInstantWest = utcInstant.ToOffset(TimeSpan.FromHours(-3));

            var builder = new TimestampWithOffsetArray.Builder();
            builder.Append(utcInstant);
            builder.Append(sameInstantEast);
            builder.Append(sameInstantWest);
            var array = builder.Build();

            // All three represent the same instant
            Assert.Equal(array.GetValue(0).Value.ToUniversalTime(),
                         array.GetValue(1).Value.ToUniversalTime());
            Assert.Equal(array.GetValue(0).Value.ToUniversalTime(),
                         array.GetValue(2).Value.ToUniversalTime());

            // But different local offsets
            Assert.Equal(TimeSpan.Zero, array.GetValue(0).Value.Offset);
            Assert.Equal(TimeSpan.FromHours(5), array.GetValue(1).Value.Offset);
            Assert.Equal(TimeSpan.FromHours(-3), array.GetValue(2).Value.Offset);
        }
    }
}
