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
using System.Collections;
using System.Collections.Generic;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// Extension definition for the "arrow.timestamp_with_offset" canonical extension type.
    /// Storage is a struct with fields "timestamp" (Timestamp(unit, "UTC")) and
    /// "offset_minutes" (Int16). The offset_minutes field may be dictionary-encoded
    /// or run-end encoded.
    /// </summary>
    public class TimestampWithOffsetExtensionDefinition : ExtensionDefinition
    {
        public static readonly TimestampWithOffsetExtensionDefinition Instance = new TimestampWithOffsetExtensionDefinition();

        public override string ExtensionName => "arrow.timestamp_with_offset";

        private TimestampWithOffsetExtensionDefinition() { }

        public override bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type)
        {
            type = null;

            if (!(storageType is StructType structType) || structType.Fields.Count != 2)
                return false;

            // Validate field order and names per spec
            Field tsField = structType.Fields[0];
            Field offsetField = structType.Fields[1];

            if (tsField.Name != "timestamp" || offsetField.Name != "offset_minutes")
                return false;

            if (!(tsField.DataType is TimestampType tsType) || tsType.Timezone != "UTC")
                return false;

            // offset_minutes must logically be Int16, but may be dict/REE encoded
            if (!IsLogicallyInt16(offsetField.DataType))
                return false;

            type = new TimestampWithOffsetType(tsType.Unit, structType);
            return true;
        }

        private static bool IsLogicallyInt16(IArrowType type)
        {
            switch (type)
            {
                case Int16Type _:
                    return true;
                case DictionaryType dictType:
                    return dictType.ValueType is Int16Type;
                case RunEndEncodedType reeType:
                    return reeType.ValuesDataType is Int16Type;
                default:
                    return false;
            }
        }
    }

    /// <summary>
    /// Extension type for timestamps with per-value UTC offset, stored as
    /// Struct(timestamp: Timestamp(unit, "UTC"), offset_minutes: Int16).
    /// </summary>
    public class TimestampWithOffsetType : ExtensionType
    {
        public static readonly TimestampWithOffsetType Default =
            new TimestampWithOffsetType(TimeUnit.Microsecond);

        public override string Name => "arrow.timestamp_with_offset";
        public override string ExtensionMetadata => "";

        public TimeUnit Unit { get; }

        public TimestampWithOffsetType(TimeUnit unit = TimeUnit.Microsecond)
            : base(CreateDefaultStorageType(unit))
        {
            Unit = unit;
        }

        internal TimestampWithOffsetType(TimeUnit unit, StructType storageType)
            : base(storageType)
        {
            Unit = unit;
        }

        public override ExtensionArray CreateArray(IArrowArray storageArray)
        {
            return new TimestampWithOffsetArray(this, storageArray);
        }

        private static StructType CreateDefaultStorageType(TimeUnit unit)
        {
            return new StructType(new[]
            {
                new Field("timestamp", new TimestampType(unit, "UTC"), nullable: false),
                new Field("offset_minutes", Int16Type.Default, nullable: false),
            });
        }
    }

    /// <summary>
    /// Extension array for the "arrow.timestamp_with_offset" canonical extension type.
    /// Implements <see cref="IReadOnlyList{T}"/> of nullable <see cref="DateTimeOffset"/>.
    /// </summary>
    public class TimestampWithOffsetArray : ExtensionArray, IReadOnlyList<DateTimeOffset?>
    {
        private readonly StructArray _struct;
        private readonly TimestampArray _timestamps;
        private readonly IReadOnlyList<short?> _offsetMinutes;

        public TimestampWithOffsetArray(TimestampWithOffsetType type, IArrowArray storage)
            : base(type, storage)
        {
            _struct = (StructArray)storage;
            var structType = (StructType)storage.Data.DataType;

            int tsIndex = structType.GetFieldIndex("timestamp");
            int offsetIndex = structType.GetFieldIndex("offset_minutes");
            if (tsIndex < 0 || offsetIndex < 0)
                throw new ArgumentException("Storage struct must have 'timestamp' and 'offset_minutes' fields.");

            _timestamps = (TimestampArray)_struct.Fields[tsIndex];
            _offsetMinutes = _struct.Fields[offsetIndex].AsDecodedReadOnlyList<short?>();
        }

        /// <summary>
        /// Gets the value at the specified index as a <see cref="DateTimeOffset"/>
        /// with the original timezone offset preserved.
        /// </summary>
        public DateTimeOffset? GetValue(int index)
        {
            if (index < 0 || index >= Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (IsNull(index))
                return null;

            DateTimeOffset utc = _timestamps.GetTimestampUnchecked(index);
            short offsetMins = _offsetMinutes[index] ?? 0;
            TimeSpan offset = TimeSpan.FromMinutes(offsetMins);
            return utc.ToOffset(offset);
        }

        public int Count => Length;
        public DateTimeOffset? this[int index] => GetValue(index);

        public IEnumerator<DateTimeOffset?> GetEnumerator()
        {
            for (int i = 0; i < Length; i++)
                yield return GetValue(i);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Builder for <see cref="TimestampWithOffsetArray"/>.
        /// </summary>
        public class Builder
        {
            private readonly TimestampArray.Builder _timestampBuilder;
            private readonly Int16Array.Builder _offsetBuilder;
            private readonly ArrowBuffer.BitmapBuilder _validityBuilder;
            private readonly TimestampWithOffsetType _type;
            private int _length;
            private int _nullCount;

            public Builder(TimeUnit unit = TimeUnit.Microsecond)
            {
                _type = new TimestampWithOffsetType(unit);
                _timestampBuilder = new TimestampArray.Builder(unit, "UTC");
                _offsetBuilder = new Int16Array.Builder();
                _validityBuilder = new ArrowBuffer.BitmapBuilder();
            }

            public Builder Append(DateTimeOffset value)
            {
                _timestampBuilder.Append(value.ToUniversalTime());
                _offsetBuilder.Append(checked((short)value.Offset.TotalMinutes));
                _validityBuilder.Append(true);
                _length++;
                return this;
            }

            public Builder AppendNull()
            {
                _timestampBuilder.Append(default(DateTimeOffset));
                _offsetBuilder.Append(0);
                _validityBuilder.Append(false);
                _length++;
                _nullCount++;
                return this;
            }

            public Builder AppendRange(IEnumerable<DateTimeOffset> values)
            {
                if (values == null)
                    throw new ArgumentNullException(nameof(values));

                foreach (var value in values)
                    Append(value);

                return this;
            }

            public Builder AppendRange(IEnumerable<DateTimeOffset?> values)
            {
                if (values == null)
                    throw new ArgumentNullException(nameof(values));

                foreach (var value in values)
                {
                    if (value.HasValue)
                        Append(value.Value);
                    else
                        AppendNull();
                }

                return this;
            }

            public TimestampWithOffsetArray Build()
            {
                TimestampArray timestamps = _timestampBuilder.Build();
                Int16Array offsets = _offsetBuilder.Build();
                ArrowBuffer validityBuffer = _validityBuilder.Build();

                var structType = (StructType)_type.StorageType;
                var structArray = new StructArray(
                    structType, _length,
                    new IArrowArray[] { timestamps, offsets },
                    validityBuffer, _nullCount);

                return new TimestampWithOffsetArray(_type, structArray);
            }
        }
    }
}
