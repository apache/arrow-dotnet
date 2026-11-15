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
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public partial class RecordBatch
    {
        public class ArrayBuilder
        {
            private readonly MemoryAllocator _allocator;

            internal ArrayBuilder(MemoryAllocator allocator)
            {
                _allocator = allocator;
            }

            public BooleanArray Boolean(Action<BooleanArray.Builder> action) => Build<BooleanArray, BooleanArray.Builder>(new BooleanArray.Builder(_allocator), action);
            public Int8Array Int8(Action<Int8Array.Builder> action) => Build<Int8Array, Int8Array.Builder>(new Int8Array.Builder(0, _allocator), action);
            public Int16Array Int16(Action<Int16Array.Builder> action) => Build<Int16Array, Int16Array.Builder>(new Int16Array.Builder(0, _allocator), action);
            public Int32Array Int32(Action<Int32Array.Builder> action) => Build<Int32Array, Int32Array.Builder>(new Int32Array.Builder(0, _allocator), action);
            public Int64Array Int64(Action<Int64Array.Builder> action) => Build<Int64Array, Int64Array.Builder>(new Int64Array.Builder(0, _allocator), action);
            public UInt8Array UInt8(Action<UInt8Array.Builder> action) => Build<UInt8Array, UInt8Array.Builder>(new UInt8Array.Builder(0, _allocator), action);
            public UInt16Array UInt16(Action<UInt16Array.Builder> action) => Build<UInt16Array, UInt16Array.Builder>(new UInt16Array.Builder(0, _allocator), action);
            public UInt32Array UInt32(Action<UInt32Array.Builder> action) => Build<UInt32Array, UInt32Array.Builder>(new UInt32Array.Builder(0, _allocator), action);
            public UInt64Array UInt64(Action<UInt64Array.Builder> action) => Build<UInt64Array, UInt64Array.Builder>(new UInt64Array.Builder(0, _allocator), action);
#if NET5_0_OR_GREATER
            public HalfFloatArray HalfFloat(Action<HalfFloatArray.Builder> action) => Build<HalfFloatArray, HalfFloatArray.Builder>(new HalfFloatArray.Builder(0, _allocator), action);
#endif
            public FloatArray Float(Action<FloatArray.Builder> action) => Build<FloatArray, FloatArray.Builder>(new FloatArray.Builder(0, _allocator), action);
            public DoubleArray Double(Action<DoubleArray.Builder> action) => Build<DoubleArray, DoubleArray.Builder>(new DoubleArray.Builder(0, _allocator), action);
            public Decimal32Array Decimal32(Decimal32Type type, Action<Decimal32Array.Builder> action) =>
                Build<Decimal32Array, Decimal32Array.Builder>(
                    new Decimal32Array.Builder(type, _allocator), action);
            public Decimal64Array Decimal64(Decimal64Type type, Action<Decimal64Array.Builder> action) =>
                Build<Decimal64Array, Decimal64Array.Builder>(
                    new Decimal64Array.Builder(type, _allocator), action);
            public Decimal128Array Decimal128(Decimal128Type type, Action<Decimal128Array.Builder> action) =>
                Build<Decimal128Array, Decimal128Array.Builder>(
                    new Decimal128Array.Builder(type, _allocator), action);
            public Decimal256Array Decimal256(Decimal256Type type, Action<Decimal256Array.Builder> action) =>
                Build<Decimal256Array, Decimal256Array.Builder>(
                    new Decimal256Array.Builder(type, _allocator), action);
            public Date32Array Date32(Action<Date32Array.Builder> action) => Build<Date32Array, Date32Array.Builder>(new Date32Array.Builder(0, _allocator), action);
            public Date64Array Date64(Action<Date64Array.Builder> action) => Build<Date64Array, Date64Array.Builder>(new Date64Array.Builder(0, _allocator), action);
            public Time32Array Time32(Action<Time32Array.Builder> action) => Build<Time32Array, Time32Array.Builder>(new Time32Array.Builder(Time32Type.Default, 0, _allocator), action);
            public Time32Array Time32(Time32Type type, Action<Time32Array.Builder> action) =>
                Build<Time32Array, Time32Array.Builder>(
                    new Time32Array.Builder(type, 0, _allocator), action);
            public Time64Array Time64(Action<Time64Array.Builder> action) => Build<Time64Array, Time64Array.Builder>(new Time64Array.Builder(Time64Type.Default, 0, _allocator), action);
            public Time64Array Time64(Time64Type type, Action<Time64Array.Builder> action) =>
                Build<Time64Array, Time64Array.Builder>(
                    new Time64Array.Builder(type, 0, _allocator), action);
            public DurationArray Duration(DurationType type, Action<DurationArray.Builder> action) =>
                Build<DurationArray, DurationArray.Builder>(new DurationArray.Builder(type, 0, _allocator), action);
            public BinaryArray Binary(Action<BinaryArray.Builder> action) => Build<BinaryArray, BinaryArray.Builder>(new BinaryArray.Builder(BinaryType.Default, _allocator), action);
            public StringArray String(Action<StringArray.Builder> action) => Build<StringArray, StringArray.Builder>(new StringArray.Builder(_allocator), action);
            public TimestampArray Timestamp(Action<TimestampArray.Builder> action) => Build<TimestampArray, TimestampArray.Builder>(new TimestampArray.Builder(TimestampType.Default, 0, _allocator), action);
            public TimestampArray Timestamp(TimestampType type, Action<TimestampArray.Builder> action) =>
                Build<TimestampArray, TimestampArray.Builder>(
                    new TimestampArray.Builder(type, 0, _allocator), action);
            public TimestampArray Timestamp(TimeUnit unit, TimeZoneInfo timezone, Action<TimestampArray.Builder> action) =>
                Build<TimestampArray, TimestampArray.Builder>(
                    new TimestampArray.Builder(new TimestampType(unit, timezone), 0, _allocator), action);

            private TArray Build<TArray, TArrayBuilder>(TArrayBuilder builder, Action<TArrayBuilder> action)
                where TArray : IArrowArray
                where TArrayBuilder : IArrowArrayBuilder<TArray>
            {
                try
                {
                    if (action == null)
                    {
                        return default;
                    }

                    action(builder);

                    return builder.Build(_allocator);
                }
                finally
                {
                    // The built array owns its memory; this releases only the builder's.
                    (builder as IDisposable)?.Dispose();
                }
            }
        }

        public class Builder
        {
            private readonly MemoryAllocator _allocator;
            private readonly ArrayBuilder _arrayBuilder;
            private readonly Schema.Builder _schemaBuilder;
            private readonly List<IArrowArray> _arrays;

            public Builder(MemoryAllocator allocator = default)
            {
                // Kept as given: a null allocator gives the column builders managed working memory
                // and copies the built arrays into MemoryAllocator.Default, as it always has.
                _allocator = allocator;
                _arrayBuilder = new ArrayBuilder(_allocator);
                _schemaBuilder = new Schema.Builder();
                _arrays = new List<IArrowArray>();
            }

            public RecordBatch Build()
            {
                Schema schema = _schemaBuilder.Build();
                int length = _arrays.Max(x => x.Length);

                // each array has its own memoryOwner, so the RecordBatch itself doesn't
                // have a memoryOwner
                IMemoryOwner<byte> memoryOwner = null;
                var batch = new RecordBatch(schema, memoryOwner, _arrays, length);

                return batch;
            }

            public Builder Clear()
            {
                _schemaBuilder.Clear();
                _arrays.Clear();
                return this;
            }

            public Builder Append(RecordBatch batch)
            {
                foreach (Field field in batch.Schema.FieldsList)
                {
                    _schemaBuilder.Field(field);
                }

                foreach (IArrowArray array in batch.Arrays)
                {
                    _arrays.Add(array);
                }

                return this;
            }

            public Builder Append<TArray>(string name, bool nullable, IArrowArrayBuilder<TArray> builder)
                where TArray : IArrowArray
            {
                return builder == null
                    ? this
                    : Append(name, nullable, builder.Build(_allocator));
            }

            public Builder Append<TArray>(string name, bool nullable, TArray array)
                where TArray : IArrowArray
            {
                if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
                if (array == null) return this;

                _arrays.Add(array);

                _schemaBuilder.Field(f => f
                    .Name(name)
                    .Nullable(nullable)
                    .DataType(array.Data.DataType));

                return this;
            }

            public Builder Append<TArray>(string name, bool nullable, Func<ArrayBuilder, TArray> action)
                where TArray : IArrowArray
            {
                if (action == null) return this;

                TArray array = action(_arrayBuilder);

                Append(name, nullable, array);

                return this;
            }
        }
    }
}
