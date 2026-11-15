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
    public class LargeListViewArray : Array
    {
        public class Builder : IArrowArrayBuilder<LargeListViewArray, Builder>, IDisposable
        {
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

            public int Length => ValueOffsetsBufferBuilder.Length;

            private ArrowBuffer.Builder<long> ValueOffsetsBufferBuilder { get; }

            private ArrowBuffer.Builder<long> SizesBufferBuilder { get; }

            private ArrowBuffer.BitmapBuilder ValidityBufferBuilder { get; }

            public int NullCount { get; protected set; }

            private IArrowType DataType { get; }

            private int Start { get; set; }

            public Builder(IArrowType valueDataType) : this(new LargeListViewType(valueDataType), null)
            {
            }

            /// <summary>
            /// Creates a builder whose working memory comes from <paramref name="allocator"/>.
            /// </summary>
            /// <param name="valueDataType">The type of the list elements.</param>
            /// <param name="allocator">
            /// Allocator for the builder's working memory, which usually becomes the built array's
            /// memory. When <c>null</c>, the builder works in managed memory and copies
            /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
            /// </param>
            public Builder(IArrowType valueDataType, MemoryAllocator allocator) : this(new LargeListViewType(valueDataType), allocator)
            {
            }

            public Builder(Field valueField) : this(new LargeListViewType(valueField), null)
            {
            }

            /// <summary>
            /// Creates a builder whose working memory comes from <paramref name="allocator"/>.
            /// </summary>
            /// <param name="valueField">The field describing the list elements.</param>
            /// <param name="allocator">
            /// Allocator for the builder's working memory, which usually becomes the built array's
            /// memory. When <c>null</c>, the builder works in managed memory and copies
            /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
            /// </param>
            public Builder(Field valueField, MemoryAllocator allocator) : this(new LargeListViewType(valueField), allocator)
            {
            }

            internal Builder(LargeListViewType dataType, MemoryAllocator allocator)
            {
                ValueBuilder = ArrowArrayBuilderFactory.Build(dataType.ValueDataType, allocator);
                ValueOffsetsBufferBuilder = new ArrowBuffer.Builder<long>(0, allocator);
                SizesBufferBuilder = new ArrowBuffer.Builder<long>(0, allocator);
                ValidityBufferBuilder = new ArrowBuffer.BitmapBuilder(0, allocator);
                DataType = dataType;
                Start = -1;
            }

            /// <summary>
            /// Start a new variable-length list slot
            ///
            /// This function should be called before beginning to append elements to the
            /// value builder.
            /// </summary>
            public Builder Append()
            {
                AppendPrevious();

                ValidityBufferBuilder.Append(true);

                return this;
            }

            public Builder AppendNull()
            {
                AppendPrevious();

                ValidityBufferBuilder.Append(false);
                ValueOffsetsBufferBuilder.Append(Start);
                SizesBufferBuilder.Append(0);
                NullCount++;
                Start = -1;

                return this;
            }

            private void AppendPrevious()
            {
                if (Start >= 0)
                {
                    ValueOffsetsBufferBuilder.Append(Start);
                    SizesBufferBuilder.Append(ValueBuilder.Length - Start);
                }
                Start = ValueBuilder.Length;
            }

            /// <summary>
            /// Releases the builder's working memory. Arrays already built are unaffected.
            /// </summary>
            /// <remarks>
            /// Disposal is required only when the working memory comes from an allocator whose
            /// <see cref="MemoryAllocator.Lifetime"/> is <see cref="AllocationLifetime.CallerOwned"/>;
            /// otherwise it only releases memory sooner.
            /// </remarks>
            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected virtual void Dispose(bool disposing)
            {
                if (disposing)
                {
                    (ValueBuilder as IDisposable)?.Dispose();
                    ValueOffsetsBufferBuilder.Dispose();
                    SizesBufferBuilder.Dispose();
                    ValidityBufferBuilder.Dispose();
                }
            }

            public LargeListViewArray Build(MemoryAllocator allocator = default)
            {
                AppendPrevious();

                ArrowBuffer validityBuffer = NullCount > 0
                                        ? ValidityBufferBuilder.Build(allocator)
                                        : ArrowBuffer.Empty;

                return new LargeListViewArray(DataType, Length,
                    ValueOffsetsBufferBuilder.Build(allocator), SizesBufferBuilder.Build(allocator),
                    ValueBuilder.Build(allocator),
                    validityBuffer, NullCount, 0);
            }

            public Builder Reserve(int capacity)
            {
                ValueOffsetsBufferBuilder.Reserve(capacity);
                SizesBufferBuilder.Reserve(capacity);
                ValidityBufferBuilder.Reserve(capacity);
                return this;
            }

            public Builder Resize(int length)
            {
                ValueOffsetsBufferBuilder.Resize(length);
                SizesBufferBuilder.Resize(length);
                ValidityBufferBuilder.Resize(length);
                return this;
            }

            public Builder Clear()
            {
                ValueOffsetsBufferBuilder.Clear();
                SizesBufferBuilder.Clear();
                ValueBuilder.Clear();
                ValidityBufferBuilder.Clear();
                NullCount = 0;
                Start = -1;
                return this;
            }
        }

        public IArrowArray Values { get; }

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

        public ReadOnlySpan<long> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<long>().Slice(Offset, Length);

        public ArrowBuffer SizesBuffer => Data.Buffers[2];

        public ReadOnlySpan<long> Sizes => SizesBuffer.Span.CastTo<long>().Slice(Offset, Length);

        public LargeListViewArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer, ArrowBuffer sizesBuffer, IArrowArray values,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(dataType, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueOffsetsBuffer, sizesBuffer }, new[] { values.Data }),
                values)
        {
        }

        public LargeListViewArray(ArrayData data)
            : this(data, ArrowArrayFactory.BuildArray(data.Children[0]))
        {
        }

        private LargeListViewArray(ArrayData data, IArrowArray values) : base(data)
        {
            data.EnsureBufferCount(3);
            data.EnsureDataType(ArrowTypeId.LargeListView);
            Values = values;
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public int GetValueLength(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            if (IsNull(index))
            {
                return 0;
            }

            return checked((int)Sizes[index]);
        }

        public IArrowArray GetSlicedValues(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            if (IsNull(index))
            {
                return null;
            }

            return ArrowArrayFactory.Slice(Values, checked((int)ValueOffsets[index]), GetValueLength(index));
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Values?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
