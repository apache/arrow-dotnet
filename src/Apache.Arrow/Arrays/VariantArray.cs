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
using System.Linq;
using Apache.Arrow.Memory;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// Extension definition for the "arrow.parquet.variant" extension type,
    /// backed by a struct with "metadata" and "value" binary fields.
    /// </summary>
    public class VariantExtensionDefinition : ExtensionDefinition
    {
        public static VariantExtensionDefinition Instance = new VariantExtensionDefinition();

        public override string ExtensionName => VariantType.ExtensionName;

        private VariantExtensionDefinition() { }

        public override bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type)
        {
            // Accept the Parquet variant storage layouts:
            //   struct<metadata: binary, value: binary>                     (unshredded)
            //   struct<metadata: binary, value: binary, typed_value: T>     (shredded with residual)
            //   struct<metadata: binary, typed_value: T>                    (fully shredded, no value column)
            // The metadata field is required. At least one of value/typed_value must be present.
            if (storageType is StructType structType &&
                FindBinaryFieldIndex(structType, "metadata") >= 0 &&
                (FindBinaryFieldIndex(structType, "value") >= 0 ||
                 structType.GetFieldIndex("typed_value") >= 0))
            {
                type = new VariantType(structType);
                return true;
            }
            type = null;
            return false;
        }

        internal static int FindBinaryFieldIndex(StructType structType, string name)
        {
            int index = structType.GetFieldIndex(name);
            if (index < 0)
            {
                return -1;
            }

            var fieldType = structType.Fields[index].DataType;
            if (fieldType is BinaryType || fieldType is LargeBinaryType || fieldType is BinaryViewType)
            {
                return index;
            }

            return -1;
        }
    }

    /// <summary>
    /// Extension type representing Parquet Variant values. The underlying storage is
    /// a struct with a required <c>metadata</c> binary field and at least one of:
    /// <list type="bullet">
    /// <item><c>value: binary</c> — the variant value bytes (possibly residual when shredded).</item>
    /// <item><c>typed_value: T</c> — a typed column populated by variant shredding, where
    /// <c>T</c> is an Arrow primitive, struct, or list per the Parquet variant shredding spec.</item>
    /// </list>
    /// Use <see cref="IsShredded"/> to check for shredded layouts. Decoding shredded
    /// values requires <c>Apache.Arrow.Operations.Shredding</c>.
    /// </summary>
    public class VariantType : ExtensionType
    {
        internal const string ExtensionName = "arrow.parquet.variant";

        public static VariantType Default = new VariantType();

        public override string Name => ExtensionName;
        public override string ExtensionMetadata => "";

        /// <summary>
        /// True if the storage layout has a <c>value</c> binary field (unshredded or
        /// partially shredded). False for fully shredded layouts that omit the column.
        /// </summary>
        public bool HasValueColumn { get; }

        /// <summary>
        /// True if the storage layout has a <c>typed_value</c> field (shredded).
        /// </summary>
        public bool HasTypedValueColumn { get; }

        /// <summary>
        /// True if the storage layout includes any shredding (has a <c>typed_value</c> column).
        /// </summary>
        public bool IsShredded => HasTypedValueColumn;

        /// <summary>
        /// The <c>typed_value</c> field when <see cref="HasTypedValueColumn"/> is true; otherwise null.
        /// </summary>
        public Field TypedValueField { get; }

        public VariantType() : base(new StructType(new[]
        {
            new Field("metadata", BinaryType.Default, false),
            new Field("value", BinaryType.Default, false),
        }))
        {
            HasValueColumn = true;
            HasTypedValueColumn = false;
            TypedValueField = null;
        }

        internal VariantType(StructType storageType) : base(storageType)
        {
            HasValueColumn = VariantExtensionDefinition.FindBinaryFieldIndex(storageType, "value") >= 0;
            int typedIdx = storageType.GetFieldIndex("typed_value");
            HasTypedValueColumn = typedIdx >= 0;
            TypedValueField = typedIdx >= 0 ? storageType.Fields[typedIdx] : null;
        }

        public override ExtensionArray CreateArray(IArrowArray storageArray)
        {
            return new VariantArray(this, storageArray);
        }
    }

    /// <summary>
    /// Extension array for Parquet Variant values, backed by a StructArray
    /// containing "metadata" and "value" binary fields.
    /// </summary>
    public class VariantArray : ExtensionArray, IReadOnlyList<VariantValue>
    {
        private readonly IIndexes _metadataIndexes;
        private readonly IBinaryArray _metadataArray;
        private readonly IIndexes _valueIndexes;
        private readonly IBinaryArray _valueArray;

        public StructArray StorageArray => (StructArray)Storage;

        /// <summary>
        /// The variant type metadata describing which columns are present.
        /// </summary>
        public VariantType VariantType => (VariantType)ExtensionType;

        /// <summary>
        /// True when the underlying column includes shredded data (has a
        /// <c>typed_value</c> column, or lacks a <c>value</c> column). Reads
        /// on shredded columns require <c>Apache.Arrow.Operations.Shredding</c>.
        /// </summary>
        public bool IsShredded => VariantType.IsShredded;

        /// <summary>
        /// The <c>typed_value</c> child array when the column is shredded; otherwise null.
        /// </summary>
        public IArrowArray TypedValueArray { get; }

        public VariantArray(VariantType variantType, IArrowArray storage)
            : base(variantType, storage)
        {
            var structType = (StructType)variantType.StorageType;
            _metadataArray = DecodeBinaryArray(StorageArray.Fields[structType.GetFieldIndex("metadata")], out _metadataIndexes);

            if (variantType.HasValueColumn)
            {
                _valueArray = DecodeBinaryArray(StorageArray.Fields[structType.GetFieldIndex("value")], out _valueIndexes);
            }

            if (variantType.HasTypedValueColumn)
            {
                TypedValueArray = StorageArray.Fields[structType.GetFieldIndex("typed_value")];
            }
        }

        public VariantArray(IArrowArray storage) : this(InferVariantType(storage), storage) { }

        private static VariantType InferVariantType(IArrowArray storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (VariantExtensionDefinition.Instance.TryCreateType(storage.Data.DataType, null, out ExtensionType ext))
            {
                return (VariantType)ext;
            }
            throw new ArgumentException(
                "Storage array does not match a variant layout (expected struct<metadata, value?, typed_value?>).",
                nameof(storage));
        }

        /// <summary>
        /// Gets the metadata bytes for the element at the given index.
        /// </summary>
        public ReadOnlySpan<byte> GetMetadataBytes(int index)
        {
            int physicalIndex = _metadataIndexes.GetPhysicalIndex(index);
            return _metadataArray.GetBytes(physicalIndex, out bool isNull);
        }

        /// <summary>
        /// Gets the value bytes for the element at the given index.
        /// </summary>
        /// <exception cref="InvalidOperationException">If the column has no <c>value</c> field.</exception>
        public ReadOnlySpan<byte> GetValueBytes(int index)
        {
            if (_valueArray == null)
            {
                throw new InvalidOperationException(
                    "This VariantArray has no 'value' column (fully shredded layout). " +
                    "Use the shredding-aware readers in Apache.Arrow.Operations.Shredding.");
            }
            int physicalIndex = _valueIndexes.GetPhysicalIndex(index);
            return _valueArray.GetBytes(physicalIndex, out bool isNull);
        }

        /// <summary>
        /// Returns true and sets <paramref name="value"/> when the element at
        /// <paramref name="index"/> has value bytes, false otherwise. Shredded
        /// elements whose residual is null will return false.
        /// </summary>
        public bool TryGetValueBytes(int index, out ReadOnlySpan<byte> value)
        {
            if (_valueArray == null)
            {
                value = default;
                return false;
            }
            int physicalIndex = _valueIndexes.GetPhysicalIndex(index);
            ReadOnlySpan<byte> bytes = _valueArray.GetBytes(physicalIndex, out bool isNull);
            if (isNull)
            {
                value = default;
                return false;
            }
            value = bytes;
            return true;
        }

        /// <summary>
        /// Gets a zero-copy <see cref="VariantReader"/> for the element at the given index.
        /// The reader is only valid while the underlying array buffers are alive.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">If <paramref name="index"/> is out of range.</exception>
        /// <exception cref="InvalidOperationException">
        /// If the element at <paramref name="index"/> is null, or the column is shredded
        /// (a <see cref="VariantReader"/> over residual bytes alone does not represent the
        /// logical variant — use <c>Apache.Arrow.Operations.Shredding</c> instead).
        /// </exception>
        public VariantReader GetVariantReader(int index)
        {
            if (index < 0 || index >= Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (IsNull(index))
                throw new InvalidOperationException("Cannot create a VariantReader for a null element.");

            if (IsShredded)
                throw new InvalidOperationException(
                    "Cannot create a VariantReader for a shredded column. Use the shredding-aware " +
                    "readers in Apache.Arrow.Operations.Shredding.");

            return new VariantReader(GetMetadataBytes(index), GetValueBytes(index));
        }

        /// <summary>
        /// Gets a materialized <see cref="VariantValue"/> for the element at the given index.
        /// Shredded columns require <c>Apache.Arrow.Operations.Shredding</c>; call the
        /// <c>GetShreddedVariant</c> / <c>GetLogicalVariantValue</c> extension methods instead.
        /// </summary>
        /// <exception cref="InvalidOperationException">If the column is shredded.</exception>
        public VariantValue GetVariantValue(int index)
        {
            if (index < 0 || index >= Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (IsNull(index))
                return VariantValue.Null;

            if (IsShredded)
                throw new InvalidOperationException(
                    "GetVariantValue is not supported on shredded VariantArrays. " +
                    "Reference Apache.Arrow.Operations.Shredding and use GetLogicalVariantValue " +
                    "(transparent materialization) or GetShreddedVariant (reader-style access).");

            var metadata = GetMetadataBytes(index);
            var value = GetValueBytes(index);
            var reader = new VariantReader(metadata, value);
            return reader.ToVariantValue();
        }

        public int Count => Length;
        public VariantValue this[int index] => GetVariantValue(index);

        public IEnumerator<VariantValue> GetEnumerator()
        {
            if (IsShredded)
            {
                throw new InvalidOperationException(
                    "Enumeration is not supported on shredded VariantArrays. " +
                    "Reference Apache.Arrow.Operations.Shredding and iterate via GetLogicalVariantValue.");
            }

            IEnumerator<int> metadataIdx = _metadataIndexes.EnumeratePhysicalIndices().GetEnumerator();
            IEnumerator<int> valueIdx = _valueIndexes.EnumeratePhysicalIndices().GetEnumerator();
            for (int i = 0; metadataIdx.MoveNext() && valueIdx.MoveNext(); i++)
            {
                if (IsNull(i))
                {
                    yield return VariantValue.Null;
                    continue;
                }
                var metadata = _metadataArray.GetBytes(metadataIdx.Current, out _);
                var value = _valueArray.GetBytes(valueIdx.Current, out _);
                yield return new VariantReader(metadata, value).ToVariantValue();
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private static IBinaryArray DecodeBinaryArray(IArrowArray array, out IIndexes indexes)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            switch (array)
            {
                case IBinaryArray binary:
                    indexes = new SimpleIndexes(binary.Length);
                    return binary;

                case DictionaryArray dict:
                    IBinaryArray values = dict.Dictionary as IBinaryArray;
                    if (values == null)
                        throw new ArgumentException(
                            $"Dictionary value type {dict.Dictionary.Data.DataType.TypeId} cannot be read as binary.");
                    indexes = dict.GetIndexes();
                    return values;

                case RunEndEncodedArray ree:
                    IBinaryArray reeValues = ree.Values as IBinaryArray;
                    if (reeValues == null)
                        throw new ArgumentException(
                            $"Run-end encoded value type {ree.Values.Data.DataType.TypeId} cannot be read as binary.");
                    indexes = ree;
                    return reeValues;
                default:
                    throw new ArgumentException(
                        $"Cannot create binary reader for array of type {array.Data.DataType.TypeId}.",
                        nameof(array));
            }
        }

        sealed class SimpleIndexes : IIndexes
        {
            public SimpleIndexes(int length)
            {
                Length = length;
            }

            public int Length { get; }
            public IEnumerable<int> EnumeratePhysicalIndices() => Enumerable.Range(0, Length);
            public int GetPhysicalIndex(int index) => index;
        }

        /// <summary>
        /// Builder for constructing <see cref="VariantArray"/> instances.
        /// </summary>
        public class Builder
        {
            private readonly BinaryArray.Builder _metadataBuilder = new BinaryArray.Builder();
            private readonly BinaryArray.Builder _valueBuilder = new BinaryArray.Builder();
            private readonly ArrowBuffer.BitmapBuilder _validityBuilder = new ArrowBuffer.BitmapBuilder();
            private readonly VariantBuilder _encoder = new VariantBuilder();
            private int _length;
            private int _nullCount;

            // Pre-encoded placeholder for struct-level nulls.
            // We use encoded VariantValue.Null so child arrays always have valid binary data.
            private static readonly Lazy<(byte[] Metadata, byte[] Value)> NullPlaceholder =
                new Lazy<(byte[], byte[])>(() => new VariantBuilder().Encode(VariantValue.Null));

            /// <summary>
            /// Gets the number of elements appended so far.
            /// </summary>
            public int Length => _length;

            /// <summary>
            /// Appends a <see cref="VariantValue"/> to the array.
            /// </summary>
            public Builder Append(VariantValue value)
            {
                var (metadata, valueBytes) = _encoder.Encode(value);
                _metadataBuilder.Append((ReadOnlySpan<byte>)metadata);
                _valueBuilder.Append((ReadOnlySpan<byte>)valueBytes);
                _validityBuilder.Append(true);
                _length++;
                return this;
            }

            /// <summary>
            /// Appends a nullable <see cref="VariantValue"/>. A null value appends
            /// a struct-level null (as opposed to a variant-encoded null).
            /// </summary>
            public Builder Append(VariantValue? value)
            {
                if (value == null)
                    return AppendNull();
                return Append(value.Value);
            }

            /// <summary>
            /// Appends a variant element from pre-encoded metadata and value bytes.
            /// The caller is responsible for providing valid variant-encoded data.
            /// </summary>
            public Builder Append(ReadOnlySpan<byte> metadata, ReadOnlySpan<byte> value)
            {
                _metadataBuilder.Append(metadata);
                _valueBuilder.Append(value);
                _validityBuilder.Append(true);
                _length++;
                return this;
            }

            /// <summary>
            /// Appends a struct-level null element. This is distinct from appending
            /// <see cref="VariantValue.Null"/>, which represents a valid slot
            /// containing a variant-encoded null value.
            /// </summary>
            public Builder AppendNull()
            {
                var placeholder = NullPlaceholder.Value;
                _metadataBuilder.Append((ReadOnlySpan<byte>)placeholder.Metadata);
                _valueBuilder.Append((ReadOnlySpan<byte>)placeholder.Value);
                _validityBuilder.Append(false);
                _length++;
                _nullCount++;
                return this;
            }

            /// <summary>
            /// Appends a range of <see cref="VariantValue"/> elements.
            /// </summary>
            public Builder AppendRange(IEnumerable<VariantValue> values)
            {
                if (values == null)
                    throw new ArgumentNullException(nameof(values));

                foreach (var value in values)
                {
                    Append(value);
                }
                return this;
            }

            /// <summary>
            /// Appends a range of nullable <see cref="VariantValue"/> elements.
            /// </summary>
            public Builder AppendRange(IEnumerable<VariantValue?> values)
            {
                if (values == null)
                    throw new ArgumentNullException(nameof(values));

                foreach (var value in values)
                {
                    Append(value);
                }
                return this;
            }

            /// <summary>
            /// Builds the <see cref="VariantArray"/> from appended values.
            /// </summary>
            public VariantArray Build(MemoryAllocator allocator = default)
            {
                var metadataArray = _metadataBuilder.Build(allocator);
                var valueArray = _valueBuilder.Build(allocator);
                var structType = (StructType)VariantType.Default.StorageType;
                var nullBitmap = _nullCount > 0 ? _validityBuilder.Build(allocator) : ArrowBuffer.Empty;
                var structArray = new StructArray(
                    structType, _length,
                    new IArrowArray[] { metadataArray, valueArray },
                    nullBitmap, _nullCount);
                return new VariantArray(VariantType.Default, structArray);
            }
        }
    }
}
