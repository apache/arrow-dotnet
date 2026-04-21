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
using Apache.Arrow.Arrays;
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
            if (storageType is StructType structType &&
                FindBinaryFieldIndex(structType, "metadata") >= 0 &&
                FindBinaryFieldIndex(structType, "value") >= 0)
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
    /// Extension type representing Parquet Variant values, stored as
    /// struct&lt;metadata: binary, value: binary&gt;.
    /// </summary>
    public class VariantType : ExtensionType
    {
        internal const string ExtensionName = "arrow.parquet.variant";

        public static VariantType Default = new VariantType();

        public override string Name => ExtensionName;
        public override string ExtensionMetadata => "";

        public VariantType() : base(new StructType(new[]
        {
            new Field("metadata", BinaryType.Default, false),
            new Field("value", BinaryType.Default, false),
        }))
        { }

        internal VariantType(StructType storageType) : base(storageType) { }

        public override ExtensionArray CreateArray(IArrowArray storageArray)
        {
            return new VariantArray(this, storageArray);
        }
    }

    /// <summary>
    /// Extension array for Parquet Variant values, backed by a StructArray
    /// containing "metadata" and "value" binary fields.
    /// </summary>
    public class VariantArray : ExtensionArray, IReadOnlyList<VariantValue?>
    {
        private readonly int _metadataFieldIndex;
        private readonly int _valueFieldIndex;

        public StructArray StorageArray => (StructArray)Storage;

        public VariantArray(VariantType variantType, IArrowArray storage)
            : base(variantType, storage)
        {
            var structType = (StructType)variantType.StorageType;
            _metadataFieldIndex = structType.GetFieldIndex("metadata");
            _valueFieldIndex = structType.GetFieldIndex("value");
        }

        public VariantArray(IArrowArray storage) : this(VariantType.Default, storage) { }

        /// <summary>
        /// Gets the metadata bytes for the element at the given index.
        /// </summary>
        public ReadOnlySpan<byte> GetMetadataBytes(int index)
        {
            return GetFieldBytes(StorageArray.Fields[_metadataFieldIndex], index);
        }

        /// <summary>
        /// Gets the value bytes for the element at the given index.
        /// </summary>
        public ReadOnlySpan<byte> GetValueBytes(int index)
        {
            return GetFieldBytes(StorageArray.Fields[_valueFieldIndex], index);
        }

        /// <summary>
        /// Gets a zero-copy <see cref="VariantReader"/> for the element at the given index.
        /// The reader is only valid while the underlying array buffers are alive.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">If <paramref name="index"/> is out of range.</exception>
        /// <exception cref="InvalidOperationException">If the element at <paramref name="index"/> is null.</exception>
        public VariantReader GetVariantReader(int index)
        {
            if (index < 0 || index >= Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (IsNull(index))
                throw new InvalidOperationException("Cannot create a VariantReader for a null element.");

            return new VariantReader(GetMetadataBytes(index), GetValueBytes(index));
        }

        /// <summary>
        /// Gets a materialized <see cref="VariantValue"/> for the element at the given index,
        /// or null if the element is null.
        /// </summary>
        public VariantValue? GetVariantValue(int index)
        {
            if (index < 0 || index >= Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (IsNull(index))
                return null;

            var metadata = GetMetadataBytes(index);
            var value = GetValueBytes(index);
            var reader = new VariantReader(metadata, value);
            return reader.ToVariantValue();
        }

        public int Count => Length;
        public VariantValue? this[int index] => GetVariantValue(index);

        public IEnumerator<VariantValue?> GetEnumerator()
        {
            for (int i = 0; i < Length; i++)
            {
                yield return GetVariantValue(i);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private static ReadOnlySpan<byte> GetFieldBytes(IArrowArray field, int index)
        {
            if (field is BinaryArray binaryArray)
            {
                return binaryArray.GetBytes(index);
            }
            if (field is LargeBinaryArray largeBinaryArray)
            {
                return largeBinaryArray.GetBytes(index);
            }
            if (field is BinaryViewArray binaryViewArray)
            {
                return binaryViewArray.GetBytes(index);
            }

            throw new InvalidOperationException(
                $"Unsupported binary field type: {field.Data.DataType.TypeId}");
        }
    }
}
