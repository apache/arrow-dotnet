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
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// Extension definition for the "arrow.uuid" extension type,
    /// backed by FixedSizeBinary(16).
    /// </summary>
    public class GuidExtensionDefinition : ExtensionDefinition
    {
        public static GuidExtensionDefinition Instance = new GuidExtensionDefinition();

        public override string ExtensionName => "arrow.uuid";

        private GuidExtensionDefinition() { }

        public override bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type)
        {
            if (storageType is FixedSizeBinaryType fsbType && fsbType.ByteWidth == GuidType.ByteWidth)
            {
                type = new GuidType(fsbType);
                return true;
            }
            type = null;
            return false;
        }
    }

    /// <summary>
    /// Extension type representing UUIDs/GUIDs, stored as FixedSizeBinary(16).
    /// </summary>
    public class GuidType : ExtensionType
    {
        public static GuidType Default = new GuidType();

        internal const int ByteWidth = 16;

        public override string Name => "arrow.uuid";
        public override string ExtensionMetadata => "";

        public GuidType() : base(new FixedSizeBinaryType(ByteWidth)) { }

        internal GuidType(FixedSizeBinaryType storageType) : base(storageType) { }

        public override ExtensionArray CreateArray(IArrowArray storageArray)
        {
            return new GuidArray(this, storageArray);
        }
    }

    /// <summary>
    /// Extension array for UUID/GUID values, backed by a FixedSizeBinaryArray.
    /// </summary>
    public class GuidArray : ExtensionArray, IReadOnlyList<Guid?>
    {
        public FixedSizeBinaryArray StorageArray => (FixedSizeBinaryArray)Storage;

        public GuidArray(GuidType guidType, IArrowArray storage) : base(guidType, storage) { }

        public GuidArray(IArrowArray storage) : base(GuidType.Default, storage) { }

        public class Builder : FixedSizeBinaryArray.BuilderBase<GuidArray, Builder>
        {
            public Builder() : base(GuidType.Default.StorageType, GuidType.ByteWidth)
            {
            }

            protected override GuidArray Build(ArrayData data)
            {
                return new GuidArray(GuidType.Default, new FixedSizeBinaryArray(data));
            }

            public Builder Append(Guid value)
            {
                Span<byte> bytes = stackalloc byte[GuidType.ByteWidth];
                GuidToRFC4122(value, bytes);
                return Append(bytes);
            }

            public Builder AppendRange(IEnumerable<Guid> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (Guid guid in values)
                {
                    Append(guid);
                }

                return Instance;
            }

            public Builder Set(int index, Guid value)
            {
                Span<byte> bytes = stackalloc byte[GuidType.ByteWidth];
                GuidToRFC4122(value, bytes);

                return Set(index, bytes);
            }
        }

        /// <summary>
        /// Converts between <see cref="Guid"/> and the RFC 4122 big-endian byte layout
        /// required by the arrow.uuid canonical extension type specification.
        /// </summary>
        public static byte[] GuidToBytes(Guid guid)
        {
            byte[] bytes = new byte[GuidType.ByteWidth];
            GuidToRFC4122(guid, bytes);
            return bytes;
        }

        /// <summary>
        /// Converts between <see cref="Guid"/> and the RFC 4122 big-endian byte layout
        /// required by the arrow.uuid canonical extension type specification.
        /// </summary>
        public static unsafe void GuidToRFC4122(Guid guid, Span<byte> bytes)
        {
            if (bytes.Length != GuidType.ByteWidth)
                throw new ArgumentException("Byte span must be exactly 16 bytes long.", nameof(bytes));

            byte* guidPtr = (byte*)&guid;
            fixed (byte* bytePtr = bytes)
            {
                bytePtr[0] = guidPtr[3];
                bytePtr[1] = guidPtr[2];
                bytePtr[2] = guidPtr[1];
                bytePtr[3] = guidPtr[0];
                bytePtr[4] = guidPtr[5];
                bytePtr[5] = guidPtr[4];
                bytePtr[6] = guidPtr[7];
                bytePtr[7] = guidPtr[6];
                ((long*)bytePtr)[1] = ((long*)guidPtr)[1];
            }
        }

        /// <summary>
        /// Converts between <see cref="Guid"/> and the RFC 4122 big-endian byte layout
        /// required by the arrow.uuid canonical extension type specification.
        /// </summary>
        public static unsafe Guid RFC4122ToGuid(ReadOnlySpan<byte> bytes)
        {
            if (bytes.Length != GuidType.ByteWidth)
                throw new ArgumentException("Byte span must be exactly 16 bytes long.", nameof(bytes));

            Guid result = new Guid();
            byte* guidPtr = (byte*)&result;
            fixed (byte* bytePtr = bytes)
            {
                guidPtr[0] = bytePtr[3];
                guidPtr[1] = bytePtr[2];
                guidPtr[2] = bytePtr[1];
                guidPtr[3] = bytePtr[0];
                guidPtr[4] = bytePtr[5];
                guidPtr[5] = bytePtr[4];
                guidPtr[6] = bytePtr[7];
                guidPtr[7] = bytePtr[6];
                ((long*)guidPtr)[1] = ((long*)bytePtr)[1];
                return result;
            }
        }

        public Guid? GetGuid(int index)
        {
            if (index < 0 || index >= Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (IsNull(index))
                return null;

            ReadOnlySpan<byte> bytes = StorageArray.GetBytes(index);
            return RFC4122ToGuid(bytes);
        }

        public int Count => Length;
        public Guid? this[int index] => GetGuid(index);

        public IEnumerator<Guid?> GetEnumerator()
        {
            for (int i = 0; i < Length; i++)
            {
                yield return GetGuid(i);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
