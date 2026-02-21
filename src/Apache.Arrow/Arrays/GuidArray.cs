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
        public override string ExtensionName => "arrow.uuid";

        public override bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type)
        {
            if (storageType is FixedSizeBinaryType fsbType && fsbType.ByteWidth == 16)
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
        public override string Name => "arrow.uuid";
        public override string ExtensionMetadata => "";

        public GuidType() : base(new FixedSizeBinaryType(16)) { }

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

        public GuidArray(IArrowArray storage) : base(new GuidType(), storage) { }

        /// <summary>
        /// Converts a <see cref="Guid"/> to the RFC 4122 big-endian byte layout
        /// required by the arrow.uuid canonical extension type specification.
        /// </summary>
        public static byte[] GuidToBytes(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();
            // .NET Guid.ToByteArray() uses mixed-endian: the first three fields
            // (Data1, Data2, Data3) are little-endian, while the last 8 bytes are
            // individual bytes. RFC 4122 stores all fields in big-endian order.
            // Reverse the byte order of the first three fields.
            byte t;
            t = bytes[0]; bytes[0] = bytes[3]; bytes[3] = t;
            t = bytes[1]; bytes[1] = bytes[2]; bytes[2] = t;
            t = bytes[4]; bytes[4] = bytes[5]; bytes[5] = t;
            t = bytes[6]; bytes[6] = bytes[7]; bytes[7] = t;
            return bytes;
        }

        public Guid? GetGuid(int index)
        {
            if (index < 0 || index >= Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (IsNull(index))
                return null;

            ReadOnlySpan<byte> bytes = StorageArray.GetBytes(index);
            return GuidFromRfc4122(bytes);
        }

        private static Guid GuidFromRfc4122(ReadOnlySpan<byte> rfc4122)
        {
            // Reverse the RFC 4122 big-endian layout back to the mixed-endian
            // layout expected by the .NET Guid constructor.
            byte[] native = new byte[16];
            native[0] = rfc4122[3];
            native[1] = rfc4122[2];
            native[2] = rfc4122[1];
            native[3] = rfc4122[0];
            native[4] = rfc4122[5];
            native[5] = rfc4122[4];
            native[6] = rfc4122[7];
            native[7] = rfc4122[6];
            for (int i = 8; i < 16; i++)
                native[i] = rfc4122[i];
            return new Guid(native);
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
