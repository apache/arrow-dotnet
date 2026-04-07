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
    /// Extension definition for the "arrow.bool8" extension type,
    /// backed by the Int8 storage type.
    /// </summary>
    public class Bool8ExtensionDefinition : ExtensionDefinition
    {
        public static Bool8ExtensionDefinition Instance = new Bool8ExtensionDefinition();

        public override string ExtensionName => "arrow.bool8";

        private Bool8ExtensionDefinition() { }

        public override bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type)
        {
            if (storageType is Int8Type i8Type)
            {
                type = new Bool8Type(i8Type);
                return true;
            }
            type = null;
            return false;
        }
    }

    /// <summary>
    /// Extension type representing 1-byte boolean values
    /// </summary>
    public class Bool8Type : ExtensionType
    {
        public static Bool8Type Default = new Bool8Type();

        public override string Name => "arrow.bool8";
        public override string ExtensionMetadata => "";

        public Bool8Type() : base(Int8Type.Default) { }

        internal Bool8Type(Int8Type storageType) : base(storageType) { }

        public override ExtensionArray CreateArray(IArrowArray storageArray)
        {
            return new Bool8Array(this, storageArray);
        }
    }

    /// <summary>
    /// Extension array for 1-byte boolean values, backed by an Int8Array.
    /// </summary>
    public class Bool8Array : ExtensionArray, IReadOnlyList<bool?>
    {
        public Int8Array StorageArray => (Int8Array)Storage;

        public Bool8Array(Bool8Type bool8Type, IArrowArray storage) : base(bool8Type, storage) { }

        public Bool8Array(IArrowArray storage) : base(Bool8Type.Default, storage) { }

        public class Builder : PrimitiveArrayBuilder<sbyte, Bool8Array, Builder>
        {
            protected override Bool8Array Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new Bool8Array(new Int8Array(valueBuffer, nullBitmapBuffer, length, nullCount, offset));

            public Builder Append(bool value)
            {
                return Append(value ? (sbyte)1 : (sbyte)0);
            }

            public Builder Append(bool? value)
            {
                if (value == null)
                {
                    return AppendNull();
                }
                return Append(value.Value);
            }

            public Builder AppendRange(IEnumerable<bool> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (bool value in values)
                {
                    Append(value);
                }

                return Instance;
            }

            public Builder AppendRange(IEnumerable<bool?> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (bool? value in values)
                {
                    Append(value);
                }

                return Instance;
            }

            public Builder Set(int index, bool value)
            {
                return Set(index, value ? (sbyte)1 : (sbyte)0);
            }
        }

        public int Count => Length;
        public bool? this[int index] => GetValue(index);

        public bool? GetValue(int index) => IsValid(index) ? StorageArray.GetValue(index).Value != 0 : null;

        public IEnumerator<bool?> GetEnumerator()
        {
            for (int i = 0; i < Length; i++)
            {
                yield return GetValue(i);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
