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

#nullable enable

using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class StringViewArray(ArrayData data) : BinaryViewArray(ArrowTypeId.StringView, data), IReadOnlyList<string?>
    {
        public static Encoding DefaultEncoding { get; } = new UTF8Encoding(false);

        public new class Builder() : BuilderBase<StringViewArray, Builder>(StringViewType.Default)
        {
            protected override StringViewArray Build(ArrayData data)
            {
                return new StringViewArray(data);
            }

            public Builder Append(string? value, Encoding? encoding = null)
            {
                if (value is null)
                {
                    return AppendNull();
                }

                encoding ??= DefaultEncoding;
                int maxByteCount = encoding.GetMaxByteCount(value.Length);
                #if NETCOREAPP
                byte[]? buffer = null;

                Span<byte> span = maxByteCount <= 1024
                    ? stackalloc byte[maxByteCount]
                    : buffer = ArrayPool<byte>.Shared.Rent(maxByteCount);

                try
                {
                    int encodeBytes = encoding.GetBytes(value, span);
                    return Append(span.Slice(0, encodeBytes));
                }
                finally
                {
                    if (buffer is not null)
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }
                }
                #else
                byte[] buffer = ArrayPool<byte>.Shared.Rent(maxByteCount);

                try
                {
                    int encodeBytes = encoding.GetBytes(value, 0, value.Length, buffer, 0);
                    return Append(buffer.AsSpan(0, encodeBytes));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
                #endif
            }

            public Builder AppendRange(IEnumerable<string?> values, Encoding? encoding = null)
            {
                foreach (string? value in values)
                {
                    Append(value, encoding);
                }

                return this;
            }
        }

        public StringViewArray
        (
            int length,
            ArrowBuffer valueOffsetsBuffer,
            ArrowBuffer dataBuffer,
            ArrowBuffer nullBitmapBuffer,
            int nullCount = 0,
            int offset = 0
        ) : this
        (
            new ArrayData
            (
                StringViewType.Default,
                length,
                nullCount,
                offset,
                [nullBitmapBuffer, valueOffsetsBuffer, dataBuffer]
            )
        )
        {
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public string? GetString(int index, Encoding? encoding = null)
        {
            encoding ??= DefaultEncoding;
            ReadOnlySpan<byte> bytes = GetBytes(index, out bool isNull);

            if (isNull)
            {
                return null;
            }

            if (bytes.Length is 0)
            {
                return string.Empty;
            }

            unsafe
            {
                fixed (byte* data = &MemoryMarshal.GetReference(bytes))
                    return encoding.GetString(data, bytes.Length);
            }
        }

        int IReadOnlyCollection<string?>.Count => Length;

        string? IReadOnlyList<string?>.this[int index] => GetString(index);

        IEnumerator<string?> IEnumerable<string?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetString(index);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<string>)this).GetEnumerator();
    }
}
