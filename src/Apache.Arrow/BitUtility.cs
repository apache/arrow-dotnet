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
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Apache.Arrow
{
    public static class BitUtility
    {
        private static ReadOnlySpan<byte> PopcountTable => new byte[] {
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
        };

        private static ReadOnlySpan<byte> BitMask => new byte[] {
            1, 2, 4, 8, 16, 32, 64, 128
        };

        public static bool GetBit(byte data, int index) =>
            ((data >> index) & 1) != 0;

        public static bool GetBit(ReadOnlySpan<byte> data, int index) =>
            (data[index / 8] & BitMask[index % 8]) != 0;

        public static void ClearBit(Span<byte> data, int index)
        {
            data[index / 8] &= (byte)~BitMask[index % 8];
        }

        public static void SetBit(Span<byte> data, int index)
        {
            data[index / 8] |= BitMask[index % 8];
        }

        public static void SetBit(Span<byte> data, int index, bool value)
        {
            int idx = index / 8;
            int mod = index % 8;
            data[idx] = value
                ? (byte)(data[idx] | BitMask[mod])
                : (byte)(data[idx] & ~BitMask[mod]);
        }

        /// <summary>
        /// Set the number of bits in a span of bytes starting
        /// at a specific index, and limiting to length.
        /// </summary>
        /// <param name="data">Span to set bits value.</param>
        /// <param name="index">Bit index to start counting from.</param>
        /// <param name="length">Maximum of bits in the span to consider.</param>
        internal static void SetBits(Span<byte> data, int index, int length, bool value)
        {
            if (length == 0)
                return;

            int endBitIndex = checked(index + length - 1);

            // Use simpler method if there aren't many values
            if (length < 20)
            {
                for (int i = index; i <= endBitIndex; i++)
                {
                    SetBit(data, i, value);
                }
                return;
            }

            // Otherwise do the work to figure out how to copy whole bytes
            int startByteIndex = index / 8;
            int startBitOffset = index % 8;
            int endByteIndex = endBitIndex / 8;
            int endBitOffset = endBitIndex % 8;

            // If the starting index and ending index are not byte-aligned,
            // we'll need to set bits the slow way. If they are
            // byte-aligned, and for all other bytes in the 'middle', we
            // can use a faster byte-aligned set.
            int fullByteStartIndex = startBitOffset == 0 ? startByteIndex : startByteIndex + 1;
            int fullByteEndIndex = endBitOffset == 7 ? endByteIndex : endByteIndex - 1;

            // Bits we will be using to finish up the first byte
            if (startBitOffset != 0)
            {
                Span<byte> slice = data.Slice(startByteIndex, 1);
                for (int i = startBitOffset; i <= 7; i++)
                    SetBit(slice, i, value);
            }

            if (fullByteEndIndex >= fullByteStartIndex)
            {
                Span<byte> slice = data.Slice(fullByteStartIndex, fullByteEndIndex - fullByteStartIndex + 1);
                byte fill = (byte)(value ? 0xFF : 0x00);

                slice.Fill(fill);
            }

            if (endBitOffset != 7)
            {
                Span<byte> slice = data.Slice(endByteIndex, 1);
                for (int i = 0; i <= endBitOffset; i++)
                    SetBit(slice, i, value);
            }
        }

        /// <summary>
        /// Copy a range of bits from one bitmap to another. Bits of the destination
        /// outside of the copied range are left unchanged.
        /// </summary>
        /// <param name="source">Bitmap to copy bits from.</param>
        /// <param name="sourceIndex">Bit index in the source to start copying from.</param>
        /// <param name="destination">Bitmap to copy bits to.</param>
        /// <param name="destinationIndex">Bit index in the destination to start copying to.</param>
        /// <param name="length">Number of bits to copy.</param>
        internal static void CopyBits(ReadOnlySpan<byte> source, int sourceIndex, Span<byte> destination, int destinationIndex, int length)
        {
            // Copy single bits until the destination is byte-aligned
            while (length > 0 && destinationIndex % 8 != 0)
            {
                SetBit(destination, destinationIndex++, GetBit(source, sourceIndex++));
                length--;
            }

            int fullBytes = length / 8;
            if (fullBytes > 0)
            {
                Span<byte> target = destination.Slice(destinationIndex / 8, fullBytes);
                int shift = sourceIndex % 8;
                if (shift == 0)
                {
                    source.Slice(sourceIndex / 8, fullBytes).CopyTo(target);
                }
                else
                {
                    // The bits of each destination byte straddle two source bytes
                    CopyShiftedBytes(source.Slice(sourceIndex / 8, fullBytes + 1), shift, target);
                }

                sourceIndex += fullBytes * 8;
                destinationIndex += fullBytes * 8;
                length -= fullBytes * 8;
            }

            for (int i = 0; i < length; i++)
            {
                SetBit(destination, destinationIndex + i, GetBit(source, sourceIndex + i));
            }
        }

        private static void CopyShiftedBytes(ReadOnlySpan<byte> source, int shift, Span<byte> destination)
        {
            Debug.Assert(shift > 0 && shift < 8);
            Debug.Assert(source.Length == destination.Length + 1);

            int i = 0;
            if (BitConverter.IsLittleEndian)
            {
                // Produce eight destination bytes at a time from nine source bytes
                ref byte sourceRef = ref MemoryMarshal.GetReference(source);
                ref byte destinationRef = ref MemoryMarshal.GetReference(destination);
                for (; i + 8 <= destination.Length; i += 8)
                {
                    ulong low = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref sourceRef, i));
                    ulong high = Unsafe.Add(ref sourceRef, i + 8);
                    Unsafe.WriteUnaligned(ref Unsafe.Add(ref destinationRef, i), (low >> shift) | (high << (64 - shift)));
                }
            }

            for (; i < destination.Length; i++)
            {
                destination[i] = (byte)((source[i] >> shift) | (source[i + 1] << (8 - shift)));
            }
        }

        public static void ToggleBit(Span<byte> data, int index)
        {
            data[index / 8] ^= BitMask[index % 8];
        }

        /// <summary>
        /// Counts the number of set bits in a span of bytes starting
        /// at a specific bit index.
        /// </summary>
        /// <param name="data">Span to count bits.</param>
        /// <param name="index">Bit index to start counting from.</param>
        /// <returns>Count of set (one) bits.</returns>
        public static int CountBits(ReadOnlySpan<byte> data, int index) =>
            CountBits(data, index, data.Length * 8 - index);

        /// <summary>
        /// Counts the number of set bits in a span of bytes starting
        /// at a specific bit index, and limiting to a certain number of bits
        /// in the span.
        /// </summary>
        /// <param name="data">Span to count bits.</param>
        /// <param name="index">Bit index to start counting from.</param>
        /// <param name="length">Maximum of bits in the span to consider.</param>
        /// <returns>Count of set (one) bits.</returns>
        public static int CountBits(ReadOnlySpan<byte> data, int index, int length)
        {
            int startByteIndex = index / 8;
            int startBitOffset = index % 8;

            int endBitIndex = index + length - 1;

            int endByteIndex = endBitIndex / 8;
            int endBitOffset = endBitIndex % 8;

            if (startBitOffset < 0)
                return 0;

            int count = 0;
            if (startByteIndex == endByteIndex)
            {
                // Range starts and ends within the same byte.
                ReadOnlySpan<byte> slice = data.Slice(startByteIndex, 1);
                for (int i = startBitOffset; i <= endBitOffset; i++)
                    count += GetBit(slice, i) ? 1 : 0;

                return count;
            }

            // If the starting index and ending index are not byte-aligned,
            // we'll need to count bits the slow way. If they are
            // byte-aligned, and for all other bytes in the 'middle', we
            // can use a faster byte-aligned count.
            int fullByteStartIndex = startBitOffset == 0 ? startByteIndex : startByteIndex + 1;
            int fullByteEndIndex = endBitOffset == 7 ? endByteIndex : endByteIndex - 1;

            if (startBitOffset != 0)
            {
                ReadOnlySpan<byte> slice = data.Slice(startByteIndex, 1);
                for (int i = startBitOffset; i <= 7; i++)
                    count += GetBit(slice, i) ? 1 : 0;
            }

            if (fullByteEndIndex >= fullByteStartIndex)
            {
                ReadOnlySpan<byte> slice = data.Slice(fullByteStartIndex, fullByteEndIndex - fullByteStartIndex + 1);
                count += CountBits(slice);
            }

            if (endBitOffset != 7)
            {
                ReadOnlySpan<byte> slice = data.Slice(endByteIndex, 1);
                for (int i = 0; i <= endBitOffset; i++)
                    count += GetBit(slice, i) ? 1 : 0;
            }

            return count;
        }

        /// <summary>
        /// Counts the number of set bits in a span of bytes.
        /// </summary>
        /// <param name="data">Span to count bits.</param>
        /// <returns>Count of set (one) bits.</returns>
        public static int CountBits(ReadOnlySpan<byte> data)
        {
            int count = 0;
            foreach (byte t in data)
                count += PopcountTable[t];
            return count;
        }

        /// <summary>
        /// Rounds an integer to the nearest multiple of 64.
        /// </summary>
        /// <param name="n">Integer to round.</param>
        /// <returns>Integer rounded to the nearest multiple of 64.</returns>
        public static long RoundUpToMultipleOf64(long n) =>
            RoundUpToMultiplePowerOfTwo(n, 64);

        /// <summary>
        /// Rounds an integer to the nearest multiple of 8.
        /// </summary>
        /// <param name="n">Integer to round.</param>
        /// <returns>Integer rounded to the nearest multiple of 8.</returns>
        public static long RoundUpToMultipleOf8(long n) =>
            RoundUpToMultiplePowerOfTwo(n, 8);

        /// <summary>
        /// Rounds an integer up to the nearest multiple of factor, where
        /// factor must be a power of two.
        ///
        /// This function does not throw when the factor is not a power of two.
        /// </summary>
        /// <param name="n">Integer to round up.</param>
        /// <param name="factor">Power of two factor to round up to.</param>
        /// <returns>Integer rounded up to the nearest power of two.</returns>
        public static long RoundUpToMultiplePowerOfTwo(long n, int factor)
        {
            // Assert that factor is a power of two.
            Debug.Assert(factor > 0 && (factor & (factor - 1)) == 0);
            return (n + (factor - 1)) & ~(factor - 1);
        }

        internal static bool IsMultipleOf8(long n) => n % 8 == 0;

        /// <summary>
        /// Calculates the number of bytes required to store n bits.
        /// </summary>
        /// <param name="n">Number of bits</param>
        /// <returns>Number of bytes</returns>
        public static int ByteCount(int n)
        {
            Debug.Assert(n >= 0);
            return n / 8 + (n % 8 != 0 ? 1 : 0); // ceil(n / 8)
        }

        internal static int ReadInt32(ReadOnlyMemory<byte> value)
        {
            Debug.Assert(value.Length >= sizeof(int));

            return Unsafe.ReadUnaligned<int>(ref MemoryMarshal.GetReference(value.Span));
        }
    }
}
