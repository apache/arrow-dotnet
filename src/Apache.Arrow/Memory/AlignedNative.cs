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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Memory
{
    /// <summary>
    /// Provides aligned memory allocation on downlevel platforms (.NET Framework / netstandard2.0)
    /// by P/Invoking <c>_aligned_malloc</c> / <c>_aligned_free</c> / <c>_aligned_realloc</c> from
    /// the Universal C Runtime (ucrtbase.dll). Falls back to <see cref="Marshal.AllocHGlobal"/>
    /// with manual alignment if the CRT functions are unavailable.
    /// </summary>
    internal static unsafe class AlignedNative
    {
        private static readonly bool s_hasCrt = ProbeCrt();

        public static void* AlignedAlloc(int size, int alignment, out int offset)
        {
            void* ptr;
            if (s_hasCrt)
            {
                ptr = UcrtInterop.AlignedMalloc((IntPtr)size, (IntPtr)alignment);
                offset = 0;
            }
            else
            {
                int length = size + alignment;
                IntPtr address = Marshal.AllocHGlobal(length);
                offset = (int)(alignment - (address.ToInt64() & (alignment - 1)));
                ptr = (void*)address;
            }

            if (ptr == null)
            {
                throw new OutOfMemoryException($"_aligned_malloc({size}, {alignment}) returned null.");
            }

            return ptr;
        }

        public static void AlignedFree(void* ptr)
        {
            if (s_hasCrt)
                UcrtInterop.AlignedFree(ptr);
            else
                Marshal.FreeHGlobal((IntPtr)ptr);
        }

        public static void* AlignedRealloc(void* ptr, int newSize, int alignment, int oldSize, ref int offset)
        {
            void* newPtr;
            if (s_hasCrt)
            {
                newPtr = UcrtInterop.AlignedRealloc(ptr, (IntPtr)newSize, (IntPtr)alignment);
                if (newPtr == null)
                    throw new OutOfMemoryException($"_aligned_realloc({newSize}, {alignment}) returned null.");
            }
            else
            {
                int length = newSize + alignment;
                IntPtr address = Marshal.AllocHGlobal(length);
                if (address == IntPtr.Zero)
                    throw new OutOfMemoryException($"_aligned_realloc({newSize}, {alignment}) returned null.");
                int newOffset = (int)(alignment - (address.ToInt64() & (alignment - 1)));
                Buffer.MemoryCopy(
                    (void*)((byte*)ptr + offset),
                    (void*)(address + newOffset),
                    newSize,
                    Math.Min(oldSize, newSize));
                offset = newOffset;
                newPtr = (void*)address;
                Marshal.FreeHGlobal((IntPtr)ptr);
            }
            return newPtr;
        }

        private static bool ProbeCrt()
        {
            try
            {
                void* ptr = UcrtInterop.AlignedMalloc((IntPtr)64, (IntPtr)64);
                if (ptr == null) return false;
                UcrtInterop.AlignedFree(ptr);
                return true;
            }
            catch (DllNotFoundException) { return false; }
            catch (EntryPointNotFoundException) { return false; }
        }

        internal static unsafe class UcrtInterop
        {
            private const string Lib = "ucrtbase.dll";

            [DllImport(Lib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "_aligned_malloc")]
            public static extern void* AlignedMalloc(IntPtr size, IntPtr alignment);

            [DllImport(Lib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "_aligned_free")]
            public static extern void AlignedFree(void* ptr);

            [DllImport(Lib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "_aligned_realloc")]
            public static extern void* AlignedRealloc(void* ptr, IntPtr size, IntPtr alignment);
        }
    }
}
