/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Runtime.InteropServices;

namespace Apache.Arrow.C
{
    internal readonly struct NativeDelegate<T> where T : Delegate
    {
        private readonly T _managedDelegate; // For lifetime management

        public NativeDelegate(T managedDelegate)
        {
            _managedDelegate = managedDelegate;
            Pointer = Marshal.GetFunctionPointerForDelegate(managedDelegate);
        }

        public IntPtr Pointer { get; }
    }

#if !NET5_0_OR_GREATER
    internal static class NativeDelegate
    {
        /// <summary>
        /// Gets a delegate of type <typeparamref name="T"/> for calling a C Data Interface callback.
        /// </summary>
        /// <remarks>
        /// If the function pointer was created in this process from a managed delegate,
        /// <see cref="Marshal.GetDelegateForFunctionPointer{TDelegate}(IntPtr)"/> returns that original
        /// delegate cast to <typeparamref name="T"/>, which throws if another library exported the callback
        /// using its own delegate type with the same signature. In that case, wrap the original delegate.
        /// </remarks>
        public static T GetDelegateForFunctionPointer<T>(IntPtr ptr) where T : Delegate
        {
            Delegate d = Marshal.GetDelegateForFunctionPointer(ptr, typeof(T));
            return d as T ?? (T)Delegate.CreateDelegate(typeof(T), d, "Invoke");
        }
    }
#endif
}
