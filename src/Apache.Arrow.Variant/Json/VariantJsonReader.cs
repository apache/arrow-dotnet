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
using System.Text;

namespace Apache.Arrow.Variant.Json
{
    /// <summary>
    /// Parses a JSON string or UTF-8 bytes directly into variant binary format
    /// (metadata + value byte arrays) without creating intermediate <see cref="VariantValue"/> objects.
    /// </summary>
    public static class VariantJsonReader
    {
        /// <summary>
        /// Parses a JSON string into variant binary format.
        /// </summary>
        /// <param name="json">The JSON string to parse.</param>
        /// <returns>A tuple of (metadata bytes, value bytes).</returns>
        public static (byte[] Metadata, byte[] Value) Parse(string json)
        {
            byte[] utf8 = Encoding.UTF8.GetBytes(json);
            return Parse(new ReadOnlySpan<byte>(utf8));
        }

        /// <summary>
        /// Parses UTF-8 encoded JSON bytes into variant binary format.
        /// </summary>
        /// <param name="utf8Json">The UTF-8 encoded JSON bytes.</param>
        /// <returns>A tuple of (metadata bytes, value bytes).</returns>
        public static (byte[] Metadata, byte[] Value) Parse(ReadOnlySpan<byte> utf8Json)
        {
            VariantBuilder builder = new VariantBuilder();
            return builder.EncodeFromJson(utf8Json);
        }
    }
}
