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

namespace Apache.Arrow.Variant
{
    /// <summary>
    /// Primitive type IDs stored in bits 2-7 of the value header byte
    /// when the basic type is <see cref="VariantBasicType.Primitive"/>.
    /// </summary>
    public enum VariantPrimitiveType : byte
    {
        NullType = 0,
        BooleanTrue = 1,
        BooleanFalse = 2,
        Int8 = 3,
        Int16 = 4,
        Int32 = 5,
        Int64 = 6,
        Double = 7,
        Decimal4 = 8,
        Decimal8 = 9,
        Decimal16 = 10,
        Date = 11,
        Timestamp = 12,
        TimestampNtz = 13,
        Float = 14,
        Binary = 15,
        String = 16,
        TimeNtz = 17,
        TimestampTzNanos = 18,
        TimestampNtzNanos = 19,
        Uuid = 20,
    }
}
