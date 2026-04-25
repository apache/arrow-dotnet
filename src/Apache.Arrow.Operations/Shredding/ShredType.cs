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

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Describes the type that a shredded typed_value column expects.
    /// Maps variant primitive types to the logical Parquet types used for shredding.
    /// </summary>
    public enum ShredType : byte
    {
        /// <summary>No typed_value column — all values go to the binary value column.</summary>
        None = 0,

        /// <summary>Boolean (Parquet BOOLEAN).</summary>
        Boolean,

        /// <summary>8-bit signed integer (Parquet INT32 with INT_8 annotation).</summary>
        Int8,

        /// <summary>16-bit signed integer (Parquet INT32 with INT_16 annotation).</summary>
        Int16,

        /// <summary>32-bit signed integer (Parquet INT32).</summary>
        Int32,

        /// <summary>64-bit signed integer (Parquet INT64).</summary>
        Int64,

        /// <summary>32-bit float (Parquet FLOAT).</summary>
        Float,

        /// <summary>64-bit double (Parquet DOUBLE).</summary>
        Double,

        /// <summary>Decimal with 4-byte unscaled value.</summary>
        Decimal4,

        /// <summary>Decimal with 8-byte unscaled value.</summary>
        Decimal8,

        /// <summary>Decimal with 16-byte unscaled value.</summary>
        Decimal16,

        /// <summary>Date as days since epoch (Parquet DATE).</summary>
        Date,

        /// <summary>Timestamp with UTC microseconds (Parquet TIMESTAMP with isAdjustedToUTC=true, MICROS).</summary>
        Timestamp,

        /// <summary>Timestamp without timezone, microseconds (Parquet TIMESTAMP with isAdjustedToUTC=false, MICROS).</summary>
        TimestampNtz,

        /// <summary>Time without timezone, microseconds (Parquet TIME with MICROS).</summary>
        TimeNtz,

        /// <summary>Timestamp with UTC nanoseconds.</summary>
        TimestampTzNanos,

        /// <summary>Timestamp without timezone, nanoseconds.</summary>
        TimestampNtzNanos,

        /// <summary>UTF-8 string (Parquet BINARY with STRING logical type).</summary>
        String,

        /// <summary>Binary data (Parquet BINARY).</summary>
        Binary,

        /// <summary>UUID (Parquet FIXED_LEN_BYTE_ARRAY(16) with UUID logical type).</summary>
        Uuid,

        /// <summary>Shredded as an object group with named sub-fields.</summary>
        Object,

        /// <summary>Shredded as an array (Parquet LIST).</summary>
        Array,
    }
}
