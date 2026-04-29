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
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Internal helpers shared by the shredded-variant reader trio.
    /// </summary>
    internal static class ShreddingHelpers
    {
        /// <summary>
        /// Builds a <see cref="ShreddedVariant"/> slot for the given index of an element-group
        /// struct (one with <c>value</c> and/or <c>typed_value</c> sub-fields). Either sub-field
        /// may be absent from the struct.
        /// </summary>
        public static ShreddedVariant BuildSlot(
            ShredSchema slotSchema,
            ReadOnlySpan<byte> metadata,
            StructArray elementGroup,
            int index)
        {
            StructType elementGroupType = (StructType)elementGroup.Data.DataType;
            int valueIdx = elementGroupType.GetFieldIndex("value");
            int typedIdx = elementGroupType.GetFieldIndex("typed_value");

            IArrowArray valueArr = valueIdx >= 0 ? elementGroup.Fields[valueIdx] : null;
            IArrowArray typedArr = typedIdx >= 0 ? elementGroup.Fields[typedIdx] : null;

            return new ShreddedVariant(slotSchema, metadata, valueArr, typedArr, index);
        }
    }
}
