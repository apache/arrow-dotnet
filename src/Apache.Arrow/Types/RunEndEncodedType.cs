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

namespace Apache.Arrow.Types
{
    /// <summary>
    /// Represents a run-end encoded array type.
    /// Contains two child arrays: run_ends and values.
    /// The run_ends child array must be a 16/32/64-bit signed integer array
    /// which encodes the indices at which the run with the value in 
    /// each corresponding index in the values child array ends.
    /// </summary>
    public sealed class RunEndEncodedType : NestedType
    {
        public override ArrowTypeId TypeId => ArrowTypeId.RunEndEncoded;
        public override string Name => "run_end_encoded";

        /// <summary>
        /// Gets the run ends field (must be Int16, Int32, or Int64).
        /// </summary>
        public Field RunEndsField => Fields[0];

        /// <summary>
        /// Gets the values field (can be any type).
        /// </summary>
        public Field ValuesField => Fields[1];

        /// <summary>
        /// Gets the data type of the run ends array.
        /// </summary>
        public IArrowType RunEndsDataType => RunEndsField.DataType;

        /// <summary>
        /// Gets the data type of the values array.
        /// </summary>
        public IArrowType ValuesDataType => ValuesField.DataType;

        /// <summary>
        /// Creates a new RunEndEncodedType with the specified run ends and values fields.
        /// </summary>
        /// <param name="runEndsField">The run ends field (must be Int16, Int32, or Int64).</param>
        /// <param name="valuesField">The values field (can be any type).</param>
        public RunEndEncodedType(Field runEndsField, Field valuesField)
            : base([runEndsField, valuesField])
        {
            ValidateRunEndsType(runEndsField.DataType);
        }

        /// <summary>
        /// Creates a new RunEndEncodedType with the specified run ends and values data types.
        /// Uses default field names "run_ends" and "values".
        /// </summary>
        /// <param name="runEndsDataType">The run ends data type (must be Int16, Int32, or Int64).</param>
        /// <param name="valuesDataType">The values data type (can be any type).</param>
        public RunEndEncodedType(IArrowType runEndsDataType, IArrowType valuesDataType)
            : this(new Field("run_ends", runEndsDataType, nullable: false),
                   new Field("values", valuesDataType, nullable: true))
        {
        }

        private static void ValidateRunEndsType(IArrowType runEndsDataType)
        {
            if (runEndsDataType.TypeId != ArrowTypeId.Int16 &&
                runEndsDataType.TypeId != ArrowTypeId.Int32 &&
                runEndsDataType.TypeId != ArrowTypeId.Int64)
            {
                throw new ArgumentException(
                    $"Run ends type must be Int16, Int32, or Int64, but got {runEndsDataType.TypeId}",
                    nameof(runEndsDataType));
            }
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);
    }
}
