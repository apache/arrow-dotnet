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

using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Serialization;

/// <summary>
/// Custom converter for Arrow serialization of a specific type.
/// Implement this interface to control how a type is stored in Arrow arrays.
/// </summary>
public interface IArrowConverter<T>
{
    /// <summary>The Arrow type used to store values of type T.</summary>
    IArrowType ArrowType { get; }

    /// <summary>Build a single-element Arrow array from a value.</summary>
    IArrowArray ToArray(T value);

    /// <summary>Build a multi-element Arrow array from a list of values.</summary>
    IArrowArray ToArray(IReadOnlyList<T> values);

    /// <summary>Read a value from an Arrow array at the given index.</summary>
    T FromArray(IArrowArray array, int index);
}

/// <summary>
/// Interface implemented by source-generated code on [ArrowSerializable] records.
/// Provides static Arrow schema, serialization, and deserialization.
/// </summary>
public interface IArrowSerializer<T> where T : IArrowSerializer<T>
{
    /// <summary>
    /// The Arrow schema for this type, derived from property types.
    /// </summary>
    static abstract Schema ArrowSchema { get; }

    /// <summary>
    /// Serialize an instance to a single-row RecordBatch.
    /// </summary>
    static abstract RecordBatch ToRecordBatch(T value);

    /// <summary>
    /// Deserialize an instance from a single-row RecordBatch.
    /// </summary>
    static abstract T FromRecordBatch(RecordBatch batch);

    /// <summary>
    /// Serialize multiple instances to a multi-row RecordBatch.
    /// </summary>
    static abstract RecordBatch ToRecordBatch(IReadOnlyList<T> values);

    /// <summary>
    /// Deserialize all rows from a RecordBatch into a list of instances.
    /// </summary>
    static abstract IReadOnlyList<T> ListFromRecordBatch(RecordBatch batch);
}
