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
/// Marker interface implemented by source-generated code on [ArrowSerializable] types.
/// Constrains the generic helpers in <see cref="ArrowSerializerExtensions"/> so they can
/// only be called on types the source generator has produced a serializer for.
/// </summary>
public interface IArrowSerializable
{
}

/// <summary>
/// Serializer for a specific type, implemented by source-generated code and resolved
/// through <see cref="ArrowSerializerRegistry"/>. The generated implementation delegates
/// to the static members emitted on the [ArrowSerializable] type itself
/// (<c>T.ArrowSchema</c>, <c>T.ToRecordBatch(...)</c>, ...), which remain directly usable.
/// </summary>
public interface IArrowSerializer<T>
{
    /// <summary>
    /// The Arrow schema for this type, derived from property types.
    /// </summary>
    Schema ArrowSchema { get; }

    /// <summary>
    /// Serialize an instance to a single-row RecordBatch.
    /// </summary>
    RecordBatch ToRecordBatch(T value);

    /// <summary>
    /// Deserialize an instance from a single-row RecordBatch.
    /// </summary>
    T FromRecordBatch(RecordBatch batch);

    /// <summary>
    /// Serialize multiple instances to a multi-row RecordBatch.
    /// </summary>
    RecordBatch ToRecordBatch(IReadOnlyList<T> values);

    /// <summary>
    /// Deserialize all rows from a RecordBatch into a list of instances.
    /// </summary>
    IReadOnlyList<T> ListFromRecordBatch(RecordBatch batch);
}

/// <summary>
/// Registry mapping types to their source-generated <see cref="IArrowSerializer{T}"/>
/// implementations. Generated module initializers register a serializer per
/// [ArrowSerializable] type before any user code in that assembly runs, so lookups
/// never race with registration. Resolution is a single static-generic field read —
/// no dictionary lookup and no reflection.
/// </summary>
public static class ArrowSerializerRegistry
{
    private static class Cache<T>
    {
        public static IArrowSerializer<T>? Instance;
    }

    /// <summary>
    /// Register the serializer for <typeparamref name="T"/>. Called by generated code;
    /// may also be called manually to override or provide a hand-written serializer.
    /// </summary>
    public static void Register<T>(IArrowSerializer<T> serializer)
    {
        Cache<T>.Instance = serializer;
    }

    /// <summary>
    /// Resolve the serializer for <typeparamref name="T"/>.
    /// </summary>
    /// <exception cref="NotSupportedException">No serializer is registered for T.</exception>
    public static IArrowSerializer<T> Get<T>()
    {
        return Cache<T>.Instance
            ?? throw new NotSupportedException(
                $"No Arrow serializer registered for {typeof(T)}. Is the type marked [ArrowSerializable] and declared partial?");
    }
}
