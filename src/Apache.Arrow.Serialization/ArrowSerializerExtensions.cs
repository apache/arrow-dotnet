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
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Serialization;

/// <summary>
/// Extension methods for Arrow serialization/deserialization.
/// </summary>
public static class ArrowSerializerExtensions
{
    /// <summary>
    /// Serialize an instance to Arrow IPC stream bytes.
    /// </summary>
    public static byte[] SerializeToBytes<T>(this T value) where T : IArrowSerializer<T>
    {
        var batch = T.ToRecordBatch(value);
        return RecordBatchToBytes(batch);
    }

    /// <summary>
    /// Deserialize an instance from Arrow IPC stream bytes.
    /// </summary>
    public static T DeserializeFromBytes<T>(byte[] data) where T : IArrowSerializer<T>
    {
        var batch = BytesToRecordBatch(data);
        return T.FromRecordBatch(batch);
    }

    /// <summary>
    /// Serialize an instance to an Arrow IPC stream.
    /// </summary>
    public static void SerializeToStream<T>(this T value, Stream destination) where T : IArrowSerializer<T>
    {
        var batch = T.ToRecordBatch(value);
        WriteRecordBatch(batch, destination);
    }

    /// <summary>
    /// Deserialize an instance from an Arrow IPC stream.
    /// </summary>
    public static T DeserializeFromStream<T>(Stream source) where T : IArrowSerializer<T>
    {
        var batch = ReadRecordBatch(source);
        return T.FromRecordBatch(batch);
    }

    /// <summary>
    /// Serialize multiple instances to a multi-row RecordBatch.
    /// </summary>
    public static RecordBatch ToRecordBatch<T>(this IEnumerable<T> items) where T : IArrowSerializer<T>
    {
        var list = items as IReadOnlyList<T> ?? items.ToList();
        return T.ToRecordBatch(list);
    }

    /// <summary>
    /// Deserialize all rows from a RecordBatch into a list of instances.
    /// </summary>
    public static IReadOnlyList<T> ToList<T>(this RecordBatch batch) where T : IArrowSerializer<T>
    {
        return T.ListFromRecordBatch(batch);
    }

    /// <summary>
    /// Serialize multiple instances to Arrow IPC stream bytes.
    /// </summary>
    public static byte[] SerializeListToBytes<T>(this IEnumerable<T> items) where T : IArrowSerializer<T>
    {
        var batch = items.ToRecordBatch();
        return RecordBatchToBytes(batch);
    }

    /// <summary>
    /// Deserialize multiple instances from Arrow IPC stream bytes.
    /// </summary>
    public static IReadOnlyList<T> DeserializeListFromBytes<T>(byte[] data) where T : IArrowSerializer<T>
    {
        var batch = BytesToRecordBatch(data);
        return T.ListFromRecordBatch(batch);
    }

    /// <summary>
    /// Serialize a RecordBatch to Arrow IPC stream bytes.
    /// </summary>
    public static byte[] RecordBatchToBytes(RecordBatch batch)
    {
        using var ms = new MemoryStream();
        WriteRecordBatch(batch, ms);
        return ms.ToArray();
    }

    /// <summary>
    /// Write a RecordBatch as an Arrow IPC stream (schema + batch + EOS).
    /// </summary>
    public static void WriteRecordBatch(RecordBatch batch, Stream destination)
    {
        var writer = new ArrowStreamWriter(destination, batch.Schema, leaveOpen: true);
        writer.WriteRecordBatch(batch);
        writer.WriteEnd();
        writer.Dispose();
    }

    /// <summary>
    /// Deserialize a RecordBatch from Arrow IPC stream bytes.
    /// </summary>
    public static RecordBatch BytesToRecordBatch(byte[] data)
    {
        using var ms = new MemoryStream(data);
        return ReadRecordBatch(ms);
    }

    /// <summary>
    /// Read a single RecordBatch from an Arrow IPC stream.
    /// </summary>
    public static RecordBatch ReadRecordBatch(Stream source)
    {
        using var reader = new ArrowStreamReader(source, leaveOpen: true);
        var batch = reader.ReadNextRecordBatch()
            ?? throw new InvalidOperationException("No RecordBatch found in Arrow IPC stream.");
        return batch;
    }
}
