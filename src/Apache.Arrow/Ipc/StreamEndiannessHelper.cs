using System;
using System.Buffers.Binary;
using System.IO;

namespace Apache.Arrow.Ipc
{
#if NET5_0_OR_GREATER
    public readonly struct StreamEndiannessHelper
    {
        public void WriteLittleEndian(Stream stream, int value)
        {
            Span<byte> buffer = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
            stream.Write(buffer);
        }

        public void WriteLittleEndian(Stream stream, int value1, int value2)
        {
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(buffer, value1);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(4), value2);
            stream.Write(buffer);
        }
    }
#else
    public readonly struct StreamEndiannessHelper
    {
        private readonly byte[] buffer = new byte[8];

        public StreamEndiannessHelper()
        {
            buffer = new byte[8];
        }

        public void WriteLittleEndian(Stream stream, int value)
        {
            BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
            stream.Write(buffer, 0, 4);
        }

        public void WriteLittleEndian(Stream stream, int value1, int value2)
        {
            BinaryPrimitives.WriteInt32LittleEndian(buffer, value1);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(4), value2);
            stream.Write(buffer, 0, 8);
        }
    }
#endif
}
