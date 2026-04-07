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
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// Defines a factory for a particular extension type. Register instances with
    /// <see cref="ExtensionTypeRegistry"/> to enable automatic resolution during deserialization.
    /// </summary>
    public abstract class ExtensionDefinition
    {
        /// <summary>
        /// The canonical extension type name (e.g. "arrow.uuid").
        /// </summary>
        public abstract string ExtensionName { get; }

        /// <summary>
        /// Attempt to create an <see cref="ExtensionType"/> from a storage type and serialized metadata.
        /// </summary>
        /// <param name="storageType">The underlying Arrow storage type.</param>
        /// <param name="metadata">The serialized extension metadata string, or null.</param>
        /// <param name="type">The created extension type, if successful.</param>
        /// <returns>True if the type was created successfully; false otherwise.</returns>
        public abstract bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type);

        /// <summary>
        /// Adds this extension type to the default registry
        /// </summary>
        public void AddToDefaultRegistry()
        {
            ExtensionTypeRegistry.Default.Register(this);
        }
    }

    /// <summary>
    /// Base class for user-defined extension types. Extension types are logical types
    /// layered on top of a built-in Arrow storage type, identified by a name string.
    /// </summary>
    public abstract class ExtensionType : IArrowType
    {
        public ArrowTypeId TypeId => ArrowTypeId.Extension;

        /// <summary>
        /// The canonical extension type name (e.g. "arrow.uuid").
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// Serialized extension metadata. May be null or empty.
        /// </summary>
        public abstract string ExtensionMetadata { get; }

        /// <summary>
        /// The underlying Arrow storage type.
        /// </summary>
        public IArrowType StorageType { get; }

        public bool IsFixedWidth => StorageType.IsFixedWidth;

        protected ExtensionType(IArrowType storageType)
        {
            StorageType = storageType ?? throw new ArgumentNullException(nameof(storageType));
        }

        public void Accept(IArrowTypeVisitor visitor)
        {
            StorageType.Accept(visitor);
        }

        /// <summary>
        /// Create the appropriate <see cref="ExtensionArray"/> wrapper for a storage array.
        /// </summary>
        public abstract ExtensionArray CreateArray(IArrowArray storageArray);
    }

    /// <summary>
    /// Base class for extension array wrappers. Delegates physical array operations
    /// to the underlying storage array.
    /// </summary>
    public class ExtensionArray : IArrowArray
    {
        public IArrowArray Storage { get; }
        public ExtensionType ExtensionType { get; }

        private ArrayData _data;

        public ExtensionArray(ExtensionType extensionType, IArrowArray storage)
        {
            ExtensionType = extensionType ?? throw new ArgumentNullException(nameof(extensionType));
            Storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public ExtensionArray(IArrowArray storage)
        {
            Storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public int Length => Storage.Length;
        public int Offset => Storage.Offset;
        public int NullCount => Storage.NullCount;

        public ArrayData Data
        {
            get
            {
                if (ExtensionType == null) return Storage.Data;
                if (_data == null)
                {
                    var sd = Storage.Data;
                    _data = new ArrayData(ExtensionType, sd.Length, sd.NullCount, sd.Offset, sd.Buffers, sd.Children, sd.Dictionary);
                }
                return _data;
            }
        }

        public bool IsNull(int index) => Storage.IsNull(index);
        public bool IsValid(int index) => Storage.IsValid(index);

        public void Accept(IArrowArrayVisitor visitor)
        {
            Storage.Accept(visitor);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Storage.Dispose();
            }
        }
    }
}
