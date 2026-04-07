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
using System.Collections.Generic;

namespace Apache.Arrow
{
    /// <summary>
    /// A registry mapping extension type names to their <see cref="ExtensionDefinition"/> factories.
    /// The <see cref="Default"/> registry starts empty; users must register extension definitions
    /// to enable automatic resolution during deserialization.
    /// </summary>
    public class ExtensionTypeRegistry
    {
        private static readonly ExtensionTypeRegistry s_default = new ExtensionTypeRegistry();

        /// <summary>
        /// The process-wide default registry. Starts empty.
        /// </summary>
        public static ExtensionTypeRegistry Default => s_default;

        private readonly Dictionary<string, ExtensionDefinition> _definitions;

        public ExtensionTypeRegistry()
        {
            _definitions = new Dictionary<string, ExtensionDefinition>();
        }

        private ExtensionTypeRegistry(Dictionary<string, ExtensionDefinition> definitions)
        {
            _definitions = new Dictionary<string, ExtensionDefinition>(definitions);
        }

        /// <summary>
        /// Register an extension definition. Overwrites any existing definition with the same name.
        /// </summary>
        public void Register(ExtensionDefinition definition)
        {
            if (definition == null) throw new ArgumentNullException(nameof(definition));
            lock (_definitions)
            {
                _definitions[definition.ExtensionName] = definition;
            }
        }

        /// <summary>
        /// Temporarily register an extension definition. Overwrites any existing definition with the same name.
        /// Restores the original definition when the returned <see cref="IDisposable"/> is disposed.
        /// </summary>
        public IDisposable RegisterTemporary(ExtensionDefinition definition)
        {
            if (definition == null) throw new ArgumentNullException(nameof(definition));
            lock (_definitions)
            {
                if (!_definitions.TryGetValue(definition.ExtensionName, out ExtensionDefinition previousDefinition))
                {
                    previousDefinition = null;
                }
                IDisposable scope = new Registration(this, definition.ExtensionName, previousDefinition);
                _definitions[definition.ExtensionName] = definition;
                return scope;
            }
        }

        /// <summary>
        /// Unregisters an extension definition
        /// </summary>
        public void Unregister(ExtensionDefinition definition)
        {
            if (definition == null) throw new ArgumentNullException(nameof(definition));
            lock (_definitions)
            {
                _definitions.Remove(definition.ExtensionName);
            }
        }

        /// <summary>
        /// Try to get a registered extension definition by name.
        /// </summary>
        public bool TryGetDefinition(string extensionName, out ExtensionDefinition definition)
        {
            lock (_definitions)
            {
                return _definitions.TryGetValue(extensionName, out definition);
            }
        }

        /// <summary>
        /// Create a snapshot copy of this registry.
        /// </summary>
        public ExtensionTypeRegistry Clone()
        {
            lock (_definitions)
            {
                return new ExtensionTypeRegistry(_definitions);
            }
        }

        sealed class Registration : IDisposable
        {
            private readonly ExtensionTypeRegistry _registry;
            private readonly string _extensionName;
            private readonly ExtensionDefinition _previousDefinition;

            public Registration(
                ExtensionTypeRegistry registry,
                string extensionName,
                ExtensionDefinition previousDefinition)
            {
                _previousDefinition = previousDefinition;
                _extensionName = extensionName;
                _registry = registry;
            }

            public void Dispose()
            {
                lock (_registry._definitions)
                {
                    if (_previousDefinition == null)
                    {
                        _registry._definitions.Remove(_extensionName);
                    }
                    else
                    {
                        _registry._definitions[_previousDefinition.ExtensionName] = _previousDefinition;
                    }
                }
            }
        }
    }
}
