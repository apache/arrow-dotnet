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
            _definitions[definition.ExtensionName] = definition;
        }

        /// <summary>
        /// Try to get a registered extension definition by name.
        /// </summary>
        public bool TryGetDefinition(string extensionName, out ExtensionDefinition definition)
        {
            return _definitions.TryGetValue(extensionName, out definition);
        }

        /// <summary>
        /// Create a snapshot copy of this registry.
        /// </summary>
        public ExtensionTypeRegistry Clone()
        {
            return new ExtensionTypeRegistry(_definitions);
        }
    }
}
