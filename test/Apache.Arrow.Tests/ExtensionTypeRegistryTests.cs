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
using System.Threading.Tasks;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Tests that <see cref="ExtensionTypeRegistry.Clone"/> produces a snapshot: it shares storage
    /// until one side is modified, but neither side ever observes the other's later changes.
    /// </summary>
    public class ExtensionTypeRegistryTests
    {
        [Fact]
        public void CloneIsUnaffectedByLaterChangesToOriginal()
        {
            TestDefinition kept = new TestDefinition("kept");
            TestDefinition removed = new TestDefinition("removed");
            TestDefinition added = new TestDefinition("added");
            ExtensionTypeRegistry original = new ExtensionTypeRegistry();
            original.Register(kept);
            original.Register(removed);

            ExtensionTypeRegistry clone = original.Clone();
            original.Unregister(removed);
            original.Register(added);
            original.Register(new TestDefinition("kept"));

            Assert.Same(kept, Get(clone, "kept"));
            Assert.Same(removed, Get(clone, "removed"));
            Assert.Null(Get(clone, "added"));
        }

        [Fact]
        public void OriginalIsUnaffectedByChangesToClone()
        {
            TestDefinition definition = new TestDefinition("a");
            ExtensionTypeRegistry original = new ExtensionTypeRegistry();
            original.Register(definition);

            ExtensionTypeRegistry clone = original.Clone();
            clone.Unregister(definition);
            clone.Register(new TestDefinition("b"));

            Assert.Same(definition, Get(original, "a"));
            Assert.Null(Get(original, "b"));
        }

        [Fact]
        public void ClonesOfClonesAreIndependent()
        {
            ExtensionTypeRegistry original = new ExtensionTypeRegistry();
            original.Register(new TestDefinition("a"));
            ExtensionTypeRegistry first = original.Clone();
            ExtensionTypeRegistry second = first.Clone();

            first.Register(new TestDefinition("first"));
            second.Register(new TestDefinition("second"));
            original.Register(new TestDefinition("original"));

            Assert.NotNull(Get(first, "first"));
            Assert.Null(Get(first, "second"));
            Assert.Null(Get(first, "original"));
            Assert.NotNull(Get(second, "second"));
            Assert.Null(Get(second, "first"));
            Assert.Null(Get(original, "first"));
            Assert.Null(Get(original, "second"));
            Assert.NotNull(Get(second, "a"));
        }

        [Fact]
        public void TemporaryRegistrationAcrossCloneRestoresOnlyItsOwnRegistry()
        {
            TestDefinition permanent = new TestDefinition("x");
            TestDefinition temporary = new TestDefinition("x");
            ExtensionTypeRegistry original = new ExtensionTypeRegistry();
            original.Register(permanent);

            IDisposable scope = original.RegisterTemporary(temporary);
            ExtensionTypeRegistry clone = original.Clone();
            scope.Dispose();

            Assert.Same(permanent, Get(original, "x"));
            Assert.Same(temporary, Get(clone, "x"));
        }

        [Fact]
        public void ContextSnapshotsDefaultRegistryAtConstruction()
        {
            string name = "snapshot-" + Guid.NewGuid();
            TestDefinition before = new TestDefinition(name + "-before");
            TestDefinition after = new TestDefinition(name + "-after");
            ExtensionTypeRegistry.Default.Register(before);
            try
            {
                ArrowContext context = new ArrowContext();

                ExtensionTypeRegistry.Default.Unregister(before);
                ExtensionTypeRegistry.Default.Register(after);

                // The context sees Default as it was when the context was created.
                Assert.Same(before, Get(context.ExtensionRegistry, before.ExtensionName));
                Assert.Null(Get(context.ExtensionRegistry, after.ExtensionName));
            }
            finally
            {
                ExtensionTypeRegistry.Default.Unregister(before);
                ExtensionTypeRegistry.Default.Unregister(after);
            }
        }

        [Fact]
        public void ConcurrentClonesAndRegistrationsAreSafe()
        {
            ExtensionTypeRegistry original = new ExtensionTypeRegistry();

            Parallel.For(0, 2000, i =>
            {
                if (i % 2 == 0)
                {
                    original.Register(new TestDefinition("d" + i));
                }
                else
                {
                    ExtensionTypeRegistry clone = original.Clone();
                    clone.Register(new TestDefinition("c" + i));
                    Assert.NotNull(Get(clone, "c" + i));
                }
            });

            for (int i = 0; i < 2000; i += 2)
            {
                Assert.NotNull(Get(original, "d" + i));
            }
            for (int i = 1; i < 2000; i += 2)
            {
                Assert.Null(Get(original, "c" + i));
            }
        }

        private static ExtensionDefinition Get(ExtensionTypeRegistry registry, string name) =>
            registry.TryGetDefinition(name, out ExtensionDefinition definition) ? definition : null;

        private sealed class TestDefinition : ExtensionDefinition
        {
            public TestDefinition(string name) => ExtensionName = name;

            public override string ExtensionName { get; }

            public override bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type)
            {
                type = null;
                return false;
            }
        }
    }
}
