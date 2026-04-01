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
using System.IO;
using System.Runtime.InteropServices;
using Python.Runtime;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Shared fixture for Python.NET initialization. Python.NET can only be initialized
    /// once per process, so this fixture is shared across all test classes via
    /// <see cref="PythonNetCollection"/>.
    /// </summary>
    public class PythonNetFixture : IDisposable
    {
        public bool Initialized { get; }

        public bool VersionMismatch { get; }

        public PythonNetFixture()
        {
            bool pythonSet = Environment.GetEnvironmentVariable("PYTHONNET_PYDLL") != null;
            if (!pythonSet)
            {
                Initialized = false;
                return;
            }

            try
            {
                PythonEngine.Initialize();
            }
            catch (NotSupportedException e) when (e.Message.Contains("Python ABI ") && e.Message.Contains("not supported"))
            {
                // An unsupported version of Python is being used
                Initialized = false;
                VersionMismatch = true;
                return;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                PythonEngine.PythonPath.IndexOf("dlls", StringComparison.OrdinalIgnoreCase) < 0)
            {
                dynamic sys = Py.Import("sys");
                sys.path.append(Path.Combine(Path.GetDirectoryName(Environment.GetEnvironmentVariable("PYTHONNET_PYDLL")), "DLLs"));
            }

            Initialized = true;
        }

        /// <summary>
        /// Ensures tests are skipped or throw appropriately when Python is not available.
        /// Call this from each test class constructor.
        /// </summary>
        public void EnsureInitialized()
        {
            if (!Initialized)
            {
                var errorReason = VersionMismatch ? "Python version is incompatible with PythonNet" : "PYTHONNET_PYDLL not set";

                bool inCIJob = Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true";
                bool inVerificationJob = Environment.GetEnvironmentVariable("TEST_CSHARP") == "1";

                // Skip these tests if this is not in CI or is a verification job and PythonNet couldn't be initialized
                Skip.If(inVerificationJob || !inCIJob, $"{errorReason}; skipping Python interop tests.");

                // Otherwise throw
                throw new Exception($"{errorReason}; cannot run Python interop tests.");
            }
        }

        public void Dispose()
        {
            if (Initialized)
            {
                PythonEngine.Shutdown();
            }
        }
    }
}
