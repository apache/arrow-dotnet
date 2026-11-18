#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Builds the arrow-go interop shim and runs the C Async Device Stream interop
# tests against it. Requires Go and a C compiler for cgo.
#
# Usage: test_go_interop.sh <source_dir>

set -euo pipefail

source_dir=$(cd "${1}" && pwd)

# Paths handed to .NET and Go must be native paths on Windows.
native_path() {
  if type cygpath >/dev/null 2>&1; then
    cygpath -m "${1}"
  else
    echo "${1}"
  fi
}

echo "::group::Toolchain"
go version
go env CC CGO_ENABLED
cc_name=$(go env CC)
if type "${cc_name}" >/dev/null 2>&1; then
  "${cc_name}" --version
else
  echo "C compiler '${cc_name}' (needed by cgo) was not found on PATH" >&2
fi
echo "::endgroup::"

echo "::group::Build arrow-go interop shim"
library=$(bash "${source_dir}/ci/scripts/build_go_interop.sh" "${source_dir}")
echo "::endgroup::"

ARROW_GO_INTEROP_LIB=$(native_path "${library}")
ARROW_GO_INTEROP_REQUIRED=1
# The test host swallows stderr, so have Go write fatal errors here instead.
crash_log="$(dirname "${library}")/go-crash.log"
rm -f "${crash_log}"
ARROW_GO_INTEROP_CRASH_LOG=$(native_path "${crash_log}")
export ARROW_GO_INTEROP_LIB ARROW_GO_INTEROP_REQUIRED ARROW_GO_INTEROP_CRASH_LOG

status=0
dotnet test "${source_dir}/test/Apache.Arrow.Tests" \
  --framework net8.0 \
  --filter "FullyQualifiedName~CAsyncDeviceStreamGoInteropTests" ||
  status=$?

if [ -s "${crash_log}" ]; then
  echo "Go runtime crash output:" >&2
  cat "${crash_log}" >&2
fi

exit "${status}"
