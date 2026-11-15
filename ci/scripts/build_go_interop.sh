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

# Builds the arrow-go interop shim (test/arrow-go-interop) as a C shared
# library and prints its path. Requires Go and a C compiler for cgo (on
# Windows, MinGW-w64 gcc).
#
# Usage: build_go_interop.sh <source_dir> [output_dir]
#
# Point the tests at the result with:
#   export ARROW_GO_INTEROP_LIB=$(ci/scripts/build_go_interop.sh .)

set -euo pipefail

source_dir=$(cd "${1}" && pwd)
output_dir=${2:-${source_dir}/artifacts/arrow-go-interop}

case "$(uname -s)" in
Linux*) library_name=libarrow_go_interop.so ;;
Darwin*) library_name=libarrow_go_interop.dylib ;;
MINGW* | MSYS* | CYGWIN*) library_name=arrow_go_interop.dll ;;
*)
  echo "Unsupported platform: $(uname -s)" >&2
  exit 1
  ;;
esac

mkdir -p "${output_dir}"
output_dir=$(cd "${output_dir}" && pwd)

CGO_ENABLED=1 go build \
  -C "${source_dir}/test/arrow-go-interop" \
  -buildmode=c-shared \
  -o "${output_dir}/${library_name}" \
  . >&2

echo "${output_dir}/${library_name}"
