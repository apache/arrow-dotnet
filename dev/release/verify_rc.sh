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

set -eu
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOP_SOURCE_DIR="$(dirname "$(dirname "${SOURCE_DIR}")")"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 22.0.0 0"
  exit 1
fi

github_actions_group_begin() {
  echo "::group::$1"
  set -x
}

github_actions_group_end() {
  set +x
  echo "::endgroup::"
}

github_actions_group_begin "Prepare"

VERSION="$1"
RC="$2"

ARROW_DIST_BASE_URL="https://dist.apache.org/repos/dist/release/arrow"
DOWNLOAD_RC_BASE_URL="https://github.com/apache/arrow-dotnet/releases/download/v${VERSION}-rc${RC}"
ARCHIVE_BASE_NAME="apache-arrow-dotnet-${VERSION}"

: "${VERIFY_DEFAULT:=1}"

: "${VERIFY_BINARY:=${VERIFY_DEFAULT}}"
: "${VERIFY_DOWNLOAD:=${VERIFY_DEFAULT}}"
: "${VERIFY_SIGN:=${VERIFY_DEFAULT}}"
: "${VERIFY_SOURCE:=${VERIFY_DEFAULT}}"

VERIFY_SUCCESS=no

setup_tmpdir() {
  cleanup() {
    if [ "${VERIFY_SUCCESS}" = "yes" ]; then
      rm -rf "${VERIFY_TMPDIR}"
    else
      echo "::endgroup::"
      echo "Failed to verify release candidate. See ${VERIFY_TMPDIR} for details."
    fi
  }

  if [ -z "${VERIFY_TMPDIR:-}" ]; then
    VERIFY_TMPDIR="$(mktemp -d -t "$1.XXXXX")"
    trap cleanup EXIT
  else
    mkdir -p "${VERIFY_TMPDIR}"
  fi
}

download() {
  curl \
    --fail \
    --location \
    --remote-name \
    --show-error \
    --silent \
    "$1"
}

download_rc_file() {
  if [ "${VERIFY_DOWNLOAD}" -gt 0 ]; then
    download "${DOWNLOAD_RC_BASE_URL}/$1"
  else
    cp "${TOP_SOURCE_DIR}/$1" "$1"
  fi
}

import_gpg_keys() {
  if [ "${VERIFY_SIGN}" -gt 0 ]; then
    download "${ARROW_DIST_BASE_URL}/KEYS"
    gpg --import KEYS
  fi
}

if type shasum >/dev/null 2>&1; then
  sha256_verify="shasum -a 256 -c"
  sha512_verify="shasum -a 512 -c"
else
  sha256_verify="sha256sum -c"
  sha512_verify="sha512sum -c"
fi

fetch_artifact() {
  local artifact="${1}"
  download_rc_file "${artifact}"
  if [ "${VERIFY_SIGN}" -gt 0 ]; then
    download_rc_file "${artifact}.asc"
    gpg --verify "${artifact}.asc" "${artifact}"
  fi
  download_rc_file "${artifact}.sha256"
  ${sha256_verify} "${artifact}.sha256"
  download_rc_file "${artifact}.sha512"
  ${sha512_verify} "${artifact}.sha512"
}

fetch_archive() {
  fetch_artifact "${ARCHIVE_BASE_NAME}.tar.gz"
}

ensure_source_directory() {
  tar xf "${ARCHIVE_BASE_NAME}".tar.gz
}

test_source_distribution() {
  if [ "${VERIFY_SOURCE}" -le 0 ]; then
    return 0
  fi

  "${TOP_SOURCE_DIR}/ci/scripts/build.sh" "$(pwd)"
  "${TOP_SOURCE_DIR}/ci/scripts/test.sh" "$(pwd)"
}

test_binary_distribution() {
  if [ "${VERIFY_BINARY}" -le 0 ]; then
    return 0
  fi

  local package
  local package_type
  for package in $(cd src && echo *); do
    for package_type in nupkg snupkg; do
      fetch_artifact "${package}.${VERSION}.${package_type}"
    done
    # TODO: Can we run tests against .nuget package?
  done
}

github_actions_group_end

github_actions_group_begin "Setup temporary directory"
setup_tmpdir "arrow-dotnet-${VERSION}-${RC}"
echo "Working in sandbox ${VERIFY_TMPDIR}"
cd "${VERIFY_TMPDIR}"
github_actions_group_end

github_actions_group_begin "Prepare source directory"
import_gpg_keys
fetch_archive
ensure_source_directory
github_actions_group_end

pushd "${ARCHIVE_BASE_NAME}"

github_actions_group_begin "Test source distribution"
test_source_distribution
github_actions_group_end

github_actions_group_begin "Test binary distribution"
test_binary_distribution
github_actions_group_end

popd

VERIFY_SUCCESS=yes
echo "RC looks good!"
