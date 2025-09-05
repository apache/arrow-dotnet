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

set -exu

version="${1}"

html_escape() {
  # & -> &amp; must be the first substitution
  sed -e "s/&/&amp;/g" \
    -e "s/</&lt;/g" \
    -e "s/>/&gt;/g" \
    -e "s/\"/&quot;/g" \
    -e "s/'/&apos;/g"
}

if ! git fetch origin asf-site; then
  git worktree add --orphan -b asf-site site
else
  git worktree add site -B asf-site origin/asf-site
fi

tar_gz="${PWD}/apache-arrow-dotnet-docs-${version}.tar.gz"

extract_docs() {
  local destination="${1}"

  rm -rf "${destination}"
  mkdir -p "${destination}"
  pushd "${destination}"
  tar xf "${tar_gz}" --strip-components=1
  popd
  git add "${destination}"
}

pushd site
# Synchronize .asf.yaml
cp -a ../.asf.yaml ./
git add .asf.yaml

# Update https://arrow.apache.org/dotnet/main/
extract_docs main

# Create .htaccess
cat >.htaccess <<HTACCESS
RedirectMatch "^/dotnet/$" "/dotnet/current/"
HTACCESS
git add .htaccess
popd
