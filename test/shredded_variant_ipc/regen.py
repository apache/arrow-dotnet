# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Converts shredded-variant Parquet test cases from
test/parquet-testing/shredded_variant/*.parquet into Arrow IPC (.arrow) files
under this directory, so that .NET tests can load them without a Parquet
reader. The Parquet test corpus comes from apache/parquet-testing.

Requires: pyarrow (tested with 23.0).

Run from the repo root:

    python test/shredded_variant_ipc/regen.py

Existing .arrow files are overwritten in place.

Test cases such as case-037 may produce different output on different platforms
because Arrow metadata is read into a hash-based collection where the hashing
function can vary across platforms.
"""

import json
import os
import sys

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq


def main() -> int:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    src = os.path.join(repo_root, "test", "parquet-testing", "shredded_variant")
    dst = os.path.join(repo_root, "test", "shredded_variant_ipc")
    cases_json = os.path.join(src, "cases.json")

    if not os.path.exists(cases_json):
        print(f"cases.json not found at {cases_json}", file=sys.stderr)
        return 1

    with open(cases_json) as f:
        cases = json.load(f)

    os.makedirs(dst, exist_ok=True)

    written = 0
    for case in cases:
        parquet_files = []
        if "parquet_file" in case:
            parquet_files.append(case["parquet_file"])

        for pf in parquet_files:
            src_path = os.path.join(src, pf)
            if not os.path.exists(src_path):
                continue

            table = pq.read_table(src_path)
            dst_path = os.path.join(dst, os.path.splitext(pf)[0] + ".arrow")

            with ipc.new_file(dst_path, table.schema) as writer:
                writer.write_table(table)

            written += 1

    print(f"Wrote {written} .arrow files to {dst}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
