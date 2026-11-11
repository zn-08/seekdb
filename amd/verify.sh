#!/usr/bin/env bash
# Check that the AMD instance can really run seekdb: the GPU, the ROCm
# toolchain, the observer and the vector search.
set -uo pipefail

FAIL=0

echo "== 1. the AMD GPU =="
if command -v rocm-smi >/dev/null 2>&1; then
  rocm-smi --showproductname --showmeminfo vram --showdriverversion || true
else
  echo "rocm-smi is missing"
  FAIL=1
fi

echo
echo "== 2. the ROCm toolchain =="
if command -v hipconfig >/dev/null 2>&1; then
  hipconfig --full | head -n 40 || true
else
  echo "hipconfig is missing"
  FAIL=1
fi
if command -v hipcc >/dev/null 2>&1; then
  hipcc --version | head -n 3 || true
else
  echo "hipcc is missing"
  FAIL=1
fi

echo
echo "== 3. the seekdb binary and the observer =="
RUN_DIR="${SEEKDB_RUN_DIR:-/var/lib/seekdb}"
BINARY="${SEEKDB_BINARY:-/seekdb/build_release/src/observer/seekdb}"
if [[ -x "${BINARY}" ]]; then
  "${BINARY}" --version || true
else
  echo "seekdb binary not found at ${BINARY}"
  FAIL=1
fi
if mysqladmin -h 127.0.0.1 -P "${SEEKDB_PORT:-2881}" -uroot ping >/dev/null 2>&1; then
  echo "observer ready"
  mysql -h 127.0.0.1 -P "${SEEKDB_PORT:-2881}" -uroot -e "SELECT VERSION()"
else
  echo "the observer does not answer on port ${SEEKDB_PORT:-2881}"
  FAIL=1
fi

echo
echo "== 4. the vector search =="
mysql -h 127.0.0.1 -P "${SEEKDB_PORT:-2881}" -uroot <<'SQL' || FAIL=1
DROP TABLE IF EXISTS amd_vector_check;
CREATE TABLE amd_vector_check (id INT PRIMARY KEY, embedding VECTOR(4));
INSERT INTO amd_vector_check VALUES (1, '[1,0,0,0]'), (2, '[0,1,0,0]'), (3, '[0,0,1,0]');
CREATE INDEX idx_amd_vec ON amd_vector_check (embedding) USING HNSW;
SELECT id, l2_distance(embedding, '[1,0,0,0]') AS distance
  FROM amd_vector_check
 ORDER BY distance
 LIMIT 3;
DROP TABLE amd_vector_check;
SQL

echo
if [[ "${FAIL}" -eq 0 ]]; then
  echo "the check passed"
else
  echo "the check failed" >&2
fi
exit "${FAIL}"
