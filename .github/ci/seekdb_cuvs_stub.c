/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* CI-only stand-in for tools/hipvs_bridge/libseekdb_cuvs_bridge.so.
 *
 * The contest evaluation links the real hipVS/cuVS bridge, which needs an AMD
 * GPU stack that no hosted runner has.  This stub exports the same C ABI so the
 * server can be built and linked with -DOB_BUILD_CUVS=ON, which is what the
 * evaluation build does.  It never runs on the evaluation machine.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *seekdb_cuvs_build(const float *base, long n, long dim)
{
  (void)base;
  (void)n;
  (void)dim;
  return (void *)0x1;
}

int seekdb_cuvs_search(void *handle, const float *query, long nq, long topk,
                        unsigned int *out_ids, float *out_dist)
{
  (void)handle;
  (void)query;
  if (out_ids != NULL) {
    memset(out_ids, 0, sizeof(unsigned int) * nq * topk);
  }
  if (out_dist != NULL) {
    memset(out_dist, 0, sizeof(float) * nq * topk);
  }
  return 0;
}

void seekdb_cuvs_free(void *handle) { (void)handle; }

int seekdb_cuvs_serialize(void *handle, const char *filename)
{
  (void)handle;
  FILE *file = fopen(filename, "wb");
  if (file == NULL) {
    return 1;
  }
  static char block[1024 * 1024];
  memset(block, 7, sizeof(block));
  for (int i = 0; i < 256; ++i) {
    if (fwrite(block, 1, sizeof(block), file) != sizeof(block)) {
      fclose(file);
      return 1;
    }
  }
  fclose(file);
  return 0;
}

void *seekdb_cuvs_deserialize(const char *filename, long n, long dim)
{
  (void)filename;
  (void)n;
  (void)dim;
  return (void *)0x1;
}

int seekdb_cuvs_cagra_knn(const float *base, long n, long dim,
                          const float *query, long nq, long topk,
                          unsigned int *out_ids)
{
  (void)base;
  (void)n;
  (void)dim;
  (void)query;
  (void)nq;
  (void)topk;
  (void)out_ids;
  return 0;
}
