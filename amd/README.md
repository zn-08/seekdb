# 在 AMD Radeon（ROCm / hipVS）上部署 seekdb

本目录是 AMD 赛题的交付材料：在 AMD Radeon Cloud 的 GPU 实例上用 ROCm / hipVS 工具链
编译并运行 seekdb，并给出可复现的部署与自检步骤。

## 1. 环境

| 项 | 版本 / 说明 |
| --- | --- |
| 硬件 | AMD Radeon（gfx942 / gfx1100 等），由 Radeon Cloud Gallery 提供 |
| 驱动 | AMDGPU 驱动 6.x 及以上（`rocm-smi` 可��） |
| 工具链 | ROCm 6.x（含 `hipcc` / `hipconfig`）与 hipVS 2024.x |
| 数据库 | seekdb（基线分支 `2026-competition`），MySQL 协议 |
| 编译选项 | `bash build.sh release --init`，向量检索走 HIP 后端（`VECTOR_INDEX_TYPE=HNSW` 默认） |

## 2. 一键部署

```bash
docker build -f amd/Dockerfile -t seekdb-amd:2026 .
docker run -d --name seekdb-amd \
  --device=/dev/kfd --device=/dev/dri --group-add video \
  -p 2881:2881 seekdb-amd:2026
```

镜像内会完成三件事：

1. 用 ROCm 的 `hipcc` 编译带向量索引的 seekdb（`src/vector_service`）；
2. 启动 `observer`，等待 `observer ready`；
3. 把实例的部署信息写进运行目录的 `run/telemetry.json`。

## 3. 自检（部署成功后执行）

```bash
bash amd/verify.sh
```

脚本按顺序核对并打印：

- `rocm-smi` 能否看到 AMD GPU（型号、显存、驱动版本）；
- `hipconfig` 与 `hipcc` 是否可用；
- seekdb 二进制是否存在、`observer` 是否 ready、版本号是什么；
- MySQL 协议能否连上（`SELECT VERSION()`）；
- 向量检索是否可用（建表 → 插入 → `ORDER BY l2_distance` 查询）。

## 4. 部署 UUID

seekdb 每次启动都会在运行目录写 `run/telemetry.json`：

```json
{"content": {"id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", ...}}
```

**`content.id` 就是本赛题要登记的部署 UUID**，需要连同部署成功的截图一起登记。
取法：

```bash
python3 -c "import json;print(json.load(open('/var/lib/seekdb/run/telemetry.json'))['content']['id'])"
```

登记之后按平台要求释放实例（Radeon Cloud → Profile → Active Instance → Destroy），
避免持续消耗算力额度。

## 5. AMD 上的优化点

1. **向量检索的内存布局**：AMD 的显存带宽与 NVIDIA 不同，HNSW 的图结构改为
   `float16` 向量 + `int32` 邻接表，减少一半以上的图内存占用；
2. **批量距离计算**：距离核在 HIP 上按 256 为一组批处理，用 `__builtin_amdgcn` 的
   wavefront 掩码跳过空槽，小批量查询的吞吐提升明显；
3. **索引构建的并行度**：索引构建阶段按 AMD 的 SM 数（`hipDeviceProp_t::multiProcessorCount`）
   而不是固定的线程数来切分任务，避免线程争用。

> 具体的编译开关与核函数改动见 `amd/patches/`；本目录只保留部署与自检脚本，
> 以免与基线分支的差异影响其它赛题。

## 6. 复现步骤摘要

1. 在 Radeon Cloud Gallery 里申请 AMD GPU 实例（每账号同时最多 1 张）；
2. 拉取本仓库，进入 `amd/`，按第 2 节构建并启动镜像；
3. 执行第 3 节的自检脚本，保存「observer ready + 版本号」的截图；
4. 取 `run/telemetry.json` 里的 `content.id`，保存部署 UUID；
5. 登记到大赛平台，附上截图与 UUID；
6. 释放实例。
