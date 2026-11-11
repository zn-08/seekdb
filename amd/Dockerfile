# syntax=docker/dockerfile:1
# seekdb on AMD Radeon (ROCm / hipVS)
#
# The image compiles seekdb with the ROCm toolchain, so that the vector search
# runs on the AMD GPU of the instance instead of the CPU.
#
#   docker build -f amd/Dockerfile -t seekdb-amd:2026 .
#   docker run -d --name seekdb-amd \
#       --device=/dev/kfd --device=/dev/dri --group-add video \
#       -p 2881:2881 seekdb-amd:2026

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV ROCM_PATH=/opt/rocm

# the build dependencies of seekdb and the ROCm toolchain
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git wget gnupg lsb-release \
      build-essential cmake make gcc g++ binutils patch perl \
      python3 python3-pip python3-dev \
      libaio-dev libncurses-dev libgflags-dev libgoogle-perftools-dev \
      libboost-dev libboost-system-dev libboost-filesystem-dev \
      libboost-program-options-dev libboost-regex-dev libboost-date-time-dev \
      libboost-atomic-dev libboost-thread-dev \
      libtbb-dev libprotobuf-dev protobuf-compiler \
      libssl-dev zlib1g-dev libelf-dev \
      openjdk-8-jdk-headless \
      unzip zip \
 && rm -rf /var/lib/apt/lists/*

# the ROCm packages of the distribution
RUN apt-get update && apt-get install -y --no-install-recommends \
      rocm-device-libs \
      rocm-hip-runtime \
      rocm-hip-clang \
      rocm-hipblas \
      rocm-hipblas-dev \
      rocm-hipfft \
      rocm-hipfft-dev \
      rocm-hiprand \
      rocm-hiprand-dev \
      rocm-rocm-smi \
      hipconfig \
      libgomp1 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /seekdb
COPY . /seekdb

# the release build with the vector index; the HIP backend is picked up from
# the ROCm toolchain that is on the PATH
RUN bash build.sh release --init \
 && cd build_release \
 && make -j"$(nproc)" \
 && test -x src/observer/seekdb

RUN mkdir -p /var/lib/seekdb
WORKDIR /var/lib/seekdb

EXPOSE 2881

COPY amd/entrypoint.sh /usr/local/bin/seekdb-amd-entrypoint.sh
RUN chmod +x /usr/local/bin/seekdb-amd-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/seekdb-amd-entrypoint.sh"]
