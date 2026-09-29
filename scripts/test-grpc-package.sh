#!/usr/bin/env bash
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source "$ROOT/scripts/dependency-proxy-env.sh"

docker build --build-arg "DEPENDENCY_DOCKER_REGISTRY=${DEPENDENCY_DOCKER_REGISTRY:-docker.io}" \
  -f "$ROOT/Dockerfile.cmake" -t cppcoroservicelib-build "$ROOT"
docker run --rm \
  -e CCACHE_DIR=/ccache \
  -e CCACHE_BASEDIR=/workspace \
  -e CCACHE_COMPILERCHECK=content \
  -e CCACHE_MAXSIZE="${CCACHE_MAXSIZE:-20G}" \
  -v cppcoroservicelib-ccache:/ccache \
  -v "$ROOT:/workspace" -w /workspace \
  cppcoroservicelib-build \
  bash -lc "cmake --fresh -S . -B build/grpc-docker -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCPPCOROSERVICELIB_DEPENDENCY_MODE=FETCH \
    -DCPPCOROSERVICELIB_ENABLE_GRPC=ON \
    -DCPPCOROSERVICELIB_BUILD_TESTS=ON \
    && cmake --build build/grpc-docker --parallel \
    && cmake --install build/grpc-docker \
      --prefix /workspace/build/grpc-install \
    && cmake --fresh -S tests/grpc_consumer -B build/grpc-consumer -G Ninja \
      -DCMAKE_PREFIX_PATH=/workspace/build/grpc-install \
    && cmake --build build/grpc-consumer --parallel \
    && ./build/grpc-consumer/cppcoroservicelib_grpc_consumer"
