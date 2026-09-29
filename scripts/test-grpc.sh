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
  -e CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-}" \
  -v cppcoroservicelib-ccache:/ccache \
  -v "$ROOT:/workspace" -w /workspace \
  cppcoroservicelib-build \
  bash -lc "cmake --fresh -S . -B build/grpc-docker -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCPPCOROSERVICELIB_DEPENDENCY_MODE=FETCH \
    -DCPPCOROSERVICELIB_ENABLE_GRPC=ON \
    -DCPPCOROSERVICELIB_BUILD_TESTS=ON \
    && cmake --build build/grpc-docker --parallel ${CMAKE_BUILD_PARALLEL_LEVEL:+"$CMAKE_BUILD_PARALLEL_LEVEL"} \
      --target cppcoroservicelib_grpc_runtime_test \
               cppcoroservicelib_grpc_endpoints_test \
               cppcoroservicelib_grpc_unary_test \
               cppcoroservicelib_grpc_streaming_test \
    && ctest --test-dir build/grpc-docker --output-on-failure \
      -R 'cppcoroservicelib_grpc_(runtime|endpoints|unary|streaming)_test'"
