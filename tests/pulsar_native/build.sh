#!/usr/bin/env bash
set -euo pipefail
# Workspace acceptance image: /coro checkout, /probe official distribution,
# /conan existing dependency cache. Override roots for another installation.
repo=${CORO_ROOT:-/coro}
probe=${PULSAR_CPP_PROBE_ROOT:-/probe}
cmake -S "$repo" -B "$probe/sdk-cmake" \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_TOOLCHAIN_FILE="${CORO_TOOLCHAIN:-$repo/build/Release/generators/conan_toolchain.cmake}" \
 -DCPPCOROSERVICELIB_DEPENDENCY_MODE=CONAN \
 -DCPPCOROSERVICELIB_ENABLE_PULSAR=ON -DPulsarNative_ROOT="$probe/install/usr" \
 -DCPPCOROSERVICELIB_ENABLE_NATS=OFF -DCPPCOROSERVICELIB_ENABLE_GRPC=OFF \
 -DCPPCOROSERVICELIB_ENABLE_KAFKA=OFF -DCPPCOROSERVICELIB_ENABLE_OTEL=OFF \
 -DCPPCOROSERVICELIB_ENABLE_CRON=OFF -DCPPCOROSERVICELIB_BUILD_TESTS=ON \
 -DCPP_CORO_IO_BACKEND=epoll -DUSE_CCACHE=OFF
cmake --build "$probe/sdk-cmake" --target cppcoroservicelib_pulsar_native_test -j2
cp "$probe/sdk-cmake/cppcoroservicelib_pulsar_native_test" "$probe/coro-sdk"
