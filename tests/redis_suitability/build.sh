#!/usr/bin/env bash
set -euo pipefail
# /probe contains the unchanged official hiredis v1.5.0 release source.
cmake -S /probe/hiredis-1.5.0 -B /probe/native-build \
 -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/probe/install \
 -DENABLE_SSL=ON -DDISABLE_TESTS=ON -DBUILD_SHARED_LIBS=ON
cmake --build /probe/native-build -j2
cmake --install /probe/native-build
cmake -S /workspace/tests/redis_suitability -B /probe/asio-build \
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_TOOLCHAIN_FILE=/workspace/build/Release/generators/conan_toolchain.cmake \
 -Dhiredis_DIR=/probe/install/lib/cmake/hiredis \
 -Dhiredis_ssl_DIR=/probe/install/lib/cmake/hiredis_ssl
cmake --build /probe/asio-build -j2
