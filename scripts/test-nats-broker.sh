#!/usr/bin/env bash
# Real native NATS integration; no production cluster, host port, or SDK patches.
set -euo pipefail
root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
work="$(mktemp -d)"
network="coro-nats-test-$$"
broker=""
cleanup() {
  [[ -z "$broker" ]] || docker rm -f "$broker" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
base="${DEPENDENCY_GITHUB_RAW_URL:-https://github.com}"
curl -fsSL "${base%/}/nats-io/nats.c/archive/refs/tags/v3.14.0.tar.gz" -o "$work/nats.tar.gz"
python3 - "$work/nats.tar.gz" <<'PY'
import hashlib, sys
assert hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest() == "1f8b450bc295d0c94be201e34713ca0b515aae2c0d1b279273c3e6e0e72fe005"
PY
tar -xzf "$work/nats.tar.gz" -C "$work"
cat > "$work/build.sh" <<'BUILD'
set -euo pipefail
sanitizer="${NATS_TEST_ASAN:-OFF}"
native_flags=""
if [[ "$sanitizer" == ON ]]; then native_flags="-fsanitize=address -fno-omit-frame-pointer"; fi
cmake -S /evidence/nats.c-3.14.0 -B /tmp/nats-build \
  -DNATS_BUILD_EXAMPLES=OFF -DNATS_BUILD_LIB_SHARED=OFF -DBUILD_TESTING=OFF \
  -DCMAKE_INSTALL_PREFIX=/tmp/nats-install -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_FLAGS="$native_flags" > /evidence/native-config.log
cmake --build /tmp/nats-build -j4 > /evidence/native-build.log
cmake --install /tmp/nats-build > /evidence/native-install.log
cmake -S /workspace -B "/workspace/build/nats-validation-$sanitizer" \
  -DCMAKE_TOOLCHAIN_FILE=/workspace/build/Release/generators/conan_toolchain.cmake \
  -DCMAKE_BUILD_TYPE=Release -DCPPCOROSERVICELIB_ENABLE_NATS=ON \
  -Dcnats_DIR=/tmp/nats-install/lib/cmake/cnats \
  -DCPPCOROSERVICELIB_ENABLE_GRPC=OFF -DCPPCOROSERVICELIB_ENABLE_KAFKA=OFF \
  -DCPPCOROSERVICELIB_ENABLE_OTEL=OFF -DCPPCOROSERVICELIB_ENABLE_CRON=OFF \
  -DCPPCOROSERVICELIB_ASAN="$sanitizer" -DCPPCOROSERVICELIB_BUILD_TESTS=ON > /evidence/coro-config.log
cmake --build "/workspace/build/nats-validation-$sanitizer" --target \
  cppcoroservicelib_nats_test cppcoroservicelib_config_loader_test \
  cppcoroservicelib_async_operations_context_test -j4
ctest --test-dir "/workspace/build/nats-validation-$sanitizer" \
  -R 'cppcoroservicelib_(nats|config_loader|async_operations_context)_test' --output-on-failure
BUILD
if [[ -n "${NATS_TEST_SUBNET:-}" ]]; then
  docker network create --subnet "$NATS_TEST_SUBNET" "$network" >/dev/null
else
  docker network create "$network" >/dev/null
fi
broker=$(docker run -d --network "$network" --network-alias broker nats:2.15.3-alpine --jetstream --http_port=8222)
docker run --rm --network "$network" \
  -e NATS_TEST_URL=nats://broker:4222 -e NATS_RECONNECT_MARKER=/evidence/reconnect \
  -e NATS_TEST_ASAN="${NATS_TEST_ASAN:-OFF}" \
  -v "$root:/workspace" -v "${DEPENDENCY_CONAN_VOLUME:-dependency-conan2}:/conan" \
  -v "$work:/evidence" --entrypoint bash \
  "${CPPCOROSERVICELIB_TEST_IMAGE:-cppcoroservicelib-build:local}" /evidence/build.sh &
runner=$!
while kill -0 "$runner" 2>/dev/null; do
  if [[ -f "$work/reconnect-ready" && ! -f "$work/reconnect-done" ]]; then
    docker restart "$broker" >/dev/null
    touch "$work/reconnect-done"
  fi
  sleep 0.2
done
wait "$runner"
