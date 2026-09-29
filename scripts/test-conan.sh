#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source "$root/scripts/dependency-proxy-env.sh"
image=${CPPCOROSERVICELIB_CONAN_IMAGE:-cppcoroservicelib-conan-build}
build_type=${1:-Debug}

docker_build_args=()
docker_run_args=()
conan_home_mount=cppcoroservicelib-conan2:/conan
docker_build_args+=(
  --build-arg "DEPENDENCY_DOCKER_REGISTRY=${DEPENDENCY_DOCKER_REGISTRY:-docker.io}"
  --build-arg "CPP_CORO_IO_BACKEND=${CPP_CORO_IO_BACKEND:-epoll}"
)
if [[ "${CPP_CORO_IO_BACKEND:-epoll}" == uring ]]; then
  docker_run_args+=(--security-opt seccomp=unconfined)
fi
if [[ -n "${DEPENDENCY_PROXY_DIR:-}" ]]; then
  proxy_host=${DEPENDENCY_PROXY_DOCKER_HOST:-host.docker.internal}
  proxy_port=${DEPENDENCY_PROXY_PORT:-18081}
  proxy_base="http://${proxy_host}:${proxy_port}/repository"
  conan_home_mount="${DEPENDENCY_CONAN_VOLUME:-dependency-conan2}:/conan"
  docker_build_args+=(
    --add-host host.docker.internal:host-gateway
    --build-arg "PIP_INDEX_URL=$proxy_base/pypi-proxy/simple"
    --build-arg "PIP_TRUSTED_HOST=$proxy_host"
    --build-arg "DEPENDENCY_APT_UBUNTU_ARCHIVE_URL=$proxy_base/apt-ubuntu-archive"
    --build-arg "DEPENDENCY_APT_UBUNTU_SECURITY_URL=$proxy_base/apt-ubuntu-security"
    --build-arg "DEPENDENCY_APT_UBUNTU_PORTS_URL=$proxy_base/apt-ubuntu-ports"
  )
  docker_run_args+=(
    --add-host host.docker.internal:host-gateway
    -e "DEPENDENCY_CONAN_REMOTE_URL=$proxy_base/conan-proxy"
    -e "DEPENDENCY_CONAN_UPLOAD_URL=$proxy_base/conan-hosted"
    -e "DEPENDENCY_CONAN_PUBLISH=1"
    -e "DEPENDENCY_CONAN_CREDENTIAL_FILE=/run/secrets/dependency_conan_credential"
    -e "DEPENDENCY_GITHUB_RAW_URL=$proxy_base/github-raw"
    -v "${DEPENDENCY_CONAN_CREDENTIAL_FILE}:/run/secrets/dependency_conan_credential:ro"
  )
else
  docker_build_args+=(
    --build-arg "PIP_INDEX_URL=${PIP_INDEX_URL:-https://pypi.org/simple}"
  )
  docker_run_args+=(
    -e "DEPENDENCY_GITHUB_RAW_URL=${DEPENDENCY_GITHUB_RAW_URL:-}"
  )
fi

docker build \
  "${docker_build_args[@]}" \
  -f "$root/Dockerfile.cmake" \
  -t "$image" \
  "$root"

docker run --rm \
  "${docker_run_args[@]}" \
  -e CONAN_HOME=/conan \
  -e "CPP_CORO_IO_BACKEND=${CPP_CORO_IO_BACKEND:-epoll}" \
  -e CPPCOROSERVICELIB_BUILD_TESTS=True \
  -e CPPCOROSERVICELIB_ENABLE_GRPC=True \
  -e CPPCOROSERVICELIB_ENABLE_KAFKA=True \
  -e "CPPCOROSERVICELIB_ENABLE_OTEL=${CPPCOROSERVICELIB_ENABLE_OTEL:-False}" \
  -v "$conan_home_mount" \
  -e "CMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL:-4}" \
  -e "CPPCORO_BUILD_TARGET=${CPPCORO_BUILD_TARGET:-}" \
  -e "CPPCORO_TEST_REGEX=${CPPCORO_TEST_REGEX:-}" \
  -v cppcoroservicelib-conan-ccache:/ccache \
  -v "$root:/workspace" \
  -w /workspace \
  "$image" \
  bash -euo pipefail -c '
    export CCACHE_DIR=/ccache
    export CCACHE_BASEDIR=/workspace
    export CCACHE_COMPILERCHECK=content
    ./scripts/conan-install.sh '"$build_type"'
    preset=conan-'"$(printf '%s' "$build_type" | tr '[:upper:]' '[:lower:]')"'
    cmake --fresh --preset "$preset"
    build_args=()
    test_args=()
    if [[ -n "$CPPCORO_BUILD_TARGET" ]]; then build_args+=(--target "$CPPCORO_BUILD_TARGET"); fi
    if [[ -n "$CPPCORO_TEST_REGEX" ]]; then test_args+=(-R "$CPPCORO_TEST_REGEX"); fi
    cmake --build --preset "$preset" "${build_args[@]}"
    ctest --preset "$preset" --output-on-failure "${test_args[@]}"
  '

# A separate network namespace supplies a deliberately nonresponsive local DNS
# peer. Keep this mandatory for full runs and resolver-target runs; never alter
# the host resolver or the concurrently running CTest processes' DNS settings.
if [[ -z "${CPPCORO_BUILD_TARGET:-}" || "${CPPCORO_BUILD_TARGET:-}" == coro_resolver_tests ]]; then
  docker run --rm --dns=127.0.0.1 \
    "${docker_run_args[@]}" \
    -e CPPCORO_TEST_LOCAL_DNS=1 \
    -v "$root:/workspace" -w /workspace "$image" \
    "/workspace/build/$build_type/coro_resolver_tests" \
    --gtest_filter='CoroResolver.Http*UnansweredDns'
fi
