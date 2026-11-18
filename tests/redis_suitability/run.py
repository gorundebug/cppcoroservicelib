"""Run against the shared test broker; remove only this probe's UUID stream."""
import os
from pathlib import Path
import subprocess
import sys
import uuid

root = Path(sys.argv[1]).resolve()
broker = os.environ.get("REDIS_TEST_CONTAINER", "servicelib-redis-streams-acceptance")
image = os.environ.get("CORO_TEST_IMAGE", "cppcoroservicelib-build:latest")
password = os.environ.get("REDIS_TEST_PASSWORD", "adapter-test")
identity = uuid.uuid4().hex
key = "sa:coro:eligibility:" + identity
container = "sa-coro-hiredis-" + identity
try:
    subprocess.run(
        ["docker", "run", "--rm", "--name", container, "--network", "container:" + broker,
         "-e", "REDIS_TEST_PASSWORD=" + password,
         "-v", str(root) + ":/probe:ro", "--entrypoint", "timeout", image,
         "10s", "/probe/asio-build/coro_hiredis_probe", "127.0.0.1", "6379", key],
        check=True, timeout=15,
    )
finally:
    subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=5)
    subprocess.run(
        ["docker", "exec", "-e", "REDISCLI_AUTH=" + password, broker,
         "redis-cli", "DEL", key], capture_output=True, check=True, timeout=5,
    )
