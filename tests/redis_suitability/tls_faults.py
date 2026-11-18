"""TLS and lost-reply gates, run inside the shared broker's network namespace.
Only UUID keys are modified. No broker configuration or production code changes.
"""
import asyncio
import os
from pathlib import Path
import ssl
import sys
import tempfile
import uuid

PASSWORD = os.environ.get("REDIS_TEST_PASSWORD", "adapter-test")

async def frame(reader):
    line = await reader.readuntil(b"\r\n")
    tag, value = line[:1], line[1:-2]
    if tag == b"*":
        parts = [await frame(reader) for _ in range(max(0, int(value)))]
        return line + b"".join(x[0] for x in parts), [x[1] for x in parts]
    if tag == b"$" and int(value) >= 0:
        data = await reader.readexactly(int(value) + 2)
        return line + data, data[:-2]
    return line, value

async def cleanup(key):
    reader, writer = await asyncio.open_connection("127.0.0.1", 6379)
    try:
        for args in [("AUTH", PASSWORD), ("DEL", key)]:
            parts = [a.encode() for a in args]
            writer.write(b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n" % len(a) + a + b"\r\n" for a in parts))
            await writer.drain()
            _, response = await frame(reader)
            assert response != b"NOAUTH Authentication required."
    finally:
        writer.close()
        await writer.wait_closed()

async def openssl(*args):
    p = await asyncio.create_subprocess_exec("openssl", *map(str, args), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    _, error = await p.communicate()
    assert p.returncode == 0, error.decode()

async def main():
    executable = sys.argv[1]
    with tempfile.TemporaryDirectory(prefix="coro-redis-tls-") as tmp:
        root = Path(tmp)
        for name, san in [("trusted", "IP:127.0.0.1"), ("wrong", "DNS:wrong.example"), ("other", "DNS:other.example")]:
            await openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", root / (name + ".key"), "-out", root / (name + ".pem"), "-days", "1", "-subj", "/CN=" + name, "-addext", "subjectAltName=" + san)
        await openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", root / "client.key", "-out", root / "client.csr", "-subj", "/CN=probe-client")
        (root / "client.ext").write_text("extendedKeyUsage=clientAuth\n")
        await openssl("x509", "-req", "-in", root / "client.csr", "-CA", root / "trusted.pem", "-CAkey", root / "trusted.key", "-CAcreateserial", "-out", root / "client.pem", "-days", "1", "-extfile", root / "client.ext")
        cases = [
            ("trusted-ca", "trusted", "trusted", False, False, True),
            ("wrong-hostname", "wrong", "wrong", False, False, False),
            ("untrusted-ca", "trusted", "other", False, False, False),
            ("mutual-tls", "trusted", "trusted", True, True, True),
            ("missing-client", "trusted", "trusted", True, False, False),
            ("lost-xadd-reply", None, None, False, False, True),
        ]
        for label, server_name, ca, require_client, identity, success in cases:
            sent = 0
            tasks, writers = set(), set()
            async def proxy(reader, writer):
                nonlocal sent
                task = asyncio.current_task(); tasks.add(task); writers.add(writer)
                upstream = None
                try:
                    remote, upstream = await asyncio.open_connection("127.0.0.1", 6379)
                    writers.add(upstream)
                    while True:
                        request, args = await frame(reader)
                        upstream.write(request); await upstream.drain()
                        response, _ = await frame(remote)
                        if args[0].upper() == b"XADD":
                            sent += 1
                            if label == "lost-xadd-reply":
                                return
                        writer.write(response); await writer.drain()
                except (asyncio.IncompleteReadError, ConnectionError):
                    pass
                finally:
                    writer.close(); writers.discard(writer)
                    if upstream: upstream.close(); writers.discard(upstream)
                    tasks.discard(task)
            context = None
            if server_name:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                context.load_cert_chain(root / (server_name + ".pem"), root / (server_name + ".key"))
                if require_client:
                    context.verify_mode = ssl.CERT_REQUIRED
                    context.load_verify_locations(root / "trusted.pem")
            server = await asyncio.start_server(proxy, "127.0.0.1", 0, ssl=context)
            port = server.sockets[0].getsockname()[1]
            key = "sa:coro:tls:" + uuid.uuid4().hex
            process = None
            try:
                env = dict(os.environ, REDIS_TEST_PASSWORD=PASSWORD, REDIS_TEST_WORKERS="4")
                for name in ["REDIS_TEST_CA", "REDIS_TEST_CERT", "REDIS_TEST_KEY", "REDIS_TEST_SERVER_NAME", "REDIS_TEST_MODE"]:
                    env.pop(name, None)
                if ca: env["REDIS_TEST_CA"] = str(root / (ca + ".pem"))
                if identity:
                    env["REDIS_TEST_CERT"] = str(root / "client.pem")
                    env["REDIS_TEST_KEY"] = str(root / "client.key")
                if label == "lost-xadd-reply": env["REDIS_TEST_MODE"] = "ambiguity"
                process = await asyncio.create_subprocess_exec(executable, "127.0.0.1", str(port), key, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
                assert (process.returncode == 0) == success, (label, process.returncode, stdout.decode(), stderr.decode())
                assert sent == (1 if success else 0), (label, sent)
                if success: assert b"PASS:" in stdout
                print("PASS", label, "wire XADD count", sent, flush=True)
            finally:
                if process and process.returncode is None: process.kill(); await process.wait()
                server.close(); await server.wait_closed()
                for writer in tuple(writers): writer.close()
                for task in tuple(tasks): task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await cleanup(key)

asyncio.run(main())
