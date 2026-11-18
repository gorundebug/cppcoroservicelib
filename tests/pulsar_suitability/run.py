"""Isolated-topic real-broker supervisor; proxy rewrites only lookup routing.
Run inside build image. No shared broker restart or policy mutation outside owned namespace.
"""

import asyncio
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import urllib.request
import uuid
from pulsar_proxy import PulsarProxy


async def main():
    host = os.getenv("PULSAR_HOST", "host.docker.internal")
    admin = f"http://{host}:18080"
    remote = f"pulsar://{host}:16650"
    ns = "public/cpp-probe-" + uuid.uuid4().hex

    def request(method, path, data=None):
        req = urllib.request.Request(
            admin + path,
            data=None if data is None else json.dumps(data).encode(),
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as result:
            return result.read()

    request("PUT", "/admin/v2/namespaces/" + ns, {})
    topics = []
    try:
        for policy, field in [
            ("autoTopicCreation", "allowAutoTopicCreation"),
            ("autoSubscriptionCreation", "allowAutoSubscriptionCreation"),
        ]:
            request("POST", f"/admin/v2/namespaces/{ns}/{policy}", {field: False})

        def topic():
            name = "events-" + uuid.uuid4().hex
            path = f"/admin/v2/persistent/{ns}/{name}"
            request("PUT", path)
            request(
                "PUT", path + "/subscription/probe", {"ledgerId": -1, "entryId": -1}
            )
            topics.append(path)
            return f"persistent://{ns}/{name}"

        async def run(url, address, ca=None, success=True, mode="tls", identity=None):
            args = [sys.argv[1], url, address] + (
                [str(ca or ""), mode] if ca or mode in ("fault", "stop") else []
            )
            env = dict(os.environ)
            if identity:
                env.update(
                    PULSAR_PROBE_CERT=str(identity[0]),
                    PULSAR_PROBE_KEY=str(identity[1]),
                )
            process = await asyncio.create_subprocess_exec(
                *args,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            try:
                output, _ = await asyncio.wait_for(process.communicate(), 12)
            except TimeoutError:
                process.kill()
                await process.wait()
                raise AssertionError("probe did not terminate within 12s")
            text = output.decode(errors="replace")
            if (process.returncode == 0) != success:
                raise AssertionError(text[-6000:])
            if success:
                print(text[text.rfind("PASS") :].strip(), flush=True)
            else:
                print(
                    "PASS native TLS rejection exit=" + str(process.returncode),
                    flush=True,
                )

        async with PulsarProxy(remote) as proxy:
            await run(proxy.url, topic())
        async with PulsarProxy(remote) as proxy:
            proxy.drop_receipts = True
            await run(proxy.url, topic(), mode="fault")
            assert proxy.sends == 4, (
                "native pending publications replayed without disconnect"
            )
        async with PulsarProxy(remote) as proxy:
            proxy.drop_receipts = True
            await run(proxy.url, topic(), mode="stop")
            assert proxy.sends == 4
        with tempfile.TemporaryDirectory(prefix="pulsar-cpp-tls-") as directory:
            d = Path(directory)
            for name, hostname in [("good", "localhost"), ("bad", "wrong.invalid")]:
                subprocess.run(
                    [
                        "openssl",
                        "req",
                        "-x509",
                        "-newkey",
                        "rsa:2048",
                        "-nodes",
                        "-keyout",
                        str(d / (name + ".key")),
                        "-out",
                        str(d / (name + ".pem")),
                        "-days",
                        "1",
                        "-subj",
                        "/CN=" + hostname,
                        "-addext",
                        "subjectAltName=DNS:" + hostname,
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            for name, ca, success in [
                ("good", "good", True),
                ("good", "bad", False),
                ("bad", "bad", False),
            ]:
                tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                tls.load_cert_chain(d / (name + ".pem"), d / (name + ".key"))
                async with PulsarProxy(remote, tls) as proxy:
                    await run(proxy.url, topic(), d / (ca + ".pem"), success)
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(d / "good.pem", d / "good.key")
            tls.load_verify_locations(d / "good.pem")
            tls.verify_mode = ssl.CERT_REQUIRED
            async with PulsarProxy(remote, tls) as proxy:
                await run(
                    proxy.url,
                    topic(),
                    d / "good.pem",
                    True,
                    identity=(d / "good.pem", d / "good.key"),
                )
                await run(proxy.url, topic(), d / "good.pem", False)
    finally:
        for path in topics:
            request("DELETE", path + "?force=true")
        request("DELETE", "/admin/v2/namespaces/" + ns)


asyncio.run(main())
