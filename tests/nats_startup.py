"""Focused native startup gate. Proxy forwards an existing official broker.
Run with --broker-host/port, optional --public-host for Docker, and -- COMMAND.
No broker restart, resource mutation, or emulated NATS handshake.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker-host", default="127.0.0.1")
    parser.add_argument("--broker-port", type=int, default=4222)
    parser.add_argument("--public-host", default="127.0.0.1")
    parser.add_argument("--artifacts", required=True)
    parser.add_argument("--subscription", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    root = Path(args.artifacts)
    root.mkdir(parents=True, exist_ok=True)
    evidence = []
    modes = ["pre-cancel", "deadline", "cancel", "configured-timeout", "control"]
    if args.subscription:
        modes += ["subscribe-cancel", "subscribe-deadline"]

    async def run(mode):
        tasks, writers = set(), set()
        connections = 0
        subscribed = False

        async def copy(reader, writer, outgoing=False):
            nonlocal subscribed
            while data := await reader.read(65536):
                if mode.startswith("subscribe-"):
                    if outgoing and b"SUB " in data:
                        subscribed = True
                    if not outgoing and subscribed:
                        await asyncio.sleep(2 if mode == "subscribe-deadline" else 0.18)
                writer.write(data)
                await writer.drain()

        async def connection(reader, writer):
            nonlocal connections
            connections += 1
            task = asyncio.current_task()
            tasks.add(task)
            writers.add(writer)
            upstream = None
            pumps = []
            try:
                await asyncio.sleep(
                    0
                    if mode.startswith("subscribe-")
                    else 2
                    if mode in ("deadline", "configured-timeout")
                    else 0.18
                )
                remote, upstream = await asyncio.open_connection(
                    args.broker_host, args.broker_port
                )
                writers.add(upstream)
                pumps = [
                    asyncio.create_task(copy(reader, upstream, True)),
                    asyncio.create_task(copy(remote, writer)),
                ]
                await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            except (OSError, asyncio.CancelledError):
                pass
            finally:
                for pump in pumps:
                    pump.cancel()
                await asyncio.gather(*pumps, return_exceptions=True)
                for stream in (writer, upstream):
                    if stream:
                        stream.close()
                        writers.discard(stream)
                tasks.discard(task)

        server = await asyncio.start_server(connection, "0.0.0.0", 0)
        env = {
            **os.environ,
            "NATS_STARTUP_MODE": mode,
            "NATS_STARTUP_URL": f"nats://{args.public_host}:{server.sockets[0].getsockname()[1]}",
        }
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            output, _ = await asyncio.wait_for(process.communicate(), 20)
            (root / (mode + ".log")).write_bytes(output)
            assert process.returncode == 0, f"{mode}: see {root}"
            assert connections == (0 if mode == "pre-cancel" else 1), (
                mode,
                connections,
            )
            if mode.startswith("subscribe-"):
                assert subscribed, "subscription flush fault not exercised"
            evidence.append(
                {"case": mode, "connections": connections, "exit": process.returncode}
            )
            print("PASS " + mode, flush=True)
        finally:
            if process and process.returncode is None:
                process.kill()
                await process.wait()
            server.close()
            await server.wait_closed()
            for writer in tuple(writers):
                writer.close()
            for task in tuple(tasks):
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    for mode in modes:
        await run(mode)
    (root / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
