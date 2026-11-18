"""Download official source/packages into a caller-selected scratch directory.
Extraction of .deb packages happens inside the Linux build image (dpkg-deb -x).
"""

import hashlib
from pathlib import Path
import sys
import tarfile
import urllib.request

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
base = "https://archive.apache.org/dist/pulsar/pulsar-client-cpp-4.2.0/"
for name in (
    "apache-pulsar-client-cpp-4.2.0.tar.gz",
    "deb-arm64/apache-pulsar-client.deb",
    "deb-arm64/apache-pulsar-client-dev.deb",
):
    data = urllib.request.urlopen(base + name, timeout=60).read()
    checksum = (
        urllib.request.urlopen(base + name + ".sha512", timeout=60).read().decode()
    )
    if hashlib.sha512(data).hexdigest() not in checksum:
        raise RuntimeError("Official release checksum mismatch")
    path = root / Path(name).name
    path.write_bytes(data)
    if name.endswith(".tar.gz"):
        with tarfile.open(path) as archive:
            archive.extractall(root, filter="data")
    print(path.name + " SHA512 verified")
